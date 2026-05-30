import os
import re

import pandas as pd

from validations.comparator import execute_query


DEFAULT_RULES_TABLE = "`dev_iceberg`.`tmkn-aws-dwh-dev-iceberg`.validation_rules"
DEFAULT_RULE_FILTER = "rule_id LIKE 'SV-%'"
DEFAULT_BRONZE_NAMESPACE = "`tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`"
DEFAULT_SILVER_NAMESPACE = "`tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`"


def read_databricks_validation_inputs(conn):
    validation_df = read_databricks_validations(conn)
    table_df = build_table_queries_from_rules(validation_df)
    return table_df, validation_df


def read_databricks_validations(conn):
    rules_table = os.getenv("DATABRICKS_VALIDATION_RULES_TABLE", DEFAULT_RULES_TABLE)
    rule_filter = os.getenv("DATABRICKS_VALIDATION_RULE_FILTER", DEFAULT_RULE_FILTER)
    query = f"SELECT * FROM {rules_table} WHERE {rule_filter}"
    validation_df = execute_query(conn, query)
    return normalize_validation_rules(validation_df)


def normalize_validation_rules(validation_df):
    validation_df = validation_df.copy()
    validation_df.columns = [str(column).strip() for column in validation_df.columns]

    rename_map = {}
    lower_columns = {column.lower(): column for column in validation_df.columns}
    for source_name, target_name in {
        "table_name": "table_name",
        "tablename": "table_name",
        "table": "table_name",
        "flag": "flag",
        "validation_type": "validation_type",
        "is_active": "flag",
        "active": "flag",
        "enabled": "flag",
        "rule_type": "validation_type",
        "validation_name": "validation_type",
        "primary_key": "pk",
        "primary_keys": "pk",
        "key_columns": "pk",
        "columns_to_compare": "compare_columns",
        "source_table": "bronze_table",
        "bronze_table": "bronze_table",
        "target_table": "silver_table",
        "silver_table": "silver_table",
        "source_query": "bronze_query",
        "bronze_query": "bronze_query",
        "target_query": "silver_query",
        "silver_query": "silver_query",
    }.items():
        if source_name in lower_columns and target_name not in validation_df.columns:
            rename_map[lower_columns[source_name]] = target_name

    if rename_map:
        validation_df = validation_df.rename(columns=rename_map)

    if "table_name" not in validation_df.columns:
        validation_df["table_name"] = validation_df.apply(derive_table_name, axis=1)

    if "flag" not in validation_df.columns:
        validation_df["flag"] = 1

    if "validation_type" not in validation_df.columns and "rule_id" in validation_df.columns:
        validation_df["validation_type"] = validation_df["rule_id"].apply(infer_validation_type)

    required_columns = {"flag", "table_name", "validation_type"}
    missing = required_columns - set(validation_df.columns)
    if missing:
        raise ValueError(
            "Databricks validation rules are missing required columns: "
            f"{sorted(missing)}. Available columns: {list(validation_df.columns)}"
        )

    return validation_df


def derive_table_name(row):
    configured_column = os.getenv("DATABRICKS_TABLE_NAME_COLUMN")
    candidates = []
    if configured_column:
        candidates.append(configured_column)
    candidates.extend(
        [
            "table_name",
            "silver_table",
            "bronze_table",
            "target_system",
            "source_system",
            "source",
            "validation_logic",
        ]
    )

    for column_name in candidates:
        if column_name not in row.index:
            continue
        table_name = extract_table_name(row.get(column_name))
        if table_name:
            return table_name

    configured_tables = split_config_list(os.getenv("DATABRICKS_VALIDATION_TABLES"))
    if len(configured_tables) == 1:
        return configured_tables[0]
    return None


def extract_table_name(value):
    if pd.isna(value):
        return None

    text_value = str(value).strip()
    if not text_value:
        return None

    table_refs = re.findall(
        r"(?:FROM|JOIN)\s+((?:`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_-]+)(?:\s*\.\s*(?:`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_-]+))*)",
        text_value,
        flags=re.IGNORECASE,
    )
    if table_refs:
        return table_ref_to_name(table_refs[0])

    if "." in text_value:
        return table_ref_to_name(text_value)

    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", text_value) and "_" in text_value:
        return text_value

    return None


def table_ref_to_name(table_ref):
    table_ref = str(table_ref).strip().rstrip(";")
    parts = [part.strip().strip("`").strip('"') for part in re.split(r"\s*\.\s*", table_ref)]
    parts = [part for part in parts if part]
    if not parts:
        return None
    return parts[-1]


def infer_validation_type(rule_id):
    rule_text = str(rule_id).strip().lower()
    if "duplicate" in rule_text or "dup" in rule_text:
        if "silver" in rule_text:
            return "duplicate_silver"
        if "bronze" in rule_text:
            return "duplicate_bronze"
        return "duplicate_silver"
    if "null" in rule_text and "pk" in rule_text:
        if "silver" in rule_text:
            return "pk_null_silver"
        if "bronze" in rule_text:
            return "pk_null_bronze"
        return "pk_null_silver"
    if "count" in rule_text:
        return "count"
    if "bronze_not_in_silver" in rule_text or "bronze-not-in-silver" in rule_text:
        return "bronze_not_in_silver"
    if "silver_not_in_bronze" in rule_text or "silver-not-in-bronze" in rule_text:
        return "silver_not_in_bronze"
    if "not_in_silver" in rule_text:
        return "bronze_not_in_silver"
    if "not_in_bronze" in rule_text:
        return "silver_not_in_bronze"
    return rule_text


def build_table_queries_from_rules(validation_df):
    bronze_namespace = os.getenv("DATABRICKS_BRONZE_NAMESPACE", DEFAULT_BRONZE_NAMESPACE)
    silver_namespace = os.getenv("DATABRICKS_SILVER_NAMESPACE", DEFAULT_SILVER_NAMESPACE)

    configured_tables = split_config_list(os.getenv("DATABRICKS_VALIDATION_TABLES"))
    table_names = (
        validation_df["table_name"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    table_names = [table_name for table_name in table_names.drop_duplicates() if table_name]
    if not table_names and configured_tables:
        table_names = configured_tables
    if not table_names:
        raise ValueError(
            "No table names could be derived from Databricks validation rules. "
            "Set DATABRICKS_VALIDATION_TABLES or DATABRICKS_TABLE_NAME_COLUMN in the Workflow environment."
        )

    rows = []
    for index, table_name in enumerate(table_names, start=1):
        table_rules = validation_df[
            validation_df["table_name"].fillna("").astype(str).str.strip().str.upper() == table_name.upper()
        ]
        first_rule = table_rules.iloc[0] if not table_rules.empty else pd.Series(dtype="object")
        bronze_query = first_configured_query(first_rule, "bronze_query")
        silver_query = first_configured_query(first_rule, "silver_query")
        bronze_table = first_configured_query(first_rule, "bronze_table")
        silver_table = first_configured_query(first_rule, "silver_table")
        rows.append(
            {
                "S.No": index,
                "flag": 1,
                "table_name": table_name,
                "bronze_query": bronze_query or build_source_query(bronze_table, bronze_namespace, table_name),
                "silver_query": silver_query or build_source_query(silver_table, silver_namespace, table_name),
            }
        )

    return pd.DataFrame(rows)


def build_source_query(configured_table, default_namespace, table_name):
    if configured_table:
        configured_table = str(configured_table).strip()
        if is_full_table_reference(configured_table):
            return f"SELECT * FROM {configured_table}"
        return f"SELECT * FROM {configured_table}.{quote_table_name(table_name)}"
    return f"SELECT * FROM {default_namespace}.{quote_table_name(table_name)}"


def is_full_table_reference(value):
    parts = [part.strip() for part in str(value).split(".") if part.strip()]
    return len(parts) >= 3


def first_configured_query(row, column_name):
    if row.empty or column_name not in row.index:
        return None
    value = row.get(column_name)
    if pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def split_config_list(value):
    if value is None:
        return []
    return [item.strip() for item in re.split(r"[,;\n\r]+", str(value)) if item.strip()]


def quote_table_name(table_name):
    table_name = str(table_name).strip()
    if table_name.startswith("`") and table_name.endswith("`"):
        return table_name
    return f"`{table_name.replace('`', '``')}`"
