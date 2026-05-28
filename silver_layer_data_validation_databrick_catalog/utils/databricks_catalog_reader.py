import os

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
    }.items():
        if source_name in lower_columns and target_name not in validation_df.columns:
            rename_map[lower_columns[source_name]] = target_name

    if rename_map:
        validation_df = validation_df.rename(columns=rename_map)

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

    table_names = (
        validation_df["table_name"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    table_names = [table_name for table_name in table_names.drop_duplicates() if table_name]

    rows = []
    for index, table_name in enumerate(table_names, start=1):
        rows.append(
            {
                "S.No": index,
                "flag": 1,
                "table_name": table_name,
                "bronze_query": f"SELECT * FROM {bronze_namespace}.{quote_table_name(table_name)}",
                "silver_query": f"SELECT * FROM {silver_namespace}.{quote_table_name(table_name)}",
            }
        )

    return pd.DataFrame(rows)


def quote_table_name(table_name):
    table_name = str(table_name).strip()
    if table_name.startswith("`") and table_name.endswith("`"):
        return table_name
    return f"`{table_name.replace('`', '``')}`"
