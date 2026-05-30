import os
import re
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "queries" / "validation_queries.xlsx"
EXCEL_PATH = Path(os.getenv("VALIDATION_EXCEL_PATH", DEFAULT_EXCEL_PATH))
DEFAULT_ENVIRONMENT = "dev"
DEFAULT_ENVIRONMENT_SCHEMAS = {
    "dev": {
        "bronze": 'dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze"',
        "silver": 'dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver"',
    },
    "uat": {
        "bronze": 'uat_iceberg."tmkn-aws-dwh-uat-iceberg-bronze"',
        "silver": 'uat_iceberg."tmkn-aws-dwh-uat-iceberg-silver"',
    },
}


def _read_first_available_sheet(sheet_names):
    excel_file = pd.ExcelFile(EXCEL_PATH)
    available = {name.upper(): name for name in excel_file.sheet_names}

    for sheet_name in sheet_names:
        actual_name = available.get(sheet_name.upper())
        if actual_name:
            return pd.read_excel(EXCEL_PATH, sheet_name=actual_name)

    raise ValueError(
        f"None of the expected sheets {sheet_names} were found in {EXCEL_PATH}. "
        f"Available sheets: {excel_file.sheet_names}"
    )


def _sheet_exists(sheet_name):
    excel_file = pd.ExcelFile(EXCEL_PATH)
    available = {name.upper(): name for name in excel_file.sheet_names}
    return available.get(sheet_name.upper())


def _require_columns(df, required_columns, sheet_description):
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"{sheet_description} is missing required columns: {sorted(missing)}")


def _clean_text(value):
    if pd.isna(value):
        return None
    text_value = str(value).strip()
    return text_value or None


def _is_enabled(value):
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    text_value = str(value).strip().upper()
    if text_value in {"TRUE", "Y", "YES"}:
        return True
    try:
        return float(text_value) == 1
    except ValueError:
        return False


def _normalize_columns(df):
    return df.rename(columns={column: str(column).strip().lower() for column in df.columns})


def _default_environment_config():
    config = {
        "environment": DEFAULT_ENVIRONMENT,
        "bronze": DEFAULT_ENVIRONMENT_SCHEMAS[DEFAULT_ENVIRONMENT]["bronze"],
        "silver": DEFAULT_ENVIRONMENT_SCHEMAS[DEFAULT_ENVIRONMENT]["silver"],
        "replacement_schemas": DEFAULT_ENVIRONMENT_SCHEMAS,
    }
    return config


def read_environment_config():
    sheet_name = _sheet_exists("environment")
    if not sheet_name:
        return _default_environment_config()

    df = _normalize_columns(pd.read_excel(EXCEL_PATH, sheet_name=sheet_name))
    _require_columns(df, ["flag", "environment", "bronze", "silver"], "environment")

    replacement_schemas = {
        env_name: schemas.copy()
        for env_name, schemas in DEFAULT_ENVIRONMENT_SCHEMAS.items()
    }
    for _, row in df.iterrows():
        environment_name = _clean_text(row.get("environment"))
        bronze_schema = _clean_text(row.get("bronze"))
        silver_schema = _clean_text(row.get("silver"))
        if environment_name and bronze_schema and silver_schema:
            replacement_schemas[environment_name.lower()] = {
                "bronze": bronze_schema,
                "silver": silver_schema,
            }

    active_df = df[df["flag"].apply(_is_enabled)]
    if active_df.empty:
        return {
            **_default_environment_config(),
            "replacement_schemas": replacement_schemas,
        }
    if len(active_df) > 1:
        active_environments = [
            _clean_text(value) or "<blank>"
            for value in active_df["environment"].tolist()
        ]
        raise ValueError(f"Only one environment row can be flagged. Active rows: {active_environments}")

    active_row = active_df.iloc[0]
    environment_name = _clean_text(active_row.get("environment"))
    bronze_schema = _clean_text(active_row.get("bronze"))
    silver_schema = _clean_text(active_row.get("silver"))
    if not environment_name or not bronze_schema or not silver_schema:
        raise ValueError("Flagged environment row must have environment, bronze, and silver values")

    return {
        "environment": environment_name.lower(),
        "bronze": bronze_schema,
        "silver": silver_schema,
        "replacement_schemas": replacement_schemas,
    }


def apply_environment_to_sql(sql_text, environment_config=None):
    if not sql_text:
        return sql_text

    environment_config = environment_config or read_environment_config()
    replacement_schemas = environment_config.get("replacement_schemas") or DEFAULT_ENVIRONMENT_SCHEMAS
    selected_bronze = environment_config["bronze"]
    selected_silver = environment_config["silver"]

    updated_sql = sql_text
    for schemas in replacement_schemas.values():
        bronze_schema = schemas.get("bronze")
        silver_schema = schemas.get("silver")
        if bronze_schema:
            updated_sql = updated_sql.replace(bronze_schema, selected_bronze)
        if silver_schema:
            updated_sql = updated_sql.replace(silver_schema, selected_silver)
    return updated_sql


def _resolve_query_file_path(value):
    query_path = Path(value)
    if query_path.exists():
        return query_path

    path_text = str(value).replace("\\", "/")
    project_folder = PROJECT_ROOT.name
    project_marker = f"/{project_folder}/"
    if project_marker in path_text:
        relative_part = path_text.split(project_marker, 1)[1]
        remapped_path = PROJECT_ROOT / relative_part
        if remapped_path.exists():
            return remapped_path

    if not query_path.is_absolute():
        query_path = PROJECT_ROOT / query_path
    return query_path


def _looks_like_query_file(value):
    query_file = _clean_text(value)
    if not query_file:
        return False

    path = Path(query_file)
    return path.suffix.lower() == ".sql"


def _read_query_file(value, table_name, query_column):
    query_file = _clean_text(value)
    if not query_file:
        return None, None
    if not _looks_like_query_file(query_file):
        return None, None

    query_path = _resolve_query_file_path(query_file)
    if not query_path.exists():
        return None, f"{query_column} file not found for {table_name}: {query_path}"
    if not query_path.is_file():
        return None, f"{query_column} path is not a file for {table_name}: {query_path}"

    query_text = query_path.read_text(encoding="utf-8-sig").strip()
    if not query_text:
        return None, f"{query_column} file is empty for {table_name}: {query_path}"

    return query_text, None


def _find_matching_parenthesis(sql_text, open_index):
    depth = 0
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    index = open_index

    while index < len(sql_text):
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if in_line_comment:
            if char in "\r\n":
                in_line_comment = False
            index += 1
            continue

        if not in_single_quote and not in_double_quote and char == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue

        if not in_single_quote and not in_double_quote and char == "-" and next_char == "-":
            in_line_comment = True
            index += 2
            continue

        if not in_double_quote and char == "'":
            in_single_quote = not in_single_quote
        elif not in_single_quote and char == '"':
            in_double_quote = not in_double_quote
        elif not in_single_quote and not in_double_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return index

        index += 1

    return -1


def _extract_cte_query(sql_text, cte_name, table_name, query_column):
    cte_match = re.search(rf"\b{re.escape(cte_name)}\s+AS\s*\(", sql_text, flags=re.IGNORECASE)
    if not cte_match:
        return None, f"{query_column} is missing {cte_name} CTE for {table_name}"

    open_index = sql_text.find("(", cte_match.end() - 1)
    close_index = _find_matching_parenthesis(sql_text, open_index)
    if close_index == -1:
        return None, f"{query_column} has an incomplete {cte_name} CTE for {table_name}"

    cte_query = sql_text[open_index + 1:close_index].strip()
    if not cte_query:
        return None, f"{query_column} has an empty {cte_name} CTE for {table_name}"

    return cte_query, None


def _apply_cte_query_file_overrides(df):
    if "sql_query" not in df.columns:
        return df

    df = df.copy()
    if "query_config_error" not in df.columns:
        df["query_config_error"] = None
    for query_column in ["bronze_query", "silver_query"]:
        if query_column not in df.columns:
            df[query_column] = None

    for index, row in df.iterrows():
        table_name = _clean_text(row.get("table_name")) or f"row {index + 2}"
        sql_query_file = _clean_text(row.get("sql_query"))
        if not sql_query_file:
            continue

        if not _looks_like_query_file(sql_query_file):
            df.at[index, "query_config_error"] = f"sql_query must be a .sql file path for {table_name}: {sql_query_file}"
            continue

        sql_text, error = _read_query_file(sql_query_file, table_name, "sql_query")
        if error:
            df.at[index, "query_config_error"] = error
            continue

        bronze_query, bronze_error = _extract_cte_query(sql_text, "bronze_layer", table_name, "sql_query")
        silver_query, silver_error = _extract_cte_query(sql_text, "silver_layer", table_name, "sql_query")
        errors = [error_text for error_text in [bronze_error, silver_error] if error_text]
        if errors:
            df.at[index, "query_config_error"] = " | ".join(errors)
            continue

        df.at[index, "bronze_query"] = bronze_query
        df.at[index, "silver_query"] = silver_query

    return df


def _apply_query_file_overrides(df):
    df = df.copy()
    if "query_config_error" not in df.columns:
        df["query_config_error"] = None
    for query_column in ["bronze_query", "silver_query"]:
        if query_column not in df.columns:
            df[query_column] = None

    query_mappings = [
        ("bronze_query", "bronze_query_file"),
        ("silver_query", "silver_query_file"),
    ]

    for index, row in df.iterrows():
        table_name = _clean_text(row.get("table_name")) or f"row {index + 2}"
        errors = []

        for query_column, file_column in query_mappings:
            if file_column not in df.columns:
                continue

            query_text, error = _read_query_file(row.get(file_column), table_name, file_column)
            if error:
                errors.append(error)
                continue
            if query_text:
                df.at[index, query_column] = query_text

        if errors:
            df.at[index, "query_config_error"] = " | ".join(errors)

    return df


def read_table_queries():
    df = _read_first_available_sheet(["TABLE_QUERIES"])
    _require_columns(
        df,
        ["flag", "table_name"],
        "TABLE_QUERIES",
    )
    df = _apply_cte_query_file_overrides(df)
    df = _apply_query_file_overrides(df)

    environment_config = read_environment_config()
    for query_column in ["bronze_query", "silver_query"]:
        if query_column in df.columns:
            df[query_column] = df[query_column].apply(
                lambda query: apply_environment_to_sql(query, environment_config) if _clean_text(query) else query
            )
    df["selected_environment"] = environment_config["environment"]
    return df


def read_validations():
    df = _read_first_available_sheet(["VALIDATION", "VALIDATIONS"])
    _require_columns(
        df,
        ["flag", "table_name", "validation_type"],
        "VALIDATION/VALIDATIONS",
    )
    return df


def read_validation():
    return read_validations()
