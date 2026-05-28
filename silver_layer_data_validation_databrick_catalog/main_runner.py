import logging
import os
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime

import pandas as pd

from config.db_connection import get_connections
from utils.excel_reader import read_table_queries, read_validations
from utils.databricks_catalog_reader import read_databricks_validation_inputs
from validations.comparator import execute_query, execute_query_scalar, get_query_columns
from validations.report_generator import generate_excel_report


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def console_table_header(sequence_number, table_name):
    sequence_number = format_sequence_number(sequence_number)
    if sequence_number:
        print(f"{sequence_number}. {table_name}")
    else:
        print(table_name)


def console_validation_line(validation_name, status):
    print(f"    {validation_name:<25} {status}")


def format_sequence_number(value):
    value = clean_value(value)
    if not value:
        return ""
    try:
        number = float(value)
        if number.is_integer():
            return str(int(number))
    except ValueError:
        pass
    return value


def is_enabled(value):
    """Return True for Excel flag values such as 1, TRUE, Y, YES."""
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


def clean_value(value, default=None):
    if pd.isna(value):
        return default
    value = str(value).strip()
    if not value or value.upper() == "NA":
        return default
    return value


def normalize_validation_type(value):
    return clean_value(value, "").lower().replace(" ", "_")


def get_table_validations(validation_df, table_name):
    df = validation_df.copy()
    df["validation_type_norm"] = df["validation_type"].apply(normalize_validation_type)

    validations = {}
    if "table_name" not in df.columns:
        for _, row in df.iterrows():
            validation_type = clean_value(row.get("validation_type_norm"))
            if validation_type:
                validations[validation_type] = row
        return validations

    table_names = df["table_name"].apply(clean_value)
    global_rows = df[table_names.isna()]
    table_rows = df[table_names.astype(str).str.upper() == table_name.upper()]

    # Global rows are defaults. Table-specific rows must override them.
    for source_df in [global_rows, table_rows]:
        for _, row in source_df.iterrows():
            validation_type = clean_value(row.get("validation_type_norm"))
            if validation_type:
                validations[validation_type] = row

    return validations


def get_default_pk(validation_df, table_name):
    key_column = get_key_config_column(validation_df)
    if not key_column or "table_name" not in validation_df.columns:
        return None

    table_rows = validation_df[
        validation_df["table_name"].astype(str).str.strip().str.upper() == table_name.upper()
    ]
    for value in table_rows[key_column]:
        pk = clean_value(value)
        if pk:
            return pk
    return None


def get_key_config_column(df):
    for column_name in ["business_keys", "business_key_columns", "pk"]:
        if column_name in df.columns:
            return column_name
    return None


def get_configured_keys(cfg, default_keys=None):
    return clean_value(cfg.get("business_keys")) or clean_value(cfg.get("business_key_columns")) or clean_value(cfg.get("pk")) or default_keys


def append_result(results, table_name, validation, status, **details):
    row = {
        "table_name": table_name,
        "validation": validation,
        "status": status,
        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    row.update(details)
    results.append(row)


def run_count_validation(bronze_conn, silver_conn, table_name, bronze_q, silver_q, results):
    bronze_count_query = f"SELECT COUNT(*) AS cnt FROM ({bronze_q}) src"
    silver_count_query = f"SELECT COUNT(*) AS cnt FROM ({silver_q}) src"

    bronze_cnt = execute_query_scalar(bronze_conn, bronze_count_query, "cnt")
    silver_cnt = execute_query_scalar(silver_conn, silver_count_query, "cnt")

    append_result(
        results,
        table_name,
        "count",
        "PASS" if bronze_cnt == silver_cnt else "FAIL",
        bronze=bronze_cnt,
        silver=silver_cnt,
        difference=bronze_cnt - silver_cnt,
    )


def run_column_names_validation(bronze_conn, silver_conn, table_name, bronze_q, silver_q, results, detail_sheets=None):
    bronze_columns = get_query_columns(bronze_conn, bronze_q)
    silver_columns = get_query_columns(silver_conn, silver_q)
    bronze_norm = [column.lower() for column in bronze_columns]
    silver_norm = [column.lower() for column in silver_columns]

    detail_rows = []
    max_len = max(len(bronze_columns), len(silver_columns))
    for index in range(max_len):
        bronze_column = bronze_columns[index] if index < len(bronze_columns) else None
        silver_column = silver_columns[index] if index < len(silver_columns) else None
        detail_rows.append(
            {
                "position": index + 1,
                "bronze_column": bronze_column,
                "silver_column": silver_column,
                "status": "PASS" if str(bronze_column).lower() == str(silver_column).lower() else "FAIL",
            }
        )

    missing_in_silver = [column for column in bronze_columns if column.lower() not in silver_norm]
    missing_in_bronze = [column for column in silver_columns if column.lower() not in bronze_norm]
    detail_df = pd.DataFrame(detail_rows)
    if detail_sheets is not None:
        add_detail_sheet(detail_sheets, "Column_Comparison", detail_df)

    append_result(
        results,
        table_name,
        "column_names",
        "PASS" if bronze_norm == silver_norm else "FAIL",
        bronze_column_count=len(bronze_columns),
        silver_column_count=len(silver_columns),
        missing_in_silver=", ".join(missing_in_silver),
        missing_in_bronze=", ".join(missing_in_bronze),
    )


def run_duplicate_validation(
    conn,
    table_name,
    source_name,
    source_query,
    pk,
    results,
    detail_sheets=None,
    detail_limit=1000,
):
    pk = clean_value(pk)
    validation_name = f"duplicate_{source_name}"

    if not pk:
        append_result(
            results,
            table_name,
            validation_name,
            "ERROR",
            message=f"PK missing for {validation_name}",
        )
        return

    query = f"""
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT {pk}
            FROM ({source_query}) src
            GROUP BY {pk}
            HAVING COUNT(*) > 1
        ) dup
    """

    duplicate_count = execute_query_scalar(conn, query, "cnt")
    if duplicate_count and detail_sheets is not None:
        detail_query = f"""
            WITH duplicate_keys AS (
                SELECT {pk}
                FROM ({source_query}) src
                GROUP BY {pk}
                HAVING COUNT(*) > 1
                LIMIT {detail_limit}
            )
            SELECT
                '{table_name}' AS table_name,
                '{source_name}' AS source_name,
                src.*
            FROM ({source_query}) src
            INNER JOIN duplicate_keys dup
                ON { " AND ".join([f"src.{quote_identifier(col.strip())} IS NOT DISTINCT FROM dup.{quote_identifier(col.strip())}" for col in pk.split(",")]) }
            LIMIT {detail_limit}
        """
        duplicate_df = execute_query(conn, detail_query)
        add_detail_sheet(detail_sheets, validation_name, duplicate_df)
        append_detail_rows(detail_sheets, "Duplicate_Differences", duplicate_df)

    append_result(
        results,
        table_name,
        validation_name,
        "PASS" if duplicate_count == 0 else "FAIL",
        count=duplicate_count,
        pk=pk,
        detail_limit=detail_limit,
    )


def build_null_pk_condition(pk):
    pk_columns = split_config_list(pk)
    if not pk_columns:
        raise ValueError("PK is required for null PK validation")
    return " OR ".join([f"{quote_identifier(column)} IS NULL" for column in pk_columns])


def run_pk_null_validation(
    conn,
    table_name,
    source_name,
    source_query,
    pk,
    results,
    detail_sheets=None,
    detail_limit=1000,
):
    pk = clean_value(pk)
    validation_name = f"pk_null_{source_name}"

    if not pk:
        append_result(
            results,
            table_name,
            validation_name,
            "ERROR",
            message=f"PK missing for {validation_name}",
        )
        return

    null_condition = build_null_pk_condition(pk)
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM ({source_query}) src
        WHERE {null_condition}
    """

    null_count = execute_query_scalar(conn, query, "cnt")

    if null_count and detail_sheets is not None:
        detail_query = f"""
            SELECT
                '{table_name}' AS table_name,
                '{source_name}' AS source_name,
                '{pk}' AS pk,
                src.*
            FROM ({source_query}) src
            WHERE {null_condition}
            LIMIT {detail_limit}
        """
        detail_df = execute_query(conn, detail_query)
        add_detail_sheet(detail_sheets, validation_name, detail_df)
        append_detail_rows(detail_sheets, "PK_Null_Differences", detail_df)

    append_result(
        results,
        table_name,
        validation_name,
        "PASS" if null_count == 0 else "FAIL",
        count=null_count,
        pk=pk,
        detail_limit=detail_limit,
    )


def run_custom_sql_validation(conn, table_name, validation_name, sql_query, results):
    sql_query = clean_value(sql_query)
    if not sql_query:
        append_result(
            results,
            table_name,
            validation_name,
            "ERROR",
            message=f"SQL query missing for {validation_name}",
        )
        return

    count = execute_query_scalar(conn, sql_query)
    append_result(
        results,
        table_name,
        validation_name,
        "PASS" if count == 0 else "FAIL",
        count=count,
    )


def split_config_list(value):
    value = clean_value(value)
    if not value:
        return []
    return [item.strip() for item in re.split(r"[,;\n\r]+", value) if item.strip()]


def get_detail_limit(value, default=1000):
    value = clean_value(value)
    if not value:
        return default
    try:
        return max(1, int(float(value)))
    except ValueError:
        return default


def quote_identifier(identifier):
    quote_char = "`" if os.getenv("VALIDATION_SOURCE", "").strip().lower() == "databricks" else '"'
    escaped_identifier = identifier.replace(quote_char, quote_char * 2)
    return quote_char + escaped_identifier + quote_char


def build_normalized_value_expr(column_name):
    value_expr = f"LOWER(TRIM(CAST({quote_identifier(column_name)} AS VARCHAR)))"
    numeric_pattern = "'^-?[0-9]+([.][0-9]+)?$'"
    timestamp_pattern = "'^[0-9]{4}-[0-9]{2}-[0-9]{2}([ t][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?)?$'"
    strip_fraction_zeros = (
        f"REGEXP_REPLACE("
        f"REGEXP_REPLACE({value_expr}, '([.][0-9]*?[1-9])0+$', '$1'), "
        f"'[.]0+$', '')"
    )
    normalized_temporal_value = (
        f"REGEXP_REPLACE({strip_fraction_zeros}, '([ t]00:00:00)$', '')"
    )

    if os.getenv("VALIDATION_SOURCE", "").strip().lower() == "databricks":
        return (
            f"CASE WHEN {value_expr} RLIKE {numeric_pattern} OR {value_expr} RLIKE {timestamp_pattern} "
            f"THEN {normalized_temporal_value} ELSE {value_expr} END"
        )

    return (
        f"CASE WHEN REGEXP_LIKE({value_expr}, {numeric_pattern}) "
        f"OR REGEXP_LIKE({value_expr}, {timestamp_pattern}) "
        f"THEN {normalized_temporal_value} ELSE {value_expr} END"
    )


def build_normalized_select(source_query, source_columns, output_columns=None):
    if output_columns and len(output_columns) != len(source_columns):
        raise ValueError("output_columns and source_columns must have the same length")

    select_parts = []
    for index, source_column in enumerate(source_columns):
        output_column = output_columns[index] if output_columns else source_column
        select_parts.append(
            f"{build_normalized_value_expr(source_column)} AS {quote_identifier(output_column)}"
        )

    return f"""
        SELECT
            {", ".join(select_parts)}
        FROM ({source_query}) src
    """


def add_detail_sheet(detail_sheets, sheet_name, df):
    if df is not None and not df.empty:
        detail_sheets[sheet_name] = df


def append_detail_rows(detail_sheets, sheet_name, df):
    if df is None or df.empty:
        return
    existing_df = detail_sheets.get(sheet_name)
    if existing_df is None or existing_df.empty:
        detail_sheets[sheet_name] = df
    else:
        detail_sheets[sheet_name] = pd.concat([existing_df, df], ignore_index=True)


def resolve_compare_columns(conn, source_query, configured_columns):
    query_columns = get_query_columns(conn, source_query)
    if not configured_columns:
        return query_columns

    query_lookup = {column.lower(): column for column in query_columns}
    resolved_columns = []
    missing_columns = []
    for configured_column in configured_columns:
        actual_column = query_lookup.get(configured_column.lower())
        if actual_column:
            resolved_columns.append(actual_column)
        else:
            missing_columns.append(configured_column)

    if missing_columns:
        raise ValueError(
            "Configured compare column(s) not found in query output: "
            f"{missing_columns}. Available columns: {query_columns}"
        )

    return resolved_columns


def fetch_except_detail(conn, left_select, right_select, detail_limit):
    query = f"""
        SELECT *
        FROM (
            {left_select}
            EXCEPT
            {right_select}
        ) diff
        LIMIT {detail_limit}
    """
    return execute_query(conn, query)


def sql_string_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def fetch_column_value_differences(
    conn,
    left_query,
    right_query,
    left_columns,
    right_columns,
    left_name,
    right_name,
    detail_limit,
):
    right_lookup = {column.lower(): column for column in right_columns}
    common_pairs = [
        (left_column, right_lookup[left_column.lower()])
        for left_column in left_columns
        if left_column.lower() in right_lookup
    ]
    if not common_pairs:
        return pd.DataFrame()

    left_count_column = f"{left_name}_count"
    right_count_column = f"{right_name}_count"
    per_column_limit = max(1, min(detail_limit, int(os.getenv("COLUMN_VALUE_DIFFS_PER_COLUMN_LIMIT", "100"))))
    frames = []

    for left_column, right_column in common_pairs:
        column_name = left_column
        left_value_expr = build_normalized_value_expr(left_column)
        right_value_expr = build_normalized_value_expr(right_column)
        query = f"""
            WITH left_values AS (
                SELECT
                    {left_value_expr} AS column_value,
                    COUNT(*) AS {quote_identifier(left_count_column)}
                FROM ({left_query}) src
                GROUP BY {left_value_expr}
            ),
            right_values AS (
                SELECT
                    {right_value_expr} AS column_value,
                    COUNT(*) AS {quote_identifier(right_count_column)}
                FROM ({right_query}) src
                GROUP BY {right_value_expr}
            )
            SELECT
                {sql_string_literal(column_name)} AS column_name,
                COALESCE(l.column_value, r.column_value) AS column_value,
                COALESCE(l.{quote_identifier(left_count_column)}, 0) AS {quote_identifier(left_count_column)},
                COALESCE(r.{quote_identifier(right_count_column)}, 0) AS {quote_identifier(right_count_column)},
                COALESCE(l.{quote_identifier(left_count_column)}, 0) - COALESCE(r.{quote_identifier(right_count_column)}, 0) AS count_difference,
                CASE
                    WHEN l.column_value IS NULL AND r.column_value IS NOT NULL THEN 'value_missing_in_{left_name}'
                    WHEN r.column_value IS NULL AND l.column_value IS NOT NULL THEN 'value_missing_in_{right_name}'
                    ELSE 'count_mismatch'
                END AS difference_type
            FROM left_values l
            FULL OUTER JOIN right_values r
                ON l.column_value IS NOT DISTINCT FROM r.column_value
            WHERE COALESCE(l.{quote_identifier(left_count_column)}, 0) <> COALESCE(r.{quote_identifier(right_count_column)}, 0)
            ORDER BY ABS(COALESCE(l.{quote_identifier(left_count_column)}, 0) - COALESCE(r.{quote_identifier(right_count_column)}, 0)) DESC, column_value
            LIMIT {per_column_limit}
        """
        diff_df = execute_query(conn, query)
        if not diff_df.empty:
            frames.append(diff_df)
            if sum(len(frame) for frame in frames) >= detail_limit:
                break

    if not frames:
        return pd.DataFrame()

    result_df = pd.concat(frames, ignore_index=True)
    result_df["_abs_difference"] = result_df["count_difference"].abs()
    result_df = result_df.sort_values(
        ["column_name", "_abs_difference"],
        ascending=[True, False],
    ).drop(columns=["_abs_difference"]).reset_index(drop=True)
    return result_df.head(detail_limit)


def build_all_differences_rows(table_name, source_name, detail_df, pk=None, missing_status=None):
    if detail_df is None or detail_df.empty:
        return pd.DataFrame()

    pk_columns = split_config_list(pk)
    ignored_columns = {"table_name", "source_name", "record_label", "record_number", "source_order", "pk"}
    rows = []

    for index, (_, data_row) in enumerate(detail_df.iterrows(), start=1):
        if pk_columns:
            pk_values = [data_row.get(pk_col) for pk_col in pk_columns]
            if len(pk_columns) == 1:
                record_id = pk_values[0]
            else:
                record_id = " | ".join([str(pk_value) for pk_value in pk_values])
        else:
            record_id = f"{source_name} record {index}"

        for column_name, value in data_row.items():
            if column_name in ignored_columns or column_name in pk_columns:
                continue
            if pd.isna(value):
                continue
            rows.append(
                {
                    "Source": source_name.title(),
                    "ID": record_id,
                    "COLUMN": column_name,
                    "VALUE": value,
                    "Status": missing_status or "mismatch",
                }
            )

    return pd.DataFrame(rows)


def build_wide_difference_rows(source_name, detail_df, missing_status):
    if detail_df is None or detail_df.empty:
        return pd.DataFrame()

    wide_df = detail_df.copy()
    wide_df.insert(0, "Source", source_name.title())
    wide_df.insert(1, "Status", missing_status)
    wide_df.insert(2, "record_id", range(1, len(wide_df) + 1))
    return wide_df


def build_flat_rows_from_column_diff(diff_df):
    if diff_df is None or diff_df.empty:
        return pd.DataFrame()

    value_columns = [
        col
        for col in diff_df.columns
        if col.endswith("_value") and col not in {"pk_value"}
    ]
    if len(value_columns) < 2:
        return pd.DataFrame()

    left_value_col = value_columns[0]
    right_value_col = value_columns[1]

    if not left_value_col or not right_value_col:
        return pd.DataFrame()

    left_source = left_value_col[:-6].title()
    right_source = right_value_col[:-6].title()

    rows = []
    for _, data_row in diff_df.iterrows():
        rows.append(
            {
                "Source": left_source,
                "ID": data_row.get("pk_value"),
                "COLUMN": data_row.get("column_name"),
                "VALUE": data_row.get(left_value_col),
                "Status": data_row.get("mismatch_type", "mismatch"),
            }
        )
        rows.append(
            {
                "Source": right_source,
                "ID": data_row.get("pk_value"),
                "COLUMN": data_row.get("column_name"),
                "VALUE": data_row.get(right_value_col),
                "Status": data_row.get("mismatch_type", "mismatch"),
            }
        )

    return pd.DataFrame(rows)


def normalize_compare_value(value):
    if pd.isna(value):
        return None
    text_value = str(value).strip().casefold()
    try:
        decimal_value = Decimal(text_value)
    except InvalidOperation:
        if re.match(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}([ t][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?)?$", text_value):
            return re.sub(r"[.]0+$", "", re.sub(r"([.][0-9]*?[1-9])0+$", r"\1", text_value))
        return text_value
    return format(decimal_value.normalize(), "f")


def fetch_column_diff_detail(
    conn,
    table_name,
    left_name,
    right_name,
    left_query,
    right_query,
    left_columns,
    right_columns,
    pk,
    detail_limit,
):
    pk = clean_value(pk)
    if not pk:
        return pd.DataFrame()

    left_lookup = {column.lower(): column for column in left_columns}
    right_lookup = {column.lower(): column for column in right_columns}
    pk_columns = split_config_list(pk)
    if not pk_columns:
        return pd.DataFrame()

    left_pk_columns = []
    right_pk_columns = []
    for pk_column in pk_columns:
        pk_key = pk_column.lower()
        if pk_key not in left_lookup or pk_key not in right_lookup:
            raise ValueError(f"Business key '{pk_column}' must exist in both queries to generate column differences")
        left_pk_columns.append(left_lookup[pk_key])
        right_pk_columns.append(right_lookup[pk_key])

    compare_pairs = [
        (left_col, right_lookup[left_col.lower()])
        for left_col in left_columns
        if left_col.lower() not in {pk_col.lower() for pk_col in pk_columns} and left_col.lower() in right_lookup
    ]

    if not compare_pairs:
        return pd.DataFrame()

    left_select_parts = []
    right_select_parts = []
    join_conditions = []
    diff_conditions = []

    for index, (left_pk, right_pk) in enumerate(zip(left_pk_columns, right_pk_columns), start=1):
        alias_name = f"pk_{index}"
        left_select_parts.append(
            f"CAST({quote_identifier(left_pk)} AS VARCHAR) AS {quote_identifier(alias_name)}"
        )
        right_select_parts.append(
            f"CAST({quote_identifier(right_pk)} AS VARCHAR) AS {quote_identifier(alias_name)}"
        )
        join_conditions.append(
            f"l.{quote_identifier(alias_name)} IS NOT DISTINCT FROM r.{quote_identifier(alias_name)}"
        )

    for left_col, right_col in compare_pairs:
        left_alias = f"l__{left_col}"
        right_alias = f"r__{left_col}"
        left_select_parts.append(
            f"{build_normalized_value_expr(left_col)} AS {quote_identifier(left_alias)}"
        )
        right_select_parts.append(
            f"{build_normalized_value_expr(right_col)} AS {quote_identifier(right_alias)}"
        )
        diff_conditions.append(
            f"l.{quote_identifier(left_alias)} IS DISTINCT FROM r.{quote_identifier(right_alias)}"
        )

    query = f"""
        WITH left_src AS (
            SELECT
                {", ".join(left_select_parts)}
            FROM ({left_query}) src
        ),
        right_src AS (
            SELECT
                {", ".join(right_select_parts)}
            FROM ({right_query}) src
        )
        SELECT
            {", ".join([f"COALESCE(l.{quote_identifier(f'pk_{i}')}, r.{quote_identifier(f'pk_{i}')}) AS {quote_identifier(f'pk_{i}')}" for i in range(1, len(pk_columns) + 1)])},
            {", ".join([f"l.{quote_identifier(f'l__{left_col}')} AS {quote_identifier(f'l__{left_col}')}, r.{quote_identifier(f'r__{left_col}')} AS {quote_identifier(f'r__{left_col}')}" for left_col, _ in compare_pairs])}
        FROM left_src l
        INNER JOIN right_src r
            ON {" AND ".join(join_conditions)}
        WHERE {" OR ".join(diff_conditions)}
        LIMIT {detail_limit}
    """

    joined_df = execute_query(conn, query)
    if joined_df.empty:
        return pd.DataFrame()

    records = []
    for _, row in joined_df.iterrows():
        pk_value = " | ".join(
            [
                str(row.get(f'pk_{idx + 1}'))
                for idx, pk_col in enumerate(pk_columns)
            ]
        )
        if len(pk_columns) == 1:
            pk_value = row.get("pk_1")
        for left_col, _ in compare_pairs:
            left_value = row.get(f"l__{left_col}")
            right_value = row.get(f"r__{left_col}")
            if pd.isna(left_value) and pd.isna(right_value):
                continue
            if normalize_compare_value(left_value) != normalize_compare_value(right_value):
                record = {
                    "table_name": table_name,
                    "business_keys": pk,
                    "business_key_value": pk_value,
                    "column_name": left_col,
                    f"{left_name}_value": left_value,
                    f"{right_name}_value": right_value,
                    "difference_type": "value_mismatch",
                }
                for idx, pk_col in enumerate(pk_columns, start=1):
                    record[pk_col] = row.get(f"pk_{idx}")
                records.append(record)

    return pd.DataFrame(records)


def run_except_validation(
    conn,
    table_name,
    validation_name,
    left_query,
    right_query,
    results,
    compare_columns=None,
    strict_schema=False,
    pk=None,
    detail_limit=1000,
    detail_sheets=None,
):
    left_columns = resolve_compare_columns(conn, left_query, compare_columns)
    right_columns = resolve_compare_columns(conn, right_query, compare_columns)

    if not left_columns or not right_columns:
        raise ValueError(f"No columns found for {validation_name}")

    if len(left_columns) != len(right_columns):
        raise ValueError(
            f"Column count mismatch for {validation_name}: "
            f"left={len(left_columns)}, right={len(right_columns)}"
        )

    if strict_schema and [col.lower() for col in left_columns] != [col.lower() for col in right_columns]:
        raise ValueError(
            f"Column name/order mismatch for {validation_name}: "
            f"left={left_columns}, right={right_columns}"
        )

    output_columns = left_columns
    left_select = build_normalized_select(left_query, left_columns, output_columns)
    right_select = build_normalized_select(right_query, right_columns, output_columns)

    query = f"""
        SELECT COUNT(*) AS cnt
        FROM (
            {left_select}
            EXCEPT
            {right_select}
        ) diff
    """

    count = execute_query_scalar(conn, query, "cnt")
    if count and detail_sheets is not None:
        except_detail_df = fetch_except_detail(conn, left_select, right_select, detail_limit)
        left_name, _, right_name = validation_name.partition("_not_in_")
        left_name = left_name or "left"
        right_name = right_name or "right"
        has_business_keys = bool(clean_value(pk))
        detail_sheet_name = validation_name if has_business_keys else f"Missing_In_{right_name.title()}"
        add_detail_sheet(
            detail_sheets,
            detail_sheet_name,
            except_detail_df,
        )

        diff_df = fetch_column_diff_detail(
            conn,
            table_name,
            left_name,
            right_name,
            left_query,
            right_query,
            left_columns,
            right_columns,
            pk,
            detail_limit,
        )
        add_detail_sheet(detail_sheets, f"{validation_name}_column_diff", diff_df)
        append_detail_rows(detail_sheets, "Business_Key_Differences", diff_df)

        if not diff_df.empty:
            append_detail_rows(detail_sheets, "Column_Differences", diff_df)
        else:
            if validation_name == "bronze_not_in_silver":
                column_value_diff_df = fetch_column_value_differences(
                    conn,
                    left_query,
                    right_query,
                    left_columns,
                    right_columns,
                    "bronze",
                    "silver",
                    detail_limit,
                )
                add_detail_sheet(detail_sheets, "Column_Value_Differences", column_value_diff_df)
            elif validation_name == "silver_not_in_bronze" and "Column_Value_Differences" not in detail_sheets:
                column_value_diff_df = fetch_column_value_differences(
                    conn,
                    right_query,
                    left_query,
                    right_columns,
                    left_columns,
                    "bronze",
                    "silver",
                    detail_limit,
                )
                add_detail_sheet(detail_sheets, "Column_Value_Differences", column_value_diff_df)

    append_result(
        results,
        table_name,
        validation_name,
        "PASS" if count == 0 else "FAIL",
        count=count,
        compare_mode="normalized_varchar",
        compared_columns=", ".join(left_columns),
        detail_limit=detail_limit,
    )


def run_validations(bronze_conn, silver_conn, table_name, bronze_q, silver_q, validation_df):
    results = []
    detail_sheets = {}
    validations = get_table_validations(validation_df, table_name)
    default_pk = get_default_pk(validation_df, table_name)

    validation_steps = [
        ("count", lambda cfg: run_count_validation(bronze_conn, silver_conn, table_name, bronze_q, silver_q, results)),
        (
            "column_names",
            lambda cfg: run_column_names_validation(
                bronze_conn,
                silver_conn,
                table_name,
                bronze_q,
                silver_q,
                results,
                detail_sheets,
            ),
        ),
        (
            "duplicate_bronze",
            lambda cfg: run_duplicate_validation(
                bronze_conn,
                table_name,
                "bronze",
                bronze_q,
                cfg.get("pk"),
                results,
                detail_sheets,
                get_detail_limit(cfg.get("detail_limit")),
            ),
        ),
        (
            "duplicate_silver",
            lambda cfg: run_duplicate_validation(
                silver_conn,
                table_name,
                "silver",
                silver_q,
                cfg.get("pk"),
                results,
                detail_sheets,
                get_detail_limit(cfg.get("detail_limit")),
            ),
        ),
        (
            "pk_null_bronze",
            lambda cfg: run_pk_null_validation(
                bronze_conn,
                table_name,
                "bronze",
                bronze_q,
                cfg.get("pk") or default_pk,
                results,
                detail_sheets,
                get_detail_limit(cfg.get("detail_limit")),
            ),
        ),
        (
            "pk_null_silver",
            lambda cfg: run_pk_null_validation(
                silver_conn,
                table_name,
                "silver",
                silver_q,
                cfg.get("pk") or default_pk,
                results,
                detail_sheets,
                get_detail_limit(cfg.get("detail_limit")),
            ),
        ),
        (
            "columns",
            lambda cfg: run_custom_sql_validation(
                bronze_conn, table_name, "columns", cfg.get("sql_query"), results
            ),
        ),
        (
            "bronze_not_in_silver",
            lambda cfg: run_except_validation(
                bronze_conn,
                table_name,
                "bronze_not_in_silver",
                bronze_q,
                silver_q,
                results,
                split_config_list(cfg.get("compare_columns")),
                is_enabled(cfg.get("strict_schema")),
                get_configured_keys(cfg, default_pk),
                get_detail_limit(cfg.get("detail_limit")),
                detail_sheets,
            ),
        ),
        (
            "silver_not_in_bronze",
            lambda cfg: run_except_validation(
                silver_conn,
                table_name,
                "silver_not_in_bronze",
                silver_q,
                bronze_q,
                results,
                split_config_list(cfg.get("compare_columns")),
                is_enabled(cfg.get("strict_schema")),
                get_configured_keys(cfg, default_pk),
                get_detail_limit(cfg.get("detail_limit")),
                detail_sheets,
            ),
        ),
    ]

    for validation_name, runner in validation_steps:
        cfg = validations.get(validation_name)
        if cfg is None or not is_enabled(cfg.get("flag")):
            continue

        try:
            runner(cfg)
            latest_status = results[-1]["status"] if results else "DONE"
            console_validation_line(validation_name, latest_status)
        except Exception as exc:
            logger.exception("Validation %s failed for %s", validation_name, table_name)
            append_result(
                results,
                table_name,
                validation_name,
                "ERROR",
                message=str(exc),
            )
            console_validation_line(validation_name, "ERROR")

    return pd.DataFrame(results), detail_sheets


def main():
    bronze_conn, silver_conn = get_connections()
    if os.getenv("VALIDATION_SOURCE", "").strip().lower() == "databricks":
        table_df, validation_df = read_databricks_validation_inputs(silver_conn)
    else:
        table_df = read_table_queries()
        validation_df = read_validations()

    for _, row in table_df.iterrows():
        table_name = clean_value(row.get("table_name"))
        if not table_name:
            continue

        if not is_enabled(row.get("flag")):
            continue

        query_config_error = clean_value(row.get("query_config_error"))
        bronze_q = clean_value(row.get("bronze_query"))
        silver_q = clean_value(row.get("silver_query"))
        selected_environment = clean_value(row.get("selected_environment"))

        if query_config_error or not bronze_q or not silver_q:
            result_df = pd.DataFrame(
                [
                    {
                        "environment": selected_environment,
                        "table_name": table_name,
                        "validation": "configuration",
                        "status": "ERROR",
                        "message": query_config_error or "bronze_query or silver_query is missing",
                        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                ]
            )
            generate_excel_report(
                result_df,
                table_name,
                "ERROR",
                table_sequence=row.get("S.No"),
                environment=selected_environment,
            )
            continue

        console_table_header(row.get("S.No"), table_name)
        result_df, detail_sheets = run_validations(
            bronze_conn,
            silver_conn,
            table_name,
            bronze_q,
            silver_q,
            validation_df,
        )

        if result_df.empty:
            overall_status = "NO_VALIDATIONS"
        elif (result_df["status"] == "PASS").all():
            overall_status = "PASS"
        elif (result_df["status"] == "ERROR").any():
            overall_status = "ERROR"
        else:
            overall_status = "FAIL"

        generate_excel_report(
            result_df,
            table_name,
            overall_status,
            detail_sheets,
            row.get("S.No"),
            environment=selected_environment,
        )


if __name__ == "__main__":
    main()
