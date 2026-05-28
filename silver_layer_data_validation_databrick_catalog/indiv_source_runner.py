import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from config.db_connection import get_connections
from validations.comparator import execute_query, execute_query_scalar, get_query_columns
from validations.report_generator import PROJECT_ROOT, generate_excel_report


logger = logging.getLogger(__name__)

DEFAULT_QUERY_ROOT = PROJECT_ROOT / "queries" / "Indivi_OS2_OS1_MIS"
DEFAULT_CONTROL_EXCEL_PATH = PROJECT_ROOT / "queries" / "Indivi_OS2_OS1_MIS" / "os2_os1_mis_SQL paths.xlsx"
DEFAULT_DETAIL_LIMIT = 1000


def clean_sql(sql_text):
    sql_text = sql_text.strip()
    while sql_text.endswith(";"):
        sql_text = sql_text[:-1].strip()
    return sql_text


def read_sql_file(path):
    return clean_sql(Path(path).read_text(encoding="utf-8-sig"))


def clean_value(value, default=None):
    if pd.isna(value):
        return default
    value = str(value).strip()
    if not value or value.upper() == "NA":
        return default
    return value


def is_enabled(value):
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


def resolve_path(value, base_dir=None):
    text_value = clean_value(value)
    if not text_value:
        return None
    path = Path(text_value)
    if not path.is_absolute():
        path = Path(base_dir or PROJECT_ROOT) / path
    return path


def resolve_sql_file_path(value, base_dir=None):
    text_value = clean_value(value)
    if not text_value:
        return None

    path = resolve_path(text_value, base_dir)
    candidates = [path]
    if path.suffix.lower() != ".sql":
        candidates.append(path.with_suffix(".sql"))

    for candidate in candidates:
        if candidate.exists() and candidate.is_file() and candidate.suffix.lower() == ".sql":
            return candidate

    if path.suffix.lower() == ".sql":
        return path
    return None


def first_existing_column(row, column_names):
    for column_name in column_names:
        if column_name in row.index:
            value = clean_value(row.get(column_name))
            if value:
                return value
    return None


def normalize_query_key(path, source_name):
    key = Path(path).stem.lower()
    key = re.sub(r"_converted$", "", key)
    key = re.sub(rf"_{re.escape(source_name.lower())}$", "", key)
    key = key.replace("enterprice", "enterprise")
    return key


def discover_sql_pairs(source_name, source_root):
    bronze_dir = Path(source_root) / "bronze"
    silver_dir = Path(source_root) / "silver"

    bronze_files = {
        normalize_query_key(path, source_name): path
        for path in bronze_dir.glob("*.sql")
    } if bronze_dir.exists() else {}
    silver_files = {
        normalize_query_key(path, source_name): path
        for path in silver_dir.glob("*.sql")
    } if silver_dir.exists() else {}

    keys = sorted(set(bronze_files) | set(silver_files))
    pairs = []
    for index, key in enumerate(keys, start=1):
        pairs.append(
            {
                "S.No": index,
                "table_name": key.upper(),
                "bronze_path": bronze_files.get(key),
                "silver_path": silver_files.get(key),
            }
        )
    return pairs


def resolve_excel_path(excel_path=None):
    configured_path = (
        excel_path
        or os.getenv("INDIV_VALIDATION_EXCEL_PATH")
        or os.getenv("VALIDATION_EXCEL_PATH")
    )
    if configured_path:
        return Path(configured_path)
    return DEFAULT_CONTROL_EXCEL_PATH


def read_source_pairs_from_excel(source_name, source_root, excel_path=None):
    excel_path = resolve_excel_path(excel_path)
    if not excel_path.exists():
        return None, f"Control Excel file not found: {excel_path}"

    excel_file = pd.ExcelFile(excel_path)
    available_sheets = {sheet_name.strip().upper(): sheet_name for sheet_name in excel_file.sheet_names}
    sheet_name = available_sheets.get(source_name.upper())
    if not sheet_name:
        return None, (
            f"Sheet '{source_name}' not found in {excel_path}. "
            f"Available sheets: {excel_file.sheet_names}"
        )

    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    df.columns = [str(column).strip() for column in df.columns]
    lower_to_actual = {column.lower(): column for column in df.columns}
    df = df.rename(columns={actual: lower for lower, actual in lower_to_actual.items()})

    if "flag" not in df.columns:
        return None, f"Sheet '{sheet_name}' must have a flag column"

    enabled_df = df[df["flag"].apply(is_enabled)].copy()
    pairs = []
    source_root = Path(source_root)
    discovered_pairs = {
        pair["table_name"].upper(): pair
        for pair in discover_sql_pairs(source_name, source_root)
    }

    for row_number, (_, row) in enumerate(enabled_df.iterrows(), start=1):
        has_config_value = any(
            clean_value(row.get(column_name))
            for column_name in ["table_name", "tablename", "table", "bronze", "srilver", "silver"]
            if column_name in row.index
        )
        if not has_config_value:
            continue

        table_name = (
            first_existing_column(row, ["table_name", "tablename", "table"])
            or f"{source_name}_{row_number}"
        )

        bronze_value = first_existing_column(
            row,
            ["bronze_query_file", "bronze_file", "bronze_path", "bronze_sql_file", "bronze_query", "bronze"],
        )
        silver_value = first_existing_column(
            row,
            ["silver_query_file", "silver_file", "silver_path", "silver_sql_file", "silver_query", "silver", "srilver"],
        )

        pair = {
            "S.No": row.get("s.no") or row.get("sno") or row_number,
            "table_name": str(table_name).strip().upper(),
            "bronze_path": None,
            "silver_path": None,
            "bronze_query": None,
            "silver_query": None,
        }

        bronze_path = resolve_sql_file_path(bronze_value, excel_path.parent)
        silver_path = resolve_sql_file_path(silver_value, excel_path.parent)

        if bronze_path:
            pair["bronze_path"] = bronze_path
        elif clean_value(bronze_value):
            pair["bronze_query"] = clean_sql(bronze_value)

        if silver_path:
            pair["silver_path"] = silver_path
        elif clean_value(silver_value):
            pair["silver_query"] = clean_sql(silver_value)

        discovered_pair = discovered_pairs.get(pair["table_name"])
        if discovered_pair:
            if not pair["bronze_path"] and not pair["bronze_query"]:
                pair["bronze_path"] = discovered_pair.get("bronze_path")
            if not pair["silver_path"] and not pair["silver_query"]:
                pair["silver_path"] = discovered_pair.get("silver_path")

        pairs.append(pair)

    return pairs, None


def quote_identifier(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


def is_temporal_column(column_name):
    column_name = str(column_name).lower()
    return any(token in column_name for token in ["date", "time", "_on", "timestamp"])


def build_normalized_expression(column_name):
    quoted_column = quote_identifier(column_name)
    if is_temporal_column(column_name):
        return (
            "COALESCE("
            f"DATE_FORMAT(TRY_CAST({quoted_column} AS TIMESTAMP), '%Y-%m-%d %H:%i:%s'), "
            f"LOWER(CAST({quoted_column} AS VARCHAR))"
            ")"
        )
    return f"LOWER(CAST({quoted_column} AS VARCHAR))"


def build_normalized_select(source_query, source_columns, output_columns):
    select_parts = [
        f"{build_normalized_expression(source_column)} AS {quote_identifier(output_columns[index])}"
        for index, source_column in enumerate(source_columns)
    ]
    return f"""
        SELECT
            {", ".join(select_parts)}
        FROM ({source_query}) src
    """


def add_result(results, table_name, validation, status, **details):
    row = {
        "table_name": table_name,
        "validation": validation,
        "status": status,
        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    row.update(details)
    results.append(row)


def add_detail(detail_sheets, sheet_name, df):
    if df is not None and not df.empty:
        detail_sheets[sheet_name] = df


def run_count_validation(bronze_conn, silver_conn, table_name, bronze_query, silver_query, results):
    bronze_count = execute_query_scalar(bronze_conn, f"SELECT COUNT(*) AS cnt FROM ({bronze_query}) src", "cnt")
    silver_count = execute_query_scalar(silver_conn, f"SELECT COUNT(*) AS cnt FROM ({silver_query}) src", "cnt")
    add_result(
        results,
        table_name,
        "counts",
        "PASS" if bronze_count == silver_count else "FAIL",
        bronze=bronze_count,
        silver=silver_count,
        difference=bronze_count - silver_count,
    )


def run_column_validation(conn, table_name, bronze_query, silver_query, results, detail_sheets):
    bronze_columns = get_query_columns(conn, bronze_query)
    silver_columns = get_query_columns(conn, silver_query)
    bronze_norm = [column.lower() for column in bronze_columns]
    silver_norm = [column.lower() for column in silver_columns]

    max_len = max(len(bronze_columns), len(silver_columns))
    detail_rows = []
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
    add_detail(detail_sheets, "Column_Comparison", detail_df)
    add_result(
        results,
        table_name,
        "column_names",
        "PASS" if bronze_norm == silver_norm else "FAIL",
        bronze_column_count=len(bronze_columns),
        silver_column_count=len(silver_columns),
        missing_in_silver=", ".join(missing_in_silver),
        missing_in_bronze=", ".join(missing_in_bronze),
    )
    return bronze_columns, silver_columns


def fetch_except_count(conn, left_select, right_select):
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM (
            {left_select}
            EXCEPT
            {right_select}
        ) diff
    """
    return execute_query_scalar(conn, query, "cnt")


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


def run_data_validation(
    bronze_conn,
    silver_conn,
    table_name,
    bronze_query,
    silver_query,
    bronze_columns,
    silver_columns,
    results,
    detail_sheets,
    detail_limit=DEFAULT_DETAIL_LIMIT,
):
    bronze_lookup = {column.lower(): column for column in bronze_columns}
    silver_lookup = {column.lower(): column for column in silver_columns}
    common_column_keys = [column.lower() for column in bronze_columns if column.lower() in silver_lookup]

    if not common_column_keys:
        add_result(
            results,
            table_name,
            "data_comparison",
            "ERROR",
            message="No common columns found for row/column data comparison",
        )
        return

    bronze_compare_columns = [bronze_lookup[key] for key in common_column_keys]
    silver_compare_columns = [silver_lookup[key] for key in common_column_keys]
    output_columns = bronze_compare_columns

    bronze_select = build_normalized_select(bronze_query, bronze_compare_columns, output_columns)
    silver_select = build_normalized_select(silver_query, silver_compare_columns, output_columns)

    bronze_not_in_silver = fetch_except_count(bronze_conn, bronze_select, silver_select)
    silver_not_in_bronze = fetch_except_count(silver_conn, silver_select, bronze_select)

    if bronze_not_in_silver:
        detail_df = fetch_except_detail(bronze_conn, bronze_select, silver_select, detail_limit)
        detail_df.insert(0, "difference_type", "missing_in_silver")
        add_detail(detail_sheets, "Bronze_Not_In_Silver", detail_df)

    if silver_not_in_bronze:
        detail_df = fetch_except_detail(silver_conn, silver_select, bronze_select, detail_limit)
        detail_df.insert(0, "difference_type", "missing_in_bronze")
        add_detail(detail_sheets, "Silver_Not_In_Bronze", detail_df)

    difference_frames = [
        detail_sheets.get("Bronze_Not_In_Silver"),
        detail_sheets.get("Silver_Not_In_Bronze"),
    ]
    difference_frames = [
        frame
        for frame in difference_frames
        if frame is not None and not frame.empty
    ]
    if difference_frames:
        add_detail(
            detail_sheets,
            "Difference_Records",
            pd.concat(difference_frames, ignore_index=True, sort=False),
        )

    add_result(
        results,
        table_name,
        "data_comparison",
        "PASS" if bronze_not_in_silver == 0 and silver_not_in_bronze == 0 else "FAIL",
        bronze_not_in_silver=bronze_not_in_silver,
        silver_not_in_bronze=silver_not_in_bronze,
        compared_columns=", ".join(output_columns),
        detail_limit=detail_limit,
    )


def run_pair(bronze_conn, silver_conn, pair, detail_limit=DEFAULT_DETAIL_LIMIT):
    table_name = pair["table_name"]
    results = []
    detail_sheets = {}

    if not pair.get("bronze_query") and pair.get("bronze_path"):
        bronze_path = Path(pair["bronze_path"])
        if bronze_path.exists() and bronze_path.is_file():
            pair["bronze_query"] = read_sql_file(bronze_path)

    if not pair.get("silver_query") and pair.get("silver_path"):
        silver_path = Path(pair["silver_path"])
        if silver_path.exists() and silver_path.is_file():
            pair["silver_query"] = read_sql_file(silver_path)

    if not pair.get("bronze_query") or not pair.get("silver_query"):
        add_result(
            results,
            table_name,
            "configuration",
            "ERROR",
            bronze_path=str(pair.get("bronze_path") or ""),
            silver_path=str(pair.get("silver_path") or ""),
            message="Matching bronze or silver SQL query/file is missing",
        )
        return pd.DataFrame(results), detail_sheets

    bronze_query = pair["bronze_query"]
    silver_query = pair["silver_query"]

    try:
        run_count_validation(bronze_conn, silver_conn, table_name, bronze_query, silver_query, results)
        bronze_columns, silver_columns = run_column_validation(
            bronze_conn, table_name, bronze_query, silver_query, results, detail_sheets
        )
        run_data_validation(
            bronze_conn,
            silver_conn,
            table_name,
            bronze_query,
            silver_query,
            bronze_columns,
            silver_columns,
            results,
            detail_sheets,
            detail_limit,
        )
    except Exception as exc:
        logger.exception("Source comparison failed for %s", table_name)
        add_result(results, table_name, "execution", "ERROR", message=str(exc))

    return pd.DataFrame(results), detail_sheets


def overall_status(result_df):
    if result_df.empty:
        return "NO_VALIDATIONS"
    if (result_df["status"] == "PASS").all():
        return "PASS"
    if (result_df["status"] == "ERROR").any():
        return "ERROR"
    return "FAIL"


def run_source(source_name, output_folder_name=None, query_root=None, detail_limit=DEFAULT_DETAIL_LIMIT, excel_path=None):
    source_name = source_name.upper()
    query_root = Path(query_root or DEFAULT_QUERY_ROOT)
    source_root = query_root / source_name
    output_folder_name = output_folder_name or f"output_indiv_{source_name.lower()}"
    output_dir = PROJECT_ROOT / output_folder_name

    previous_output_dir = os.environ.get("VALIDATION_OUTPUT_DIR")
    previous_excel_path = os.environ.get("VALIDATION_EXCEL_PATH")
    os.environ["VALIDATION_OUTPUT_DIR"] = str(output_dir)
    control_excel_path = resolve_excel_path(excel_path)
    os.environ["VALIDATION_EXCEL_PATH"] = str(control_excel_path)

    try:
        pairs, excel_error = read_source_pairs_from_excel(source_name, source_root, control_excel_path)
        if pairs is None:
            logger.warning("%s. Falling back to folder discovery.", excel_error)
            pairs = discover_sql_pairs(source_name, source_root)
        if not pairs:
            result_df = pd.DataFrame(
                [
                    {
                        "table_name": source_name,
                        "validation": "configuration",
                        "status": "ERROR",
                        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "message": f"No SQL files found under {source_root}",
                    }
                ]
            )
            generate_excel_report(result_df, f"{source_name}_SOURCE_SUMMARY", "ERROR")
            return output_dir

        bronze_conn, silver_conn = get_connections()

        for pair in pairs:
            print(f"{pair['S.No']}. {pair['table_name']}")
            result_df, detail_sheets = run_pair(bronze_conn, silver_conn, pair, detail_limit)
            status = overall_status(result_df)
            generate_excel_report(result_df, pair["table_name"], status, detail_sheets, pair["S.No"])

        return output_dir
    finally:
        if previous_output_dir is None:
            os.environ.pop("VALIDATION_OUTPUT_DIR", None)
        else:
            os.environ["VALIDATION_OUTPUT_DIR"] = previous_output_dir
        if previous_excel_path is None:
            os.environ.pop("VALIDATION_EXCEL_PATH", None)
        else:
            os.environ["VALIDATION_EXCEL_PATH"] = previous_excel_path
