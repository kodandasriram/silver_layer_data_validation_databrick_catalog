import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "queries" / "validation_queries.xlsx"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "dev_uat_reccount_output"
DEFAULT_TARGET_LAYER = "silver"
ENVIRONMENTS = ("dev", "uat")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def clean_value(value, default=None):
    if pd.isna(value):
        return default
    value = str(value).strip()
    return value if value else default


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


def read_sheet(excel_path, expected_names):
    excel_file = pd.ExcelFile(excel_path)
    available = {sheet_name.upper(): sheet_name for sheet_name in excel_file.sheet_names}

    for expected_name in expected_names:
        actual_name = available.get(expected_name.upper())
        if actual_name:
            return pd.read_excel(excel_path, sheet_name=actual_name)

    raise ValueError(
        f"None of the expected sheets {expected_names} were found in {excel_path}. "
        f"Available sheets: {excel_file.sheet_names}"
    )


def normalize_columns(df):
    return df.rename(columns={column: str(column).strip().lower() for column in df.columns})


def split_namespace(namespace):
    namespace = clean_value(namespace)
    if not namespace or "." not in namespace:
        raise ValueError(f"Invalid namespace value: {namespace}")

    catalog, schema = namespace.split(".", 1)
    return catalog.strip().strip('"'), schema.strip().strip('"')


def read_environment_namespaces(excel_path, target_layer):
    environment_df = normalize_columns(read_sheet(excel_path, ["environment"]))
    required_columns = {"environment", target_layer}
    missing = required_columns - set(environment_df.columns)
    if missing:
        raise ValueError(f"environment sheet is missing required columns: {sorted(missing)}")

    namespaces = {}
    for _, row in environment_df.iterrows():
        environment = clean_value(row.get("environment"))
        namespace = clean_value(row.get(target_layer))
        if environment and namespace:
            namespaces[environment.lower()] = split_namespace(namespace)

    missing_environments = [environment for environment in ENVIRONMENTS if environment not in namespaces]
    if missing_environments:
        raise ValueError(f"environment sheet is missing rows for: {missing_environments}")

    return namespaces


def read_table_names(excel_path, only_flagged=False):
    table_df = normalize_columns(read_sheet(excel_path, ["TABLE_QUERIES"]))
    if "table_name" not in table_df.columns:
        raise ValueError("TABLE_QUERIES sheet is missing required column: table_name")

    if only_flagged and "flag" in table_df.columns:
        table_df = table_df[table_df["flag"].apply(is_enabled)]

    table_names = []
    seen = set()
    for table_name in table_df["table_name"]:
        cleaned = clean_value(table_name)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key not in seen:
            table_names.append(cleaned)
            seen.add(key)

    if not table_names:
        raise ValueError("No table names found in TABLE_QUERIES.table_name")
    return table_names


def read_db_rows(excel_path, target_layer):
    db_df = normalize_columns(read_sheet(excel_path, ["DB_CONFIG", "DB_CONNECTION", "DB CONNECTION", "DB", "db"]))
    required_columns = {"db_name", "host", "port", "user", "catalog", "schema"}
    missing = required_columns - set(db_df.columns)
    if missing:
        raise ValueError(f"db sheet is missing required columns: {sorted(missing)}")

    namespaces = read_environment_namespaces(excel_path, target_layer)
    db_rows = {}
    db_df = db_df.copy()
    db_df["db_name_norm"] = db_df["db_name"].astype(str).str.strip().str.lower()
    layer_db_name = f"{target_layer.lower()}_db"

    for environment, (catalog, schema) in namespaces.items():
        rows = db_df[
            (db_df["catalog"].astype(str).str.strip().str.lower() == catalog.lower())
            & (db_df["schema"].astype(str).str.strip().str.strip('"').str.lower() == schema.lower())
        ]
        preferred_rows = rows[rows["db_name_norm"] == layer_db_name]
        selected_rows = preferred_rows if not preferred_rows.empty else rows
        if selected_rows.empty:
            raise ValueError(
                f"No db row found for {environment} {target_layer}: catalog={catalog}, schema={schema}"
            )
        db_rows[environment] = selected_rows.iloc[0]

    return db_rows


def truthy(value):
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() in {"1", "TRUE", "Y", "YES"}


def create_connection(row):
    try:
        import trino
        from trino.auth import BasicAuthentication
    except ImportError as exc:
        raise RuntimeError(
            "The 'trino' package is required to connect to DEV/UAT. "
            "Install project requirements with: pip install -r requirements.txt"
        ) from exc

    ssl_enabled = truthy(row.get("ssl"))
    ssl_verification = str(row.get("ssl_verification", "")).strip().upper()
    auth_type = str(row.get("auth_type", "")).strip().upper() or "AUTO"
    password = clean_value(row.get("password")) or os.getenv(f"{str(row['db_name']).upper()}_PASSWORD")
    auth_types = ["BASIC", "NONE"] if auth_type == "AUTO" else [auth_type]
    errors = []

    for candidate_auth_type in auth_types:
        connect_kwargs = {
            "host": row["host"],
            "port": int(row["port"]),
            "user": row["user"],
            "catalog": row["catalog"],
            "schema": row["schema"],
            "http_scheme": "https" if ssl_enabled else "http",
            "verify": ssl_verification not in {"NONE", "FALSE", "NO", "0"},
        }

        if candidate_auth_type == "BASIC":
            if not password:
                errors.append("BASIC: password missing")
                continue
            connect_kwargs["auth"] = BasicAuthentication(row["user"], password)
        elif candidate_auth_type != "NONE":
            errors.append(f"{candidate_auth_type}: unsupported auth_type")
            continue

        if connect_kwargs["http_scheme"] == "https" and connect_kwargs["verify"] is False:
            try:
                import urllib3
                from urllib3.exceptions import InsecureRequestWarning

                urllib3.disable_warnings(InsecureRequestWarning)
            except ImportError:
                pass

        conn = trino.dbapi.connect(**connect_kwargs)
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchall()
            finally:
                cursor.close()
            return conn
        except Exception as exc:
            errors.append(f"{candidate_auth_type}: {exc}")
            try:
                conn.close()
            except Exception:
                pass

    raise RuntimeError(f"Unable to connect to {row['db_name']}. Errors: {' | '.join(errors)}")


def quote_identifier(identifier):
    identifier = str(identifier).strip()
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table_name(row, table_name):
    return ".".join(
        [
            quote_identifier(row["catalog"]),
            quote_identifier(row["schema"]),
            quote_identifier(table_name),
        ]
    )


def is_missing_table_error(exc):
    error_name = getattr(exc, "error_name", "") or ""
    message = str(exc)
    return (
        "TABLE_NOT_FOUND" in error_name.upper()
        or "does not exist" in message.lower()
        or "not found" in message.lower()
    )


def get_table_metrics(conn, db_row, table_name):
    table_ref = qualified_table_name(db_row, table_name)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) AS record_count FROM {table_ref}")
        rows = cursor.fetchall()
        record_count = int(rows[0][0]) if rows else 0

        cursor.execute(f"SELECT * FROM {table_ref} WHERE 1 = 0")
        column_count = len(cursor.description) if cursor.description else 0

        return {
            "record_count": record_count,
            "column_count": column_count,
            "status": "SUCCESS",
            "error": None,
        }
    except Exception as exc:
        message = str(exc)
        return {
            "record_count": None,
            "column_count": None,
            "status": "TABLE IS MISSING" if is_missing_table_error(exc) else "ERROR",
            "error": message,
        }
    finally:
        cursor.close()


def comparison_status(dev_value, dev_status, uat_value, uat_status, matched_status, not_matched_status):
    if dev_status != "SUCCESS" or uat_status != "SUCCESS":
        return "NOT COMPARED"
    return matched_status if dev_value == uat_value else not_matched_status


def run_counts(excel_path, output_dir, target_layer, only_flagged):
    table_names = read_table_names(excel_path, only_flagged=only_flagged)
    db_rows = read_db_rows(excel_path, target_layer)
    connections = {}
    results = []

    try:
        for environment in ENVIRONMENTS:
            logger.info("Connecting to %s %s", environment, target_layer)
            connections[environment] = create_connection(db_rows[environment])

        for index, table_name in enumerate(table_names, start=1):
            logger.info("Counting %s (%s/%s)", table_name, index, len(table_names))
            dev_metrics = get_table_metrics(connections["dev"], db_rows["dev"], table_name)
            uat_metrics = get_table_metrics(connections["uat"], db_rows["uat"], table_name)
            record_difference = (
                dev_metrics["record_count"] - uat_metrics["record_count"]
                if dev_metrics["status"] == "SUCCESS" and uat_metrics["status"] == "SUCCESS"
                else None
            )
            column_difference = (
                dev_metrics["column_count"] - uat_metrics["column_count"]
                if dev_metrics["status"] == "SUCCESS" and uat_metrics["status"] == "SUCCESS"
                else None
            )

            results.append(
                {
                    "table_name": table_name,
                    "layer_type": target_layer.upper(),
                    "dev_record_count": dev_metrics["record_count"],
                    "dev_column_count": dev_metrics["column_count"],
                    "dev_status": dev_metrics["status"],
                    "uat_record_count": uat_metrics["record_count"],
                    "uat_column_count": uat_metrics["column_count"],
                    "uat_status": uat_metrics["status"],
                    "count_difference": record_difference,
                    "column_count_difference": column_difference,
                    "comparison_status": comparison_status(
                        dev_metrics["record_count"],
                        dev_metrics["status"],
                        uat_metrics["record_count"],
                        uat_metrics["status"],
                        "COUNT MATCHED",
                        "COUNT NOT MATCHED",
                    ),
                    "column_comparison_status": comparison_status(
                        dev_metrics["column_count"],
                        dev_metrics["status"],
                        uat_metrics["column_count"],
                        uat_metrics["status"],
                        "COLUMN COUNT MATCHED",
                        "COLUMN COUNT NOT MATCHED",
                    ),
                    "dev_error": dev_metrics["error"],
                    "uat_error": uat_metrics["error"],
                }
            )
    finally:
        for conn in connections.values():
            try:
                conn.close()
            except Exception:
                pass

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"dev_uat_table_counts_{timestamp}.xlsx"
    pd.DataFrame(results).to_excel(output_path, sheet_name="dev_uat_counts", index=False)
    return output_path


def parse_args():
    parser = argparse.ArgumentParser(description="Count configured tables in DEV and UAT.")
    parser.add_argument("--excel-path", default=str(DEFAULT_EXCEL_PATH), help="Path to validation_queries.xlsx")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Folder where the Excel output is saved")
    parser.add_argument(
        "--target-layer",
        default=DEFAULT_TARGET_LAYER,
        choices=["bronze", "silver"],
        help="Environment layer/schema to count tables from",
    )
    parser.add_argument(
        "--only-flagged",
        action="store_true",
        help="Count only rows in TABLE_QUERIES where flag is enabled",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_path = run_counts(
        excel_path=Path(args.excel_path),
        output_dir=Path(args.output_dir),
        target_layer=args.target_layer,
        only_flagged=args.only_flagged,
    )
    print(f"Output written: {output_path}")


if __name__ == "__main__":
    main()
