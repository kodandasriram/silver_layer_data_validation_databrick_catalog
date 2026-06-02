import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from config.runtime import is_databricks_mode
from validations.delta_report_writer import write_validation_results


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABRICKS_LOCAL_OUTPUT_DIR = Path(
    "/Volumes/dev_iceberg/tmkn-aws-dwh-dev-iceberg/dev_volume"
)
LOCAL_EXCEL_WRITE_DIR = Path(tempfile.gettempdir()) / "silver_layer_validation_output"


def get_output_dir():
    configured_output_dir = os.getenv("VALIDATION_OUTPUT_DIR")
    if configured_output_dir:
        return configured_output_dir
    if is_databricks_mode():
        return DEFAULT_DATABRICKS_LOCAL_OUTPUT_DIR
    return PROJECT_ROOT / "output"


def _safe_file_part(value):
    value = str(value).strip()
    for char in '<>:"/\\|?*':
        value = value.replace(char, "_")
    return value or "validation_report"


def generate_excel_report(df, table_name, status, detail_sheets=None, table_sequence=None, environment=None):
    output_dir = get_output_dir()
    local_output_dir, final_output_dir = resolve_output_dirs(output_dir)
    local_output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_environment = _safe_file_part(environment).lower() if environment else ""
    safe_table_name = _safe_file_part(table_name)
    safe_status = _safe_file_part(status)

    sequence_prefix = _sequence_prefix(table_sequence)
    environment_prefix = f"{safe_environment}_" if safe_environment else ""
    file_name = f"{sequence_prefix}{environment_prefix}{safe_table_name}_{timestamp}_{safe_status}.xlsx"
    full_path = local_output_dir / file_name

    detail_sheets = detail_sheets or {}
    with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
        summary_df = df.copy()
        if environment and "environment" not in summary_df.columns:
            summary_df.insert(0, "environment", str(environment).strip().lower())
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        for sheet_name, detail_df in detail_sheets.items():
            if detail_df is None or detail_df.empty:
                continue
            safe_sheet_name = _safe_sheet_name(sheet_name)
            detail_df = _prepare_detail_sheet(sheet_name, detail_df)
            detail_df.to_excel(writer, sheet_name=safe_sheet_name, index=False)

    final_path = publish_report(full_path, final_output_dir, file_name)
    print(f"Report generated: {final_path}")
    print_download_hint(final_path)
    write_validation_results(
        df,
        detail_sheets,
        table_name,
        status,
        final_path,
        table_sequence,
        environment,
    )
    return final_path


def resolve_output_dirs(output_dir):
    output_dir_text = str(output_dir)
    if output_dir_text.startswith("dbfs:/") or output_dir_text.startswith("/dbfs/"):
        return LOCAL_EXCEL_WRITE_DIR, to_dbfs_uri(output_dir_text)
    if output_dir_text.startswith("/Volumes/"):
        return LOCAL_EXCEL_WRITE_DIR, Path(output_dir_text)
    return Path(output_dir_text), None


def to_dbfs_uri(path_text):
    if path_text.startswith("dbfs:/"):
        return path_text.rstrip("/")
    if path_text.startswith("/dbfs/"):
        return "dbfs:/" + path_text[len("/dbfs/"):].strip("/")
    return path_text.rstrip("/")


def publish_report(local_path, final_output_dir, file_name):
    if not final_output_dir:
        return local_path

    if isinstance(final_output_dir, Path):
        final_path = final_output_dir / file_name
        try:
            final_output_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, final_path)
            return final_path
        except Exception as exc:
            print(f"WARNING: Could not copy report to {final_path}: {exc}")
            print(f"Report remains on local driver path: {local_path}")
            return local_path

    final_uri = f"{final_output_dir}/{file_name}"
    try:
        dbutils = get_dbutils()
        dbutils.fs.mkdirs(final_output_dir)
        dbutils.fs.cp(f"file:{local_path}", final_uri, True)
        return final_uri
    except Exception as exc:
        print(f"WARNING: Could not copy report to {final_uri}: {exc}")
        print(f"Report remains on local driver path: {local_path}")
        return local_path


def get_dbutils():
    try:
        return dbutils
    except NameError:
        pass

    try:
        from IPython import get_ipython

        shell = get_ipython()
        if shell and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:
        pass

    raise RuntimeError("dbutils is not available in this execution context")


def print_download_hint(full_path):
    full_path = str(full_path)
    if full_path.startswith("dbfs:/FileStore/"):
        file_store_path = full_path.replace("dbfs:/FileStore/", "", 1).replace("\\", "/")
        print(f"Download from Databricks Files: /files/{file_store_path}")
    elif full_path.startswith("/dbfs/FileStore/"):
        file_store_path = full_path.replace("/dbfs/FileStore/", "", 1).replace("\\", "/")
        print(f"Download from Databricks Files: /files/{file_store_path}")


def _safe_sheet_name(value):
    value = str(value).strip()
    for char in '[]:*?/\\':
        value = value.replace(char, "_")
    return (value or "Details")[:31]


def _sequence_prefix(value):
    if value is None or pd.isna(value):
        return ""
    try:
        number = float(value)
        if number.is_integer():
            return f"{int(number)}."
    except (TypeError, ValueError):
        pass
    return f"{_safe_file_part(value)}."


def _prepare_detail_sheet(sheet_name, detail_df):
    detail_df = detail_df.copy()
    if sheet_name == "All_Differences" and {"record_number", "source_order"}.issubset(detail_df.columns):
        detail_df = detail_df.sort_values(["record_number", "source_order"]).reset_index(drop=True)
        detail_df = detail_df.drop(columns=["source_order"])
    if sheet_name == "Column_Differences_on_pk" and "column_name" in detail_df.columns:
        detail_df["column_name"] = detail_df["column_name"].astype(str).str.upper()
    if sheet_name in {"All_Differences", "Column_Differences_on_pk"}:
        if sheet_name == "All_Differences" and "COLUMN" in detail_df.columns:
            detail_df["COLUMN"] = detail_df["COLUMN"].astype(str).str.upper()
        detail_df = detail_df.drop_duplicates().reset_index(drop=True)
    if sheet_name == "Difference_Records":
        detail_df = detail_df.drop_duplicates().reset_index(drop=True)
    if sheet_name == "All_Differences" and {"ID", "COLUMN", "Source"}.issubset(detail_df.columns):
        source_order = {"Bronze": 1, "Silver": 2}
        detail_df["_source_order"] = detail_df["Source"].map(source_order).fillna(9)
        detail_df = detail_df.sort_values(["ID", "COLUMN", "_source_order"]).drop(columns=["_source_order"]).reset_index(drop=True)
    if sheet_name == "Difference_Records" and {"Source", "Status", "record_id"}.issubset(detail_df.columns):
        source_order = {"Bronze": 1, "Silver": 2}
        detail_df["_source_order"] = detail_df["Source"].map(source_order).fillna(9)
        detail_df = detail_df.sort_values(["_source_order", "record_id"]).drop(columns=["_source_order"]).reset_index(drop=True)
    if sheet_name == "Column_Value_Differences" and {"column_name", "count_difference"}.issubset(detail_df.columns):
        detail_df["_abs_difference"] = detail_df["count_difference"].abs()
        detail_df = detail_df.sort_values(
            ["_abs_difference", "column_name"],
            ascending=[False, True],
        ).drop(columns=["_abs_difference"]).reset_index(drop=True)
    if sheet_name in {"Business_Key_Differences", "Column_Differences"}:
        front_columns = [
            column
            for column in [
                "table_name",
                "business_keys",
                "business_key_value",
                "column_name",
                "bronze_value",
                "silver_value",
                "difference_type",
            ]
            if column in detail_df.columns
        ]
        remaining_columns = [column for column in detail_df.columns if column not in front_columns]
        detail_df = detail_df[front_columns + remaining_columns].drop_duplicates().reset_index(drop=True)
    return detail_df
