import os
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_output_dir():
    return Path(os.getenv("VALIDATION_OUTPUT_DIR", PROJECT_ROOT / "output"))


def _safe_file_part(value):
    value = str(value).strip()
    for char in '<>:"/\\|?*':
        value = value.replace(char, "_")
    return value or "validation_report"


def generate_excel_report(df, table_name, status, detail_sheets=None, table_sequence=None, environment=None):
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_environment = _safe_file_part(environment).lower() if environment else ""
    safe_table_name = _safe_file_part(table_name)
    safe_status = _safe_file_part(status)

    sequence_prefix = _sequence_prefix(table_sequence)
    environment_prefix = f"{safe_environment}_" if safe_environment else ""
    file_name = f"{sequence_prefix}{environment_prefix}{safe_table_name}_{timestamp}_{safe_status}.xlsx"
    full_path = output_dir / file_name

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

    print(f"Report generated: {full_path}")
    return full_path


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
