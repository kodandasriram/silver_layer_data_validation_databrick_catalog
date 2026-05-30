import json
import os
import uuid
from datetime import datetime

import pandas as pd

from config.runtime import is_databricks_mode


RUN_ID = os.getenv("VALIDATION_RUN_ID") or datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
DEFAULT_RESULTS_NAMESPACE = "`dev_iceberg`.`tmkn-aws-dwh-dev-iceberg`"
DEFAULT_SUMMARY_TABLE = f"{DEFAULT_RESULTS_NAMESPACE}.validation_run_summary"
DEFAULT_DETAIL_TABLE = f"{DEFAULT_RESULTS_NAMESPACE}.validation_failed_details"


def should_write_delta_results():
    configured = os.getenv("VALIDATION_WRITE_DELTA", "").strip().lower()
    if configured:
        return configured in {"1", "true", "yes", "y"}
    return False


def write_validation_results(
    result_df,
    detail_sheets,
    table_name,
    overall_status,
    report_path,
    table_sequence=None,
    environment=None,
):
    if not should_write_delta_results():
        return

    try:
        spark = get_spark_session()
        summary_rows = build_summary_rows(
            result_df,
            table_name,
            overall_status,
            report_path,
            table_sequence,
            environment,
        )
        detail_rows = build_detail_rows(detail_sheets or {}, table_name, overall_status, report_path, environment)

        append_rows_to_delta(spark, summary_rows, get_summary_table())
        append_rows_to_delta(spark, detail_rows, get_detail_table())
    except Exception as exc:
        print(f"WARNING: Delta validation result write skipped: {exc}")


def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def get_summary_table():
    return os.getenv("VALIDATION_SUMMARY_TABLE", DEFAULT_SUMMARY_TABLE)


def get_detail_table():
    return os.getenv("VALIDATION_DETAIL_TABLE", DEFAULT_DETAIL_TABLE)


def build_summary_rows(result_df, table_name, overall_status, report_path, table_sequence=None, environment=None):
    if result_df is None or result_df.empty:
        result_df = pd.DataFrame(
            [
                {
                    "table_name": table_name,
                    "validation": "overall",
                    "status": overall_status,
                }
            ]
        )

    rows = []
    for _, row in result_df.iterrows():
        row_dict = clean_record(row.to_dict())
        rows.append(
            {
                "run_id": RUN_ID,
                "run_timestamp": row_dict.get("run_timestamp") or current_timestamp(),
                "environment": clean_value(environment),
                "table_sequence": clean_value(table_sequence),
                "table_name": clean_value(row_dict.get("table_name") or table_name),
                "validation": clean_value(row_dict.get("validation")),
                "status": clean_value(row_dict.get("status")),
                "overall_status": clean_value(overall_status),
                "bronze_count": clean_value(first_present(row_dict, ["bronze", "bronze_count", "bronze_layer_count"])),
                "silver_count": clean_value(first_present(row_dict, ["silver", "silver_count", "silver_layer_count"])),
                "difference_count": clean_value(first_present(row_dict, ["difference", "count", "count_difference"])),
                "message": clean_value(row_dict.get("message")),
                "report_path": clean_value(report_path),
                "details_json": to_json(row_dict),
            }
        )
    return rows


def build_detail_rows(detail_sheets, table_name, overall_status, report_path, environment=None):
    rows = []
    for sheet_name, detail_df in detail_sheets.items():
        if detail_df is None or detail_df.empty:
            continue

        prepared_df = detail_df.copy().reset_index(drop=True)
        for index, data_row in prepared_df.iterrows():
            row_dict = clean_record(data_row.to_dict())
            rows.append(
                {
                    "run_id": RUN_ID,
                    "run_timestamp": current_timestamp(),
                    "environment": clean_value(environment),
                    "table_name": clean_value(row_dict.get("table_name") or table_name),
                    "overall_status": clean_value(overall_status),
                    "validation_sheet": clean_value(sheet_name),
                    "row_number": clean_value(index + 1),
                    "source": clean_value(first_present(row_dict, ["Source", "source", "source_name"])),
                    "status": clean_value(first_present(row_dict, ["Status", "status", "difference_type"])),
                    "business_keys": clean_value(first_present(row_dict, ["business_keys", "pk"])),
                    "business_key_value": clean_value(first_present(row_dict, ["business_key_value", "ID", "id", "record_id"])),
                    "column_name": clean_value(first_present(row_dict, ["column_name", "COLUMN", "column"])),
                    "bronze_value": clean_value(first_present(row_dict, ["bronze_value", "Bronze_Value", "bronze"])),
                    "silver_value": clean_value(first_present(row_dict, ["silver_value", "Silver_Value", "silver"])),
                    "difference_type": clean_value(first_present(row_dict, ["difference_type", "Status", "status"])),
                    "report_path": clean_value(report_path),
                    "details_json": to_json(row_dict),
                }
            )
    return rows


def append_rows_to_delta(spark, rows, table_name):
    if not rows:
        return

    df = spark.createDataFrame(pd.DataFrame(rows).astype(str))
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Delta validation results written: {table_name} ({len(rows)} rows)")


def first_present(record, keys):
    for key in keys:
        if key in record and record.get(key) is not None:
            return record.get(key)
    return None


def clean_record(record):
    cleaned = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def clean_value(value):
    if value is None:
        return None
    if pd.isna(value):
        return None
    return str(value)


def to_json(record):
    return json.dumps(record, default=str, ensure_ascii=False)


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
