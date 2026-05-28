import os
import logging
from pathlib import Path

import pandas as pd
import trino
from trino.auth import BasicAuthentication
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from config.databricks_connection import create_databricks_connection
from utils.excel_reader import read_environment_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "queries" / "validation_queries.xlsx"
EXCEL_PATH = Path(os.getenv("VALIDATION_EXCEL_PATH", DEFAULT_EXCEL_PATH))
logger = logging.getLogger(__name__)


def _read_first_available_sheet(sheet_names):
    excel_file = pd.ExcelFile(EXCEL_PATH)
    available = {name.upper(): name for name in excel_file.sheet_names}

    for sheet_name in sheet_names:
        actual_name = available.get(sheet_name.upper())
        if actual_name:
            return pd.read_excel(EXCEL_PATH, sheet_name=actual_name)

    raise ValueError(
        f"None of the expected DB config sheets {sheet_names} were found in {EXCEL_PATH}. "
        f"Available sheets: {excel_file.sheet_names}"
    )


def get_db_config():
    df = _read_first_available_sheet(["DB_CONFIG", "DB_CONNECTION", "DB CONNECTION", "DB", "db"])
    required_columns = {"db_name", "host", "port", "user", "catalog", "schema"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"DB config is missing required columns: {sorted(missing)}")
    return df


def _truthy(value):
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() in {"1", "TRUE", "Y", "YES"}


def _clean(value, default=None):
    if pd.isna(value):
        return default
    value = str(value).strip()
    return value if value else default


def _split_namespace(namespace):
    namespace = _clean(namespace)
    if not namespace or "." not in namespace:
        raise ValueError(f"Invalid environment namespace: {namespace}")

    catalog, schema = namespace.split(".", 1)
    return catalog.strip().strip('"'), schema.strip().strip('"')


def _matching_db_rows(df, db_name, catalog, schema):
    return df[
        (df["db_name_norm"] == db_name)
        & (df["catalog"].astype(str).str.strip().str.lower() == catalog.lower())
        & (df["schema"].astype(str).str.strip().str.strip('"').str.lower() == schema.lower())
    ]


def select_environment_db_rows(df):
    environment_config = read_environment_config()
    bronze_catalog, bronze_schema = _split_namespace(environment_config["bronze"])
    silver_catalog, silver_schema = _split_namespace(environment_config["silver"])

    df = df.copy()
    df["db_name_norm"] = df["db_name"].astype(str).str.strip().str.lower()

    bronze_rows = _matching_db_rows(df, "bronze_db", bronze_catalog, bronze_schema)
    if bronze_rows.empty:
        raise ValueError(
            "bronze_db not found in DB config for selected environment "
            f"'{environment_config['environment']}' using catalog={bronze_catalog}, schema={bronze_schema}"
        )

    silver_rows = _matching_db_rows(df, "silver_db", silver_catalog, silver_schema)
    if silver_rows.empty:
        raise ValueError(
            "silver_db not found in DB config for selected environment "
            f"'{environment_config['environment']}' using catalog={silver_catalog}, schema={silver_schema}"
        )

    logger.info(
        "Selected %s DB config: bronze=%s.%s silver=%s.%s",
        environment_config["environment"],
        bronze_catalog,
        bronze_schema,
        silver_catalog,
        silver_schema,
    )
    return bronze_rows.iloc[0], silver_rows.iloc[0]


def _build_connection_kwargs(row, auth_type):
    ssl_enabled = _truthy(row.get("ssl"))
    ssl_verification = str(row.get("ssl_verification", "")).strip().upper()
    password = _clean(row.get("password")) or os.getenv(f"{str(row['db_name']).upper()}_PASSWORD")

    connect_kwargs = {
        "host": row["host"],
        "port": int(row["port"]),
        "user": row["user"],
        "catalog": row["catalog"],
        "schema": row["schema"],
        "http_scheme": "https" if ssl_enabled else "http",
        "verify": ssl_verification not in {"NONE", "FALSE", "NO", "0"},
    }

    if auth_type == "BASIC":
        if not password:
            raise ValueError(f"Password is required for BASIC auth for {row['db_name']}")
        connect_kwargs["auth"] = BasicAuthentication(row["user"], password)
    elif auth_type != "NONE":
        raise ValueError(
            f"Unsupported auth_type '{auth_type}' for {row['db_name']}. Use AUTO, BASIC, or NONE."
        )

    if connect_kwargs["http_scheme"] == "https" and connect_kwargs["verify"] is False:
        urllib3.disable_warnings(InsecureRequestWarning)

    return connect_kwargs, bool(password)


def _test_connection(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.fetchall()
    finally:
        cursor.close()


def create_connection(row):
    requested_auth_type = str(row.get("auth_type", "")).strip().upper() or "AUTO"
    auth_types = ["BASIC", "NONE"] if requested_auth_type == "AUTO" else [requested_auth_type]
    errors = []

    for auth_type in auth_types:
        connect_kwargs, password_present = _build_connection_kwargs(row, auth_type)

        logger.debug(
            "Creating Trino connection: db_name=%s host=%s port=%s user=%s catalog=%s schema=%s ssl=%s verify=%s auth_type=%s password_present=%s",
            row["db_name"],
            row["host"],
            row["port"],
            row["user"],
            row["catalog"],
            row["schema"],
            connect_kwargs["http_scheme"],
            connect_kwargs["verify"],
            auth_type,
            password_present,
        )

        conn = trino.dbapi.connect(**connect_kwargs)
        try:
            _test_connection(conn)
            logger.debug("Trino connection validated for %s using auth_type=%s", row["db_name"], auth_type)
            return conn
        except Exception as exc:
            errors.append(f"{auth_type}: {exc}")
            try:
                conn.close()
            except Exception:
                pass

    raise RuntimeError(
        f"Unable to authenticate Trino connection for {row['db_name']}. "
        f"Tried auth_type(s): {', '.join(auth_types)}. "
        f"Errors: {' | '.join(errors)}"
    )


def get_connections():
    if os.getenv("VALIDATION_SOURCE", "").strip().lower() == "databricks":
        conn = create_databricks_connection()
        return conn, conn

    bronze_row, silver_row = select_environment_db_rows(get_db_config())
    return create_connection(bronze_row), create_connection(silver_row)
