import logging
import time

import pandas as pd
from trino import exceptions as trino_exceptions


logger = logging.getLogger(__name__)


def execute_query(conn, query):
    start = time.perf_counter()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description] if cursor.description else []
        return pd.DataFrame(rows, columns=columns)
    except trino_exceptions.HttpError as exc:
        raise RuntimeError(f"Trino HTTP error: {exc}") from exc
    except trino_exceptions.TrinoUserError as exc:
        raise RuntimeError(f"Trino query error: {exc}") from exc
    finally:
        cursor.close()
        elapsed = time.perf_counter() - start
        logger.debug("Query completed in %.2f seconds", elapsed)


def execute_query_scalar(conn, query, column_name=None):
    df = execute_query(conn, query)
    if df.empty:
        return 0

    if column_name and column_name in df.columns:
        return normalize_scalar_value(df.iloc[0][column_name])

    return normalize_scalar_value(df.iloc[0, 0])


def normalize_scalar_value(value):
    if pd.isna(value):
        return 0
    if isinstance(value, str):
        stripped = value.strip()
        try:
            numeric_value = pd.to_numeric(stripped)
            if hasattr(numeric_value, "item"):
                return numeric_value.item()
            return numeric_value
        except (TypeError, ValueError):
            return value
    return value


def get_query_columns(conn, query):
    metadata_query = f"SELECT * FROM ({query}) src WHERE 1 = 0"
    cursor = conn.cursor()

    try:
        cursor.execute(metadata_query)
        return [column[0] for column in cursor.description] if cursor.description else []
    finally:
        cursor.close()
