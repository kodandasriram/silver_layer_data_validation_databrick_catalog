import logging
import os
import time

import pandas as pd
import requests


logger = logging.getLogger(__name__)


DEFAULT_WORKSPACE_URL = "https://dbc-40a7125f-a6ba.cloud.databricks.com"
DEFAULT_WAREHOUSE_ID = "cf033317deeec23c"


class DatabricksStatementConnection:
    def __init__(self, workspace_url, access_token, warehouse_id, poll_interval=2, timeout=300):
        self.workspace_url = workspace_url.rstrip("/")
        self.access_token = access_token
        self.warehouse_id = warehouse_id
        self.poll_interval = poll_interval
        self.timeout = timeout

    def cursor(self):
        return DatabricksStatementCursor(self)

    def close(self):
        return None


class DatabricksStatementCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self._rows = []

    def execute(self, statement):
        result = self._execute_statement(statement)
        manifest = result.get("manifest") or {}
        schema = manifest.get("schema") or {}
        columns = schema.get("columns") or []
        self.description = [(column.get("name"),) for column in columns]

        data = result.get("result") or {}
        self._rows = data.get("data_array") or []
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        return None

    def _execute_statement(self, statement):
        url = f"{self.connection.workspace_url}/api/2.0/sql/statements"
        headers = {
            "Authorization": f"Bearer {self.connection.access_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "warehouse_id": self.connection.warehouse_id,
            "statement": statement,
            "disposition": "INLINE",
            "wait_timeout": "30s",
        }

        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        result = response.json()

        statement_id = result.get("statement_id")
        started_at = time.monotonic()
        while _statement_state(result) in {"PENDING", "RUNNING"}:
            if not statement_id:
                break
            if time.monotonic() - started_at > self.connection.timeout:
                raise TimeoutError(f"Databricks statement timed out after {self.connection.timeout} seconds")
            time.sleep(self.connection.poll_interval)
            result = self._get_statement(statement_id, headers)

        state = _statement_state(result)
        if state in {"FAILED", "CANCELED", "CLOSED"}:
            error = (result.get("status") or {}).get("error") or {}
            message = error.get("message") or result
            raise RuntimeError(f"Databricks statement {state}: {message}")

        return result

    def _get_statement(self, statement_id, headers):
        url = f"{self.connection.workspace_url}/api/2.0/sql/statements/{statement_id}"
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()


def _statement_state(result):
    return ((result.get("status") or {}).get("state") or "").upper()


def create_databricks_connection():
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN") or os.getenv("DATABRICKS_TOKEN")
    if not access_token:
        raise ValueError(
            "DATABRICKS_ACCESS_TOKEN is required for Databricks catalog mode. "
            "Set it in your environment instead of storing it in code."
        )

    workspace_url = os.getenv("DATABRICKS_WORKSPACE_URL", DEFAULT_WORKSPACE_URL)
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID)
    poll_interval = int(os.getenv("DATABRICKS_POLL_INTERVAL", "2"))
    timeout = int(os.getenv("DATABRICKS_STATEMENT_TIMEOUT", "300"))

    logger.info("Using Databricks SQL warehouse %s at %s", warehouse_id, workspace_url)
    return DatabricksStatementConnection(
        workspace_url=workspace_url,
        access_token=access_token,
        warehouse_id=warehouse_id,
        poll_interval=poll_interval,
        timeout=timeout,
    )


def databricks_result_to_dataframe(result):
    manifest = result.get("manifest") or {}
    schema = manifest.get("schema") or {}
    columns = [column.get("name") for column in schema.get("columns") or []]
    rows = (result.get("result") or {}).get("data_array") or []
    return pd.DataFrame(rows, columns=columns)
