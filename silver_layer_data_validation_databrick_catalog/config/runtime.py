import os


def is_databricks_mode():
    validation_source = os.getenv("VALIDATION_SOURCE", "").strip().lower()
    execution_mode = os.getenv("EXECUTION_MODE", "").strip().lower()

    return (
        validation_source == "databricks"
        or execution_mode in {"databricks", "spark"}
        or bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))
    )


def activate_databricks_mode():
    if is_databricks_mode():
        os.environ.setdefault("VALIDATION_SOURCE", "databricks")
