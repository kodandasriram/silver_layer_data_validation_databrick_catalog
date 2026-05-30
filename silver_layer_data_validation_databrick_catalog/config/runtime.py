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


def use_databricks_rules_config():
    config_source = os.getenv("VALIDATION_CONFIG_SOURCE", "").strip().lower()
    if config_source:
        return config_source in {"databricks", "catalog", "rules", "validation_rules"}
    return os.getenv("VALIDATION_SOURCE", "").strip().lower() == "databricks"
