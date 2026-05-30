import logging
import re
import time


logger = logging.getLogger(__name__)


class SparkSqlConnection:
    def __init__(self, spark):
        self.spark = spark

    def cursor(self):
        return SparkSqlCursor(self.spark)

    def close(self):
        return None


class SparkSqlCursor:
    def __init__(self, spark):
        self.spark = spark
        self.description = []
        self._rows = []

    def execute(self, statement):
        start = time.perf_counter()
        statement = normalize_spark_sql(statement)
        df = self.spark.sql(statement)
        self.description = [(field.name,) for field in df.schema.fields]
        self._rows = [tuple(row) for row in df.collect()]
        elapsed = time.perf_counter() - start
        logger.debug("Spark SQL completed in %.2f seconds", elapsed)
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        return None


def create_spark_connection():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "pyspark is required when VALIDATION_SOURCE=databricks. "
            "Run this mode inside a Databricks Workflow task."
        ) from exc

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return SparkSqlConnection(spark)


def normalize_spark_sql(statement):
    return re.sub(
        r"CAST\s*\(\s*CURRENT_TIMESTAMP\s+AT\s+TIME\s+ZONE\s+'UTC'\s+AS\s+TIMESTAMP\s*\)",
        "CURRENT_TIMESTAMP()",
        statement,
        flags=re.IGNORECASE,
    )
