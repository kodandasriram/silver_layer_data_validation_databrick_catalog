WITH
bronze_layer AS (
-- Standalone Trino SQL generated from service_fee_base.sql.
-- Final column order aligned to silver_layer_query/service_fee_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : service_fee
   Description   : This model extracts and transforms training delivery type
                   reference data from the NEO2 (OS2) Bronze Layer and loads
                   it into the TRAINING_DELIVERY_TYPE_BASE target table as
                   part of the Silver Layer data pipeline.

                   The model captures training delivery type labels and
                   standardizes metadata fields for audit, lineage, and
                   reporting purposes.

                   The model applies timestamp standardization and source
                   system normalization to ensure consistency across the
                   curated Silver Layer.

   Source Tables : neo2.OSUSR_VW9_TRAININGDELIVERYTYPE

   Target Table  : SERVICE_FEE_BASE

   Load Type     : Full Load
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily

   Business Rules:
   ---------------------------------------------------------------------------
   1. Source system is hardcoded as:
        - 'NEO2'

   2. IS_DELETED flag is defaulted to:
        - FALSE

   3. Timestamp fields are standardized using:
        - safe_cast_timestamp()

   4. SOURCE_SYSTEM_NAME is normalized using:
        - clean_string_upper()

   5. DBT audit timestamp is generated using:
        - to_utc_timestamp(current_timestamp(), current_timezone())

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author        | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-05-12   | Siva       | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/  

with CTE_OSUSR_VW9_TRAININGDELIVERYTYPE AS
(
SELECT
        TDT.LABEL,
        FALSE as IS_DELETED,
        'NEO2' AS SOURCE_SYSTEM_NAME,
        TDT.BRONZE_UPDATED_ON as UPDATEDON,
        TDT.BRONZE_CREATED_ON as CREATEDON,
        cast(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS DBT_UPDATED_AT
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAININGDELIVERYTYPE TDT
)
SELECT
    LABEL AS label,
    IS_DELETED AS is_deleted,
    UPPER(NULLIF(TRIM(CAST(SOURCE_SYSTEM_NAME AS STRING)), '')) AS source_system_name,
    TRY_CAST(NULLIF(CAST(UPDATEDON AS STRING), '') AS TIMESTAMP) AS updatedon,
    TRY_CAST(NULLIF(CAST(CREATEDON AS STRING), '') AS TIMESTAMP) AS createdon,
    TRY_CAST(NULLIF(CAST(DBT_UPDATED_AT AS STRING), '') AS TIMESTAMP) AS dbt_updated_at
FROM CTE_OSUSR_VW9_TRAININGDELIVERYTYPE TDT
),

silver_layer AS (
SELECT
    label,
    is_deleted,
    source_system_name,
    updatedon,
    createdon,
    dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.service_fee_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'label'),
        (2, 'is_deleted'),
        (3, 'source_system_name'),
        (4, 'updatedon'),
        (5, 'createdon'),
        (6, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'label'),
        (2, 'is_deleted'),
        (3, 'source_system_name'),
        (4, 'updatedon'),
        (5, 'createdon'),
        (6, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(`label` AS STRING) AS `label`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`label` AS STRING) AS `label`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM silver_layer
),

bronze_minus_silver AS (
    SELECT * FROM bronze_normalized
    EXCEPT ALL
    SELECT * FROM silver_normalized
),

silver_minus_bronze AS (
    SELECT * FROM silver_normalized
    EXCEPT ALL
    SELECT * FROM bronze_normalized
),

validation_results AS (
    SELECT
        'service_fee_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'service_fee_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'service_fee_base' AS table_name,
        'column_names_match' AS validation_point,
        CAST((
            SELECT COUNT(*)
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL OR s.column_name IS NULL
        ) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN NOT EXISTS (
            SELECT 1
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL OR s.column_name IS NULL
        ) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'service_fee_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'service_fee_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
