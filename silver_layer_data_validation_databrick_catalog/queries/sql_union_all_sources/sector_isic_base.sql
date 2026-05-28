-- Compare bronze-layer query output with silver-layer table output for sector_isic_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\sector_isic_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\sector_isic_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Trino SQL generated from sector_isic_base.sql.
-- Final column order aligned to silver_layer_query/sector_isic_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : SECTOR_ISIC_BASE
Description : This model extracts and transforms sector ISIC reference data
              from the NEO2 (OS2) source system Bronze Layer and loads into the
              SECTOR_ISIC_BASE target table as part of the Silver Layer
              data pipeline.

              It enriches the sector ISIC record with the activity label by
              joining with the sector ISIC activity reference table.

Source Tables : neo2.OSUSR_3QQ_SECTORISIC3
                neo2.OSUSR_3QQ_SECTORISICACTIVITY3

Target Table : SECTOR_ISIC_BASE
Load Type    : Full Load
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------
Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-03-23 | Rejeesh  | Initial version

================================================================================================= 
*/
with cte_sector as (
SELECT
    a.id,
    a.isiccode,
    b.LABEL AS sectorisicactivity,
    'NEO2' AS source_system_name,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISIC3 a
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISICACTIVITY3 b
        ON a.SECTORISICACTIVITYID = b.CODE
)
SELECT
    TRY_CAST(NULLIF(CAST(id AS VARCHAR), '') AS BIGINT) AS id,
    isiccode,
    sectorisicactivity,
    FALSE AS is_deleted,
    UPPER(NULLIF(TRIM(CAST(source_system_name AS VARCHAR)), '')) AS source_system_name,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS VARCHAR), '') AS TIMESTAMP) AS dbt_updated_at
from 
cte_sector
),

silver_layer AS (
SELECT
    id,
    isiccode,
    sectorisicactivity,
    is_deleted,
    source_system_name,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".sector_isic_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'isiccode'),
        (3, 'sectorisicactivity'),
        (4, 'is_deleted'),
        (5, 'source_system_name'),
        (6, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'isiccode'),
        (3, 'sectorisicactivity'),
        (4, 'is_deleted'),
        (5, 'source_system_name'),
        (6, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("id" AS VARCHAR) AS "id",
        CAST("isiccode" AS VARCHAR) AS "isiccode",
        CAST("sectorisicactivity" AS VARCHAR) AS "sectorisicactivity",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("id" AS VARCHAR) AS "id",
        CAST("isiccode" AS VARCHAR) AS "isiccode",
        CAST("sectorisicactivity" AS VARCHAR) AS "sectorisicactivity",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
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
        'sector_isic_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'sector_isic_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'sector_isic_base' AS table_name,
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
        'sector_isic_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'sector_isic_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
