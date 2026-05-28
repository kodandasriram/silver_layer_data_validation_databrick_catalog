-- Compare bronze-layer query output with silver-layer table output for support_structure_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\support_structure_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\support_structure_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Trino SQL generated from support_structure_base.sql.
-- Final column order aligned to silver_layer_query/support_structure_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : SUPPORT_STRUCTURE_BASE_OS2
   Description   : This incremental model extracts and transforms support 
                   structure data from the NEO2 (OS2) source system Bronze 
                   Layer and loads it into the SUPPORT_STRUCTURE_BASE_OS2 
                   target table as part of the Silver Layer data pipeline.

                   It captures application support structure details including 
                   program support sub-items, requested and approved amounts, 
                   Tamkeen share allocations, eligibility flags, claim tracking, 
                   and inspection requirements.

                   The model enriches data by joining with program support type 
                   sub-item and eligibility reference tables to provide 
                   descriptive labels.

                   It implements an incremental load strategy using MERGE 
                   based on the unique key (APPLICATIONSUPPORTID), processing 
                   only new and updated records using CREATEDON and UPDATEDON 
                   timestamps.

                   A post-hook ensures soft deletion handling by marking 
                   records as IS_DELETED = TRUE when they no longer exist 
                   in the source table.

   Source Tables : neo2.OSUSR_2DA_SUPPORTSTRUCTURE
                   neo2.OSUSR_3QQ_PROGRAMSUPPORTTYPESUBITEM3
                   neo2.OSUSR_MM5_YESNOOPTION4

   Target Table  : SUPPORT_STRUCTURE_BASE_OS2

   Load Type     : Incremental (Merge)
   Materialized  : Incremental
   Format        : PARQUET
   Tags          : neo2, daily

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author       | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-03-24   | Swetha     | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/
with cte_OSUSR_2DA_SUPPORTSTRUCTURE as 
(
SELECT 
    SS.APPLICATIONSUPPORTID,
    PTS.NAMEEN AS PROGRAMSUPPORTTYPESUBITEMID,
    SS.DESCRIPTION,
    SS.REQUESTEDAMT,
    SS.TKSHAREOVR,
    SS.TKSHARE,
    Y.LABEL AS ISELIGIBLE,
    SS.ELIGIBLEERRORMESAGE,
    SS.DUEDATE,
    SS.REQUIRESINSPECTION,
    SS.ITEMAMTAVAILABLE,
    SS.ITEMAMTINPROGRESS,
    SS.ITEMAMTCLAIMED,
    SS.NUMBEROFCLAIMS,
    FALSE as IS_DELETED,
    'NEO2' AS SOURCE_SYSTEM_NAME,
    SS.UPDATEDON,
    SS.CREATEDON,
    cast(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT,
         ROW_NUMBER() OVER (PARTITION BY SS.APPLICATIONSUPPORTID ORDER BY SS.UPDATEDON DESC NULLS LAST, SS.CREATEDON DESC NULLS LAST) AS rnk
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_SUPPORTSTRUCTURE SS
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMSUPPORTTYPESUBITEM3 PTS 
on PTS.id = SS.PROGRAMSUPPORTTYPESUBITEMID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_YESNOOPTION4 Y
ON Y.ID = SS.ISELIGIBLE
)
SELECT
    TRY_CAST(NULLIF(CAST(APPLICATIONSUPPORTID AS VARCHAR), '') AS BIGINT) AS applicationsupportid,
    PROGRAMSUPPORTTYPESUBITEMID AS programsupporttypesubitemid,
    DESCRIPTION AS description,
    REQUESTEDAMT AS requestedamt,
    TKSHAREOVR AS tkshareovr,
    TKSHARE AS tkshare,
    ISELIGIBLE AS iseligible,
    ELIGIBLEERRORMESAGE AS eligibleerrormesage,
    TRY_CAST(NULLIF(CAST(DUEDATE AS VARCHAR), '') AS DATE) AS duedate,
    TRY_CAST(NULLIF(CAST(REQUIRESINSPECTION AS VARCHAR), '') AS BIGINT) AS requiresinspection,
    ITEMAMTAVAILABLE AS itemamtavailable,
    ITEMAMTINPROGRESS AS itemamtinprogress,
    ITEMAMTCLAIMED AS itemamtclaimed,
    TRY_CAST(NULLIF(CAST(NUMBEROFCLAIMS AS VARCHAR), '') AS BIGINT) AS numberofclaims,
    IS_DELETED AS is_deleted,
    UPPER(NULLIF(TRIM(CAST(SOURCE_SYSTEM_NAME AS VARCHAR)), '')) AS source_system_name,
    TRY_CAST(NULLIF(CAST(UPDATEDON AS VARCHAR), '') AS TIMESTAMP) AS updatedon,
    TRY_CAST(NULLIF(CAST(CREATEDON AS VARCHAR), '') AS TIMESTAMP) AS createdon,
    TRY_CAST(NULLIF(CAST(DBT_UPDATED_AT AS VARCHAR), '') AS TIMESTAMP) AS dbt_updated_at
from cte_OSUSR_2DA_SUPPORTSTRUCTURE A
WHERE rnk=1
),

silver_layer AS (
SELECT
    applicationsupportid,
    programsupporttypesubitemid,
    description,
    requestedamt,
    tkshareovr,
    tkshare,
    iseligible,
    eligibleerrormesage,
    duedate,
    requiresinspection,
    itemamtavailable,
    itemamtinprogress,
    itemamtclaimed,
    numberofclaims,
    is_deleted,
    source_system_name,
    updatedon,
    createdon,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".support_structure_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'applicationsupportid'),
        (2, 'programsupporttypesubitemid'),
        (3, 'description'),
        (4, 'requestedamt'),
        (5, 'tkshareovr'),
        (6, 'tkshare'),
        (7, 'iseligible'),
        (8, 'eligibleerrormesage'),
        (9, 'duedate'),
        (10, 'requiresinspection'),
        (11, 'itemamtavailable'),
        (12, 'itemamtinprogress'),
        (13, 'itemamtclaimed'),
        (14, 'numberofclaims'),
        (15, 'is_deleted'),
        (16, 'source_system_name'),
        (17, 'updatedon'),
        (18, 'createdon'),
        (19, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'applicationsupportid'),
        (2, 'programsupporttypesubitemid'),
        (3, 'description'),
        (4, 'requestedamt'),
        (5, 'tkshareovr'),
        (6, 'tkshare'),
        (7, 'iseligible'),
        (8, 'eligibleerrormesage'),
        (9, 'duedate'),
        (10, 'requiresinspection'),
        (11, 'itemamtavailable'),
        (12, 'itemamtinprogress'),
        (13, 'itemamtclaimed'),
        (14, 'numberofclaims'),
        (15, 'is_deleted'),
        (16, 'source_system_name'),
        (17, 'updatedon'),
        (18, 'createdon'),
        (19, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("applicationsupportid" AS VARCHAR) AS "applicationsupportid",
        CAST("programsupporttypesubitemid" AS VARCHAR) AS "programsupporttypesubitemid",
        CAST("description" AS VARCHAR) AS "description",
        CAST("requestedamt" AS VARCHAR) AS "requestedamt",
        CAST("tkshareovr" AS VARCHAR) AS "tkshareovr",
        CAST("tkshare" AS VARCHAR) AS "tkshare",
        CAST("iseligible" AS VARCHAR) AS "iseligible",
        CAST("eligibleerrormesage" AS VARCHAR) AS "eligibleerrormesage",
        CAST("duedate" AS VARCHAR) AS "duedate",
        CAST("requiresinspection" AS VARCHAR) AS "requiresinspection",
        CAST("itemamtavailable" AS VARCHAR) AS "itemamtavailable",
        CAST("itemamtinprogress" AS VARCHAR) AS "itemamtinprogress",
        CAST("itemamtclaimed" AS VARCHAR) AS "itemamtclaimed",
        CAST("numberofclaims" AS VARCHAR) AS "numberofclaims",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("updatedon" AS VARCHAR) AS "updatedon",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("applicationsupportid" AS VARCHAR) AS "applicationsupportid",
        CAST("programsupporttypesubitemid" AS VARCHAR) AS "programsupporttypesubitemid",
        CAST("description" AS VARCHAR) AS "description",
        CAST("requestedamt" AS VARCHAR) AS "requestedamt",
        CAST("tkshareovr" AS VARCHAR) AS "tkshareovr",
        CAST("tkshare" AS VARCHAR) AS "tkshare",
        CAST("iseligible" AS VARCHAR) AS "iseligible",
        CAST("eligibleerrormesage" AS VARCHAR) AS "eligibleerrormesage",
        CAST("duedate" AS VARCHAR) AS "duedate",
        CAST("requiresinspection" AS VARCHAR) AS "requiresinspection",
        CAST("itemamtavailable" AS VARCHAR) AS "itemamtavailable",
        CAST("itemamtinprogress" AS VARCHAR) AS "itemamtinprogress",
        CAST("itemamtclaimed" AS VARCHAR) AS "itemamtclaimed",
        CAST("numberofclaims" AS VARCHAR) AS "numberofclaims",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("updatedon" AS VARCHAR) AS "updatedon",
        CAST("createdon" AS VARCHAR) AS "createdon",
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
        'support_structure_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'support_structure_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'support_structure_base' AS table_name,
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
        'support_structure_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'support_structure_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
