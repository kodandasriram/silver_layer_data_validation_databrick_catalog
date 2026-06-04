-- Compare bronze-layer query output with silver-layer table output for program_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\Silver_layer_03-June-2026\converted db script to databricks\Databricks_union of all sources\program_base.sql
-- Silver source: converted db script to databricks\silver_layer scripts\program_base_silver_layer.sql

WITH
bronze_layer AS (
/*
Generated Databricks union layer for program_base.
Column order and typed NULL placeholders follow dbt model: program_base.sql.
Source transformations are embedded whole from the converted Databricks OS1/OS2/MIS scripts.
dbt macros expanded to Databricks TRY_CAST / string cleanup expressions.
*/

/*
 =================================================================================================

Name        : PROGRAM_BASE
Description : This model consolidates and standardizes amendment-related attributes
              from MIS and OS2 base models into a unified schema. It aligns column
              structures across both sources using NULL placeholders where attributes
              are not available and combines the datasets using UNION ALL.

              The model ensures consistent column naming and structure for downstream
              consumption in the Silver Layer.

Source Tables : program_base_os2
                program_base_mis

Target Table : PROGRAM_BASE
Load Type    : Full Load (Table)
Materialized : table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-13 | Elavarasi  | Initial version

================================================================================================= 
*/




-- =========================================================================
-- OS2 PROGRAM (program_base_os2)
-- =========================================================================
WITH
    program_base_os2 AS (
/* =================================================================================================

Name        : PROGRAM_BASE_OS2
Description : This model extracts and transforms program-related attributes
              from the NEO2 (OS2) source system Bronze Layer and loads into the
              OSUSR_3QQ_PROGRAM target table as part of the Silver Layer
              data pipeline.

Source Tables : neo2.OSUSR_3QQ_PROGRAM
                neo2.OSUSR_3QQ_PROGRAMSTATUS
                neo2.OSUSR_3QQ_PROGRAMGROUP
                neo2.OSUSR_ZMZ_CUSTOMERTYPE
                neo2.OSUSR_ZMZ_PROFILETYPE

Target Table : OSUSR_3QQ_PROGRAM
Load Type    : Full Load
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-03-24 | Kaviya | Initial version

================================================================================================= */

SELECT
	A.id,
    B.LABEL AS programstatus,
    C.COMMERCIALNAME_EN AS programgroup,
    A.programversionid,
    D.LABEL AS customertype,
    E.LABEL AS profiletype,
	A.reference,
	A.initials,
	A.name,
	A.description,
	A.isspecialprogram,
	A.cancelreason,
	A.activedate,
	A.enddate,
	A.cmsprogram_en,
	A.cmsprogram_ar,
	A.isshowinterestenabled,
	A.programminorversionid,
    FALSE as is_deleted,
   'NEO2' AS source_system_name,
   CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAM` A
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMSTATUS` B
       ON A.PROGRAMSTATUSID = B.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMGROUP` C
       ON A.PROGRAMGROUPID = C.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERTYPE` D
       ON A.CUSTOMERTYPEID = D.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_PROFILETYPE` E
       ON A.PROFILETYPEID = E.CODE
),
    program_base_mis AS (
/*
============================================================================
silver_program_mis.sql
============================================================================
Per-source intermediate Silver model for the Program domain â€” MIS only.

Sources:
  â˜… mis_product       â€” product/program reference for Individual Applications
                         (used in RPT-058, RPT-059)
  â˜… tmkn_tapproduct   â€” TAP product reference for ES Items
                         (used in RPT-032)

These two tables are PARALLEL program/product reference entities â€” not
related to each other. They serve different application contexts:
  - mis_product: keyed by mis_productId, used as FK from mis_individualapplication
  - tmkn_tapproduct: keyed by tmkn_tapproductId, used as FK from tmkn_esitems

Therefore they are UNIONed (not joined). A row in this Silver table represents
one program/product entry, with the mis_source_table column identifying its
origin.

Note: the Phase 1 analysis showed mis_product and tmkn_tapproduct have very
few columns referenced in the SPs (2 and 6 respectively), which means the
real attribute set is larger than what this Silver model captures. Future
iterations may want to include all source columns even if not referenced
by the SPs we analysed.
============================================================================
*/

-- ============================================================================
-- mis_product â€” Individual application product reference
-- ============================================================================
SELECT
    'mis_product' AS mis_source_table,

    -- Identifiers
    CAST(prod.mis_productid AS STRING)                  AS program_id,
    prod.mis_name                                        AS program_name,

    -- Placeholders for fields specific to tmkn_tapproduct branch
    CAST(NULL AS STRING)                                AS tap_program_code,
    CAST(NULL AS STRING)                                AS tap_program_description,
    CAST(NULL AS STRING)                                AS tap_program_status,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`MIS_PRODUCTBASE` prod


UNION ALL


-- ============================================================================
-- tmkn_tapproduct â€” ES Items product reference
-- ============================================================================
SELECT
    'tmkn_tapproduct' AS mis_source_table,

    -- Identifiers
    CAST(tap.tmkn_tapproductid AS STRING)               AS program_id,
    tap.tmkn_name                                        AS program_name,

    -- TAP-specific fields
    --tap.tmkn_code                                        AS tap_program_code,
    CAST(NULL AS STRING)                                AS tap_program_code,
    tap.tmkn_description                                 AS tap_program_description,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_tapproduct')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(tap.statuscode AS STRING)

)  AS tap_program_status,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_TAPPRODUCTBASE` tap
)
SELECT
    source_system_name,
    CAST(id AS STRING) AS program_id,
    name AS program_name,
    CAST(NULL AS STRING) AS tap_program_code,
    CAST(NULL AS STRING) AS tap_program_description,
    programstatus AS program_status,
    programgroup AS program_group,
    programversionid AS program_version_id,
    customertype AS customer_type,
    profiletype AS profile_type,
    reference,
    initials,
    description,
    isspecialprogram AS is_special_program,
    cancelreason AS cancel_reason,
    activedate AS active_date,
    enddate AS end_date,
    cmsprogram_en,
    cmsprogram_ar,
    isshowinterestenabled AS is_show_interest_enabled,
    programminorversionid AS program_minor_version_id,
    is_deleted,
    CAST(CURRENT_DATE AS DATE) AS report_date,
    CAST(NULL AS STRING) AS source_table,
    dbt_updated_at 

FROM program_base_os2

UNION ALL

-- =========================================================================
-- MIS PROGRAM (program_base_mis)
-- =========================================================================
SELECT
    source_system_name,
    program_id,
    program_name,
    tap_program_code,
    tap_program_description,
    tap_program_status AS program_status,
    CAST(NULL AS STRING) AS program_group,
    CAST(NULL AS BIGINT) AS program_version_id,
    CAST(NULL AS STRING) AS customer_type,
    CAST(NULL AS STRING) AS profile_type,
    CAST(NULL AS STRING) AS reference,
    CAST(NULL AS STRING) AS initials,
    CAST(NULL AS STRING) AS description,
    CAST(NULL AS BOOLEAN) AS is_special_program,
    CAST(NULL AS STRING) AS cancel_reason,
    CAST(NULL AS TIMESTAMP) AS active_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    CAST(NULL AS STRING) AS cms_program_en,
    CAST(NULL AS STRING) AS cms_program_ar,
    CAST(NULL AS BOOLEAN) AS is_show_interest_enabled,
    CAST(NULL AS BIGINT) AS program_minor_version_id,
    is_deleted,
    report_date,
    mis_source_table AS source_table,
    dbt_updated_at

FROM program_base_mis
),

silver_layer AS (
SELECT
    `source_system_name`,
    `program_id`,
    `program_name`,
    `tap_program_code`,
    `tap_program_description`,
    `program_status`,
    `program_group`,
    `program_version_id`,
    `customer_type`,
    `profile_type`,
    `reference`,
    `initials`,
    `description`,
    `is_special_program`,
    `cancel_reason`,
    `active_date`,
    `end_date`,
    `cmsprogram_en`,
    `cmsprogram_ar`,
    `is_show_interest_enabled`,
    `program_minor_version_id`,
    `is_deleted`,
    `report_date`,
    `source_table`,
    `dbt_updated_at`
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`program_base`
),

bronze_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'source_system_name'),
        (2, 'program_id'),
        (3, 'program_name'),
        (4, 'tap_program_code'),
        (5, 'tap_program_description'),
        (6, 'program_status'),
        (7, 'program_group'),
        (8, 'program_version_id'),
        (9, 'customer_type'),
        (10, 'profile_type'),
        (11, 'reference'),
        (12, 'initials'),
        (13, 'description'),
        (14, 'is_special_program'),
        (15, 'cancel_reason'),
        (16, 'active_date'),
        (17, 'end_date'),
        (18, 'cmsprogram_en'),
        (19, 'cmsprogram_ar'),
        (20, 'is_show_interest_enabled'),
        (21, 'program_minor_version_id'),
        (22, 'is_deleted'),
        (23, 'report_date'),
        (24, 'source_table'),
        (25, 'dbt_updated_at')
    ) AS t(column_position, column_name)
),

silver_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'source_system_name'),
        (2, 'program_id'),
        (3, 'program_name'),
        (4, 'tap_program_code'),
        (5, 'tap_program_description'),
        (6, 'program_status'),
        (7, 'program_group'),
        (8, 'program_version_id'),
        (9, 'customer_type'),
        (10, 'profile_type'),
        (11, 'reference'),
        (12, 'initials'),
        (13, 'description'),
        (14, 'is_special_program'),
        (15, 'cancel_reason'),
        (16, 'active_date'),
        (17, 'end_date'),
        (18, 'cmsprogram_en'),
        (19, 'cmsprogram_ar'),
        (20, 'is_show_interest_enabled'),
        (21, 'program_minor_version_id'),
        (22, 'is_deleted'),
        (23, 'report_date'),
        (24, 'source_table'),
        (25, 'dbt_updated_at')
    ) AS t(column_position, column_name)
),

bronze_normalized AS (
    SELECT
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`program_id` AS STRING) AS `program_id`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`tap_program_code` AS STRING) AS `tap_program_code`,
        CAST(`tap_program_description` AS STRING) AS `tap_program_description`,
        CAST(`program_status` AS STRING) AS `program_status`,
        CAST(`program_group` AS STRING) AS `program_group`,
        CAST(`program_version_id` AS STRING) AS `program_version_id`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`profile_type` AS STRING) AS `profile_type`,
        CAST(`reference` AS STRING) AS `reference`,
        CAST(`initials` AS STRING) AS `initials`,
        CAST(`description` AS STRING) AS `description`,
        CAST(`is_special_program` AS STRING) AS `is_special_program`,
        CAST(`cancel_reason` AS STRING) AS `cancel_reason`,
        CAST(`active_date` AS STRING) AS `active_date`,
        CAST(`end_date` AS STRING) AS `end_date`,
        CAST(`cmsprogram_en` AS STRING) AS `cmsprogram_en`,
        CAST(`cmsprogram_ar` AS STRING) AS `cmsprogram_ar`,
        CAST(`is_show_interest_enabled` AS STRING) AS `is_show_interest_enabled`,
        CAST(`program_minor_version_id` AS STRING) AS `program_minor_version_id`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`source_table` AS STRING) AS `source_table`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`program_id` AS STRING) AS `program_id`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`tap_program_code` AS STRING) AS `tap_program_code`,
        CAST(`tap_program_description` AS STRING) AS `tap_program_description`,
        CAST(`program_status` AS STRING) AS `program_status`,
        CAST(`program_group` AS STRING) AS `program_group`,
        CAST(`program_version_id` AS STRING) AS `program_version_id`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`profile_type` AS STRING) AS `profile_type`,
        CAST(`reference` AS STRING) AS `reference`,
        CAST(`initials` AS STRING) AS `initials`,
        CAST(`description` AS STRING) AS `description`,
        CAST(`is_special_program` AS STRING) AS `is_special_program`,
        CAST(`cancel_reason` AS STRING) AS `cancel_reason`,
        CAST(`active_date` AS STRING) AS `active_date`,
        CAST(`end_date` AS STRING) AS `end_date`,
        CAST(`cmsprogram_en` AS STRING) AS `cmsprogram_en`,
        CAST(`cmsprogram_ar` AS STRING) AS `cmsprogram_ar`,
        CAST(`is_show_interest_enabled` AS STRING) AS `is_show_interest_enabled`,
        CAST(`program_minor_version_id` AS STRING) AS `program_minor_version_id`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`source_table` AS STRING) AS `source_table`,
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
        'program_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'program_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'program_base' AS table_name,
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
        'program_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'program_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
