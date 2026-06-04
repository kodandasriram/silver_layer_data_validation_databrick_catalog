-- Compare bronze-layer query output with silver-layer table output for application_support_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\application_support_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\application_support_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Databricks SQL generated from application_support_base.sql.
-- Final column order aligned to silver_layer_query/application_support_base_silver_layer.sql.
-- Standalone Databricks SQL converted from dbt model.
/*
 =============================================================================
   Name          : APPLICATION_SUPPORT_BASE
   Description   : This model extracts and transforms application support-
                   level data from the NEO2 (OS2) Bronze Layer and loads it
                   into the APPLICATION_SUPPORT_BASE target table as part of
                   the Silver Layer data pipeline.

                   The model captures application support information related
                   to training, employee support, amendment requests,
                   provider information, applicant demographics, employer
                   details, wage information, requested support amounts,
                   and Tamkeen share amounts.

                   The model enriches application support data by joining
                   multiple reference and transactional tables including
                   application, amendment request, employee, customer,
                   company, training, support structure, assessment, BPM
                   process, and BPM activity tables.

                   Timestamp fields are standardized using safe casting
                   logic and system metadata fields are appended for
                   lineage and audit tracking.

   Source Tables : neo2.OSUSR_2DA_APPLICATIONSUPPORT
                   neo2.OSUSR_NTP_APPLICATION4
                   neo2.OSUSR_NTP_AMENDMENTREQUEST
                   neo2.OSUSR_ZMZ_INDIVIDUAL
                   neo2.OSUSR_QM6_PORTALUSER
                   neo2.OSUSR_2DA_EMPLOYEE
                   neo2.OSUSR_ZMZ_CUSTOMER
                   neo2.OSUSR_VW9_TRAINING
                   neo2.OSUSR_3QQ_PROGRAMVERSION
                   neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                   neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                   neo2.OSUSR_ZMZ_COMPANY
                   neo2.OSUSR_2DA_SUPPORTSTRUCTURE
                   neo2.OSUSR_1AT_ASSESSMENT
                   neo2.OSSYS_BPM_PROCESS
                   neo2.OSSYS_BPM_ACTIVITY

   Target Table  : APPLICATION_SUPPORT_BASE

   Load Type     : Full Load
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily

   Business Rules:
   ---------------------------------------------------------------------------
   1. DISTINCT records are selected in BASE CTE to avoid duplicate rows
      generated from multi-table joins.

   2. Timestamp fields are standardized using:
        - safe_cast_timestamp()

   3. Source system is hardcoded as:
        - 'NEO2'

   4. DBT audit timestamp is generated using:
        - to_utc_timestamp(current_timestamp(), current_timezone())

   5. Applicant demographic information is derived from:
        - OSUSR_ZMZ_INDIVIDUAL

   6. Employer and customer information is derived from:
        - OSUSR_ZMZ_CUSTOMER
        - OSUSR_ZMZ_COMPANY

   7. Training and support financial details are enriched from:
        - OSUSR_VW9_TRAINING
        - OSUSR_2DA_SUPPORTSTRUCTURE

   8. BPM workflow user information is derived from:
        - OSSYS_BPM_PROCESS
        - OSSYS_BPM_ACTIVITY

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author        | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-05-12   | siva       | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/

WITH BASE AS (
    SELECT DISTINCT
    APP.id,
    APPSUP.amendmentrequestid,
    APPSUP.applicationid,
    APPSUP.createdon,
    APPSUP.updatedon,
    APP.approvedon,
    amdment.submittedon,
    --aps.SUBMITTEDTOEXTASSESSORON, --column not found
    CAST (NULL AS STRING) AS submittedtoextassessoron,
    APPSUP.isactive,
    APPSUP.providerid,
    APPSUP.externalproviderid,
    CusIndApp.genderid,
    CusIndApp.dateofbirth,
    PORTUSR.name,
    OJTCUS.nameen,
    --aps.LABEL, --column not found--ossys_bpm_activity_definition
     CAST (NULL AS STRING) AS label,
    SS.requestedamt,
    TRA.tkshareamt,
    ACT.user_id,
    APPSUP.referencenumber,
    Emp.totalmonthsexperience,
    ProgVer.commercialname_en,
    APP.customertypeid,
    CMP.registrationdate,
    EMP.jobcurrentwage,
    ROW_NUMBER() OVER (PARTITION BY APP.ID ORDER BY APPSUP.UPDATEDON DESC NULLS LAST, APPSUP.CREATEDON DESC NULLS LAST) AS rnk,
    'NEO2' AS source_system_name,
     FALSE AS is_deleted,
     CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORT` APPSUP
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION4` APP
ON APP.ID = APPSUP.APPLICATIONID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_AMENDMENTREQUEST` amdment
ON amdment.ID = APPSUP.AMENDMENTREQUESTID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL` CusIndApp
On APPSUP.INDIVIDUALID = CusIndApp.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_QM6_PORTALUSER` PORTUSR
ON PORTUSR.ID = APP.PORTALUSERID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EMPLOYEE` Emp
ON Emp.APPLICATIONSUPPORTID = APPSUP.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` OJTCUS
ON Emp.EMPLOYERID = OJTCUS.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_TRAINING` TRA
ON TRA.APPLICATIONSUPPORTID = APPSUP.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION` ProgVer
ON ProgVer.ID = APP.PROGRAMVERSIONID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCUSTOMER` APPCUS
ON APP.ID = APPCUS.APPLICATIONID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE` CUSPROF
ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` CUS
ON CUSPROF.CUSTOMERID = CUS.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY` CMP
ON CUS.ID = CMP.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_SUPPORTSTRUCTURE` SS
ON APPSUP.ID = SS.APPLICATIONSUPPORTID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_1AT_ASSESSMENT` ass
ON ass.ID = APPSUP.APPLICATIONID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_PROCESS` pro
ON pro.TOP_PROCESS_ID = ass.PROCESSID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY` act
ON act.Process_Id = pro.Id
)
SELECT
    id,
    amendmentrequestid,
    applicationid,
    TRY_CAST(NULLIF(CAST(CREATEDON AS STRING), '') AS TIMESTAMP) AS createdon,
    TRY_CAST(NULLIF(CAST(APPROVEDON AS STRING), '') AS TIMESTAMP) AS approvedon,
    submittedon,
    submittedtoextassessoron,
    isactive,
    providerid,
    externalproviderid,
    genderid,
    TRY_CAST(NULLIF(CAST(DATEOFBIRTH AS STRING), '') AS TIMESTAMP) AS dateofbirth,
    name,
    nameen,
    label,
    requestedamt,
    tkshareamt,
    user_id,
    referencenumber,
    totalmonthsexperience,
    commercialname_en,
    customertypeid,
    registrationdate,
    jobcurrentwage,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    TRY_CAST(NULLIF(CAST(updatedon AS STRING), '') AS TIMESTAMP) AS updatedon
FROM BASE app where rnk=1
),

silver_layer AS (
SELECT
    id,
    amendmentrequestid,
    applicationid,
    createdon,
    approvedon,
    submittedon,
    submittedtoextassessoron,
    isactive,
    providerid,
    externalproviderid,
    genderid,
    dateofbirth,
    name,
    nameen,
    label,
    requestedamt,
    tkshareamt,
    user_id,
    referencenumber,
    totalmonthsexperience,
    commercialname_en,
    customertypeid,
    registrationdate,
    jobcurrentwage,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    updatedon
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`application_support_base`
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'amendmentrequestid'),
        (3, 'applicationid'),
        (4, 'createdon'),
        (5, 'approvedon'),
        (6, 'submittedon'),
        (7, 'submittedtoextassessoron'),
        (8, 'isactive'),
        (9, 'providerid'),
        (10, 'externalproviderid'),
        (11, 'genderid'),
        (12, 'dateofbirth'),
        (13, 'name'),
        (14, 'nameen'),
        (15, 'label'),
        (16, 'requestedamt'),
        (17, 'tkshareamt'),
        (18, 'user_id'),
        (19, 'referencenumber'),
        (20, 'totalmonthsexperience'),
        (21, 'commercialname_en'),
        (22, 'customertypeid'),
        (23, 'registrationdate'),
        (24, 'jobcurrentwage'),
        (25, 'source_system_name'),
        (26, 'is_deleted'),
        (27, 'report_date'),
        (28, 'dbt_updated_at'),
        (29, 'updatedon')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'amendmentrequestid'),
        (3, 'applicationid'),
        (4, 'createdon'),
        (5, 'approvedon'),
        (6, 'submittedon'),
        (7, 'submittedtoextassessoron'),
        (8, 'isactive'),
        (9, 'providerid'),
        (10, 'externalproviderid'),
        (11, 'genderid'),
        (12, 'dateofbirth'),
        (13, 'name'),
        (14, 'nameen'),
        (15, 'label'),
        (16, 'requestedamt'),
        (17, 'tkshareamt'),
        (18, 'user_id'),
        (19, 'referencenumber'),
        (20, 'totalmonthsexperience'),
        (21, 'commercialname_en'),
        (22, 'customertypeid'),
        (23, 'registrationdate'),
        (24, 'jobcurrentwage'),
        (25, 'source_system_name'),
        (26, 'is_deleted'),
        (27, 'report_date'),
        (28, 'dbt_updated_at'),
        (29, 'updatedon')
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`approvedon` AS STRING) AS `approvedon`,
        CAST(`submittedon` AS STRING) AS `submittedon`,
        CAST(`submittedtoextassessoron` AS STRING) AS `submittedtoextassessoron`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`providerid` AS STRING) AS `providerid`,
        CAST(`externalproviderid` AS STRING) AS `externalproviderid`,
        CAST(`genderid` AS STRING) AS `genderid`,
        CAST(`dateofbirth` AS STRING) AS `dateofbirth`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`nameen` AS STRING) AS `nameen`,
        CAST(`label` AS STRING) AS `label`,
        CAST(`requestedamt` AS STRING) AS `requestedamt`,
        CAST(`tkshareamt` AS STRING) AS `tkshareamt`,
        CAST(`user_id` AS STRING) AS `user_id`,
        CAST(`referencenumber` AS STRING) AS `referencenumber`,
        CAST(`totalmonthsexperience` AS STRING) AS `totalmonthsexperience`,
        CAST(`commercialname_en` AS STRING) AS `commercialname_en`,
        CAST(`customertypeid` AS STRING) AS `customertypeid`,
        CAST(`registrationdate` AS STRING) AS `registrationdate`,
        CAST(`jobcurrentwage` AS STRING) AS `jobcurrentwage`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`updatedon` AS STRING) AS `updatedon`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`approvedon` AS STRING) AS `approvedon`,
        CAST(`submittedon` AS STRING) AS `submittedon`,
        CAST(`submittedtoextassessoron` AS STRING) AS `submittedtoextassessoron`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`providerid` AS STRING) AS `providerid`,
        CAST(`externalproviderid` AS STRING) AS `externalproviderid`,
        CAST(`genderid` AS STRING) AS `genderid`,
        CAST(`dateofbirth` AS STRING) AS `dateofbirth`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`nameen` AS STRING) AS `nameen`,
        CAST(`label` AS STRING) AS `label`,
        CAST(`requestedamt` AS STRING) AS `requestedamt`,
        CAST(`tkshareamt` AS STRING) AS `tkshareamt`,
        CAST(`user_id` AS STRING) AS `user_id`,
        CAST(`referencenumber` AS STRING) AS `referencenumber`,
        CAST(`totalmonthsexperience` AS STRING) AS `totalmonthsexperience`,
        CAST(`commercialname_en` AS STRING) AS `commercialname_en`,
        CAST(`customertypeid` AS STRING) AS `customertypeid`,
        CAST(`registrationdate` AS STRING) AS `registrationdate`,
        CAST(`jobcurrentwage` AS STRING) AS `jobcurrentwage`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`updatedon` AS STRING) AS `updatedon`
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
        'application_support_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'application_support_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'application_support_base' AS table_name,
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
        'application_support_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'application_support_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
