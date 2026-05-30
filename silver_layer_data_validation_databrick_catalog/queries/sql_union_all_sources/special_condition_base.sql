-- Compare bronze-layer query output with silver-layer table output for special_condition_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\special_condition_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\special_condition_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Trino SQL generated from special_condition_base.sql.
-- Final column order aligned to silver_layer_query/special_condition_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : RPT_265_NEOTAMKEEN_OS2_SPECIAL_CONDITIONS

   Description   : This model extracts and transforms special condition data
                   from the NEO2 (OS2) Bronze Layer and loads it into the
                   RPT_265_NEOTAMKEEN_OS2_SPECIAL_CONDITIONS target table
                   as part of the Silver Layer data pipeline.

                   It captures special condition lifecycle details including
                   application linkage, workflow status, approvals, rejection
                   tracking, support area, customer type, and program info.

                   The model also enriches data using multiple reference
                   tables including application, customer, program version,
                   workflow status, and BPM process tracking tables.

   Source Tables : neo2.OSUSR_L68_SPECIALCONDITION
                   neo2.OSUSR_765_SPECIALCONDITIONFULFILMENT
                   neo2.OSUSR_NTP_APPLICATION
                   neo2.OSUSR_398_APPLICATIONSTATUS
                   neo2.OSUSR_3QQ_PROGRAMVERSION
                   neo2.OSUSR_ZMZ_CUSTOMER
                   neo2.OSUSR_ZMZ_COMPANY
                   neo2.OSSYS_BPM_PROCESS
                   neo2.OSSYS_BPM_ACTIVITY
                   neo2.OSSYS_USER

   Target Table  : RPT_265_NEOTAMKEEN_OS2_SPECIAL_CONDITIONS

   Load Type     : Full Load / Reporting Table
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily, mis

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author       | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-05-11   | Elavarasi     | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/
WITH PROCESS AS
(
    SELECT
        SCF.PROCESSID                                                        AS PROCESSID,
        CASE
            WHEN ACT.USER_ID = 0 THEN 'Activity not assigned yet'
            ELSE U.NAME
        END                                                                  AS OWNER,
        ACTDEF.LABEL                                                         AS ACTIVITY_LABEL,
        ROW_NUMBER() OVER (
            PARTITION BY SCF.ID
            ORDER BY ACT.ID DESC
        )                                                                    AS RN,
        ACT.CLOSED                                                           AS CLOSED,
        SCF.BRONZE_CREATED_ON,
        SCF.BRONZE_UPDATED_ON
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_765_SPECIALCONDITIONFULFILMENT SCF
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_PROCESS PRO
        ON PRO.ID = SCF.PROCESSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY ACT
        ON ACT.PROCESS_ID = PRO.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY_DEFINITION ACTDEF
        ON ACT.ACTIVITY_DEF_ID = ACTDEF.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_HMY_ACTIVITYEXTENDED ACT_EXT
        ON ACT_EXT.ID = ACT.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2FH_APPLICATIONASSESSMENTACTIONS ACTIONS
        ON ACTIONS.KEY = ACT_EXT.SELECTEDACTIONKEY
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY_DEF_ROLE ADR
        ON ACTDEF.ID = ADR.ACTIVITY_DEF_ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_ROLE R
        ON ADR.ROLE_ID = R.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER U
        ON ACT.USER_ID = U.ID
    WHERE ACTDEF.KIND = (
        SELECT ID
        FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY_KIND
        WHERE NAME = 'Human Activity'
    )
),

CTE_SPECIAL_CONDITIONS AS
(
    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS date)                AS EXTRACT_DATE,
        PROGVER.COMMERCIALNAME_EN                                           AS PROGRAM_NAME,
        CASE
            WHEN SPCONDTION.REFERENCENUMBER = '' THEN SCF.REFERENCENUMBER
            ELSE SPCONDTION.REFERENCENUMBER
        END                                                                  AS SPECIAL_CONDITION_REQUEST_NO,
        SPCONDTION.ID                                                      AS ID_SPECIAL_CONDITION,
		SPCONDTION.APPLICATIONID,
		SPCONDTION.SUPPORTAREAID,
		SPCONDTION.SPECIALCONDITIONBOID,
		SPCONDTION.SPECIALCONDITIONSTATUSID,
		SPCONDTION.SPECIALCONDITIONTARGETID,
		SPCONDTION.SPECIALCONDITIONLEVELID,
		SPCONDTION.DESCRIPTION,
		SPCONDTION.ISACTIVE,
		SPCONDTION.AMENDREQUESTID,
		SPCONDTION.PARENTSPECIALCONDITIONID,
		SPCONDTION.ACTIVESTATUSID,
		SPCONDTION.REFERENCENUMBER,
		SPCONDTION.FIXEDCONDITIONBOID,
		SPCONDTION.PRIORITY,
        APP.REFERENCENUMBER                                                 AS APPLICATION_NO,
        APP.ID                                                              AS APPLICATION_ID,
        APPWFS.LABEL                                                        AS WORKFLOW_STATUS_APPLICATION,
        SCF_STAT.LABEL                                                      AS WORKFLOW_STATUS_SPECIAL_CONDITION,
        SPCONDTION_STAT.LABEL                                               AS WORKFLOW_STATUS_SPECIAL_CONDITION_DETAILED,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'CMP' THEN 'Enterprise'
            ELSE 'Individual'
        END                                                                  AS CUSTOMER_TYPE,
        CASE
            WHEN CUS.CUSTOMERTYPEID = 'CMP' THEN UPPER(LTRIM(RTRIM(CUS.NAMEEN)))
            ELSE NULL
        END                                                                  AS COMMERCIAL_NAME,
        CASE
            WHEN CUS.CUSTOMERTYPEID = 'CMP' THEN LTRIM(RTRIM(CMP.CODE))
            ELSE NULL
        END                                                                  AS CR_LICENSE_NO,
        SPECIALCONDITIONTYPE.LABEL                                          AS SPECIAL_CONDITION_TYPE,
        SPECIALCONDITIONLEVEL.LABEL                                         AS SPECIAL_CONDITION_LEVEL,
        SPCONDTIONBO.DESCRIPTION_EN                                         AS SPECIAL_CONDITION_DESCRIPTION,
        SPCON_TAR.LABEL                                                     AS SPECIAL_CONDITION_TARGET,
        SPCONDTION.TARGETVALUE                                              AS SPECIAL_CONDITION_TARGET_VALUE,
        SCF.REMARKSCUSTOMER                                                 AS SPECIAL_CONDITION_CUSTOMER_COMMENT,
        SCF.REMARKSTK                                                       AS SPECIAL_CONDITION_TAMKEEN_COMMENT,
        SUPPAREA.LABEL                                                      AS SUPPORT_AREA,
        PROCESS.OWNER                                                       AS OWNER,
        CASE
            WHEN SCF.SUBMISSIONDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE SCF.SUBMISSIONDATE + INTERVAL '3' HOUR
        END                                                                 AS SUBMITTED_ON,
        PROCESS_ASSESSOR.CLOSED + INTERVAL '3' HOUR                        AS VERIFIED_ON,
        PROCESS_ASSESSOR.OWNER                                              AS VERIFIED_BY,
        PROCESS_APPROVAL.CLOSED + INTERVAL '3' HOUR                        AS APPROVED_ON,
        PROCESS_APPROVAL.OWNER                                              AS APPROVED_BY,
        CASE
            WHEN SCF_STAT.LABEL = 'Rejected' THEN SCF.UPDATEDON + INTERVAL '3' HOUR
            ELSE NULL
        END                                                                  AS REJECTED_ON,
        CASE
            WHEN SCF_STAT.LABEL = 'Rejected' THEN PROCESS_ASSESSOR.OWNER
            ELSE NULL
        END                                                                  AS REJECTED_BY,
        SCF.UPDATEDON + INTERVAL '3' HOUR                                     AS DECISION_DATE,
        CASE
            WHEN SPCONDTION.ISACTIVE THEN 'TRUE'
            ELSE 'FALSE'
        END                                                                  AS IS_ACTIVE,
        FALSE                                                                AS IS_DELETED,
        'NEO2'                                                               AS SOURCE_SYSTEM_NAME,
        SCF.BRONZE_CREATED_ON as createdon,
        SCF.BRONZE_UPDATED_ON as updatedon,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)             AS DBT_UPDATED_AT
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_L68_SPECIALCONDITION SPCONDTION
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_765_SPECIALCONDITIONFULFILMENT SCF
        ON SCF.SPECIALCONDITIONID = SPCONDTION.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_765_MONITORINGSTATUS SCF_STAT
        ON SCF_STAT.CODE = SCF.MONITORINGSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_L68_SPECIALCONDITIONSTATUS SPCONDTION_STAT
        ON SPCONDTION.SPECIALCONDITIONSTATUSID = SPCONDTION_STAT.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION APP
        ON SPCONDTION.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS APPWFS
        ON APPWFS.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION PROGVER
        ON PROGVER.ID = APP.PROGRAMVERSIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APP.ID = APPCUS.APPLICATIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_COMPANY CMP
        ON CUS.ID = CMP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_SUPPORTAREA SUPPAREA
        ON SUPPAREA.CODE = SPCONDTION.SUPPORTAREAID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_L68_SPECIALCONDITIONTARGET SPCON_TAR
        ON SPCONDTION.SPECIALCONDITIONTARGETID = SPCON_TAR.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_T8J_SPECIALCONDITIONBO SPCONDTIONBO
        ON SPCONDTION.SPECIALCONDITIONBOID = SPCONDTIONBO.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_T8J_SPECIALCONDITIONTYPE SPECIALCONDITIONTYPE
        ON SPECIALCONDITIONTYPE.ID = SPCONDTIONBO.SPECIALCONDITIONTYPECODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_T8J_SPECIALCONDITIONLEVEL SPECIALCONDITIONLEVEL
        ON SPECIALCONDITIONLEVEL.CODE = SPCONDTIONBO.SPECIALCONDITIONLEVELCODE
    LEFT JOIN PROCESS
        ON PROCESS.PROCESSID = SCF.PROCESSID
        AND PROCESS.RN = 1
    LEFT JOIN PROCESS PROCESS_ASSESSOR
        ON PROCESS_ASSESSOR.PROCESSID = SCF.PROCESSID
        AND PROCESS_ASSESSOR.ACTIVITY_LABEL = 'SC Agent'
    LEFT JOIN PROCESS PROCESS_APPROVAL
        ON PROCESS_APPROVAL.PROCESSID = SCF.PROCESSID
        AND PROCESS_APPROVAL.ACTIVITY_LABEL = 'SC Director'
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER U
        ON U.USERNAME = SCF.UPDATEDBY

), FINAL AS
(
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID_SPECIAL_CONDITION ORDER BY UPDATEDON DESC NULLS LAST, CREATEDON DESC NULLS LAST) AS RNK
    FROM CTE_SPECIAL_CONDITIONS
)

SELECT
    TRY_CAST(NULLIF(CAST(EXTRACT_DATE AS STRING), '') AS DATE) AS extract_date,
    PROGRAM_NAME AS program_name,
    SPECIAL_CONDITION_REQUEST_NO AS special_condition_request_no,
    TRY_CAST(NULLIF(CAST(ID_SPECIAL_CONDITION AS STRING), '') AS BIGINT) AS id_special_condition,
    APPLICATION_NO AS application_no,
    TRY_CAST(NULLIF(CAST(APPLICATION_ID AS STRING), '') AS BIGINT) AS application_id,
    WORKFLOW_STATUS_APPLICATION AS workflow_status_application,
    WORKFLOW_STATUS_SPECIAL_CONDITION AS workflow_status_special_condition,
    WORKFLOW_STATUS_SPECIAL_CONDITION_DETAILED AS workflow_status_special_condition_detailed,
    CUSTOMER_TYPE AS customer_type,
    COMMERCIAL_NAME AS commercial_name,
    CR_LICENSE_NO AS cr_license_no,
    SPECIAL_CONDITION_TYPE AS special_condition_type,
    SPECIAL_CONDITION_LEVEL AS special_condition_level,
    SPECIAL_CONDITION_DESCRIPTION AS special_condition_description,
    SPECIAL_CONDITION_TARGET AS special_condition_target,
    SPECIAL_CONDITION_TARGET_VALUE AS special_condition_target_value,
    SPECIAL_CONDITION_CUSTOMER_COMMENT AS special_condition_customer_comment,
    SPECIAL_CONDITION_TAMKEEN_COMMENT AS special_condition_tamkeen_comment,
    SUPPORT_AREA AS support_area,
    OWNER AS owner,
    TRY_CAST(NULLIF(CAST(SUBMITTED_ON AS STRING), '') AS TIMESTAMP) AS submitted_on,
    TRY_CAST(NULLIF(CAST(VERIFIED_ON AS STRING), '') AS TIMESTAMP) AS verified_on,
    VERIFIED_BY AS verified_by,
    TRY_CAST(NULLIF(CAST(APPROVED_ON AS STRING), '') AS TIMESTAMP) AS approved_on,
    APPROVED_BY AS approved_by,
    TRY_CAST(NULLIF(CAST(REJECTED_ON AS STRING), '') AS TIMESTAMP) AS rejected_on,
    REJECTED_BY AS rejected_by,
    TRY_CAST(NULLIF(CAST(DECISION_DATE AS STRING), '') AS TIMESTAMP) AS decision_date,
    IS_ACTIVE AS is_active,
    IS_DELETED AS is_deleted,
    UPPER(NULLIF(TRIM(CAST(SOURCE_SYSTEM_NAME AS STRING)), '')) AS source_system_name,
    TRY_CAST(NULLIF(CAST(createdon AS STRING), '') AS TIMESTAMP) AS createdon,
    TRY_CAST(NULLIF(CAST(updatedon AS STRING), '') AS TIMESTAMP) AS updatedon,
    TRY_CAST(NULLIF(CAST(DBT_UPDATED_AT AS STRING), '') AS TIMESTAMP) AS dbt_updated_at
FROM FINAL app
WHERE RNK = 1
),

silver_layer AS (
SELECT
    extract_date,
    program_name,
    special_condition_request_no,
    id_special_condition,
    application_no,
    application_id,
    workflow_status_application,
    workflow_status_special_condition,
    workflow_status_special_condition_detailed,
    customer_type,
    commercial_name,
    cr_license_no,
    special_condition_type,
    special_condition_level,
    special_condition_description,
    special_condition_target,
    special_condition_target_value,
    special_condition_customer_comment,
    special_condition_tamkeen_comment,
    support_area,
    owner,
    submitted_on,
    verified_on,
    verified_by,
    approved_on,
    approved_by,
    rejected_on,
    rejected_by,
    decision_date,
    is_active,
    is_deleted,
    source_system_name,
    createdon,
    updatedon,
    dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.special_condition_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'program_name'),
        (3, 'special_condition_request_no'),
        (4, 'id_special_condition'),
        (5, 'application_no'),
        (6, 'application_id'),
        (7, 'workflow_status_application'),
        (8, 'workflow_status_special_condition'),
        (9, 'workflow_status_special_condition_detailed'),
        (10, 'customer_type'),
        (11, 'commercial_name'),
        (12, 'cr_license_no'),
        (13, 'special_condition_type'),
        (14, 'special_condition_level'),
        (15, 'special_condition_description'),
        (16, 'special_condition_target'),
        (17, 'special_condition_target_value'),
        (18, 'special_condition_customer_comment'),
        (19, 'special_condition_tamkeen_comment'),
        (20, 'support_area'),
        (21, 'owner'),
        (22, 'submitted_on'),
        (23, 'verified_on'),
        (24, 'verified_by'),
        (25, 'approved_on'),
        (26, 'approved_by'),
        (27, 'rejected_on'),
        (28, 'rejected_by'),
        (29, 'decision_date'),
        (30, 'is_active'),
        (31, 'is_deleted'),
        (32, 'source_system_name'),
        (33, 'createdon'),
        (34, 'updatedon'),
        (35, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'program_name'),
        (3, 'special_condition_request_no'),
        (4, 'id_special_condition'),
        (5, 'application_no'),
        (6, 'application_id'),
        (7, 'workflow_status_application'),
        (8, 'workflow_status_special_condition'),
        (9, 'workflow_status_special_condition_detailed'),
        (10, 'customer_type'),
        (11, 'commercial_name'),
        (12, 'cr_license_no'),
        (13, 'special_condition_type'),
        (14, 'special_condition_level'),
        (15, 'special_condition_description'),
        (16, 'special_condition_target'),
        (17, 'special_condition_target_value'),
        (18, 'special_condition_customer_comment'),
        (19, 'special_condition_tamkeen_comment'),
        (20, 'support_area'),
        (21, 'owner'),
        (22, 'submitted_on'),
        (23, 'verified_on'),
        (24, 'verified_by'),
        (25, 'approved_on'),
        (26, 'approved_by'),
        (27, 'rejected_on'),
        (28, 'rejected_by'),
        (29, 'decision_date'),
        (30, 'is_active'),
        (31, 'is_deleted'),
        (32, 'source_system_name'),
        (33, 'createdon'),
        (34, 'updatedon'),
        (35, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`special_condition_request_no` AS STRING) AS `special_condition_request_no`,
        CAST(`id_special_condition` AS STRING) AS `id_special_condition`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`workflow_status_application` AS STRING) AS `workflow_status_application`,
        CAST(`workflow_status_special_condition` AS STRING) AS `workflow_status_special_condition`,
        CAST(`workflow_status_special_condition_detailed` AS STRING) AS `workflow_status_special_condition_detailed`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`special_condition_type` AS STRING) AS `special_condition_type`,
        CAST(`special_condition_level` AS STRING) AS `special_condition_level`,
        CAST(`special_condition_description` AS STRING) AS `special_condition_description`,
        CAST(`special_condition_target` AS STRING) AS `special_condition_target`,
        CAST(`special_condition_target_value` AS STRING) AS `special_condition_target_value`,
        CAST(`special_condition_customer_comment` AS STRING) AS `special_condition_customer_comment`,
        CAST(`special_condition_tamkeen_comment` AS STRING) AS `special_condition_tamkeen_comment`,
        CAST(`support_area` AS STRING) AS `support_area`,
        CAST(`owner` AS STRING) AS `owner`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`verified_on` AS STRING) AS `verified_on`,
        CAST(`verified_by` AS STRING) AS `verified_by`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`approved_by` AS STRING) AS `approved_by`,
        CAST(`rejected_on` AS STRING) AS `rejected_on`,
        CAST(`rejected_by` AS STRING) AS `rejected_by`,
        CAST(`decision_date` AS STRING) AS `decision_date`,
        CAST(`is_active` AS STRING) AS `is_active`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`special_condition_request_no` AS STRING) AS `special_condition_request_no`,
        CAST(`id_special_condition` AS STRING) AS `id_special_condition`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`workflow_status_application` AS STRING) AS `workflow_status_application`,
        CAST(`workflow_status_special_condition` AS STRING) AS `workflow_status_special_condition`,
        CAST(`workflow_status_special_condition_detailed` AS STRING) AS `workflow_status_special_condition_detailed`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`special_condition_type` AS STRING) AS `special_condition_type`,
        CAST(`special_condition_level` AS STRING) AS `special_condition_level`,
        CAST(`special_condition_description` AS STRING) AS `special_condition_description`,
        CAST(`special_condition_target` AS STRING) AS `special_condition_target`,
        CAST(`special_condition_target_value` AS STRING) AS `special_condition_target_value`,
        CAST(`special_condition_customer_comment` AS STRING) AS `special_condition_customer_comment`,
        CAST(`special_condition_tamkeen_comment` AS STRING) AS `special_condition_tamkeen_comment`,
        CAST(`support_area` AS STRING) AS `support_area`,
        CAST(`owner` AS STRING) AS `owner`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`verified_on` AS STRING) AS `verified_on`,
        CAST(`verified_by` AS STRING) AS `verified_by`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`approved_by` AS STRING) AS `approved_by`,
        CAST(`rejected_on` AS STRING) AS `rejected_on`,
        CAST(`rejected_by` AS STRING) AS `rejected_by`,
        CAST(`decision_date` AS STRING) AS `decision_date`,
        CAST(`is_active` AS STRING) AS `is_active`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
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
        'special_condition_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'special_condition_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'special_condition_base' AS table_name,
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
        'special_condition_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'special_condition_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
