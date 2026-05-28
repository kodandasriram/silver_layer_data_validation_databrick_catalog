-- Compare bronze-layer query output with silver-layer table output for wage_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\union of os2_os1_mis\wage_base_os2_os1_mis_union_bronze_layer.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\wage_base_silver_layer.sql

WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for wage_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: wage_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH wage_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
=================================================================================================

Name        : WAGE_BASE
Description : This model extracts wage records from the NEO2 bronze layer and
              loads them into the Silver layer. It captures details of new job
              levels, titles, departments, responsibilities, requested increments,
              wages, stipends, placement information, supervisors, and wage tracking.
              Incremental logic ensures only new or updated records are processed,
              while a soft delete mechanism marks missing records as deleted.

Source Tables :
    - neo2.OSUSR_VYW_WAGE
    - neo2.OSUSR_3QQ_WAGETRACK
    - neo2.OSUSR_MM5_COUNTRY4
    - neo2.OSUSR_VYW_FREQUENCYOFPAYMENT
    - neo2.OSUSR_2DA_SALARYSTATUS
    - neo2.OSUSR_2DA_JOBLEVEL
    - neo2.OSUSR_VYW_WAGESTIPENDTYPE

Target Table :
    - wage_base_os2

Load Type    : Incremental (MERGE)
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily

Logic :
    - Load only new or updated records using CREATEDON / UPDATEDON timestamps
    - Enrich with descriptive labels from related reference tables (job level, wage track, frequency of payment, salary status, country, stipend type)
    - Maintain schema alignment with sync_all_columns
    - Implement soft delete via post_hook (records missing in source marked deleted)
    - Add audit columns (DBT_UPDATED_AT, SOURCE_SYSTEM_NAME)

Revision History:
--------------------------------------------------------------
Version | Date       | Author | Description
--------------------------------------------------------------
1.0     | 2026-03-25 | Siva    | Initial version

=================================================================================================
*/
with source_cte as (
select
    a.APPLICATIONSUPPORTID,
    a.STARTDATE,
    a.ENDDATE,
    JL.CODE as NEWJOBLEVELID,
    JL.LABEL AS NEWJOBLEVEL,
    a.NEWTITLE,
    a.NEWDEPARTMENT,
    a.NEWRESPONSABILITIES,
    a.REQUESTEDINCREMENTAMOUNT,
    a.NEWWAGE,
    a.REQUESTEDSTIPEND,
    C.COUNTRYNAME AS PLACEMENTLOCATION,
    a.HOSTORGANIZATIONNAME,
    a.PLACEMENTJOBTITLE,
    a.PLACEMENTJOBRESPONSABILITIES,
    a.PLACEMENTSKILLSANDKNOWLEDGE,
    a.DIRECTSUPERVISORNAME,
    a.DIRECTSUPERVISORMOBILEPREFIX,
    a.DIRECTSUPERVISORCONTACTNUMBE,
    a.PLACEMENTSTARTDATE,
    a.PLACEMENTENDDATE,
    a.TOTALDURATION,
    a.TKSHAREAMT,
    a.CUSTOMERSHAREAMT,
    FOP.LABEL AS FREQUENCYOFPAYMENT,
    a.ISELIGIBLE,
    a.TKSHAREUNAMT,
    WT.CODE as WAGETRACKID,
    WT.LABEL AS WAGETRACK,
    SS.CODE as NEWWAGESALARYSTATUSID,
    SS.LABEL AS NEWWAGESALARYSTATUS,
    a.NEWWAGESOURCEINDICATOR,
    a.STIPENDTRAINING_PROVIDERNAME,
    a.STIPENDTRAINING_PROGRAMID,
    a.STIPENDTRAINING_PROGRAMNAME,
    a.INCREMENTPERCENTAGE,
    a.TYPEOFPLEDGEID,
    a.EXTRAS,
    a.ISNEWWAGEABOVE1800,
    a.CREATEDON,
    a.UPDATEDON,
    FALSE AS IS_DELETED,
    'NEO2' as SOURCE_SYSTEM_NAME,
    cast(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT,
    a.CURRENTWAGEOVERRIDE,
    a.NEWWAGEOVERRIDE,
    ROW_NUMBER() OVER (PARTITION BY a.APPLICATIONSUPPORTID ORDER BY a.UPDATEDON DESC NULLS LAST, a.CREATEDON DESC NULLS LAST) AS RNK
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VYW_WAGE a
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_WAGETRACK WT
    ON CAST(WT.CODE AS VARCHAR) = CAST(a.WAGETRACKID AS VARCHAR)
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_COUNTRY4 C
    ON CAST(C.ID AS VARCHAR) = CAST(a.PLACEMENTLOCATION AS VARCHAR)
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VYW_FREQUENCYOFPAYMENT FOP
    ON CAST(FOP.CODE AS VARCHAR) = CAST(a.FREQUENCYOFPAYMENT AS VARCHAR)
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_SALARYSTATUS SS
    ON CAST(SS.CODE AS VARCHAR) = CAST(a.NEWWAGESALARYSTATUSID AS VARCHAR)
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_JOBLEVEL JL
    ON CAST(JL.CODE AS VARCHAR) = CAST(a.NEWJOBLEVELID AS VARCHAR)
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VYW_WAGESTIPENDTYPE WST
    ON WST.WAGEID = a.APPLICATIONSUPPORTID
)

SELECT
  TRY_CAST(NULLIF(CAST(APPLICATIONSUPPORTID AS VARCHAR), '') AS BIGINT) AS applicationsupportid,
TRY_CAST(NULLIF(CAST(STARTDATE AS VARCHAR), '') AS TIMESTAMP) AS startdate,
TRY_CAST(NULLIF(CAST(ENDDATE AS VARCHAR), '') AS TIMESTAMP) AS enddate,
NEWJOBLEVELID AS newjoblevelid,
NEWJOBLEVEL AS newjoblevel,
NEWTITLE AS newtitle,
NEWDEPARTMENT AS newdepartment,
NEWRESPONSABILITIES AS newresponsabilities,
REQUESTEDINCREMENTAMOUNT AS requestedincrementamount,
NEWWAGE AS newwage,
TRY_CAST(NULLIF(CAST(REQUESTEDSTIPEND AS VARCHAR), '') AS BIGINT) AS requestedstipend,
PLACEMENTLOCATION AS placementlocation,
HOSTORGANIZATIONNAME AS hostorganizationname,
PLACEMENTJOBTITLE AS placementjobtitle,
PLACEMENTJOBRESPONSABILITIES AS placementjobresponsabilities,
PLACEMENTSKILLSANDKNOWLEDGE AS placementskillsandknowledge,
DIRECTSUPERVISORNAME AS directsupervisorname,
DIRECTSUPERVISORMOBILEPREFIX AS directsupervisormobileprefix,
DIRECTSUPERVISORCONTACTNUMBE AS directsupervisorcontactnumbe,
TRY_CAST(NULLIF(CAST(PLACEMENTSTARTDATE AS VARCHAR), '') AS TIMESTAMP) AS placementstartdate,
TRY_CAST(NULLIF(CAST(PLACEMENTENDDATE AS VARCHAR), '') AS TIMESTAMP) AS placementenddate,
TRY_CAST(NULLIF(CAST(TOTALDURATION AS VARCHAR), '') AS BIGINT) AS totalduration,
TKSHAREAMT AS tkshareamt,
CUSTOMERSHAREAMT AS customershareamt,
FREQUENCYOFPAYMENT AS frequencyofpayment,
ISELIGIBLE AS iseligible,
TKSHAREUNAMT AS tkshareunamt,
WAGETRACKID AS wagetrackid,
WAGETRACK AS wagetrack,
NEWWAGESALARYSTATUSID AS newwagesalarystatusid,
NEWWAGESALARYSTATUS AS newwagesalarystatus,
NEWWAGESOURCEINDICATOR AS newwagesourceindicator,
STIPENDTRAINING_PROVIDERNAME AS stipendtraining_providername,
TRY_CAST(NULLIF(CAST(STIPENDTRAINING_PROGRAMID AS VARCHAR), '') AS BIGINT) AS stipendtraining_programid,
STIPENDTRAINING_PROGRAMNAME AS stipendtraining_programname,
INCREMENTPERCENTAGE AS incrementpercentage,
TYPEOFPLEDGEID AS typeofpledgeid,
EXTRAS AS extras,
ISNEWWAGEABOVE1800 AS isnewwageabove1800,
TRY_CAST(NULLIF(CAST(CREATEDON AS VARCHAR), '') AS TIMESTAMP) AS createdon,
TRY_CAST(NULLIF(CAST(UPDATEDON AS VARCHAR), '') AS TIMESTAMP) AS updatedon,
IS_DELETED AS is_deleted,
UPPER(NULLIF(TRIM(CAST(SOURCE_SYSTEM_NAME AS VARCHAR)), '')) AS source_system_name,
TRY_CAST(NULLIF(CAST(DBT_UPDATED_AT AS VARCHAR), '') AS TIMESTAMP) AS dbt_updated_at,
CURRENTWAGEOVERRIDE AS currentwageoverride,
NEWWAGEOVERRIDE AS newwageoverride
from source_cte a
WHERE rnk = 1
),
wage_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR) AS option_key,
        max(sm.value) AS option_value
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".STRINGMAP sm
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR)
),
option_set_map AS (
    SELECT map_agg(option_key, option_value) AS option_values
    FROM option_set_values
)
/*
============================================================================
SILVER_WAGE_MIS.SQL
============================================================================
PER-SOURCE INTERMEDIATE SILVER MODEL FOR THE WAGE DOMAIN â€” MIS ONLY.

SOURCES (WAGE DOMAIN ENTITIES):
  â˜… TWS_WAGESUBSIDY                â€” WAGE SUBSIDY APPLICATIONS (PARENT)
    TWS_PAYSUBSIDY                 â€” PAY SUBSIDY INSTALLMENTS (CHILD OF WAGESUBSIDY)
  â˜… TWS_WAGEINCREMENT              â€” WAGE INCREMENT APPLICATIONS (PARENT)
    TWS_PAYINCREMENT               â€” PAY INCREMENT INSTALLMENTS (CHILD OF WAGEINCREMENT)
    TWS_WAGES_SUPPORT_CONFIGURATION â€” CONFIG / SEGMENT REFERENCE

REFERENCE SPS:
  - RPT-045_WAGE_SUBSIDY_APPLICATIONS
  - RPT-046_PAY_SUBSIDIES
  - RPT-047_WAGE_INCREMENT_APPLICATIONS
  - RPT-048_PAY_INCREMENTS
============================================================================
*/

-- ============================================================================
-- SUB-TYPE 1: WAGE SUBSIDY APPLICATIONS
-- ============================================================================

SELECT
    'WAGE_SUBSIDY' AS wage_subtype,
    'TWS_WAGESUBSIDY' AS mis_source_table,

    -- IDENTIFIERS
    CAST(WS.TWS_WAGESUBSIDYID AS VARCHAR)                AS wage_id,
    WS.TWS_NAME                                          AS wage_application_no,

    -- FOREIGN KEYS
    CAST(WS.TWS_EMPLOYEE_APPLICATION AS VARCHAR)         AS employee_application_id,
    CAST(WS.TWS_INDIVIDUAL_REFERENCE AS VARCHAR)         AS individual_id,
    CAST(WS.TWS_SEGMENT_REFERENCE AS VARCHAR)            AS segment_reference_id,
    CAST(WS.TWS_PRODUCT AS VARCHAR)                      AS product_pid_id,
    CAST(WS.TMKN_ESAPPLICATION AS VARCHAR)               AS es_application_id,
    CAST(NULL AS VARCHAR)                                AS parent_wage_id,
    CAST(NULL AS VARCHAR)                                AS payment_request_id,

    -- DISPLAY NAMES
    WS.TWS_COMPANY                                       AS company_name_denorm,
    WS.TWS_EMPLOYEE_APPLICATION                          AS employee_application_name,
    WS.TWS_ENTERPRISE_APPLICATION                        AS enterprise_application_name,
    WS.TWS_SEGMENT_REFERENCE                             AS segment_reference_name,
    WS.TWS_PRODUCT                                       AS product_pid_name,
    WS.TWS_SPONSORSHIP                                   AS sponsorship,
    WS.OWNERID                                           AS owner_name,
    WS.TMKN_CREATEDBYPARTNER                             AS created_by_partner,

    -- WAGE DETAILS
    WS.TWS_CURRENT_WAGE                                  AS current_wage,
    CAST(NULL AS DECIMAL(18,2))                          AS pay_amount,
    CAST(NULL AS DECIMAL(18,2))                          AS pay_amount_old,
    CAST(NULL AS VARCHAR)                                AS pay_year,
    CAST(NULL AS DATE)                                   AS pay_due_date,
    CAST(NULL AS INTEGER)                                AS pay_no_of_months,

    -- DATES
    WS.TWS_SUPPORT_START_DATE                            AS support_start_date,
    WS.TWS_SUBMITTEDON                                   AS submitted_on,
    WS.TWS_APPROVEDON                                    AS approved_on,
    WS.CREATEDON                                         AS created_on,

    -- WORKFLOW / STATUS
    CASE WHEN ws.tws_workflow_status IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('tws_workflow_status') || '|' || CAST(ws.tws_workflow_status AS VARCHAR)) END AS workflow_status,
    CASE WHEN ws.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('statecode') || '|' || CAST(ws.statecode AS VARCHAR)) END AS state,
    CASE WHEN ws.tws_support_level IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('tws_support_level') || '|' || CAST(ws.tws_support_level AS VARCHAR)) END AS support_level,
    CASE WHEN ws.tws_recommendation_to IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('tws_recommendation_to') || '|' || CAST(ws.tws_recommendation_to AS VARCHAR)) END AS recommendation_to,
    CASE WHEN ws.tws_payment_structure IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('tws_payment_structure') || '|' || CAST(ws.tws_payment_structure AS VARCHAR)) END AS payment_structure,
    CASE WHEN ws.tws_disapprove_reason IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('tws_disapprove_reason') || '|' || CAST(ws.tws_disapprove_reason AS VARCHAR)) END AS disapprove_reason,
    CASE WHEN ws.tws_checker_recommendation_to IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy') || '|' || lower('tws_checker_recommendation_to') || '|' || CAST(ws.tws_checker_recommendation_to AS VARCHAR)) END AS checker_recommendation_to,

    -- CONFIGURATION
    CO.TWS_CAP_AMOUNT                                    AS config_cap_amount,
    CASE WHEN co.tws_back_dated IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wages_support_configuration') || '|' || lower('tws_back_dated') || '|' || CAST(co.tws_back_dated AS VARCHAR)) END AS config_back_dated,
    CASE WHEN co.tws_created_by_mol IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wages_support_configuration') || '|' || lower('tws_created_by_mol') || '|' || CAST(co.tws_created_by_mol AS VARCHAR)) END AS config_created_by_mol,
    CASE WHEN co.tws_grace_period IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wages_support_configuration') || '|' || lower('tws_grace_Period') || '|' || CAST(co.tws_grace_period AS VARCHAR)) END AS config_grace_period,
    CASE WHEN co.tws_jobseekers IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wages_support_configuration') || '|' || lower('tws_jobseekers') || '|' || CAST(co.tws_jobseekers AS VARCHAR)) END AS config_jobseekers,
    CASE WHEN co.tws_terminated_employee IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wages_support_configuration') || '|' || lower('tws_terminated_employee') || '|' || CAST(co.tws_terminated_employee AS VARCHAR)) END AS config_terminated_employee,

    -- MIGRATION
    WS.TMKN_ISMIGRATED                                   AS is_migrated,
    NULL AS tws_required_increment,
    NULL AS tws_new_wage,
    -- STANDARD AUDIT
    'MIS'                                                AS source_system_name,
    FALSE                                                AS is_deleted,
    CURRENT_DATE                                         AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_WAGESUBSIDYBASE WS

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_WAGES_SUPPORT_CONFIGURATIONBASE CO
    ON CO.TWS_WAGES_SUPPORT_CONFIGURATIONID = WS.TWS_SEGMENT_REFERENCE

UNION ALL

-- ============================================================================
-- SUB-TYPE 2: PAY SUBSIDY INSTALLMENTS
-- ============================================================================

SELECT
    'PAY_SUBSIDY' AS wage_subtype,
    'TWS_PAYSUBSIDY' AS mis_source_table,

    -- IDENTIFIERS
    CAST(PS.TWS_PAYSUBSIDYID AS VARCHAR)                 AS wage_id,
    PS.TWS_NAME                                          AS wage_application_no,

    -- FOREIGN KEYS
    CAST(WS.TWS_EMPLOYEE_APPLICATION AS VARCHAR)         AS employee_application_id,
    CAST(WS.TWS_INDIVIDUAL_REFERENCE AS VARCHAR)         AS individual_id,
    CAST(WS.TWS_SEGMENT_REFERENCE AS VARCHAR)            AS segment_reference_id,
    CAST(WS.TWS_PRODUCT AS VARCHAR)                      AS product_pid_id,
    CAST(WS.TMKN_ESAPPLICATION AS VARCHAR)               AS es_application_id,
    CAST(WS.TWS_WAGESUBSIDYID AS VARCHAR)                AS parent_wage_id,
    CAST(PS.TWS_PAYMENTREQUEST AS VARCHAR)               AS payment_request_id,

    -- DISPLAY NAMES
    WS.TWS_COMPANY                                       AS company_name_denorm,
    WS.TWS_EMPLOYEE_APPLICATION                          AS employee_application_name,
    WS.TWS_ENTERPRISE_APPLICATION                        AS enterprise_application_name,
    WS.TWS_SEGMENT_REFERENCE                             AS segment_reference_name,
    WS.TWS_PRODUCT                                       AS product_pid_name,
    WS.TWS_SPONSORSHIP                                   AS sponsorship,
    PS.OWNERID                                           AS owner_name,
    WS.TMKN_CREATEDBYPARTNER                             AS created_by_partner,

    -- WAGE DETAILS
    WS.TWS_CURRENT_WAGE                                  AS current_wage,
    PS.TWS_AMOUNT                                        AS pay_amount,
    PS.TWS_AMOUNT_OLD                                    AS pay_amount_old,
    PS.TWS_YEAR                                          AS pay_year,
    PS.TWS_DUE_DATE                                      AS pay_due_date,
    PS.TWS_NO_OF_MONTHS                                  AS pay_no_of_months,

    -- DATES
    WS.TWS_SUPPORT_START_DATE                            AS support_start_date,
    WS.TWS_SUBMITTEDON                                   AS submitted_on,
    WS.TWS_APPROVEDON                                    AS approved_on,
    PS.CREATEDON                                         AS created_on,

    -- WORKFLOW / STATUS
    CASE WHEN ps.tws_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_paysubsidy') || '|' || lower('tws_workflowstatus') || '|' || CAST(ps.tws_workflowstatus AS VARCHAR)) END AS workflow_status,
    CASE WHEN ps.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_paysubsidy') || '|' || lower('statuscode') || '|' || CAST(ps.statuscode AS VARCHAR)) END AS state,
    CAST(NULL AS VARCHAR)                                AS support_level,
    CAST(NULL AS VARCHAR)                                AS recommendation_to,
    CAST(NULL AS VARCHAR)                                AS payment_structure,
    CAST(NULL AS VARCHAR)                                AS disapprove_reason,
    CAST(NULL AS VARCHAR)                                AS checker_recommendation_to,

    -- CONFIGURATION
    CAST(NULL AS DECIMAL(18,2))                          AS config_cap_amount,
    CAST(NULL AS VARCHAR)                                AS config_back_dated,
    CAST(NULL AS VARCHAR)                                AS config_created_by_mol,
    CAST(NULL AS VARCHAR)                                AS config_grace_period,
    CAST(NULL AS VARCHAR)                                AS config_jobseekers,
    CAST(NULL AS VARCHAR)                                AS config_terminated_employee,

    -- MIGRATION
    PS.TMKN_ISMIGRATED                                   AS is_migrated,
    NULL AS tws_required_increment,
    NULL AS tws_new_wage,
    -- STANDARD AUDIT
    'MIS'                                                AS source_system_name,
    FALSE                                                AS is_deleted,
    CURRENT_DATE                                         AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_PAYSUBSIDYBASE PS

INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_WAGESUBSIDYBASE WS
    ON WS.TWS_WAGESUBSIDYID = PS.TWS_WAGE_SUBSIDY_REFERENCE

UNION ALL

-- ============================================================================
-- SUB-TYPE 3: WAGE INCREMENT APPLICATIONS
-- ============================================================================

SELECT
    'WAGE_INCREMENT' AS wage_subtype,
    'TWS_WAGEINCREMENT' AS mis_source_table,

    -- IDENTIFIERS
    CAST(WI.TWS_WAGEINCREMENTID AS VARCHAR)              AS wage_id,
    WI.TWS_NAME                                          AS wage_application_no,

    -- FOREIGN KEYS
    CAST(WI.TWS_EMPLOYEE_APPLICATION AS VARCHAR)         AS employee_application_id,
    CAST(WI.TWS_INDIVIDUAL_REFERENCE AS VARCHAR)         AS individual_id,
    CAST(WI.TWS_SEGMENT_REFERENCE AS VARCHAR)            AS segment_reference_id,
    CAST(WI.TWS_PRODUCT AS VARCHAR)                      AS product_pid_id,
    CAST(NULL AS VARCHAR)                                AS es_application_id,
    CAST(NULL AS VARCHAR)                                AS parent_wage_id,
    CAST(NULL AS VARCHAR)                                AS payment_request_id,

    -- DISPLAY NAMES
    WI.TWS_COMPANY                                       AS company_name_denorm,
    WI.TWS_EMPLOYEE_APPLICATION                          AS employee_application_name,
    WI.TWS_ENTERPRISE_APPLICATION                        AS enterprise_application_name,
    WI.TWS_SEGMENT_REFERENCE                             AS segment_reference_name,
    WI.TWS_PRODUCT                                       AS product_pid_name,
    WI.TWS_SPONSORSHIP                                   AS sponsorship,
    WI.OWNERID                                           AS owner_name,
    WI.TMKN_CREATEDBYPARTNER                             AS created_by_partner,

    -- WAGE DETAILS
    WI.TWS_CURRENT_WAGE                                  AS current_wage,
    CAST(NULL AS DECIMAL(18,2))                          AS pay_amount,
    CAST(NULL AS DECIMAL(18,2))                          AS pay_amount_old,
    CAST(NULL AS VARCHAR)                                AS pay_year,
    CAST(NULL AS DATE)                                   AS pay_due_date,
    CAST(NULL AS INTEGER)                                AS pay_no_of_months,

    -- DATES
    WI.TWS_SUPPORT_START_DATE                            AS support_start_date,
    WI.TWS_SUBMITTEDON                                   AS submitted_on,
    WI.TWS_APPROVEDON                                    AS approved_on,
    WI.CREATEDON                                         AS created_on,

    -- WORKFLOW / STATUS
    CASE WHEN wi.tws_workflow_status IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wageincrement') || '|' || lower('tws_workflow_status') || '|' || CAST(wi.tws_workflow_status AS VARCHAR)) END AS workflow_status,
    CASE WHEN wi.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wageincrement') || '|' || lower('statecode') || '|' || CAST(wi.statecode AS VARCHAR)) END AS state,
    CAST(NULL AS VARCHAR)                                AS support_level,
    CAST(NULL AS VARCHAR)                                AS recommendation_to,
    CAST(NULL AS VARCHAR)                                AS payment_structure,
    CASE WHEN wi.tws_disapprove_reason IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wageincrement') || '|' || lower('tws_disapprove_reason') || '|' || CAST(wi.tws_disapprove_reason AS VARCHAR)) END AS disapprove_reason,
    CAST(NULL AS VARCHAR)                                AS checker_recommendation_to,

    -- CONFIGURATION
    CAST(NULL AS DECIMAL(18,2))                          AS config_cap_amount,
    CAST(NULL AS VARCHAR)                                AS config_back_dated,
    CAST(NULL AS VARCHAR)                                AS config_created_by_mol,
    CAST(NULL AS VARCHAR)                                AS config_grace_period,
    CAST(NULL AS VARCHAR)                                AS config_jobseekers,
    CAST(NULL AS VARCHAR)                                AS config_terminated_employee,

    -- MIGRATION
    WI.TMKN_ISMIGRATED                                   AS is_migrated,
    WI.tws_required_increment,
    WI.tws_new_wage,
    -- STANDARD AUDIT
    'MIS'                                                AS source_system_name,
    FALSE                                                AS is_deleted,
    CURRENT_DATE                                         AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_WAGEINCREMENTBASE WI

UNION ALL

-- ============================================================================
-- SUB-TYPE 4: PAY INCREMENT INSTALLMENTS
-- ============================================================================

SELECT
    'PAY_INCREMENT' AS wage_subtype,
    'TWS_PAYINCREMENT' AS mis_source_table,

    -- IDENTIFIERS
    CAST(PI.TWS_PAYINCREMENTID AS VARCHAR)               AS wage_id,
    PI.TWS_NAME                                          AS wage_application_no,

    -- FOREIGN KEYS
    CAST(WI.TWS_EMPLOYEE_APPLICATION AS VARCHAR)         AS employee_application_id,
    CAST(WI.TWS_INDIVIDUAL_REFERENCE AS VARCHAR)         AS individual_id,
    CAST(WI.TWS_SEGMENT_REFERENCE AS VARCHAR)            AS segment_reference_id,
    CAST(WI.TWS_PRODUCT AS VARCHAR)                      AS product_pid_id,
    CAST(NULL AS VARCHAR)                                AS es_application_id,
    CAST(WI.TWS_WAGEINCREMENTID AS VARCHAR)              AS parent_wage_id,
    CAST(PI.TWS_PAYMENTREQUEST AS VARCHAR)               AS payment_request_id,

    -- DISPLAY NAMES
    WI.TWS_COMPANY                                       AS company_name_denorm,
    WI.TWS_EMPLOYEE_APPLICATION                          AS employee_application_name,
    WI.TWS_ENTERPRISE_APPLICATION                        AS enterprise_application_name,
    WI.TWS_SEGMENT_REFERENCE                             AS segment_reference_name,
    WI.TWS_PRODUCT                                       AS product_pid_name,
    WI.TWS_SPONSORSHIP                                   AS sponsorship,
    PI.OWNERID                                           AS owner_name,
    WI.TMKN_CREATEDBYPARTNER                             AS created_by_partner,

    -- WAGE DETAILS
    WI.TWS_CURRENT_WAGE                                  AS current_wage,
    PI.TWS_AMOUNT                                        AS pay_amount,
    PI.TWS_AMOUNT_OLD                                    AS pay_amount_old,
    PI.TWS_YEAR                                          AS pay_year,
    PI.TWS_DUE_DATE                                      AS pay_due_date,
    PI.TWS_NO_OF_MONTHS                                  AS pay_no_of_months,

    -- DATES
    WI.TWS_SUPPORT_START_DATE                            AS support_start_date,
    WI.TWS_SUBMITTEDON                                   AS submitted_on,
    WI.TWS_APPROVEDON                                    AS approved_on,
    PI.CREATEDON                                         AS created_on,

    -- WORKFLOW / STATUS
    CASE WHEN pi.tws_workflow_status IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_payincrement') || '|' || lower('tws_workflowstatus') || '|' || CAST(pi.tws_workflow_status AS VARCHAR)) END AS workflow_status,
    CASE WHEN pi.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_payincrement') || '|' || lower('statuscode') || '|' || CAST(pi.statuscode AS VARCHAR)) END AS state,
    CAST(NULL AS VARCHAR)                                AS support_level,
    CAST(NULL AS VARCHAR)                                AS recommendation_to,
    CAST(NULL AS VARCHAR)                                AS payment_structure,
    CAST(NULL AS VARCHAR)                                AS disapprove_reason,
    CAST(NULL AS VARCHAR)                                AS checker_recommendation_to,

    -- CONFIGURATION
    CAST(NULL AS DECIMAL(18,2))                          AS config_cap_amount,
    CAST(NULL AS VARCHAR)                                AS config_back_dated,
    CAST(NULL AS VARCHAR)                                AS config_created_by_mol,
    CAST(NULL AS VARCHAR)                                AS config_grace_period,
    CAST(NULL AS VARCHAR)                                AS config_jobseekers,
    CAST(NULL AS VARCHAR)                                AS config_terminated_employee,

    -- MIGRATION
    PI.TMKN_ISMIGRATED                                   AS is_migrated,
    WI.tws_required_increment,
    WI.tws_new_wage,
    -- STANDARD AUDIT
    'MIS'                                                AS source_system_name,
    FALSE                                                AS is_deleted,
    CURRENT_DATE                                         AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_PAYINCREMENTBASE PI

INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_WAGEINCREMENTBASE WI
    ON WI.TWS_WAGEINCREMENTID = PI.TWS_WAGE_INCREMENT_REFERENCE
)
SELECT
    -- OS2 COLUMNS
    CAST(applicationsupportid AS VARCHAR)                 AS applicationsupportid,
    TRY_CAST(NULLIF(CAST(startdate AS VARCHAR), '') AS DATE)                     AS startdate,
    TRY_CAST(NULLIF(CAST(enddate AS VARCHAR), '') AS DATE)                       AS enddate,
    CAST(newjoblevelid AS VARCHAR)                        AS newjoblevelid,
    CAST(newjoblevel AS VARCHAR)                          AS newjoblevel,
    CAST(newtitle AS VARCHAR)                             AS newtitle,
    CAST(newdepartment AS VARCHAR)                        AS newdepartment,
    CAST(newresponsabilities AS VARCHAR)                  AS newresponsabilities,
    TRY_CAST(NULLIF(CAST(requestedincrementamount AS VARCHAR), '') AS BIGINT)   AS requestedincrementamount,
    TRY_CAST(NULLIF(CAST(newwage AS VARCHAR), '') AS BIGINT)                    AS newwage,
    TRY_CAST(NULLIF(CAST(requestedstipend AS VARCHAR), '') AS BIGINT)           AS requestedstipend,
    CAST(placementlocation AS VARCHAR)                    AS placementlocation,
    CAST(hostorganizationname AS VARCHAR)                 AS hostorganizationname,
    CAST(placementjobtitle AS VARCHAR)                    AS placementjobtitle,
    CAST(placementjobresponsabilities AS VARCHAR)         AS placementjobresponsabilities,
    CAST(placementskillsandknowledge AS VARCHAR)          AS placementskillsandknowledge,
    CAST(directsupervisorname AS VARCHAR)                 AS directsupervisorname,
    CAST(directsupervisormobileprefix AS VARCHAR)         AS directsupervisormobileprefix,
    CAST(directsupervisorcontactnumbe AS VARCHAR)         AS directsupervisorcontactnumbe,
    TRY_CAST(NULLIF(CAST(placementstartdate AS VARCHAR), '') AS DATE)            AS placementstartdate,
    TRY_CAST(NULLIF(CAST(placementenddate AS VARCHAR), '') AS DATE)              AS placementenddate,
    CAST(totalduration AS VARCHAR)                        AS totalduration,
    TRY_CAST(NULLIF(CAST(tkshareamt AS VARCHAR), '') AS BIGINT)                 AS tkshareamt,
    TRY_CAST(NULLIF(CAST(customershareamt AS VARCHAR), '') AS BIGINT)           AS customershareamt,
    CAST(frequencyofpayment AS VARCHAR)                   AS frequencyofpayment,
    CAST(iseligible AS BOOLEAN)                           AS iseligible,
    TRY_CAST(NULLIF(CAST(tkshareunamt AS VARCHAR), '') AS BIGINT)               AS tkshareunamt,
    CAST(wagetrackid AS VARCHAR)                          AS wagetrackid,
    CAST(wagetrack AS VARCHAR)                            AS wagetrack,
    CAST(newwagesalarystatusid AS VARCHAR)                AS newwagesalarystatusid,
    CAST(newwagesalarystatus AS VARCHAR)                  AS newwagesalarystatus,
    CAST(newwagesourceindicator AS VARCHAR)               AS newwagesourceindicator,
    CAST(stipendtraining_providername AS VARCHAR)         AS stipendtraining_providername,
    CAST(stipendtraining_programid AS VARCHAR)            AS stipendtraining_programid,
    CAST(stipendtraining_programname AS VARCHAR)          AS stipendtraining_programname,
    TRY_CAST(NULLIF(CAST(incrementpercentage AS VARCHAR), '') AS BIGINT)        AS incrementpercentage,
    CAST(typeofpledgeid AS VARCHAR)                       AS typeofpledgeid,
    CAST(extras AS VARCHAR)                               AS extras,
    CAST(isnewwageabove1800 AS BOOLEAN)                   AS isnewwageabove1800,
    TRY_CAST(NULLIF(CAST(currentwageoverride AS VARCHAR), '') AS BIGINT)        AS currentwageoverride,
    TRY_CAST(NULLIF(CAST(newwageoverride AS VARCHAR), '') AS BIGINT)            AS newwageoverride,

    -- MIS COLUMNS
    CAST(NULL AS VARCHAR)                                 AS wage_subtype,
    CAST(NULL AS VARCHAR)                                 AS mis_source_table,
    CAST(NULL AS VARCHAR)                                 AS wage_id,
    CAST(NULL AS VARCHAR)                                 AS wage_application_no,
    CAST(NULL AS VARCHAR)                                 AS employee_application_id,
    CAST(NULL AS VARCHAR)                                 AS individual_id,
    CAST(NULL AS VARCHAR)                                 AS segment_reference_id,
    CAST(NULL AS VARCHAR)                                 AS product_pid_id,
    CAST(NULL AS VARCHAR)                                 AS es_application_id,
    CAST(NULL AS VARCHAR)                                 AS parent_wage_id,
    CAST(NULL AS VARCHAR)                                 AS payment_request_id,
    CAST(NULL AS VARCHAR)                                 AS company_name_denorm,
    CAST(NULL AS VARCHAR)                                 AS employee_application_name,
    CAST(NULL AS VARCHAR)                                 AS enterprise_application_name,
    CAST(NULL AS VARCHAR)                                 AS segment_reference_name,
    CAST(NULL AS VARCHAR)                                 AS product_pid_name,
    CAST(NULL AS VARCHAR)                                 AS sponsorship,
    CAST(NULL AS VARCHAR)                                 AS owner_name,
    CAST(NULL AS VARCHAR)                                 AS created_by_partner,
    CAST(NULL AS BIGINT)                                  AS current_wage,
    CAST(NULL AS BIGINT)                                  AS pay_amount,
    CAST(NULL AS BIGINT)                                  AS pay_amount_old,
    CAST(NULL AS VARCHAR)                                 AS pay_year,
    CAST(NULL AS DATE)                                    AS pay_due_date,
    CAST(NULL AS VARCHAR)                                 AS pay_no_of_months,
    CAST(NULL AS DATE)                                    AS support_start_date,
    CAST(NULL AS TIMESTAMP)                               AS submitted_on,
    CAST(NULL AS TIMESTAMP)                               AS approved_on,
    CAST(NULL AS VARCHAR)                                 AS workflow_status,
    CAST(NULL AS VARCHAR)                                 AS state,
    CAST(NULL AS VARCHAR)                                 AS support_level,
    CAST(NULL AS VARCHAR)                                 AS recommendation_to,
    CAST(NULL AS VARCHAR)                                 AS payment_structure,
    CAST(NULL AS VARCHAR)                                 AS disapprove_reason,
    CAST(NULL AS VARCHAR)                                 AS checker_recommendation_to,
    CAST(NULL AS BIGINT)                                  AS config_cap_amount,
    CAST(NULL AS BOOLEAN)                                 AS config_back_dated,
    CAST(NULL AS BOOLEAN)                                 AS config_created_by_mol,
    CAST(NULL AS VARCHAR)                                 AS config_grace_period,
    CAST(NULL AS BOOLEAN)                                 AS config_jobseekers,
    CAST(NULL AS BOOLEAN)                                 AS config_terminated_employee,
    CAST(NULL AS BOOLEAN)                                 AS is_migrated,
    CAST(NULL AS DECIMAL)                                 AS tws_required_increment,
	CAST(NULL AS DECIMAL)                                 AS tws_new_wage,
    -- COMMON AUDIT COLUMNS
    TRY_CAST(NULLIF(CAST(createdon AS VARCHAR), '') AS TIMESTAMP)                AS created_on,
    TRY_CAST(NULLIF(CAST(updatedon AS VARCHAR), '') AS TIMESTAMP)                AS updated_on,
    is_deleted,
    source_system_name,
    CAST(CURRENT_DATE AS DATE)                                    AS report_date,
    dbt_updated_at

from wage_base_os2_source

UNION ALL

SELECT
    -- OS2 COLUMNS
    CAST(NULL AS VARCHAR)                                 AS applicationsupportid,
    CAST(NULL AS DATE)                                    AS startdate,
    CAST(NULL AS DATE)                                    AS enddate,
    CAST(NULL AS VARCHAR)                                 AS newjoblevelid,
    CAST(NULL AS VARCHAR)                                 AS newjoblevel,
    CAST(NULL AS VARCHAR)                                 AS newtitle,
    CAST(NULL AS VARCHAR)                                 AS newdepartment,
    CAST(NULL AS VARCHAR)                                 AS newresponsabilities,
    CAST(NULL AS BIGINT)                                  AS requestedincrementamount,
    CAST(NULL AS BIGINT)                                  AS newwage,
    CAST(NULL AS BIGINT)                                  AS requestedstipend,
    CAST(NULL AS VARCHAR)                                 AS placementlocation,
    CAST(NULL AS VARCHAR)                                 AS hostorganizationname,
    CAST(NULL AS VARCHAR)                                 AS placementjobtitle,
    CAST(NULL AS VARCHAR)                                 AS placementjobresponsabilities,
    CAST(NULL AS VARCHAR)                                 AS placementskillsandknowledge,
    CAST(NULL AS VARCHAR)                                 AS directsupervisorname,
    CAST(NULL AS VARCHAR)                                 AS directsupervisormobileprefix,
    CAST(NULL AS VARCHAR)                                 AS directsupervisorcontactnumbe,
    CAST(NULL AS DATE)                                    AS placementstartdate,
    CAST(NULL AS DATE)                                    AS placementenddate,
    CAST(NULL AS VARCHAR)                                 AS totalduration,
    CAST(NULL AS BIGINT)                                  AS tkshareamt,
    CAST(NULL AS BIGINT)                                  AS customershareamt,
    CAST(NULL AS VARCHAR)                                 AS frequencyofpayment,
    CAST(NULL AS BOOLEAN)                                 AS iseligible,
    CAST(NULL AS BIGINT)                                  AS tkshareunamt,
    CAST(NULL AS VARCHAR)                                 AS wagetrackid,
    CAST(NULL AS VARCHAR)                                 AS wagetrack,
    CAST(NULL AS VARCHAR)                                 AS newwagesalarystatusid,
    CAST(NULL AS VARCHAR)                                 AS newwagesalarystatus,
    CAST(NULL AS VARCHAR)                                 AS newwagesourceindicator,
    CAST(NULL AS VARCHAR)                                 AS stipendtraining_providername,
    CAST(NULL AS VARCHAR)                                 AS stipendtraining_programid,
    CAST(NULL AS VARCHAR)                                 AS stipendtraining_programname,
    CAST(NULL AS BIGINT)                                  AS incrementpercentage,
    CAST(NULL AS VARCHAR)                                 AS typeofpledgeid,
    CAST(NULL AS VARCHAR)                                 AS extras,
    CAST(NULL AS BOOLEAN)                                 AS isnewwageabove1800,
    CAST(NULL AS BIGINT)                                  AS currentwageoverride,
    CAST(NULL AS BIGINT)                                  AS newwageoverride,

    -- MIS COLUMNS
    CAST(wage_subtype AS VARCHAR)                         AS wage_subtype,
    CAST(mis_source_table AS VARCHAR)                     AS mis_source_table,
    CAST(wage_id AS VARCHAR)                              AS wage_id,
    CAST(wage_application_no AS VARCHAR)                  AS wage_application_no,
    CAST(employee_application_id AS VARCHAR)              AS employee_application_id,
    CAST(individual_id AS VARCHAR)                        AS individual_id,
    CAST(segment_reference_id AS VARCHAR)                 AS segment_reference_id,
    CAST(product_pid_id AS VARCHAR)                       AS product_pid_id,
    CAST(es_application_id AS VARCHAR)                    AS es_application_id,
    CAST(parent_wage_id AS VARCHAR)                       AS parent_wage_id,
    CAST(payment_request_id AS VARCHAR)                   AS payment_request_id,
    CAST(company_name_denorm AS VARCHAR)                  AS company_name_denorm,
    CAST(employee_application_name AS VARCHAR)            AS employee_application_name,
    CAST(enterprise_application_name AS VARCHAR)          AS enterprise_application_name,
    CAST(segment_reference_name AS VARCHAR)               AS segment_reference_name,
    CAST(product_pid_name AS VARCHAR)                     AS product_pid_name,
    CAST(sponsorship AS VARCHAR)                          AS sponsorship,
    CAST(owner_name AS VARCHAR)                           AS owner_name,
    CAST(created_by_partner AS VARCHAR)                   AS created_by_partner,
    TRY_CAST(NULLIF(CAST(current_wage AS VARCHAR), '') AS BIGINT)               AS current_wage,
    TRY_CAST(NULLIF(CAST(pay_amount AS VARCHAR), '') AS BIGINT)                 AS pay_amount,
    TRY_CAST(NULLIF(CAST(pay_amount_old AS VARCHAR), '') AS BIGINT)             AS pay_amount_old,
    CAST(pay_year AS VARCHAR)                             AS pay_year,
    TRY_CAST(NULLIF(CAST(pay_due_date AS VARCHAR), '') AS DATE)                  AS pay_due_date,
    CAST(pay_no_of_months AS VARCHAR)                     AS pay_no_of_months,
    TRY_CAST(NULLIF(CAST(support_start_date AS VARCHAR), '') AS DATE)            AS support_start_date,
    TRY_CAST(NULLIF(CAST(submitted_on AS VARCHAR), '') AS TIMESTAMP)             AS submitted_on,
    TRY_CAST(NULLIF(CAST(approved_on AS VARCHAR), '') AS TIMESTAMP)              AS approved_on,
    CAST(workflow_status AS VARCHAR)                      AS workflow_status,
    CAST(state AS VARCHAR)                                AS state,
    CAST(support_level AS VARCHAR)                        AS support_level,
    CAST(recommendation_to AS VARCHAR)                    AS recommendation_to,
    CAST(payment_structure AS VARCHAR)                    AS payment_structure,
    CAST(disapprove_reason AS VARCHAR)                    AS disapprove_reason,
    CAST(checker_recommendation_to AS VARCHAR)            AS checker_recommendation_to,
    TRY_CAST(NULLIF(CAST(config_cap_amount AS VARCHAR), '') AS BIGINT)          AS config_cap_amount,
    CAST(config_back_dated AS BOOLEAN)                    AS config_back_dated,
    CAST(config_created_by_mol AS BOOLEAN)                AS config_created_by_mol,
    CAST(config_grace_period AS VARCHAR)                  AS config_grace_period,
    CAST(config_jobseekers AS BOOLEAN)                    AS config_jobseekers,
    CAST(config_terminated_employee AS BOOLEAN)           AS config_terminated_employee,
    CAST(is_migrated AS BOOLEAN)                          AS is_migrated,
	CAST(tws_required_increment AS DECIMAL)               AS tws_required_increment,
    CAST(tws_new_wage AS DECIMAL)                         AS tws_new_wage,

    -- COMMON AUDIT COLUMNS
    TRY_CAST(NULLIF(CAST(created_on AS VARCHAR), '') AS TIMESTAMP)               AS created_on,
    CAST(NULL AS TIMESTAMP)                               AS updated_on,
    is_deleted,
    source_system_name,
    TRY_CAST(NULLIF(CAST(report_date AS VARCHAR), '') AS DATE)                   AS report_date,
    dbt_updated_at

from wage_base_mis_source
),

silver_layer AS (
SELECT
    applicationsupportid,
    startdate,
    enddate,
    newjoblevelid,
    newjoblevel,
    newtitle,
    newdepartment,
    newresponsabilities,
    requestedincrementamount,
    newwage,
    requestedstipend,
    placementlocation,
    hostorganizationname,
    placementjobtitle,
    placementjobresponsabilities,
    placementskillsandknowledge,
    directsupervisorname,
    directsupervisormobileprefix,
    directsupervisorcontactnumbe,
    placementstartdate,
    placementenddate,
    totalduration,
    tkshareamt,
    customershareamt,
    frequencyofpayment,
    iseligible,
    tkshareunamt,
    wagetrackid,
    wagetrack,
    newwagesalarystatusid,
    newwagesalarystatus,
    newwagesourceindicator,
    stipendtraining_providername,
    stipendtraining_programid,
    stipendtraining_programname,
    incrementpercentage,
    typeofpledgeid,
    extras,
    isnewwageabove1800,
    currentwageoverride,
    newwageoverride,
    wage_subtype,
    mis_source_table,
    wage_id,
    wage_application_no,
    employee_application_id,
    individual_id,
    segment_reference_id,
    product_pid_id,
    es_application_id,
    parent_wage_id,
    payment_request_id,
    company_name_denorm,
    employee_application_name,
    enterprise_application_name,
    segment_reference_name,
    product_pid_name,
    sponsorship,
    owner_name,
    created_by_partner,
    current_wage,
    pay_amount,
    pay_amount_old,
    pay_year,
    pay_due_date,
    pay_no_of_months,
    support_start_date,
    submitted_on,
    approved_on,
    workflow_status,
    state,
    support_level,
    recommendation_to,
    payment_structure,
    disapprove_reason,
    checker_recommendation_to,
    config_cap_amount,
    config_back_dated,
    config_created_by_mol,
    config_grace_period,
    config_jobseekers,
    config_terminated_employee,
    is_migrated,
    tws_required_increment,
    tws_new_wage,
    created_on,
    updated_on,
    is_deleted,
    source_system_name,
    report_date,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".wage_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'applicationsupportid'),
        (2, 'startdate'),
        (3, 'enddate'),
        (4, 'newjoblevelid'),
        (5, 'newjoblevel'),
        (6, 'newtitle'),
        (7, 'newdepartment'),
        (8, 'newresponsabilities'),
        (9, 'requestedincrementamount'),
        (10, 'newwage'),
        (11, 'requestedstipend'),
        (12, 'placementlocation'),
        (13, 'hostorganizationname'),
        (14, 'placementjobtitle'),
        (15, 'placementjobresponsabilities'),
        (16, 'placementskillsandknowledge'),
        (17, 'directsupervisorname'),
        (18, 'directsupervisormobileprefix'),
        (19, 'directsupervisorcontactnumbe'),
        (20, 'placementstartdate'),
        (21, 'placementenddate'),
        (22, 'totalduration'),
        (23, 'tkshareamt'),
        (24, 'customershareamt'),
        (25, 'frequencyofpayment'),
        (26, 'iseligible'),
        (27, 'tkshareunamt'),
        (28, 'wagetrackid'),
        (29, 'wagetrack'),
        (30, 'newwagesalarystatusid'),
        (31, 'newwagesalarystatus'),
        (32, 'newwagesourceindicator'),
        (33, 'stipendtraining_providername'),
        (34, 'stipendtraining_programid'),
        (35, 'stipendtraining_programname'),
        (36, 'incrementpercentage'),
        (37, 'typeofpledgeid'),
        (38, 'extras'),
        (39, 'isnewwageabove1800'),
        (40, 'currentwageoverride'),
        (41, 'newwageoverride'),
        (42, 'wage_subtype'),
        (43, 'mis_source_table'),
        (44, 'wage_id'),
        (45, 'wage_application_no'),
        (46, 'employee_application_id'),
        (47, 'individual_id'),
        (48, 'segment_reference_id'),
        (49, 'product_pid_id'),
        (50, 'es_application_id'),
        (51, 'parent_wage_id'),
        (52, 'payment_request_id'),
        (53, 'company_name_denorm'),
        (54, 'employee_application_name'),
        (55, 'enterprise_application_name'),
        (56, 'segment_reference_name'),
        (57, 'product_pid_name'),
        (58, 'sponsorship'),
        (59, 'owner_name'),
        (60, 'created_by_partner'),
        (61, 'current_wage'),
        (62, 'pay_amount'),
        (63, 'pay_amount_old'),
        (64, 'pay_year'),
        (65, 'pay_due_date'),
        (66, 'pay_no_of_months'),
        (67, 'support_start_date'),
        (68, 'submitted_on'),
        (69, 'approved_on'),
        (70, 'workflow_status'),
        (71, 'state'),
        (72, 'support_level'),
        (73, 'recommendation_to'),
        (74, 'payment_structure'),
        (75, 'disapprove_reason'),
        (76, 'checker_recommendation_to'),
        (77, 'config_cap_amount'),
        (78, 'config_back_dated'),
        (79, 'config_created_by_mol'),
        (80, 'config_grace_period'),
        (81, 'config_jobseekers'),
        (82, 'config_terminated_employee'),
        (83, 'is_migrated'),
        (84, 'tws_required_increment'),
        (85, 'tws_new_wage'),
        (86, 'created_on'),
        (87, 'updated_on'),
        (88, 'is_deleted'),
        (89, 'source_system_name'),
        (90, 'report_date'),
        (91, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'applicationsupportid'),
        (2, 'startdate'),
        (3, 'enddate'),
        (4, 'newjoblevelid'),
        (5, 'newjoblevel'),
        (6, 'newtitle'),
        (7, 'newdepartment'),
        (8, 'newresponsabilities'),
        (9, 'requestedincrementamount'),
        (10, 'newwage'),
        (11, 'requestedstipend'),
        (12, 'placementlocation'),
        (13, 'hostorganizationname'),
        (14, 'placementjobtitle'),
        (15, 'placementjobresponsabilities'),
        (16, 'placementskillsandknowledge'),
        (17, 'directsupervisorname'),
        (18, 'directsupervisormobileprefix'),
        (19, 'directsupervisorcontactnumbe'),
        (20, 'placementstartdate'),
        (21, 'placementenddate'),
        (22, 'totalduration'),
        (23, 'tkshareamt'),
        (24, 'customershareamt'),
        (25, 'frequencyofpayment'),
        (26, 'iseligible'),
        (27, 'tkshareunamt'),
        (28, 'wagetrackid'),
        (29, 'wagetrack'),
        (30, 'newwagesalarystatusid'),
        (31, 'newwagesalarystatus'),
        (32, 'newwagesourceindicator'),
        (33, 'stipendtraining_providername'),
        (34, 'stipendtraining_programid'),
        (35, 'stipendtraining_programname'),
        (36, 'incrementpercentage'),
        (37, 'typeofpledgeid'),
        (38, 'extras'),
        (39, 'isnewwageabove1800'),
        (40, 'currentwageoverride'),
        (41, 'newwageoverride'),
        (42, 'wage_subtype'),
        (43, 'mis_source_table'),
        (44, 'wage_id'),
        (45, 'wage_application_no'),
        (46, 'employee_application_id'),
        (47, 'individual_id'),
        (48, 'segment_reference_id'),
        (49, 'product_pid_id'),
        (50, 'es_application_id'),
        (51, 'parent_wage_id'),
        (52, 'payment_request_id'),
        (53, 'company_name_denorm'),
        (54, 'employee_application_name'),
        (55, 'enterprise_application_name'),
        (56, 'segment_reference_name'),
        (57, 'product_pid_name'),
        (58, 'sponsorship'),
        (59, 'owner_name'),
        (60, 'created_by_partner'),
        (61, 'current_wage'),
        (62, 'pay_amount'),
        (63, 'pay_amount_old'),
        (64, 'pay_year'),
        (65, 'pay_due_date'),
        (66, 'pay_no_of_months'),
        (67, 'support_start_date'),
        (68, 'submitted_on'),
        (69, 'approved_on'),
        (70, 'workflow_status'),
        (71, 'state'),
        (72, 'support_level'),
        (73, 'recommendation_to'),
        (74, 'payment_structure'),
        (75, 'disapprove_reason'),
        (76, 'checker_recommendation_to'),
        (77, 'config_cap_amount'),
        (78, 'config_back_dated'),
        (79, 'config_created_by_mol'),
        (80, 'config_grace_period'),
        (81, 'config_jobseekers'),
        (82, 'config_terminated_employee'),
        (83, 'is_migrated'),
        (84, 'tws_required_increment'),
        (85, 'tws_new_wage'),
        (86, 'created_on'),
        (87, 'updated_on'),
        (88, 'is_deleted'),
        (89, 'source_system_name'),
        (90, 'report_date'),
        (91, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("applicationsupportid" AS VARCHAR) AS "applicationsupportid",
        CAST("startdate" AS VARCHAR) AS "startdate",
        CAST("enddate" AS VARCHAR) AS "enddate",
        CAST("newjoblevelid" AS VARCHAR) AS "newjoblevelid",
        CAST("newjoblevel" AS VARCHAR) AS "newjoblevel",
        CAST("newtitle" AS VARCHAR) AS "newtitle",
        CAST("newdepartment" AS VARCHAR) AS "newdepartment",
        CAST("newresponsabilities" AS VARCHAR) AS "newresponsabilities",
        CAST("requestedincrementamount" AS VARCHAR) AS "requestedincrementamount",
        CAST("newwage" AS VARCHAR) AS "newwage",
        CAST("requestedstipend" AS VARCHAR) AS "requestedstipend",
        CAST("placementlocation" AS VARCHAR) AS "placementlocation",
        CAST("hostorganizationname" AS VARCHAR) AS "hostorganizationname",
        CAST("placementjobtitle" AS VARCHAR) AS "placementjobtitle",
        CAST("placementjobresponsabilities" AS VARCHAR) AS "placementjobresponsabilities",
        CAST("placementskillsandknowledge" AS VARCHAR) AS "placementskillsandknowledge",
        CAST("directsupervisorname" AS VARCHAR) AS "directsupervisorname",
        CAST("directsupervisormobileprefix" AS VARCHAR) AS "directsupervisormobileprefix",
        CAST("directsupervisorcontactnumbe" AS VARCHAR) AS "directsupervisorcontactnumbe",
        CAST("placementstartdate" AS VARCHAR) AS "placementstartdate",
        CAST("placementenddate" AS VARCHAR) AS "placementenddate",
        CAST("totalduration" AS VARCHAR) AS "totalduration",
        CAST("tkshareamt" AS VARCHAR) AS "tkshareamt",
        CAST("customershareamt" AS VARCHAR) AS "customershareamt",
        CAST("frequencyofpayment" AS VARCHAR) AS "frequencyofpayment",
        CAST("iseligible" AS VARCHAR) AS "iseligible",
        CAST("tkshareunamt" AS VARCHAR) AS "tkshareunamt",
        CAST("wagetrackid" AS VARCHAR) AS "wagetrackid",
        CAST("wagetrack" AS VARCHAR) AS "wagetrack",
        CAST("newwagesalarystatusid" AS VARCHAR) AS "newwagesalarystatusid",
        CAST("newwagesalarystatus" AS VARCHAR) AS "newwagesalarystatus",
        CAST("newwagesourceindicator" AS VARCHAR) AS "newwagesourceindicator",
        CAST("stipendtraining_providername" AS VARCHAR) AS "stipendtraining_providername",
        CAST("stipendtraining_programid" AS VARCHAR) AS "stipendtraining_programid",
        CAST("stipendtraining_programname" AS VARCHAR) AS "stipendtraining_programname",
        CAST("incrementpercentage" AS VARCHAR) AS "incrementpercentage",
        CAST("typeofpledgeid" AS VARCHAR) AS "typeofpledgeid",
        CAST("extras" AS VARCHAR) AS "extras",
        CAST("isnewwageabove1800" AS VARCHAR) AS "isnewwageabove1800",
        CAST("currentwageoverride" AS VARCHAR) AS "currentwageoverride",
        CAST("newwageoverride" AS VARCHAR) AS "newwageoverride",
        CAST("wage_subtype" AS VARCHAR) AS "wage_subtype",
        CAST("mis_source_table" AS VARCHAR) AS "mis_source_table",
        CAST("wage_id" AS VARCHAR) AS "wage_id",
        CAST("wage_application_no" AS VARCHAR) AS "wage_application_no",
        CAST("employee_application_id" AS VARCHAR) AS "employee_application_id",
        CAST("individual_id" AS VARCHAR) AS "individual_id",
        CAST("segment_reference_id" AS VARCHAR) AS "segment_reference_id",
        CAST("product_pid_id" AS VARCHAR) AS "product_pid_id",
        CAST("es_application_id" AS VARCHAR) AS "es_application_id",
        CAST("parent_wage_id" AS VARCHAR) AS "parent_wage_id",
        CAST("payment_request_id" AS VARCHAR) AS "payment_request_id",
        CAST("company_name_denorm" AS VARCHAR) AS "company_name_denorm",
        CAST("employee_application_name" AS VARCHAR) AS "employee_application_name",
        CAST("enterprise_application_name" AS VARCHAR) AS "enterprise_application_name",
        CAST("segment_reference_name" AS VARCHAR) AS "segment_reference_name",
        CAST("product_pid_name" AS VARCHAR) AS "product_pid_name",
        CAST("sponsorship" AS VARCHAR) AS "sponsorship",
        CAST("owner_name" AS VARCHAR) AS "owner_name",
        CAST("created_by_partner" AS VARCHAR) AS "created_by_partner",
        CAST("current_wage" AS VARCHAR) AS "current_wage",
        CAST("pay_amount" AS VARCHAR) AS "pay_amount",
        CAST("pay_amount_old" AS VARCHAR) AS "pay_amount_old",
        CAST("pay_year" AS VARCHAR) AS "pay_year",
        CAST("pay_due_date" AS VARCHAR) AS "pay_due_date",
        CAST("pay_no_of_months" AS VARCHAR) AS "pay_no_of_months",
        CAST("support_start_date" AS VARCHAR) AS "support_start_date",
        CAST("submitted_on" AS VARCHAR) AS "submitted_on",
        CAST("approved_on" AS VARCHAR) AS "approved_on",
        CAST("workflow_status" AS VARCHAR) AS "workflow_status",
        CAST("state" AS VARCHAR) AS "state",
        CAST("support_level" AS VARCHAR) AS "support_level",
        CAST("recommendation_to" AS VARCHAR) AS "recommendation_to",
        CAST("payment_structure" AS VARCHAR) AS "payment_structure",
        CAST("disapprove_reason" AS VARCHAR) AS "disapprove_reason",
        CAST("checker_recommendation_to" AS VARCHAR) AS "checker_recommendation_to",
        CAST("config_cap_amount" AS VARCHAR) AS "config_cap_amount",
        CAST("config_back_dated" AS VARCHAR) AS "config_back_dated",
        CAST("config_created_by_mol" AS VARCHAR) AS "config_created_by_mol",
        CAST("config_grace_period" AS VARCHAR) AS "config_grace_period",
        CAST("config_jobseekers" AS VARCHAR) AS "config_jobseekers",
        CAST("config_terminated_employee" AS VARCHAR) AS "config_terminated_employee",
        CAST("is_migrated" AS VARCHAR) AS "is_migrated",
        CAST("tws_required_increment" AS VARCHAR) AS "tws_required_increment",
        CAST("tws_new_wage" AS VARCHAR) AS "tws_new_wage",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("updated_on" AS VARCHAR) AS "updated_on",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("report_date" AS VARCHAR) AS "report_date",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("applicationsupportid" AS VARCHAR) AS "applicationsupportid",
        CAST("startdate" AS VARCHAR) AS "startdate",
        CAST("enddate" AS VARCHAR) AS "enddate",
        CAST("newjoblevelid" AS VARCHAR) AS "newjoblevelid",
        CAST("newjoblevel" AS VARCHAR) AS "newjoblevel",
        CAST("newtitle" AS VARCHAR) AS "newtitle",
        CAST("newdepartment" AS VARCHAR) AS "newdepartment",
        CAST("newresponsabilities" AS VARCHAR) AS "newresponsabilities",
        CAST("requestedincrementamount" AS VARCHAR) AS "requestedincrementamount",
        CAST("newwage" AS VARCHAR) AS "newwage",
        CAST("requestedstipend" AS VARCHAR) AS "requestedstipend",
        CAST("placementlocation" AS VARCHAR) AS "placementlocation",
        CAST("hostorganizationname" AS VARCHAR) AS "hostorganizationname",
        CAST("placementjobtitle" AS VARCHAR) AS "placementjobtitle",
        CAST("placementjobresponsabilities" AS VARCHAR) AS "placementjobresponsabilities",
        CAST("placementskillsandknowledge" AS VARCHAR) AS "placementskillsandknowledge",
        CAST("directsupervisorname" AS VARCHAR) AS "directsupervisorname",
        CAST("directsupervisormobileprefix" AS VARCHAR) AS "directsupervisormobileprefix",
        CAST("directsupervisorcontactnumbe" AS VARCHAR) AS "directsupervisorcontactnumbe",
        CAST("placementstartdate" AS VARCHAR) AS "placementstartdate",
        CAST("placementenddate" AS VARCHAR) AS "placementenddate",
        CAST("totalduration" AS VARCHAR) AS "totalduration",
        CAST("tkshareamt" AS VARCHAR) AS "tkshareamt",
        CAST("customershareamt" AS VARCHAR) AS "customershareamt",
        CAST("frequencyofpayment" AS VARCHAR) AS "frequencyofpayment",
        CAST("iseligible" AS VARCHAR) AS "iseligible",
        CAST("tkshareunamt" AS VARCHAR) AS "tkshareunamt",
        CAST("wagetrackid" AS VARCHAR) AS "wagetrackid",
        CAST("wagetrack" AS VARCHAR) AS "wagetrack",
        CAST("newwagesalarystatusid" AS VARCHAR) AS "newwagesalarystatusid",
        CAST("newwagesalarystatus" AS VARCHAR) AS "newwagesalarystatus",
        CAST("newwagesourceindicator" AS VARCHAR) AS "newwagesourceindicator",
        CAST("stipendtraining_providername" AS VARCHAR) AS "stipendtraining_providername",
        CAST("stipendtraining_programid" AS VARCHAR) AS "stipendtraining_programid",
        CAST("stipendtraining_programname" AS VARCHAR) AS "stipendtraining_programname",
        CAST("incrementpercentage" AS VARCHAR) AS "incrementpercentage",
        CAST("typeofpledgeid" AS VARCHAR) AS "typeofpledgeid",
        CAST("extras" AS VARCHAR) AS "extras",
        CAST("isnewwageabove1800" AS VARCHAR) AS "isnewwageabove1800",
        CAST("currentwageoverride" AS VARCHAR) AS "currentwageoverride",
        CAST("newwageoverride" AS VARCHAR) AS "newwageoverride",
        CAST("wage_subtype" AS VARCHAR) AS "wage_subtype",
        CAST("mis_source_table" AS VARCHAR) AS "mis_source_table",
        CAST("wage_id" AS VARCHAR) AS "wage_id",
        CAST("wage_application_no" AS VARCHAR) AS "wage_application_no",
        CAST("employee_application_id" AS VARCHAR) AS "employee_application_id",
        CAST("individual_id" AS VARCHAR) AS "individual_id",
        CAST("segment_reference_id" AS VARCHAR) AS "segment_reference_id",
        CAST("product_pid_id" AS VARCHAR) AS "product_pid_id",
        CAST("es_application_id" AS VARCHAR) AS "es_application_id",
        CAST("parent_wage_id" AS VARCHAR) AS "parent_wage_id",
        CAST("payment_request_id" AS VARCHAR) AS "payment_request_id",
        CAST("company_name_denorm" AS VARCHAR) AS "company_name_denorm",
        CAST("employee_application_name" AS VARCHAR) AS "employee_application_name",
        CAST("enterprise_application_name" AS VARCHAR) AS "enterprise_application_name",
        CAST("segment_reference_name" AS VARCHAR) AS "segment_reference_name",
        CAST("product_pid_name" AS VARCHAR) AS "product_pid_name",
        CAST("sponsorship" AS VARCHAR) AS "sponsorship",
        CAST("owner_name" AS VARCHAR) AS "owner_name",
        CAST("created_by_partner" AS VARCHAR) AS "created_by_partner",
        CAST("current_wage" AS VARCHAR) AS "current_wage",
        CAST("pay_amount" AS VARCHAR) AS "pay_amount",
        CAST("pay_amount_old" AS VARCHAR) AS "pay_amount_old",
        CAST("pay_year" AS VARCHAR) AS "pay_year",
        CAST("pay_due_date" AS VARCHAR) AS "pay_due_date",
        CAST("pay_no_of_months" AS VARCHAR) AS "pay_no_of_months",
        CAST("support_start_date" AS VARCHAR) AS "support_start_date",
        CAST("submitted_on" AS VARCHAR) AS "submitted_on",
        CAST("approved_on" AS VARCHAR) AS "approved_on",
        CAST("workflow_status" AS VARCHAR) AS "workflow_status",
        CAST("state" AS VARCHAR) AS "state",
        CAST("support_level" AS VARCHAR) AS "support_level",
        CAST("recommendation_to" AS VARCHAR) AS "recommendation_to",
        CAST("payment_structure" AS VARCHAR) AS "payment_structure",
        CAST("disapprove_reason" AS VARCHAR) AS "disapprove_reason",
        CAST("checker_recommendation_to" AS VARCHAR) AS "checker_recommendation_to",
        CAST("config_cap_amount" AS VARCHAR) AS "config_cap_amount",
        CAST("config_back_dated" AS VARCHAR) AS "config_back_dated",
        CAST("config_created_by_mol" AS VARCHAR) AS "config_created_by_mol",
        CAST("config_grace_period" AS VARCHAR) AS "config_grace_period",
        CAST("config_jobseekers" AS VARCHAR) AS "config_jobseekers",
        CAST("config_terminated_employee" AS VARCHAR) AS "config_terminated_employee",
        CAST("is_migrated" AS VARCHAR) AS "is_migrated",
        CAST("tws_required_increment" AS VARCHAR) AS "tws_required_increment",
        CAST("tws_new_wage" AS VARCHAR) AS "tws_new_wage",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("updated_on" AS VARCHAR) AS "updated_on",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("report_date" AS VARCHAR) AS "report_date",
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
        'wage_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'wage_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'wage_base' AS table_name,
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
        'wage_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'wage_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
