-- Compare bronze-layer query output with silver-layer table output for amendment_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\Silver_layer_03-June-2026\converted db script to databricks\Databricks_union of all sources\amendment_base.sql
-- Silver source: converted db script to databricks\silver_layer scripts\amendment_base_silver_layer.sql

WITH
bronze_layer AS (
/*
Generated Databricks union layer for amendment_base.
Column order and typed NULL placeholders follow dbt model: amendment_base.sql.
Source transformations are embedded whole from the converted Databricks OS1/OS2/MIS scripts.
dbt macros expanded to Databricks TRY_CAST / string cleanup expressions.
*/

/*
 =================================================================================================

Name        : AMENDMENT_BASE
Description : This model consolidates and standardizes amendment-related attributes
              from MIS and OS2 base models into a unified schema. It aligns column
              structures across both sources using NULL placeholders where attributes
              are not available and combines the datasets using UNION ALL.

              The model ensures consistent column naming and structure for downstream
              consumption in the Silver Layer.

Source Tables : amendment_base_os2
                amendment_base_mis

Target Table : AMENDMENT_BASE
Load Type    : Full Load (Table)
Materialized : table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-12 | Kaviya  | Initial version

================================================================================================= 
*/





WITH
    amendment_base_os2 AS (
/* =================================================================================================
 
Name        : AMENDMENT_BASE_OS2
Description : This model extracts and transforms application and amendment-related
              attributes from the OS2 source system Bronze Layer and loads them
              into the APPLICATION target table as part of the Silver Layer
              data pipeline.
 
              The model combines both base applications and amendment requests
              using UNION ALL, derives customer and program-related attributes,
              applies date and timestamp standardization, and identifies the
              latest application/amendment record using ROW_NUMBER ranking logic.
 
Source Tables : neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_NTP_AMENDMENTREQUEST
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_3QQ_PROGRAM
                neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_INDIVIDUAL
                neo2.OSUSR_ZMZ_COMPANY
                neo2.OSUSR_398_APPLICATIONSTATUS
 
Target Table : AMENDMENT_BASE_OS2
Load Type    : Full Load
Materialized : table
Format       : PARQUET
Tags         : os2, daily
 
Revision History:
--------------------------------------------------------------
 
Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-12 | Venkatesh | Initial version
 
================================================================================================= */

WITH COMBINED_APP AS (

    SELECT
        PROGVER.COMMERCIALNAME_EN                        AS program_name,
        PROGRAM.PROFILETYPEID                            AS program_type,
        APP.REFERENCENUMBER                              AS reference,
        APP.ID                                           AS application_id,
        APST.LABEL                                       AS application_status,
        CAST(NULL AS BIGINT)                             AS amendmentno,
		CAST(NULL AS BIGINT)                             AS utilizedamount,
		CAST(NULL AS BIGINT)                             AS unutilizedamount,
		CAST(NULL AS BIGINT)                             AS totalapprovedamount,
		CAST(NULL AS BIGINT)                             AS totalavailableamt,
		CAST(NULL AS BIGINT)                             AS utilizedamt,
		CAST(NULL AS BIGINT)                             AS unutilizedamt,
        APP.customershareamt,
		APP.haswagesupportmolemployees,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'IND' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END                                               AS cpr_number,
        CUS.NAMEEN                                      AS customer_enterprise_name,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.APPROVEDON + INTERVAL 3 HOURS
        END                                               AS approved_on_date,
        CASE
            WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.STARTON AS DATE)
        END                                               AS contract_start_date,
        CASE
            WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.MONITORINGDUEDATE AS DATE)
        END                                               AS monitoring_due_date,
        CASE
            WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.ENDON AS DATE)
        END                                               AS contract_end_date,
        APP.TKSHAREAMT                                  AS total_approved_amount_tamkeen_share,
        CASE
            WHEN APP.CREATEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.CREATEDON + INTERVAL 3 HOURS
        END                                               AS created_on,
        CASE
            WHEN APP.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SUBMITTEDON + INTERVAL 3 HOURS
        END                                               AS submitted_on,
        CASE
            WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL 3 HOURS 
        END                                               AS spending_period_end_date,
--        CASE
--            WHEN APP.APPROVALLETTERACCEPTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
--            ELSE APP.APPROVALLETTERACCEPTEDON + INTERVAL 3 HOURS 
--        END                                               AS APPROVAL_LETTER_CONFIRMED,
    CAST (NULL AS STRING) AS approval_letter_confirmed,
    'NEO2' AS source_system_name,
     FALSE AS is_deleted,
     CURRENT_DATE AS report_date,
     CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION4` APP
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION` PROGVER
        ON APP.PROGRAMVERSIONID = PROGVER.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAM` PROGRAM
        ON PROGVER.PROGRAMID = PROGRAM.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCUSTOMER` APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE` CUSPROF
        ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_APPLICATIONSTATUS` APST
        ON APP.APPLICATIONSTATUSID = APST.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL` IND
        ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY` CMP
        ON CUSPROF.CUSTOMERID = CMP.ID

    UNION ALL

    SELECT
        PROGVER.COMMERCIALNAME_EN                        AS program_name,
        PROGRAM.PROFILETYPEID                            AS program_type,
        AMED.REFERENCENUMBER                             AS reference,
        APP.ID                                           AS application_id,
        APST.LABEL                                      AS application_status,
        AMED.amendmentno,
		-- AMED.utilizedamount,
        CAST(NULL AS DECIMAL)   as utilizedamount,
		-- AMED.unutilizedamount,
        CAST(NULL AS DECIMAL)    as unutilizedamount,
		-- AMED.totalapprovedamount,
        CAST(NULL AS DECIMAL)   as totalapprovedamount,
		AMED.totalavailableamt,
		AMED.utilizedamt,
		AMED.unutilizedamt,
		AMED.customershareamt,
		AMED.haswagesupportmolemployees,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'IND' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END                                               AS cpr_number,
        CUS.NAMEEN                                      AS customer_enterprise_name,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.APPROVEDON + INTERVAL 3 HOURS
        END                                               AS approved_on_date,
        CASE
            WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.STARTON AS DATE)
        END                                               AS contract_start_date,
        CASE
            WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.MONITORINGDUEDATE AS DATE)
        END                                               AS monitoring_due_date,
        CASE
            WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.ENDON AS DATE)
        END                                               AS contract_end_date,
        AMED.TKSHAREAMT                                 AS total_approved_amount_tamkeen_share,
        CASE
            WHEN AMED.CREATEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.CREATEDON + INTERVAL 3 HOURS
        END                                               AS created_on,
        CASE
            WHEN AMED.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.SUBMITTEDON + INTERVAL 3 HOURS
        END                                               AS submitted_on,
        CASE
            WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL 3 HOURS
        END                                               AS spending_period_end_date,
--        CASE
--            WHEN APP.APPROVALLETTERACCEPTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
--            ELSE APP.APPROVALLETTERACCEPTEDON + INTERVAL 3 HOURS
--        END                                               AS APPROVAL_LETTER_CONFIRMED,
    CAST (NULL AS STRING) AS approval_letter_confirmed,
    'NEO2' AS source_system_name,
     FALSE AS is_deleted,
     CURRENT_DATE AS report_date,
     CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION4` APP
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_AMENDMENTREQUEST4` AMED
        ON AMED.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION` PROGVER
        ON APP.PROGRAMVERSIONID = PROGVER.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAM` PROGRAM
        ON PROGVER.PROGRAMID = PROGRAM.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCUSTOMER` APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE` CUSPROF
        ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL` IND
        ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY` CMP
        ON CUSPROF.CUSTOMERID = CMP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_APPLICATIONSTATUS` APST
        ON AMED.AMENDMENTSTATUSID = APST.CODE
    WHERE APST.LABEL = 'Active'

),

RANKED_DATA AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY APPLICATION_ID
               ORDER BY CREATED_ON DESC
           ) AS rnk
    FROM COMBINED_APP

)

SELECT  DISTINCT
    program_name,
    program_type,
    reference,
    application_id,
    application_status,
    amendmentno,
    utilizedamount,
    unutilizedamount,
    totalapprovedamount,
    totalavailableamt,
    utilizedamt,
    unutilizedamt,
    customershareamt,
    haswagesupportmolemployees,
    cpr_number,
    customer_enterprise_name,
    TRY_CAST(APPROVED_ON_DATE AS DATE) approved_on_date,
    TRY_CAST(CONTRACT_START_DATE AS DATE) contract_start_date,
    TRY_CAST(MONITORING_DUE_DATE AS DATE) monitoring_due_date,
    TRY_CAST(CONTRACT_END_DATE AS DATE) contract_end_date,
    total_approved_amount_tamkeen_share,
    TRY_CAST(CREATED_ON AS TIMESTAMP) created_on,
    TRY_CAST(SUBMITTED_ON AS TIMESTAMP) submitted_on,
    TRY_CAST(SPENDING_PERIOD_END_DATE AS TIMESTAMP) spending_period_end_date,
    approval_letter_confirmed,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    CASE WHEN RNK = 1 THEN 'Yes' ELSE 'No' END AS latest
FROM RANKED_DATA
where 1=1
),
    amendment_base_mis AS (
/*
============================================================================
silver_amendment_mis.sql
============================================================================
Per-source intermediate Silver model for the Amendment domain â€” MIS only.

Source: tmkn_amendment (the ES Amendment Request entity)
Reference SP: RPT-098_ES_Amendment_Request

The Amendment domain is a single-table domain in MIS â€” tmkn_amendment is the
only table, with no related entities to join. All enrichment is via option-set
decoding for status, type, reason, and workflow status fields.

Cleansing only â€” no business logic, no calculations. Mapping to OS2 column
names will happen in the unified Silver layer downstream.
============================================================================
*/

SELECT
    'tmkn_amendment' AS mis_source_table,

    -- Identifiers
    CAST(amnd.tmkn_amendmentid AS STRING)               AS amendment_id,
    amnd.tmkn_name                                       AS amendment_name,

    -- Foreign keys (display names denormalised at source â€” keep as-is)
    amnd.tmkn_application                          AS application_name,
    amnd.tmkn_maincompany                            AS main_company_name,

    -- Amendment details
    amnd.tmkn_details                                    AS details,
    amnd.tmkn_total                                      AS total_amount,
    amnd.tmkn_totalbcshare                               AS total_bc_share,
    amnd.tmkn_totaltmknshare                             AS total_tamkeen_share,
    amnd.tmkn_tamkeenshare                               AS tamkeen_share,
    amnd.tmkn_tamkeenshare_state                         AS tamkeen_share_state,
    amnd.tmkn_tamkeenshare_date                          AS tamkeen_share_last_updated_on,

    -- Option-set decoded fields
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_amendmentbase')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.tmkn_amedned AS STRING)

)         AS amended_flag,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_products')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.tmkn_products AS STRING)

)        AS products,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_amendmentreason')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.tmkn_amendmentreason AS STRING)

) AS amendment_reason,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('mis_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.mis_workflowstatus AS STRING)

)   AS old_workflow_status,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_reason')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.tmkn_reason AS STRING)

)          AS reason,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_type')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.tmkn_type AS STRING)

)            AS amendment_type,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.tmkn_workflowstatus AS STRING)

)  AS workflow_status,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.statuscode AS STRING)

)           AS status_reason,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('TMKN_AMENDMENTBASE')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(amnd.statecode AS STRING)

)            AS state,

    -- Owner / audit
    amnd.ownerid                                    AS owner_name,
    amnd.identity_createdby                          AS identity_created_by,
    amnd.identity_modifiedby                         AS identity_modified_by,
    amnd.createdby                             AS created_by,
    amnd.modifiedby                                  AS modified_by,
    amnd.identity_createdon                              AS identity_created_on,
    amnd.identity_modifiedon                             AS identity_modified_on,
    amnd.createdon                                       AS created_on,
    amnd.modifiedon                                      AS modified_on,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_AMENDMENTBASE` amnd
)
select
program_name,
program_type,
reference,
application_id,
application_status,
amendmentno ,
utilizedamount,
unutilizedamount,
totalapprovedamount,
totalavailableamt,
utilizedamt,
unutilizedamt,
customershareamt,
haswagesupportmolemployees,
cpr_number,
customer_enterprise_name,
approved_on_date,
contract_start_date,
monitoring_due_date,
contract_end_date,
total_approved_amount_tamkeen_share,
created_on,
submitted_on,
spending_period_end_date,
approval_letter_confirmed,
dbt_updated_at,
cast(null as STRING) as mis_source_table,
cast(null as STRING) as amendment_id,
cast(null as STRING) as amendment_name,
cast(null as STRING) as application_name,
cast(null as STRING) as main_company_name,
cast(null as STRING) as details,
cast(null as decimal) as total_amount,
cast(null as decimal) as total_bc_share,
cast(null as decimal) as total_tamkeen_share,
cast(null as decimal) as tamkeen_share,
cast(null as integer) as tamkeen_share_state,
cast(null as timestamp) as tamkeen_share_last_updated_on,
cast(null as STRING) as amended_flag,
cast(null as STRING) as products,
cast(null as STRING) as amendment_reason,
cast(null as STRING) as old_workflow_status,
cast(null as STRING) as reason,
cast(null as STRING) as amendment_type,
cast(null as STRING) as workflow_status,
cast(null as STRING) as status_reason,
cast(null as STRING) as state,
cast(null as STRING) as owner_name,
cast(null as STRING) as identity_created_by,
cast(null as STRING) as identity_modified_by,
cast(null as STRING) as created_by,
cast(null as STRING) as modified_by,
cast(null as timestamp) as identity_created_on,
cast(null as timestamp) as identity_modified_on,
cast(null as timestamp) as modified_on,
source_system_name,
is_deleted,
cast(null as timestamp) as report_date
from amendment_base_os2

union all

select
cast(null as STRING) as program_name,
cast(null as STRING) as program_type,
cast(null as STRING) as reference,
cast(null as bigint) as application_id,
cast(null as STRING) as application_status,
cast(null as bigint) as amendmentno,
cast(null as decimal) as utilizedamount,
cast(null as decimal) as unutilizedamount,
cast(null as decimal) as totalapprovedamount,
cast(null as decimal) as totalavailableamt,
cast(null as decimal) as utilizedamt,
cast(null as decimal) as unutilizedamt,
cast(null as decimal) as customershareamt,
cast(null as boolean) as haswagesupportmolemployees,
cast(null as STRING) as cpr_number,
cast(null as STRING) as customer_enterprise_name,
cast(null as timestamp) as approved_on_date,
cast(null as timestamp) as contract_start_date,
cast(null as timestamp) as monitoring_due_date,
cast(null as timestamp) as contract_end_date,
cast(null as decimal) as total_approved_amount_tamkeen_share,
cast(null as timestamp) as created_on,
cast(null as timestamp) as submitted_on,
cast(null as timestamp) as spending_period_end_date,
cast(null as STRING) as approval_letter_confirmed,
dbt_updated_at,
mis_source_table,
amendment_id,
amendment_name,
application_name,
main_company_name,
details,
total_amount,
total_bc_share,
total_tamkeen_share,
tamkeen_share,
tamkeen_share_state,
tamkeen_share_last_updated_on,
amended_flag,
products,
amendment_reason,
old_workflow_status,
reason,
amendment_type,
workflow_status,
status_reason,
state,
owner_name,
identity_created_by,
identity_modified_by,
created_by,
modified_by,
identity_created_on,
identity_modified_on,
modified_on,
source_system_name,
is_deleted,
report_date
from amendment_base_mis
),

silver_layer AS (
SELECT
    `program_name`,
    `program_type`,
    `reference`,
    `application_id`,
    `application_status`,
    `amendmentno`,
    `utilizedamount`,
    `unutilizedamount`,
    `totalapprovedamount`,
    `totalavailableamt`,
    `utilizedamt`,
    `unutilizedamt`,
    `customershareamt`,
    `haswagesupportmolemployees`,
    `cpr_number`,
    `customer_enterprise_name`,
    `approved_on_date`,
    `contract_start_date`,
    `monitoring_due_date`,
    `contract_end_date`,
    `total_approved_amount_tamkeen_share`,
    `created_on`,
    `submitted_on`,
    `spending_period_end_date`,
    `approval_letter_confirmed`,
    `dbt_updated_at`,
    `mis_source_table`,
    `amendment_id`,
    `amendment_name`,
    `application_name`,
    `main_company_name`,
    `details`,
    `total_amount`,
    `total_bc_share`,
    `total_tamkeen_share`,
    `tamkeen_share`,
    `tamkeen_share_state`,
    `tamkeen_share_last_updated_on`,
    `amended_flag`,
    `products`,
    `amendment_reason`,
    `old_workflow_status`,
    `reason`,
    `amendment_type`,
    `workflow_status`,
    `status_reason`,
    `state`,
    `owner_name`,
    `identity_created_by`,
    `identity_modified_by`,
    `created_by`,
    `modified_by`,
    `identity_created_on`,
    `identity_modified_on`,
    `modified_on`,
    `source_system_name`,
    `is_deleted`,
    `report_date`
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`amendment_base`
),

bronze_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'program_name'),
        (2, 'program_type'),
        (3, 'reference'),
        (4, 'application_id'),
        (5, 'application_status'),
        (6, 'amendmentno'),
        (7, 'utilizedamount'),
        (8, 'unutilizedamount'),
        (9, 'totalapprovedamount'),
        (10, 'totalavailableamt'),
        (11, 'utilizedamt'),
        (12, 'unutilizedamt'),
        (13, 'customershareamt'),
        (14, 'haswagesupportmolemployees'),
        (15, 'cpr_number'),
        (16, 'customer_enterprise_name'),
        (17, 'approved_on_date'),
        (18, 'contract_start_date'),
        (19, 'monitoring_due_date'),
        (20, 'contract_end_date'),
        (21, 'total_approved_amount_tamkeen_share'),
        (22, 'created_on'),
        (23, 'submitted_on'),
        (24, 'spending_period_end_date'),
        (25, 'approval_letter_confirmed'),
        (26, 'dbt_updated_at'),
        (27, 'mis_source_table'),
        (28, 'amendment_id'),
        (29, 'amendment_name'),
        (30, 'application_name'),
        (31, 'main_company_name'),
        (32, 'details'),
        (33, 'total_amount'),
        (34, 'total_bc_share'),
        (35, 'total_tamkeen_share'),
        (36, 'tamkeen_share'),
        (37, 'tamkeen_share_state'),
        (38, 'tamkeen_share_last_updated_on'),
        (39, 'amended_flag'),
        (40, 'products'),
        (41, 'amendment_reason'),
        (42, 'old_workflow_status'),
        (43, 'reason'),
        (44, 'amendment_type'),
        (45, 'workflow_status'),
        (46, 'status_reason'),
        (47, 'state'),
        (48, 'owner_name'),
        (49, 'identity_created_by'),
        (50, 'identity_modified_by'),
        (51, 'created_by'),
        (52, 'modified_by'),
        (53, 'identity_created_on'),
        (54, 'identity_modified_on'),
        (55, 'modified_on'),
        (56, 'source_system_name'),
        (57, 'is_deleted'),
        (58, 'report_date')
    ) AS t(column_position, column_name)
),

silver_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'program_name'),
        (2, 'program_type'),
        (3, 'reference'),
        (4, 'application_id'),
        (5, 'application_status'),
        (6, 'amendmentno'),
        (7, 'utilizedamount'),
        (8, 'unutilizedamount'),
        (9, 'totalapprovedamount'),
        (10, 'totalavailableamt'),
        (11, 'utilizedamt'),
        (12, 'unutilizedamt'),
        (13, 'customershareamt'),
        (14, 'haswagesupportmolemployees'),
        (15, 'cpr_number'),
        (16, 'customer_enterprise_name'),
        (17, 'approved_on_date'),
        (18, 'contract_start_date'),
        (19, 'monitoring_due_date'),
        (20, 'contract_end_date'),
        (21, 'total_approved_amount_tamkeen_share'),
        (22, 'created_on'),
        (23, 'submitted_on'),
        (24, 'spending_period_end_date'),
        (25, 'approval_letter_confirmed'),
        (26, 'dbt_updated_at'),
        (27, 'mis_source_table'),
        (28, 'amendment_id'),
        (29, 'amendment_name'),
        (30, 'application_name'),
        (31, 'main_company_name'),
        (32, 'details'),
        (33, 'total_amount'),
        (34, 'total_bc_share'),
        (35, 'total_tamkeen_share'),
        (36, 'tamkeen_share'),
        (37, 'tamkeen_share_state'),
        (38, 'tamkeen_share_last_updated_on'),
        (39, 'amended_flag'),
        (40, 'products'),
        (41, 'amendment_reason'),
        (42, 'old_workflow_status'),
        (43, 'reason'),
        (44, 'amendment_type'),
        (45, 'workflow_status'),
        (46, 'status_reason'),
        (47, 'state'),
        (48, 'owner_name'),
        (49, 'identity_created_by'),
        (50, 'identity_modified_by'),
        (51, 'created_by'),
        (52, 'modified_by'),
        (53, 'identity_created_on'),
        (54, 'identity_modified_on'),
        (55, 'modified_on'),
        (56, 'source_system_name'),
        (57, 'is_deleted'),
        (58, 'report_date')
    ) AS t(column_position, column_name)
),

bronze_normalized AS (
    SELECT
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_type` AS STRING) AS `program_type`,
        CAST(`reference` AS STRING) AS `reference`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`application_status` AS STRING) AS `application_status`,
        CAST(`amendmentno` AS STRING) AS `amendmentno`,
        CAST(`utilizedamount` AS STRING) AS `utilizedamount`,
        CAST(`unutilizedamount` AS STRING) AS `unutilizedamount`,
        CAST(`totalapprovedamount` AS STRING) AS `totalapprovedamount`,
        CAST(`totalavailableamt` AS STRING) AS `totalavailableamt`,
        CAST(`utilizedamt` AS STRING) AS `utilizedamt`,
        CAST(`unutilizedamt` AS STRING) AS `unutilizedamt`,
        CAST(`customershareamt` AS STRING) AS `customershareamt`,
        CAST(`haswagesupportmolemployees` AS STRING) AS `haswagesupportmolemployees`,
        CAST(`cpr_number` AS STRING) AS `cpr_number`,
        CAST(`customer_enterprise_name` AS STRING) AS `customer_enterprise_name`,
        CAST(`approved_on_date` AS STRING) AS `approved_on_date`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`monitoring_due_date` AS STRING) AS `monitoring_due_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`total_approved_amount_tamkeen_share` AS STRING) AS `total_approved_amount_tamkeen_share`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`spending_period_end_date` AS STRING) AS `spending_period_end_date`,
        CAST(`approval_letter_confirmed` AS STRING) AS `approval_letter_confirmed`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`amendment_id` AS STRING) AS `amendment_id`,
        CAST(`amendment_name` AS STRING) AS `amendment_name`,
        CAST(`application_name` AS STRING) AS `application_name`,
        CAST(`main_company_name` AS STRING) AS `main_company_name`,
        CAST(`details` AS STRING) AS `details`,
        CAST(`total_amount` AS STRING) AS `total_amount`,
        CAST(`total_bc_share` AS STRING) AS `total_bc_share`,
        CAST(`total_tamkeen_share` AS STRING) AS `total_tamkeen_share`,
        CAST(`tamkeen_share` AS STRING) AS `tamkeen_share`,
        CAST(`tamkeen_share_state` AS STRING) AS `tamkeen_share_state`,
        CAST(`tamkeen_share_last_updated_on` AS STRING) AS `tamkeen_share_last_updated_on`,
        CAST(`amended_flag` AS STRING) AS `amended_flag`,
        CAST(`products` AS STRING) AS `products`,
        CAST(`amendment_reason` AS STRING) AS `amendment_reason`,
        CAST(`old_workflow_status` AS STRING) AS `old_workflow_status`,
        CAST(`reason` AS STRING) AS `reason`,
        CAST(`amendment_type` AS STRING) AS `amendment_type`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`identity_created_by` AS STRING) AS `identity_created_by`,
        CAST(`identity_modified_by` AS STRING) AS `identity_modified_by`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`identity_created_on` AS STRING) AS `identity_created_on`,
        CAST(`identity_modified_on` AS STRING) AS `identity_modified_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_type` AS STRING) AS `program_type`,
        CAST(`reference` AS STRING) AS `reference`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`application_status` AS STRING) AS `application_status`,
        CAST(`amendmentno` AS STRING) AS `amendmentno`,
        CAST(`utilizedamount` AS STRING) AS `utilizedamount`,
        CAST(`unutilizedamount` AS STRING) AS `unutilizedamount`,
        CAST(`totalapprovedamount` AS STRING) AS `totalapprovedamount`,
        CAST(`totalavailableamt` AS STRING) AS `totalavailableamt`,
        CAST(`utilizedamt` AS STRING) AS `utilizedamt`,
        CAST(`unutilizedamt` AS STRING) AS `unutilizedamt`,
        CAST(`customershareamt` AS STRING) AS `customershareamt`,
        CAST(`haswagesupportmolemployees` AS STRING) AS `haswagesupportmolemployees`,
        CAST(`cpr_number` AS STRING) AS `cpr_number`,
        CAST(`customer_enterprise_name` AS STRING) AS `customer_enterprise_name`,
        CAST(`approved_on_date` AS STRING) AS `approved_on_date`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`monitoring_due_date` AS STRING) AS `monitoring_due_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`total_approved_amount_tamkeen_share` AS STRING) AS `total_approved_amount_tamkeen_share`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`spending_period_end_date` AS STRING) AS `spending_period_end_date`,
        CAST(`approval_letter_confirmed` AS STRING) AS `approval_letter_confirmed`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`amendment_id` AS STRING) AS `amendment_id`,
        CAST(`amendment_name` AS STRING) AS `amendment_name`,
        CAST(`application_name` AS STRING) AS `application_name`,
        CAST(`main_company_name` AS STRING) AS `main_company_name`,
        CAST(`details` AS STRING) AS `details`,
        CAST(`total_amount` AS STRING) AS `total_amount`,
        CAST(`total_bc_share` AS STRING) AS `total_bc_share`,
        CAST(`total_tamkeen_share` AS STRING) AS `total_tamkeen_share`,
        CAST(`tamkeen_share` AS STRING) AS `tamkeen_share`,
        CAST(`tamkeen_share_state` AS STRING) AS `tamkeen_share_state`,
        CAST(`tamkeen_share_last_updated_on` AS STRING) AS `tamkeen_share_last_updated_on`,
        CAST(`amended_flag` AS STRING) AS `amended_flag`,
        CAST(`products` AS STRING) AS `products`,
        CAST(`amendment_reason` AS STRING) AS `amendment_reason`,
        CAST(`old_workflow_status` AS STRING) AS `old_workflow_status`,
        CAST(`reason` AS STRING) AS `reason`,
        CAST(`amendment_type` AS STRING) AS `amendment_type`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`identity_created_by` AS STRING) AS `identity_created_by`,
        CAST(`identity_modified_by` AS STRING) AS `identity_modified_by`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`identity_created_on` AS STRING) AS `identity_created_on`,
        CAST(`identity_modified_on` AS STRING) AS `identity_modified_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`
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
        'amendment_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'amendment_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'amendment_base' AS table_name,
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
        'amendment_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'amendment_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
