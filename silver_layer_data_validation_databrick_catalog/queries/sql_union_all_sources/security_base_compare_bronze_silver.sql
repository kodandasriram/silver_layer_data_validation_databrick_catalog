-- Compare bronze-layer query output with silver-layer table output for security_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\security_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\security_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Databricks SQL generated from security_base.sql.
-- Final column order aligned to silver_layer_query/security_base_silver_layer.sql.
-- Standalone Databricks SQL converted from dbt model.
/*
 =================================================================================================

Name        : security_base_os2
Description : This model consolidates Security Cheque ticket and application-related
              information from OS2 source tables. It captures cheque workflow status,
              cheque details, program information, customer details, collection
              methods, bank information, and submitted locations.

              The model standardizes timestamps, replaces default placeholder dates
              with NULL values, and enriches the dataset with workflow status details
              for downstream Silver Layer consumption.

Source Tables : `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_TICKET`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYCHEQUE`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCUSTOMER`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_COLLECTIONMETHOD`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SUBMITTEDLOCATION`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_BANK`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYTYPE`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYSTATUS`
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYCHEQUESTATUS`

Target Table : SECURITY_CHEQUE

Load Type    : Full Load (Table)

Materialized : table

Format       : PARQUET

Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-12 | Vignesh | Initial version

================================================================================================= 
*/
SELECT
    sc.id                                         as id,
    APP.ID                                               AS application_id,
    APP.REFERENCENUMBER                                  AS application_no,
    sc.securityid                                 as security_id,
    SC.SECURITYNUMBER                                    AS security_number,
    sectype.LABEL                                        AS security_type,
    sc.securitytypeid                             as security_type_id,
    sc.securitystatusid                           as security_status_id,
    sc.securitychequestatus                       as security_cheque_status,
    SC.SECURITYID                                        AS security_cheque_ref,
    tik.REFNUMBER                                        AS security_cheque_no_tickets,
    sc.securitychequeprocessingstep               as security_cheque_processing_step,
    secstat.LABEL                                        AS workflow_status_security_cheque,
    secstatdetailed.LABEL                                AS workflow_status_detailed_security_cheque,
    ProgVer.COMMERCIALNAME_EN                            AS program_name,
    collect.LABEL                                        AS collection_method,
    sc.collectionmethodid                         as collection_method_id,
    SC.SECURITYAMOUNT                                    AS cheque_amount,
    sc.chequerefnumber                            as cheque_ref_number,
    sc.securityamount                             as security_amount,
    sc.applicationid                              as security_application_id,
    sc.bankid                                     as bank_id,
    CASE
        WHEN SC.BANKID = 0
        THEN 'Unclassified'
        ELSE UPPER(TRIM(bank.BankName))
    END                                                  AS bank_name,
    sc.submittedlocationid                        as submitted_location_id,
    sublocation.LABEL                                    AS submitted_location,
    sc.deliverymethodid                           as delivery_method_id,
    sc.portaluser                                 as portal_user,
    sc.releaseagentuserid                         as release_agent_user_id,
    sc.releasecomments                            as release_comments,
    sc.collectorcpr                               as collector_cpr,
    sc.collectorname                              as collector_name,
    sc.collectorsrelationshiptoente               as collector_relationship_to_ente,
    usr.NAME                                             AS owner,
    UPPER(TRIM(CUS.NAMEEN))                              AS commercial_name,
    CMP.CODE                                             AS cr_license_no,
    CASE
        WHEN SC.RELEASEDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN 'No'
        ELSE 'Yes'
    END                                                  AS replaced,
    sc.echequeprocessexternalevents               as echeque_process_external_events,
    FALSE AS is_deleted,
    'NEO2' AS source_system_name,
    CURRENT_TIMESTAMP + INTERVAL 3 HOURS                AS extract_date,
    sc.securitydate                               as security_date,
    sc.issuedate                                  as issue_date,
    sc.releasedon                                 as released_on,
    CASE
        WHEN SC.REPLACEDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE SC.REPLACEDON + INTERVAL 3 HOURS
    END                                                  AS replaced_on,
    CASE
        WHEN SC.CREATEDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE SC.CREATEDON + INTERVAL 3 HOURS
    END                                                  AS date_collected,
    CASE
        WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE CAST(APP.ENDON AS DATE)
    END                                                  AS contact_end_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_TICKET` tik

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_TICKETSTATUS` tikstat
    ON tik.TICKETSTATUSID = tikstat.CODE

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYCHEQUE` SC
    ON tik.ENTITYIDENTIFIER = SC.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION` APP
    ON APP.ID = SC.APPLICATIONID

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

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_COLLECTIONMETHOD` collect
    ON SC.CollectionMethodId = collect.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SUBMITTEDLOCATION` sublocation
    ON sublocation.ID = SC.SubmittedLocationId

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER` usr
    ON usr.USERNAME = SC.CREATEDBY

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_BANK` bank
    ON bank.ID = SC.BANKID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYTYPE` sectype
    ON sectype.ID = SC.SecurityTypeId

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYSTATUS` secstat
    ON secstat.ID = SC.SECURITYSTATUSID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5HX_SECURITYCHEQUESTATUS` secstatdetailed
    ON secstatdetailed.CODE = SC.SECURITYCHEQUESTATUS

WHERE tik.TICKETTYPEID = 'SCK'
),

silver_layer AS (
SELECT
    id,
    application_id,
    application_no,
    security_id,
    security_number,
    security_type,
    security_type_id,
    security_status_id,
    security_cheque_status,
    security_cheque_ref,
    security_cheque_no_tickets,
    security_cheque_processing_step,
    workflow_status_security_cheque,
    workflow_status_detailed_security_cheque,
    program_name,
    collection_method,
    collection_method_id,
    cheque_amount,
    cheque_ref_number,
    security_amount,
    security_application_id,
    bank_id,
    bank_name,
    submitted_location_id,
    submitted_location,
    delivery_method_id,
    portal_user,
    release_agent_user_id,
    release_comments,
    collector_cpr,
    collector_name,
    collector_relationship_to_ente,
    owner,
    commercial_name,
    cr_license_no,
    replaced,
    echeque_process_external_events,
    is_deleted,
    source_system_name,
    extract_date,
    security_date,
    issue_date,
    released_on,
    replaced_on,
    date_collected,
    contact_end_date,
    dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`security_base`
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'application_id'),
        (3, 'application_no'),
        (4, 'security_id'),
        (5, 'security_number'),
        (6, 'security_type'),
        (7, 'security_type_id'),
        (8, 'security_status_id'),
        (9, 'security_cheque_status'),
        (10, 'security_cheque_ref'),
        (11, 'security_cheque_no_tickets'),
        (12, 'security_cheque_processing_step'),
        (13, 'workflow_status_security_cheque'),
        (14, 'workflow_status_detailed_security_cheque'),
        (15, 'program_name'),
        (16, 'collection_method'),
        (17, 'collection_method_id'),
        (18, 'cheque_amount'),
        (19, 'cheque_ref_number'),
        (20, 'security_amount'),
        (21, 'security_application_id'),
        (22, 'bank_id'),
        (23, 'bank_name'),
        (24, 'submitted_location_id'),
        (25, 'submitted_location'),
        (26, 'delivery_method_id'),
        (27, 'portal_user'),
        (28, 'release_agent_user_id'),
        (29, 'release_comments'),
        (30, 'collector_cpr'),
        (31, 'collector_name'),
        (32, 'collector_relationship_to_ente'),
        (33, 'owner'),
        (34, 'commercial_name'),
        (35, 'cr_license_no'),
        (36, 'replaced'),
        (37, 'echeque_process_external_events'),
        (38, 'is_deleted'),
        (39, 'source_system_name'),
        (40, 'extract_date'),
        (41, 'security_date'),
        (42, 'issue_date'),
        (43, 'released_on'),
        (44, 'replaced_on'),
        (45, 'date_collected'),
        (46, 'contact_end_date'),
        (47, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'application_id'),
        (3, 'application_no'),
        (4, 'security_id'),
        (5, 'security_number'),
        (6, 'security_type'),
        (7, 'security_type_id'),
        (8, 'security_status_id'),
        (9, 'security_cheque_status'),
        (10, 'security_cheque_ref'),
        (11, 'security_cheque_no_tickets'),
        (12, 'security_cheque_processing_step'),
        (13, 'workflow_status_security_cheque'),
        (14, 'workflow_status_detailed_security_cheque'),
        (15, 'program_name'),
        (16, 'collection_method'),
        (17, 'collection_method_id'),
        (18, 'cheque_amount'),
        (19, 'cheque_ref_number'),
        (20, 'security_amount'),
        (21, 'security_application_id'),
        (22, 'bank_id'),
        (23, 'bank_name'),
        (24, 'submitted_location_id'),
        (25, 'submitted_location'),
        (26, 'delivery_method_id'),
        (27, 'portal_user'),
        (28, 'release_agent_user_id'),
        (29, 'release_comments'),
        (30, 'collector_cpr'),
        (31, 'collector_name'),
        (32, 'collector_relationship_to_ente'),
        (33, 'owner'),
        (34, 'commercial_name'),
        (35, 'cr_license_no'),
        (36, 'replaced'),
        (37, 'echeque_process_external_events'),
        (38, 'is_deleted'),
        (39, 'source_system_name'),
        (40, 'extract_date'),
        (41, 'security_date'),
        (42, 'issue_date'),
        (43, 'released_on'),
        (44, 'replaced_on'),
        (45, 'date_collected'),
        (46, 'contact_end_date'),
        (47, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`security_id` AS STRING) AS `security_id`,
        CAST(`security_number` AS STRING) AS `security_number`,
        CAST(`security_type` AS STRING) AS `security_type`,
        CAST(`security_type_id` AS STRING) AS `security_type_id`,
        CAST(`security_status_id` AS STRING) AS `security_status_id`,
        CAST(`security_cheque_status` AS STRING) AS `security_cheque_status`,
        CAST(`security_cheque_ref` AS STRING) AS `security_cheque_ref`,
        CAST(`security_cheque_no_tickets` AS STRING) AS `security_cheque_no_tickets`,
        CAST(`security_cheque_processing_step` AS STRING) AS `security_cheque_processing_step`,
        CAST(`workflow_status_security_cheque` AS STRING) AS `workflow_status_security_cheque`,
        CAST(`workflow_status_detailed_security_cheque` AS STRING) AS `workflow_status_detailed_security_cheque`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`collection_method` AS STRING) AS `collection_method`,
        CAST(`collection_method_id` AS STRING) AS `collection_method_id`,
        CAST(`cheque_amount` AS STRING) AS `cheque_amount`,
        CAST(`cheque_ref_number` AS STRING) AS `cheque_ref_number`,
        CAST(`security_amount` AS STRING) AS `security_amount`,
        CAST(`security_application_id` AS STRING) AS `security_application_id`,
        CAST(`bank_id` AS STRING) AS `bank_id`,
        CAST(`bank_name` AS STRING) AS `bank_name`,
        CAST(`submitted_location_id` AS STRING) AS `submitted_location_id`,
        CAST(`submitted_location` AS STRING) AS `submitted_location`,
        CAST(`delivery_method_id` AS STRING) AS `delivery_method_id`,
        CAST(`portal_user` AS STRING) AS `portal_user`,
        CAST(`release_agent_user_id` AS STRING) AS `release_agent_user_id`,
        CAST(`release_comments` AS STRING) AS `release_comments`,
        CAST(`collector_cpr` AS STRING) AS `collector_cpr`,
        CAST(`collector_name` AS STRING) AS `collector_name`,
        CAST(`collector_relationship_to_ente` AS STRING) AS `collector_relationship_to_ente`,
        CAST(`owner` AS STRING) AS `owner`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`replaced` AS STRING) AS `replaced`,
        CAST(`echeque_process_external_events` AS STRING) AS `echeque_process_external_events`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`security_date` AS STRING) AS `security_date`,
        CAST(`issue_date` AS STRING) AS `issue_date`,
        CAST(`released_on` AS STRING) AS `released_on`,
        CAST(`replaced_on` AS STRING) AS `replaced_on`,
        CAST(`date_collected` AS STRING) AS `date_collected`,
        CAST(`contact_end_date` AS STRING) AS `contact_end_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`security_id` AS STRING) AS `security_id`,
        CAST(`security_number` AS STRING) AS `security_number`,
        CAST(`security_type` AS STRING) AS `security_type`,
        CAST(`security_type_id` AS STRING) AS `security_type_id`,
        CAST(`security_status_id` AS STRING) AS `security_status_id`,
        CAST(`security_cheque_status` AS STRING) AS `security_cheque_status`,
        CAST(`security_cheque_ref` AS STRING) AS `security_cheque_ref`,
        CAST(`security_cheque_no_tickets` AS STRING) AS `security_cheque_no_tickets`,
        CAST(`security_cheque_processing_step` AS STRING) AS `security_cheque_processing_step`,
        CAST(`workflow_status_security_cheque` AS STRING) AS `workflow_status_security_cheque`,
        CAST(`workflow_status_detailed_security_cheque` AS STRING) AS `workflow_status_detailed_security_cheque`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`collection_method` AS STRING) AS `collection_method`,
        CAST(`collection_method_id` AS STRING) AS `collection_method_id`,
        CAST(`cheque_amount` AS STRING) AS `cheque_amount`,
        CAST(`cheque_ref_number` AS STRING) AS `cheque_ref_number`,
        CAST(`security_amount` AS STRING) AS `security_amount`,
        CAST(`security_application_id` AS STRING) AS `security_application_id`,
        CAST(`bank_id` AS STRING) AS `bank_id`,
        CAST(`bank_name` AS STRING) AS `bank_name`,
        CAST(`submitted_location_id` AS STRING) AS `submitted_location_id`,
        CAST(`submitted_location` AS STRING) AS `submitted_location`,
        CAST(`delivery_method_id` AS STRING) AS `delivery_method_id`,
        CAST(`portal_user` AS STRING) AS `portal_user`,
        CAST(`release_agent_user_id` AS STRING) AS `release_agent_user_id`,
        CAST(`release_comments` AS STRING) AS `release_comments`,
        CAST(`collector_cpr` AS STRING) AS `collector_cpr`,
        CAST(`collector_name` AS STRING) AS `collector_name`,
        CAST(`collector_relationship_to_ente` AS STRING) AS `collector_relationship_to_ente`,
        CAST(`owner` AS STRING) AS `owner`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`replaced` AS STRING) AS `replaced`,
        CAST(`echeque_process_external_events` AS STRING) AS `echeque_process_external_events`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`security_date` AS STRING) AS `security_date`,
        CAST(`issue_date` AS STRING) AS `issue_date`,
        CAST(`released_on` AS STRING) AS `released_on`,
        CAST(`replaced_on` AS STRING) AS `replaced_on`,
        CAST(`date_collected` AS STRING) AS `date_collected`,
        CAST(`contact_end_date` AS STRING) AS `contact_end_date`,
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
        'security_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'security_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'security_base' AS table_name,
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
        'security_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'security_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
