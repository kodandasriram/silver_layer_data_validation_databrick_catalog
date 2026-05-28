WITH
option_set_values AS (
    SELECT
        LOWER(elv.name) || '|' || LOWER(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING) AS option_key,
        MAX(sm.value) AS option_value
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.STRINGMAP sm
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        LOWER(elv.name) || '|' || LOWER(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING)
),
option_set_map AS (
    SELECT map_from_entries(collect_list(named_struct('key', option_key, 'value', option_value))) AS option_values
    FROM option_set_values
),
os2_combined_app AS (
    SELECT
        PROGVER.COMMERCIALNAME_EN AS program_name,
        PROGRAM.PROFILETYPEID AS program_type,
        APP.REFERENCENUMBER AS reference,
        APP.ID AS application_id,
        APST.LABEL AS application_status,
        CAST(NULL AS BIGINT) AS amendmentno,
        CAST(NULL AS DECIMAL(38, 10)) AS utilizedamount,
        CAST(NULL AS DECIMAL(38, 10)) AS unutilizedamount,
        CAST(NULL AS DECIMAL(38, 10)) AS totalapprovedamount,
        CAST(NULL AS DECIMAL(38, 10)) AS totalavailableamt,
        CAST(NULL AS DECIMAL(38, 10)) AS utilizedamt,
        CAST(NULL AS DECIMAL(38, 10)) AS unutilizedamt,
        APP.customershareamt,
        APP.haswagesupportmolemployees,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'IND' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END AS cpr_number,
        CUS.NAMEEN AS customer_enterprise_name,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.APPROVEDON + INTERVAL '3' HOUR
        END AS approved_on_date,
        CASE
            WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.STARTON AS DATE)
        END AS contract_start_date,
        CASE
            WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.MONITORINGDUEDATE AS DATE)
        END AS monitoring_due_date,
        CASE
            WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.ENDON AS DATE)
        END AS contract_end_date,
        APP.TKSHAREAMT AS total_approved_amount_tamkeen_share,
        CASE
            WHEN APP.CREATEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.CREATEDON + INTERVAL '3' HOUR
        END AS created_on,
        CASE
            WHEN APP.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SUBMITTEDON + INTERVAL '3' HOUR
        END AS submitted_on,
        CASE
            WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL '3' HOUR
        END AS spending_period_end_date,
        CAST(NULL AS STRING) AS approval_letter_confirmed,
        'NEO2' AS source_system_name,
        FALSE AS is_deleted,
        CURRENT_DATE AS report_date,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION APP
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION PROGVER
        ON APP.PROGRAMVERSIONID = PROGVER.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAM PROGRAM
        ON PROGVER.PROGRAMID = PROGRAM.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS APST
        ON APP.APPLICATIONSTATUSID = APST.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_INDIVIDUAL IND
        ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_COMPANY CMP
        ON CUSPROF.CUSTOMERID = CMP.ID

    UNION ALL

    SELECT
        PROGVER.COMMERCIALNAME_EN AS program_name,
        PROGRAM.PROFILETYPEID AS program_type,
        AMED.REFERENCENUMBER AS reference,
        APP.ID AS application_id,
        APST.LABEL AS application_status,
        AMED.amendmentno,
        AMED.utilizedamount,
        AMED.unutilizedamount,
        AMED.totalapprovedamount,
        AMED.totalavailableamt,
        AMED.utilizedamt,
        AMED.unutilizedamt,
        AMED.customershareamt,
        AMED.haswagesupportmolemployees,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'IND' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END AS cpr_number,
        CUS.NAMEEN AS customer_enterprise_name,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.APPROVEDON + INTERVAL '3' HOUR
        END AS approved_on_date,
        CASE
            WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.STARTON AS DATE)
        END AS contract_start_date,
        CASE
            WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.MONITORINGDUEDATE AS DATE)
        END AS monitoring_due_date,
        CASE
            WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.ENDON AS DATE)
        END AS contract_end_date,
        AMED.TKSHAREAMT AS total_approved_amount_tamkeen_share,
        CASE
            WHEN AMED.CREATEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.CREATEDON + INTERVAL '3' HOUR
        END AS created_on,
        CASE
            WHEN AMED.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.SUBMITTEDON + INTERVAL '3' HOUR
        END AS submitted_on,
        CASE
            WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL '3' HOUR
        END AS spending_period_end_date,
        CAST(NULL AS STRING) AS approval_letter_confirmed,
        'NEO2' AS source_system_name,
        FALSE AS is_deleted,
        CURRENT_DATE AS report_date,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION APP
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_AMENDMENTREQUEST AMED
        ON AMED.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION PROGVER
        ON APP.PROGRAMVERSIONID = PROGVER.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAM PROGRAM
        ON PROGVER.PROGRAMID = PROGRAM.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_INDIVIDUAL IND
        ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_COMPANY CMP
        ON CUSPROF.CUSTOMERID = CMP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS APST
        ON AMED.AMENDMENTSTATUSID = APST.CODE
    WHERE APST.LABEL = 'Active'
),
os2_ranked_data AS (
    SELECT
        os2_combined_app.*,
        ROW_NUMBER() OVER (
            PARTITION BY application_id
            ORDER BY created_on DESC
        ) AS rnk
    FROM os2_combined_app
),
os2_data AS (
    SELECT DISTINCT
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
        TRY_CAST(NULLIF(CAST(approved_on_date AS STRING), '') AS TIMESTAMP) AS approved_on_date,
        TRY_CAST(NULLIF(CAST(contract_start_date AS STRING), '') AS TIMESTAMP) AS contract_start_date,
        TRY_CAST(NULLIF(CAST(monitoring_due_date AS STRING), '') AS TIMESTAMP) AS monitoring_due_date,
        TRY_CAST(NULLIF(CAST(contract_end_date AS STRING), '') AS TIMESTAMP) AS contract_end_date,
        total_approved_amount_tamkeen_share,
        TRY_CAST(NULLIF(CAST(created_on AS STRING), '') AS TIMESTAMP) AS created_on,
        TRY_CAST(NULLIF(CAST(submitted_on AS STRING), '') AS TIMESTAMP) AS submitted_on,
        TRY_CAST(NULLIF(CAST(spending_period_end_date AS STRING), '') AS TIMESTAMP) AS spending_period_end_date,
        approval_letter_confirmed,
        dbt_updated_at,
        source_system_name,
        is_deleted,
        CAST(report_date AS TIMESTAMP) AS report_date,
        CAST(NULL AS TIMESTAMP) AS dbt_updated_on
    FROM os2_ranked_data
),
os1_data AS (
    SELECT
        CAST(NULL AS STRING) AS program_name,
        CAST(NULL AS STRING) AS program_type,
        CAST(NULL AS STRING) AS reference,
        CAST(NULL AS BIGINT) AS application_id,
        CAST(NULL AS STRING) AS application_status,
        CAST(NULL AS BIGINT) AS amendmentno,
        CAST(NULL AS DECIMAL(38, 10)) AS utilizedamount,
        CAST(NULL AS DECIMAL(38, 10)) AS unutilizedamount,
        CAST(NULL AS DECIMAL(38, 10)) AS totalapprovedamount,
        CAST(NULL AS DECIMAL(38, 10)) AS totalavailableamt,
        CAST(NULL AS DECIMAL(38, 10)) AS utilizedamt,
        CAST(NULL AS DECIMAL(38, 10)) AS unutilizedamt,
        CAST(NULL AS DECIMAL(38, 10)) AS customershareamt,
        CAST(NULL AS BOOLEAN) AS haswagesupportmolemployees,
        CAST(NULL AS STRING) AS cpr_number,
        CAST(NULL AS STRING) AS customer_enterprise_name,
        CAST(NULL AS TIMESTAMP) AS approved_on_date,
        CAST(NULL AS TIMESTAMP) AS contract_start_date,
        CAST(NULL AS TIMESTAMP) AS monitoring_due_date,
        CAST(NULL AS TIMESTAMP) AS contract_end_date,
        CAST(NULL AS DECIMAL(38, 10)) AS total_approved_amount_tamkeen_share,
        CAST(NULL AS TIMESTAMP) AS created_on,
        CAST(NULL AS TIMESTAMP) AS submitted_on,
        CAST(NULL AS TIMESTAMP) AS spending_period_end_date,
        CAST(NULL AS STRING) AS approval_letter_confirmed,
        CAST(NULL AS TIMESTAMP) AS dbt_updated_at,
        'NEO1' AS source_system_name,
        CAST(FALSE AS BOOLEAN) AS is_deleted,
        CAST(NULL AS TIMESTAMP) AS report_date,
        CAST(NULL AS TIMESTAMP) AS dbt_updated_on
    WHERE FALSE
),
mis_data AS (
    SELECT
        'tmkn_amendment' AS mis_source_table,
        CAST(amnd.tmkn_amendmentid AS STRING) AS amendment_id,
        amnd.tmkn_name AS amendment_name,
        amnd.tmkn_application AS application_name,
        amnd.tmkn_maincompany AS main_company_name,
        amnd.tmkn_details AS details,
        amnd.tmkn_total AS total_amount,
        amnd.tmkn_totalbcshare AS total_bc_share,
        amnd.tmkn_totaltmknshare AS total_tamkeen_share,
        amnd.tmkn_tamkeenshare AS tamkeen_share,
        amnd.tmkn_tamkeenshare_state AS tamkeen_share_state,
        amnd.tmkn_tamkeenshare_date AS tamkeen_share_last_updated_on,
        CASE WHEN amnd.tmkn_amedned IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('tmkn_amendmentbase') || '|' || CAST(amnd.tmkn_amedned AS STRING)) END AS amended_flag,
        CASE WHEN amnd.tmkn_products IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('tmkn_products') || '|' || CAST(amnd.tmkn_products AS STRING)) END AS products,
        CASE WHEN amnd.tmkn_amendmentreason IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('tmkn_amendmentreason') || '|' || CAST(amnd.tmkn_amendmentreason AS STRING)) END AS amendment_reason,
        CASE WHEN amnd.mis_workflowstatus IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('mis_workflowstatus') || '|' || CAST(amnd.mis_workflowstatus AS STRING)) END AS old_workflow_status,
        CASE WHEN amnd.tmkn_reason IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('tmkn_reason') || '|' || CAST(amnd.tmkn_reason AS STRING)) END AS reason,
        CASE WHEN amnd.tmkn_type IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('tmkn_type') || '|' || CAST(amnd.tmkn_type AS STRING)) END AS amendment_type,
        CASE WHEN amnd.tmkn_workflowstatus IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('tmkn_workflowstatus') || '|' || CAST(amnd.tmkn_workflowstatus AS STRING)) END AS workflow_status,
        CASE WHEN amnd.statuscode IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('statuscode') || '|' || CAST(amnd.statuscode AS STRING)) END AS status_reason,
        CASE WHEN amnd.statecode IS NULL THEN NULL ELSE ELEMENT_AT((SELECT option_values FROM option_set_map), LOWER('TMKN_AMENDMENTBASE') || '|' || LOWER('statecode') || '|' || CAST(amnd.statecode AS STRING)) END AS state,
        amnd.ownerid AS owner_name,
        amnd.identity_createdby AS identity_created_by,
        amnd.identity_modifiedby AS identity_modified_by,
        amnd.createdby AS created_by,
        amnd.modifiedby AS modified_by,
        amnd.identity_createdon AS identity_created_on,
        amnd.identity_modifiedon AS identity_modified_on,
        amnd.createdon AS created_on,
        amnd.modifiedon AS modified_on,
        'MIS' AS source_system_name,
        FALSE AS is_deleted,
        CAST(CURRENT_DATE AS TIMESTAMP) AS report_date,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_on
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_AMENDMENTBASE amnd
),
-- Bronze-layer UNION ALL for amendment_base across OS2, OS1, and MIS.
-- Output column order follows the amendment_base silver/dbt comparison shape.
-- Note: no OS1 amendment_base source was present in the supplied/current OS1 scripts.
-- The OS1 branch is kept as a typed zero-row branch so the source is planned
-- explicitly without inventing a table or business logic.
bronze_layer AS (
SELECT
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
    CAST(NULL AS STRING) AS mis_source_table,
    CAST(NULL AS STRING) AS amendment_id,
    CAST(NULL AS STRING) AS amendment_name,
    CAST(NULL AS STRING) AS application_name,
    CAST(NULL AS STRING) AS main_company_name,
    CAST(NULL AS STRING) AS details,
    CAST(NULL AS DECIMAL(38, 10)) AS total_amount,
    CAST(NULL AS DECIMAL(38, 10)) AS total_bc_share,
    CAST(NULL AS DECIMAL(38, 10)) AS total_tamkeen_share,
    CAST(NULL AS DECIMAL(38, 10)) AS tamkeen_share,
    CAST(NULL AS INTEGER) AS tamkeen_share_state,
    CAST(NULL AS TIMESTAMP) AS tamkeen_share_last_updated_on,
    CAST(NULL AS STRING) AS amended_flag,
    CAST(NULL AS STRING) AS products,
    CAST(NULL AS STRING) AS amendment_reason,
    CAST(NULL AS STRING) AS old_workflow_status,
    CAST(NULL AS STRING) AS reason,
    CAST(NULL AS STRING) AS amendment_type,
    CAST(NULL AS STRING) AS workflow_status,
    CAST(NULL AS STRING) AS status_reason,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS STRING) AS owner_name,
    CAST(NULL AS STRING) AS identity_created_by,
    CAST(NULL AS STRING) AS identity_modified_by,
    CAST(NULL AS STRING) AS created_by,
    CAST(NULL AS STRING) AS modified_by,
    CAST(NULL AS TIMESTAMP) AS identity_created_on,
    CAST(NULL AS TIMESTAMP) AS identity_modified_on,
    CAST(NULL AS TIMESTAMP) AS modified_on,
    source_system_name,
    is_deleted,
    report_date
    -- dbt_updated_on
FROM os2_data

UNION ALL

SELECT
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
    CAST(NULL AS STRING) AS mis_source_table,
    CAST(NULL AS STRING) AS amendment_id,
    CAST(NULL AS STRING) AS amendment_name,
    CAST(NULL AS STRING) AS application_name,
    CAST(NULL AS STRING) AS main_company_name,
    CAST(NULL AS STRING) AS details,
    CAST(NULL AS DECIMAL(38, 10)) AS total_amount,
    CAST(NULL AS DECIMAL(38, 10)) AS total_bc_share,
    CAST(NULL AS DECIMAL(38, 10)) AS total_tamkeen_share,
    CAST(NULL AS DECIMAL(38, 10)) AS tamkeen_share,
    CAST(NULL AS INTEGER) AS tamkeen_share_state,
    CAST(NULL AS TIMESTAMP) AS tamkeen_share_last_updated_on,
    CAST(NULL AS STRING) AS amended_flag,
    CAST(NULL AS STRING) AS products,
    CAST(NULL AS STRING) AS amendment_reason,
    CAST(NULL AS STRING) AS old_workflow_status,
    CAST(NULL AS STRING) AS reason,
    CAST(NULL AS STRING) AS amendment_type,
    CAST(NULL AS STRING) AS workflow_status,
    CAST(NULL AS STRING) AS status_reason,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS STRING) AS owner_name,
    CAST(NULL AS STRING) AS identity_created_by,
    CAST(NULL AS STRING) AS identity_modified_by,
    CAST(NULL AS STRING) AS created_by,
    CAST(NULL AS STRING) AS modified_by,
    CAST(NULL AS TIMESTAMP) AS identity_created_on,
    CAST(NULL AS TIMESTAMP) AS identity_modified_on,
    CAST(NULL AS TIMESTAMP) AS modified_on,
    source_system_name,
    is_deleted,
    report_date
    -- dbt_updated_on
FROM os1_data

UNION ALL

SELECT
    CAST(NULL AS STRING) AS program_name,
    CAST(NULL AS STRING) AS program_type,
    CAST(NULL AS STRING) AS reference,
    CAST(NULL AS BIGINT) AS application_id,
    CAST(NULL AS STRING) AS application_status,
    CAST(NULL AS BIGINT) AS amendmentno,
    CAST(NULL AS DECIMAL(38, 10)) AS utilizedamount,
    CAST(NULL AS DECIMAL(38, 10)) AS unutilizedamount,
    CAST(NULL AS DECIMAL(38, 10)) AS totalapprovedamount,
    CAST(NULL AS DECIMAL(38, 10)) AS totalavailableamt,
    CAST(NULL AS DECIMAL(38, 10)) AS utilizedamt,
    CAST(NULL AS DECIMAL(38, 10)) AS unutilizedamt,
    CAST(NULL AS DECIMAL(38, 10)) AS customershareamt,
    CAST(NULL AS BOOLEAN) AS haswagesupportmolemployees,
    CAST(NULL AS STRING) AS cpr_number,
    CAST(NULL AS STRING) AS customer_enterprise_name,
    CAST(NULL AS TIMESTAMP) AS approved_on_date,
    CAST(NULL AS TIMESTAMP) AS contract_start_date,
    CAST(NULL AS TIMESTAMP) AS monitoring_due_date,
    CAST(NULL AS TIMESTAMP) AS contract_end_date,
    CAST(NULL AS DECIMAL(38, 10)) AS total_approved_amount_tamkeen_share,
    CAST(NULL AS TIMESTAMP) AS created_on,
    CAST(NULL AS TIMESTAMP) AS submitted_on,
    CAST(NULL AS TIMESTAMP) AS spending_period_end_date,
    CAST(NULL AS STRING) AS approval_letter_confirmed,
    CAST(NULL AS TIMESTAMP) AS dbt_updated_at,
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
    -- dbt_updated_on
FROM mis_data
),

silver_layer AS (
SELECT
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
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.amendment_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
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
        -- (59, 'dbt_updated_on')
),

silver_columns(column_position, column_name) AS (
    VALUES
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
