WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for ticket_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: ticket_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH ticket_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================
Name        : TICKET_BASE_OS2
Description : This model extracts and transforms ticket-related attributes
              from the NEO2 (OS2) source system Bronze Layer and loads into the
              OSUSR_5HX_TICKET target table as part of the Silver Layer
              data pipeline.
Source Tables : neo2.OSUSR_5HX_TICKET
                neo2.OSUSR_5HX_TICKETTYPE
                neo2.OSUSR_5HX_TICKETSTATUS
                neo2.OSSYS_BPM_ACTIVITY
                neo2.OSSYS_USER
                neo2.OSSYS_BPM_ACTIVITY_DEFINITION
                neo2.OSSYS_BPM_ACTIVITY_KIND
Target Table : OSUSR_5HX_TICKET
Load Type    : Full Load
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily
Revision History:
--------------------------------------------------------------
Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-05-12 |    Kaviya      | Initial version
================================================================================================= 
*/
WITH ACTIVITY_CTE AS (

    SELECT
        tik.ID,
        tik.GUID,
        tik.PROCESSID,
        tik.ENTITYIDENTIFIER,
        tik.REFNUMBER,
        tik.ISACTIVE,
        tik.TKCHANNELID,
        tik.TICKETTYPEID,
        tik.TICKETSTATUSID,

        tikstat.LABEL AS STATUS,
        tiktype.LABEL AS TYPE,

        CASE
            WHEN tik.CREATEDON = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
            ELSE tik.CREATEDON + INTERVAL '3' HOUR
        END AS RECEIVED_ON,

        CASE
            WHEN tik.CLOSEDON = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
            ELSE tik.CLOSEDON + INTERVAL '3' HOUR
        END AS ACTIONED_ON,

        CASE
            WHEN tik.CLOSEDON = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
            ELSE TIMESTAMPDIFF(HOUR, tik.CREATEDON, tik.CLOSEDON)
        END AS AVERAGE_RESOLUTION_TIME_HOUR,

        CASE
            WHEN tik.CLOSEDON = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
            ELSE TIMESTAMPDIFF(DAY, tik.CREATEDON, tik.CLOSEDON)
        END AS AVERAGE_RESOLUTION_TIME_DAYS,

        U.NAME AS AGENT_NAME,

        ROW_NUMBER() OVER (PARTITION BY tik.ID ORDER BY tik.UPDATEDON DESC NULLS LAST, tik.CREATEDON DESC NULLS LAST) AS RNK,
           createdon,
            updatedon,

        FALSE AS IS_DELETED,
        'NEO2' AS SOURCE_SYSTEM_NAME,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS DBT_UPDATED_AT

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_5HX_TICKET tik

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_5HX_TICKETTYPE tiktype
        ON tik.TICKETTYPEID = tiktype.ID

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_5HX_TICKETSTATUS tikstat
        ON tik.TICKETSTATUSID = tikstat.CODE

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY AC
        ON AC.PROCESS_ID = tik.PROCESSID

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER U
        ON AC.USER_ID = U.ID

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY_DEFINITION actdef
        ON AC.ACTIVITY_DEF_ID = actdef.ID

    WHERE actdef.KIND = (
        SELECT ID
        FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY_KIND
        WHERE NAME = 'Human Activity'
    )
)

SELECT
    id,
    guid,
    processid,
    entityidentifier,
    refnumber,
    isactive,
    tkchannelid,
    tickettypeid,
    ticketstatusid,
    status,
    type,
    received_on,
    actioned_on,
    average_resolution_time_hour,
    average_resolution_time_days,
    agent_name,
    createdon,  
    updatedon,
    is_deleted,
    source_system_name,
    dbt_updated_at
FROM ACTIVITY_CTE app
 WHERE RNK = 1
),
ticket_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING) AS option_key,
        max(sm.value) AS option_value
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.STRINGMAP sm
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING)
),
option_set_map AS (
    SELECT map_from_entries(collect_list(named_struct('key', option_key, 'value', option_value))) AS option_values
    FROM option_set_values
),
/*
============================================================================
silver_ticket_mis.sql
============================================================================
Per-source intermediate Silver model for the Ticket domain Ã¢â‚¬â€ MIS only.

Sources (Ticket domain entities):
  Ã¢Ëœâ€¦ TMKN_VIOLATIONBASE                          Ã¢â‚¬â€ anchor: violation/ticket entity
    TMKN_VIOLATIONBASEtypes                     Ã¢â‚¬â€ lookup: violation type names
    mis_TMKN_VIOLATIONBASE_TMKN_VIOLATIONBASEtypes  Ã¢â‚¬â€ M2M bridge: violation Ã¢â€ â€ types

Reference SP:
  - RPT-143_Violations_Ticket

Structure:
  - TMKN_VIOLATIONBASE is the single anchor Ã¢â‚¬â€ there's only one ticket entity in MIS.
  - The relationship to TMKN_VIOLATIONBASEtypes is many-to-many via the bridge
    table mis_TMKN_VIOLATIONBASE_TMKN_VIOLATIONBASEtypes. A single violation can be
    classified under multiple types.
  - The original SP uses SQL Server's STUFF + FOR XML PATH to aggregate type
    names into a pipe-delimited string. The Trino equivalent is LISTAGG.
  - Per the RPT-143 SP, the violation entity has many denormalised `Name`
    columns referring to other entities (Site Visit, ES Payment, Application,
    Director, Manager, etc.) Ã¢â‚¬â€ these are kept as-is. Cross-domain joins are
    NOT performed here.

Note: TMKN_VIOLATIONBASE has 100+ columns in MIS. This Silver model captures the
columns referenced by RPT-143; if other use cases need additional columns
later, they can be added incrementally.
============================================================================
*/


-- ============================================================================
-- Pre-aggregate violation types via the M2M bridge table
-- Replaces the SP's STUFF + FOR XML PATH pattern with LISTAGG
-- ============================================================================
violation_types_agg AS (
    SELECT
        vt.tmkn_violationid                              AS violation_id,
        LISTAGG(t.tmkn_name, ' | ') WITHIN GROUP (ORDER BY t.tmkn_name)  AS violation_types
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_TMKN_VIOLATION_TMKN_VIOLATIONTYPESBASE vt
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_VIOLATIONTYPESBASE t
           ON t.tmkn_violationtypesid = vt.tmkn_violationtypesid
    GROUP BY vt.tmkn_violationid
)


SELECT
    'TMKN_VIOLATIONBASE' AS mis_source_table,

    -- Identifiers
    CAST(viol.tmkn_violationid AS STRING)               AS violation_id,
    viol.tmkn_violationno                                AS violation_no,
    viol.fvr_referencenumber                             AS reference_number,
    viol.tmkn_crlicenseno                                AS cr_license_no,

    -- Violation type (aggregated from M2M bridge)
    vt_agg.violation_types                               AS violation_types,
    CASE WHEN viol.tmkn_assignedviolationtype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_assignedviolationtype') || '|' || CAST(viol.tmkn_assignedviolationtype AS STRING)) END     AS assigned_violation_type,
    CASE WHEN viol.tmkn_suggestedviolationtype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_suggestedviolationtype') || '|' || CAST(viol.tmkn_suggestedviolationtype AS STRING)) END    AS suggested_violation_type,
    CASE WHEN viol.mis_suggestedviolationlevel IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('mis_suggestedviolationlevel') || '|' || CAST(viol.mis_suggestedviolationlevel AS STRING)) END    AS suggested_violation_level,
    CASE WHEN viol.tmkn_category IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_category') || '|' || CAST(viol.tmkn_category AS STRING)) END                  AS category,
    CASE WHEN viol.tmkn_scheme IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_scheme') || '|' || CAST(viol.tmkn_scheme AS STRING)) END                    AS scheme,

    -- Status / workflow (decoded)
    CASE WHEN viol.tmkn_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_workflowstatus') || '|' || CAST(viol.tmkn_workflowstatus AS STRING)) END            AS workflow_status,
    CASE WHEN viol.tmkn_violationsworkflowstatuslist IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_violationsworkflowstatuslist') || '|' || CAST(viol.tmkn_violationsworkflowstatuslist AS STRING)) END AS violations_workflow_status_list,
    CASE WHEN viol.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('statuscode') || '|' || CAST(viol.statuscode AS STRING)) END                     AS status_reason,
    CASE WHEN viol.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('statecode') || '|' || CAST(viol.statecode AS STRING)) END                      AS state,

    -- Action option-sets (multi-action checkboxes from RPT-143)
    CASE WHEN viol.tmkn_sendwarningletter IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_sendwarningletter') || '|' || CAST(viol.tmkn_sendwarningletter AS STRING)) END                AS action_send_warning_letter,
    CASE WHEN viol.tmkn_payorganizer IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_payorganizer') || '|' || CAST(viol.tmkn_payorganizer AS STRING)) END                     AS action_pay_organizer,
    CASE WHEN viol.tmkn_sendtoeconomiccrimesdivision IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_sendtoeconomiccrimesdivision') || '|' || CAST(viol.tmkn_sendtoeconomiccrimesdivision AS STRING)) END     AS action_send_to_ecd,
    CASE WHEN viol.tmkn_sendforsitevisitinspection IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_sendforsitevisitinspection') || '|' || CAST(viol.tmkn_sendforsitevisitinspection AS STRING)) END       AS action_send_for_site_visit,
    CASE WHEN viol.tmkn_sendtocentralinformaticsorganisation IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_sendtocentralinformaticsorganisation') || '|' || CAST(viol.tmkn_sendtocentralinformaticsorganisation AS STRING)) END AS action_send_to_cio,
    CASE WHEN viol.tmkn_addtoblacklist IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_addtoblacklist') || '|' || CAST(viol.tmkn_addtoblacklist AS STRING)) END                   AS action_add_to_blacklist,
    CASE WHEN viol.tmkn_otherdecisions IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_otherdecisions') || '|' || CAST(viol.tmkn_otherdecisions AS STRING)) END                   AS action_other_decisions,
    CASE WHEN viol.tmkn_batchcreated IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_batchcreated') || '|' || CAST(viol.tmkn_batchcreated AS STRING)) END                     AS payment_batch_created,
    CASE WHEN viol.tmkn_supportbc IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_supportbc') || '|' || CAST(viol.tmkn_supportbc AS STRING)) END                        AS action_support_bc,
    CASE WHEN viol.tmkn_encashguaranteecheque IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_encashguaranteecheque') || '|' || CAST(viol.tmkn_encashguaranteecheque AS STRING)) END            AS action_encash_guarantee_cheque,
    viol.tmkn_otherdecisionstext                         AS other_decisions_text,

    -- Case description option-sets
    CASE WHEN viol.tmkn_bcviolation IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_bcviolation') || '|' || CAST(viol.tmkn_bcviolation AS STRING)) END            AS case_desc_bc_violation,
    CASE WHEN viol.tmkn_forcemajeure IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_forcemajeure') || '|' || CAST(viol.tmkn_forcemajeure AS STRING)) END           AS case_desc_force_majeure,
    CASE WHEN viol.tmkn_bcwithdrawal IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_bcwithdrawal') || '|' || CAST(viol.tmkn_bcwithdrawal AS STRING)) END           AS case_desc_bc_withdrawal,
    CASE WHEN viol.tmkn_other IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_other') || '|' || CAST(viol.tmkn_other AS STRING)) END                  AS case_desc_other,
    CASE WHEN viol.tmkn_cancellationofexhibition IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_cancellationofexhibition') || '|' || CAST(viol.tmkn_cancellationofexhibition AS STRING)) END AS case_desc_cancellation_exhibition,
    CASE WHEN viol.tmkn_vendorviolation IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_VIOLATIONBASE') || '|' || lower('tmkn_vendorviolation') || '|' || CAST(viol.tmkn_vendorviolation AS STRING)) END        AS case_desc_vendor_violation,

    -- Dates
    viol.tmkn_violationfraud                             AS violation_fraud_date,
    viol.tmkn_contractstartdate                          AS contract_start_date,
    viol.tmkn_contractenddate                            AS contract_end_date,

    -- Financial
    viol.fvr_amounttobecollected                         AS amount_to_be_collected,
    viol.fvr_collectedamount                             AS collected_amount,
    viol.fvr_collectedamount_state                       AS collected_amount_state,
    viol.fvr_collectedamount_date                        AS collected_amount_last_updated_on,
    viol.fvr_balance                                     AS balance,
    viol.tmkn_tamkeenshare                               AS tamkeen_share,
    viol.tmkn_bcshare                                    AS bc_share,
    viol.tmkn_totalcontractcost                          AS total_contract_cost,
    viol.exchangerate                                    AS exchange_rate,

    -- Free-text descriptive fields
    viol.tmkn_followupresult                             AS follow_up_result,
    viol.tmkn_managercomment                             AS manager_comment,
    viol.tmkn_directorcomment                            AS director_comment,
    viol.tmkn_initiatorcomments                          AS initiator_comments,
    viol.tmkn_casedetails                                AS case_details,
    viol.tmkn_tamkeensdecision                           AS tamkeens_decision,
    viol.tmkn_consultantjustificationremarks             AS consultant_justification_remarks,
    viol.tmkn_grievancedecisions                         AS grievance_decisions,

    -- Address
    viol.tmkn_building                                   AS building,
    viol.tmkn_road                                       AS road,
    viol.tmkn_flat                                       AS flat,
    viol.tmkn_block                                      AS block,
    viol.tmkn_area                                       AS area,

    -- Contact
    viol.tmkn_commercialname                             AS commercial_name,
    viol.tmkn_mobile                                     AS mobile,
    viol.tmkn_email                                      AS email,
    viol.tmkn_representativename                         AS representative_name,

    -- Cross-entity references (denormalised at source as <name> columns)
    -- These are FK display-name columns. The actual FKs (GUIDs) can be added
    -- if the team needs them for cross-domain joining.
    viol.tmkn_supplier                                   AS supplier_name,
    viol.tmkn_director                                   AS director_name,
    viol.tmkn_manager                                    AS manager_name,
    viol.tmkn_officeranalyst                             AS officer_analyst_name,
    viol.tmkn_consultant                                 AS consultant_name,
    viol.tmkn_organizer                                  AS organizer_name,
    viol.tmkn_contractnumber                             AS contract_number_name,
    viol.tmkn_serviceprovider                            AS service_provider_new_name,
    viol.mis_serviceprovider                             AS service_provider_name,

    viol.tmkn_sitevisit                                  AS site_visit_name,
    viol.tmkn_monitoringticket                           AS monitoring_ticket_name,
    viol.tmkn_applicationno                              AS application_no_name,
    viol.tmkn_individual                                 AS individual_name,
    viol.tmkn_individualapplicationid                    AS individual_application_name,
    viol.tmkn_establishmentapplication                   AS establishment_application_name,
    viol.tmkn_buscontsupapp                              AS bc_support_application_name,
    viol.tmkn_businesscontinuitysupportpayment           AS bc_support_payment_name,
    viol.tmkn_tws_enterpriseapplication                  AS tws_enterprise_application_new_name,
    viol.tmkn_twsenterpriseapplication                   AS tws_enterprise_application_name,
    viol.tmkn_tws_employeeapplication                    AS tws_employee_application_name,
    viol.tmkn_exhibition                                 AS exhibition_name,
    viol.tmkn_fromdepartment                             AS from_department_name,

    -- Payment-related references
    viol.tmkn_bdspayment                                 AS bds_payment_name,
    viol.tmkn_e7trfpaymentid                             AS e7trf_payment_name,
    viol.tmkn_ictpayment                                 AS ict_payment_name,
    viol.tmkn_gappayment                                 AS gap_payment_name,
    viol.tmkn_maspayment                                 AS mas_payment_name,
    viol.tmkn_qmspayment                                 AS qms_payment_name,
    viol.tmkn_tappayment                                 AS tap_payment_name,
    viol.tmkn_espaymentname                              AS es_payment_name_text,
    viol.tmkn_espaymentreq                               AS es_payment_request_name,

    -- Execution record references
    viol.tmkn_bdsexecutionrecord                         AS bds_execution_record_name,
    viol.tmkn_gapexecutionrecord                         AS gap_execution_record_name,
    viol.tmkn_ictexecutionrecords                        AS ict_execution_record_name,
    viol.tmkn_qmsexecutionrecord                         AS qms_execution_record_name,
    viol.tmkn_masexecutionrecord                         AS mas_execution_record_name,
    viol.tmkn_tapexecutionrecord                         AS tap_execution_record_name,

    -- CPP / FVR references
    viol.tmkn_cppclient                                  AS cpp_client_name,
    viol.tmkn_cppbeneficiary                             AS cpp_beneficiary_name,
    viol.tmkn_cpppiinvoice                               AS cpp_pi_invoice_name,
    viol.tmkn_cpptsp                                     AS cpp_tsp_name,
    viol.tmkn_cpptspinvoice                              AS cpp_tsp_invoice_name,
    viol.tmkn_fvrmemeber                                 AS fvr_member_name,

    -- Owner / audit
    viol.ownerid                                         AS owner_name,
    viol.createdby                                       AS created_by,
    viol.modifiedby                                      AS modified_by,
    viol.createdon                                       AS created_on,
    viol.modifiedon                                      AS modified_on,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_VIOLATIONBASE viol
LEFT JOIN VIOLATION_TYPES_AGG vt_agg ON vt_agg.violation_id = viol.tmkn_violationid
)
select 
id,
guid, 
processid,
entityidentifier,
refnumber,
isactive,
tkchannelid,
tickettypeid,
ticketstatusid,
status,
type,
received_on,
actioned_on,
average_resolution_time_hour,
average_resolution_time_days,
agent_name,
is_deleted,
source_system_name,
dbt_updated_at,
createdon,
updatedon,
cast (null as STRING) as mis_source_table,
cast (null as STRING) as violation_id,
cast (null as STRING) as violation_no,
cast (null as STRING) as reference_number,
cast (null as STRING) as cr_license_no,
cast (null as STRING) as violation_types,
cast (null as STRING) as assigned_violation_type,
cast (null as STRING) as suggested_violation_type,
cast (null as STRING) as suggested_violation_level,
cast (null as STRING) as category,
cast (null as STRING) as scheme,
cast (null as STRING) as workflow_status,
cast (null as STRING) as violations_workflow_status_list,
cast (null as STRING) as status_reason,
cast (null as STRING) as state,
cast (null as STRING) as action_send_warning_letter,
cast (null as STRING) as action_pay_organizer,
cast (null as STRING) as action_send_to_ecd,
cast (null as STRING) as action_send_for_site_visit,
cast (null as STRING) as action_send_to_cio,
cast (null as STRING) as action_add_to_blacklist,
cast (null as STRING) as action_other_decisions,
cast (null as STRING) as payment_batch_created,
cast (null as STRING) as action_support_bc,
cast (null as STRING) as action_encash_guarantee_cheque,
cast (null as STRING) as other_decisions_text,
cast (null as STRING) as case_desc_bc_violation,
cast (null as STRING) as case_desc_force_majeure,
cast (null as STRING) as case_desc_bc_withdrawal,
cast (null as STRING) as case_desc_other,
cast (null as STRING) as case_desc_cancellation_exhibition,
cast (null as STRING) as case_desc_vendor_violation,
cast (null as timestamp) as violation_fraud_date,
cast (null as timestamp) as contract_start_date,
cast (null as timestamp) as contract_end_date,
cast (null as decimal) as amount_to_be_collected,
cast (null as decimal) as collected_amount,
cast (null as integer) as collected_amount_state,
cast (null as timestamp) as collected_amount_last_updated_on,
cast (null as decimal) as balance,
cast (null as decimal) as tamkeen_share,
cast (null as decimal) as bc_share,
cast (null as decimal) as total_contract_cost,
cast (null as decimal) as exchange_rate,
cast (null as STRING) as follow_up_result,
cast (null as STRING) as manager_comment,
cast (null as STRING) as director_comment,
cast (null as STRING) as initiator_comments,
cast (null as STRING) as case_details,
cast (null as STRING) as tamkeens_decision,
cast (null as STRING) as consultant_justification_remarks,
cast (null as STRING) as grievance_decisions,
cast (null as STRING) as building,
cast (null as STRING) as road,
cast (null as STRING) as flat,
cast (null as STRING) as block,
cast (null as STRING) as area,
cast (null as STRING) as commercial_name,
cast (null as STRING) as mobile,
cast (null as STRING) as email,
cast (null as STRING) as representative_name,
cast (null as STRING) as supplier_name,
cast (null as STRING) as director_name,
cast (null as STRING) as manager_name,
cast (null as STRING) as officer_analyst_name,
cast (null as STRING) as consultant_name,
cast (null as STRING) as organizer_name,
cast (null as STRING) as contract_number_name,
cast (null as STRING) as service_provider_new_name,
cast (null as STRING) as service_provider_name,
cast (null as STRING) as site_visit_name,
cast (null as STRING) as monitoring_ticket_name,
cast (null as STRING) as application_no_name,
cast (null as STRING) as individual_name,
cast (null as STRING) as individual_application_name,
cast (null as STRING) as establishment_application_name,
cast (null as STRING) as bc_support_application_name,
cast (null as STRING) as bc_support_payment_name,
cast (null as STRING) as tws_enterprise_application_new_name,
cast (null as STRING) as tws_enterprise_application_name,
cast (null as STRING) as tws_employee_application_name,
cast (null as STRING) as exhibition_name,
cast (null as STRING) as from_department_name,
cast (null as STRING) as bds_payment_name,
cast (null as STRING) as e7trf_payment_name,
cast (null as STRING) as ict_payment_name,
cast (null as STRING) as gap_payment_name,
cast (null as STRING) as mas_payment_name,
cast (null as STRING) as qms_payment_name,
cast (null as STRING) as tap_payment_name,
cast (null as STRING) as es_payment_name_text,
cast (null as STRING) as es_payment_request_name,
cast (null as STRING) as bds_execution_record_name,
cast (null as STRING) as gap_execution_record_name,
cast (null as STRING) as ict_execution_record_name,
cast (null as STRING) as qms_execution_record_name,
cast (null as STRING) as mas_execution_record_name,
cast (null as STRING) as tap_execution_record_name,
cast (null as STRING) as cpp_client_name,
cast (null as STRING) as cpp_beneficiary_name,
cast (null as STRING) as cpp_pi_invoice_name,
cast (null as STRING) as cpp_tsp_name,
cast (null as STRING) as cpp_tsp_invoice_name,
cast (null as STRING) as fvr_member_name,
cast (null as STRING) as owner_name,
cast (null as STRING) as created_by,
cast (null as STRING) as modified_by,
cast (null as timestamp) as created_on,
cast (null as timestamp) as modified_on,
cast (null as date) as report_date
from ticket_base_os2_source

union all

select  
cast (null as bigint ) as id,
cast (null as STRING ) as guid,
cast (null as integer ) as processid,
cast (null as bigint ) as entityidentifier,
cast (null as STRING ) as refnumber,
cast (null as boolean ) as isactive,
cast (null as STRING ) as tkchannelid,
cast (null as STRING ) as tickettypeid,
cast (null as STRING ) as ticketstatusid,
cast (null as STRING ) as status,
cast (null as STRING ) as type,
cast (null as timestamp ) as received_on,
cast (null as timestamp ) as actioned_on,
cast (null as decimal ) as average_resolution_time_hour,
cast (null as decimal ) as average_resolution_time_days,
cast (null as STRING ) as agent_name,
is_deleted,
source_system_name,
dbt_updated_at,
created_on as createdon,
cast(null as timestamp) as updatedon,
mis_source_table,
violation_id,
violation_no,
reference_number,
cr_license_no,
violation_types,
assigned_violation_type,
suggested_violation_type,
suggested_violation_level,
category,
scheme,
workflow_status,
violations_workflow_status_list,
status_reason,
state,
action_send_warning_letter,
action_pay_organizer,
action_send_to_ecd,
action_send_for_site_visit,
action_send_to_cio,
action_add_to_blacklist,
action_other_decisions,
payment_batch_created,
action_support_bc,
action_encash_guarantee_cheque,
other_decisions_text,
case_desc_bc_violation,
case_desc_force_majeure,
case_desc_bc_withdrawal,
case_desc_other,
case_desc_cancellation_exhibition,
case_desc_vendor_violation,
violation_fraud_date,
contract_start_date,
contract_end_date,
amount_to_be_collected,
collected_amount,
collected_amount_state,
collected_amount_last_updated_on,
balance,
tamkeen_share,
bc_share,
total_contract_cost,
exchange_rate,
follow_up_result,
manager_comment,
director_comment,
initiator_comments,
case_details,
tamkeens_decision,
consultant_justification_remarks,
grievance_decisions,
building,
road,
flat,
block,
area,
commercial_name,
mobile,
email,
representative_name,
supplier_name,
director_name,
manager_name,
officer_analyst_name,
consultant_name,
organizer_name,
contract_number_name,
service_provider_new_name,
service_provider_name,
site_visit_name,
monitoring_ticket_name,
application_no_name,
individual_name,
individual_application_name,
establishment_application_name,
bc_support_application_name,
bc_support_payment_name,
tws_enterprise_application_new_name,
tws_enterprise_application_name,
tws_employee_application_name,
exhibition_name,
from_department_name,
bds_payment_name,
e7trf_payment_name,
ict_payment_name,
gap_payment_name,
mas_payment_name,
qms_payment_name,
tap_payment_name,
es_payment_name_text,
es_payment_request_name,
bds_execution_record_name,
gap_execution_record_name,
ict_execution_record_name,
qms_execution_record_name,
mas_execution_record_name,
tap_execution_record_name,
cpp_client_name,
cpp_beneficiary_name,
cpp_pi_invoice_name,
cpp_tsp_name,
cpp_tsp_invoice_name,
fvr_member_name,
owner_name,
created_by,
modified_by,
created_on,
modified_on,
report_date
from ticket_base_mis_source
),

silver_layer AS (
SELECT
    id,
    guid,
    processid,
    entityidentifier,
    refnumber,
    isactive,
    tkchannelid,
    tickettypeid,
    ticketstatusid,
    status,
    type,
    received_on,
    actioned_on,
    average_resolution_time_hour,
    average_resolution_time_days,
    agent_name,
    is_deleted,
    source_system_name,
    dbt_updated_at,
    createdon,
    updatedon,
    mis_source_table,
    violation_id,
    violation_no,
    reference_number,
    cr_license_no,
    violation_types,
    assigned_violation_type,
    suggested_violation_type,
    suggested_violation_level,
    category,
    scheme,
    workflow_status,
    violations_workflow_status_list,
    status_reason,
    state,
    action_send_warning_letter,
    action_pay_organizer,
    action_send_to_ecd,
    action_send_for_site_visit,
    action_send_to_cio,
    action_add_to_blacklist,
    action_other_decisions,
    payment_batch_created,
    action_support_bc,
    action_encash_guarantee_cheque,
    other_decisions_text,
    case_desc_bc_violation,
    case_desc_force_majeure,
    case_desc_bc_withdrawal,
    case_desc_other,
    case_desc_cancellation_exhibition,
    case_desc_vendor_violation,
    violation_fraud_date,
    contract_start_date,
    contract_end_date,
    amount_to_be_collected,
    collected_amount,
    collected_amount_state,
    collected_amount_last_updated_on,
    balance,
    tamkeen_share,
    bc_share,
    total_contract_cost,
    exchange_rate,
    follow_up_result,
    manager_comment,
    director_comment,
    initiator_comments,
    case_details,
    tamkeens_decision,
    consultant_justification_remarks,
    grievance_decisions,
    building,
    road,
    flat,
    block,
    area,
    commercial_name,
    mobile,
    email,
    representative_name,
    supplier_name,
    director_name,
    manager_name,
    officer_analyst_name,
    consultant_name,
    organizer_name,
    contract_number_name,
    service_provider_new_name,
    service_provider_name,
    site_visit_name,
    monitoring_ticket_name,
    application_no_name,
    individual_name,
    individual_application_name,
    establishment_application_name,
    bc_support_application_name,
    bc_support_payment_name,
    tws_enterprise_application_new_name,
    tws_enterprise_application_name,
    tws_employee_application_name,
    exhibition_name,
    from_department_name,
    bds_payment_name,
    e7trf_payment_name,
    ict_payment_name,
    gap_payment_name,
    mas_payment_name,
    qms_payment_name,
    tap_payment_name,
    es_payment_name_text,
    es_payment_request_name,
    bds_execution_record_name,
    gap_execution_record_name,
    ict_execution_record_name,
    qms_execution_record_name,
    mas_execution_record_name,
    tap_execution_record_name,
    cpp_client_name,
    cpp_beneficiary_name,
    cpp_pi_invoice_name,
    cpp_tsp_name,
    cpp_tsp_invoice_name,
    fvr_member_name,
    owner_name,
    created_by,
    modified_by,
    created_on,
    modified_on,
    report_date
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.ticket_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'guid'),
        (3, 'processid'),
        (4, 'entityidentifier'),
        (5, 'refnumber'),
        (6, 'isactive'),
        (7, 'tkchannelid'),
        (8, 'tickettypeid'),
        (9, 'ticketstatusid'),
        (10, 'status'),
        (11, 'type'),
        (12, 'received_on'),
        (13, 'actioned_on'),
        (14, 'average_resolution_time_hour'),
        (15, 'average_resolution_time_days'),
        (16, 'agent_name'),
        (17, 'is_deleted'),
        (18, 'source_system_name'),
        (19, 'dbt_updated_at'),
        (20, 'createdon'),
        (21, 'updatedon'),
        (22, 'mis_source_table'),
        (23, 'violation_id'),
        (24, 'violation_no'),
        (25, 'reference_number'),
        (26, 'cr_license_no'),
        (27, 'violation_types'),
        (28, 'assigned_violation_type'),
        (29, 'suggested_violation_type'),
        (30, 'suggested_violation_level'),
        (31, 'category'),
        (32, 'scheme'),
        (33, 'workflow_status'),
        (34, 'violations_workflow_status_list'),
        (35, 'status_reason'),
        (36, 'state'),
        (37, 'action_send_warning_letter'),
        (38, 'action_pay_organizer'),
        (39, 'action_send_to_ecd'),
        (40, 'action_send_for_site_visit'),
        (41, 'action_send_to_cio'),
        (42, 'action_add_to_blacklist'),
        (43, 'action_other_decisions'),
        (44, 'payment_batch_created'),
        (45, 'action_support_bc'),
        (46, 'action_encash_guarantee_cheque'),
        (47, 'other_decisions_text'),
        (48, 'case_desc_bc_violation'),
        (49, 'case_desc_force_majeure'),
        (50, 'case_desc_bc_withdrawal'),
        (51, 'case_desc_other'),
        (52, 'case_desc_cancellation_exhibition'),
        (53, 'case_desc_vendor_violation'),
        (54, 'violation_fraud_date'),
        (55, 'contract_start_date'),
        (56, 'contract_end_date'),
        (57, 'amount_to_be_collected'),
        (58, 'collected_amount'),
        (59, 'collected_amount_state'),
        (60, 'collected_amount_last_updated_on'),
        (61, 'balance'),
        (62, 'tamkeen_share'),
        (63, 'bc_share'),
        (64, 'total_contract_cost'),
        (65, 'exchange_rate'),
        (66, 'follow_up_result'),
        (67, 'manager_comment'),
        (68, 'director_comment'),
        (69, 'initiator_comments'),
        (70, 'case_details'),
        (71, 'tamkeens_decision'),
        (72, 'consultant_justification_remarks'),
        (73, 'grievance_decisions'),
        (74, 'building'),
        (75, 'road'),
        (76, 'flat'),
        (77, 'block'),
        (78, 'area'),
        (79, 'commercial_name'),
        (80, 'mobile'),
        (81, 'email'),
        (82, 'representative_name'),
        (83, 'supplier_name'),
        (84, 'director_name'),
        (85, 'manager_name'),
        (86, 'officer_analyst_name'),
        (87, 'consultant_name'),
        (88, 'organizer_name'),
        (89, 'contract_number_name'),
        (90, 'service_provider_new_name'),
        (91, 'service_provider_name'),
        (92, 'site_visit_name'),
        (93, 'monitoring_ticket_name'),
        (94, 'application_no_name'),
        (95, 'individual_name'),
        (96, 'individual_application_name'),
        (97, 'establishment_application_name'),
        (98, 'bc_support_application_name'),
        (99, 'bc_support_payment_name'),
        (100, 'tws_enterprise_application_new_name'),
        (101, 'tws_enterprise_application_name'),
        (102, 'tws_employee_application_name'),
        (103, 'exhibition_name'),
        (104, 'from_department_name'),
        (105, 'bds_payment_name'),
        (106, 'e7trf_payment_name'),
        (107, 'ict_payment_name'),
        (108, 'gap_payment_name'),
        (109, 'mas_payment_name'),
        (110, 'qms_payment_name'),
        (111, 'tap_payment_name'),
        (112, 'es_payment_name_text'),
        (113, 'es_payment_request_name'),
        (114, 'bds_execution_record_name'),
        (115, 'gap_execution_record_name'),
        (116, 'ict_execution_record_name'),
        (117, 'qms_execution_record_name'),
        (118, 'mas_execution_record_name'),
        (119, 'tap_execution_record_name'),
        (120, 'cpp_client_name'),
        (121, 'cpp_beneficiary_name'),
        (122, 'cpp_pi_invoice_name'),
        (123, 'cpp_tsp_name'),
        (124, 'cpp_tsp_invoice_name'),
        (125, 'fvr_member_name'),
        (126, 'owner_name'),
        (127, 'created_by'),
        (128, 'modified_by'),
        (129, 'created_on'),
        (130, 'modified_on'),
        (131, 'report_date')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'guid'),
        (3, 'processid'),
        (4, 'entityidentifier'),
        (5, 'refnumber'),
        (6, 'isactive'),
        (7, 'tkchannelid'),
        (8, 'tickettypeid'),
        (9, 'ticketstatusid'),
        (10, 'status'),
        (11, 'type'),
        (12, 'received_on'),
        (13, 'actioned_on'),
        (14, 'average_resolution_time_hour'),
        (15, 'average_resolution_time_days'),
        (16, 'agent_name'),
        (17, 'is_deleted'),
        (18, 'source_system_name'),
        (19, 'dbt_updated_at'),
        (20, 'createdon'),
        (21, 'updatedon'),
        (22, 'mis_source_table'),
        (23, 'violation_id'),
        (24, 'violation_no'),
        (25, 'reference_number'),
        (26, 'cr_license_no'),
        (27, 'violation_types'),
        (28, 'assigned_violation_type'),
        (29, 'suggested_violation_type'),
        (30, 'suggested_violation_level'),
        (31, 'category'),
        (32, 'scheme'),
        (33, 'workflow_status'),
        (34, 'violations_workflow_status_list'),
        (35, 'status_reason'),
        (36, 'state'),
        (37, 'action_send_warning_letter'),
        (38, 'action_pay_organizer'),
        (39, 'action_send_to_ecd'),
        (40, 'action_send_for_site_visit'),
        (41, 'action_send_to_cio'),
        (42, 'action_add_to_blacklist'),
        (43, 'action_other_decisions'),
        (44, 'payment_batch_created'),
        (45, 'action_support_bc'),
        (46, 'action_encash_guarantee_cheque'),
        (47, 'other_decisions_text'),
        (48, 'case_desc_bc_violation'),
        (49, 'case_desc_force_majeure'),
        (50, 'case_desc_bc_withdrawal'),
        (51, 'case_desc_other'),
        (52, 'case_desc_cancellation_exhibition'),
        (53, 'case_desc_vendor_violation'),
        (54, 'violation_fraud_date'),
        (55, 'contract_start_date'),
        (56, 'contract_end_date'),
        (57, 'amount_to_be_collected'),
        (58, 'collected_amount'),
        (59, 'collected_amount_state'),
        (60, 'collected_amount_last_updated_on'),
        (61, 'balance'),
        (62, 'tamkeen_share'),
        (63, 'bc_share'),
        (64, 'total_contract_cost'),
        (65, 'exchange_rate'),
        (66, 'follow_up_result'),
        (67, 'manager_comment'),
        (68, 'director_comment'),
        (69, 'initiator_comments'),
        (70, 'case_details'),
        (71, 'tamkeens_decision'),
        (72, 'consultant_justification_remarks'),
        (73, 'grievance_decisions'),
        (74, 'building'),
        (75, 'road'),
        (76, 'flat'),
        (77, 'block'),
        (78, 'area'),
        (79, 'commercial_name'),
        (80, 'mobile'),
        (81, 'email'),
        (82, 'representative_name'),
        (83, 'supplier_name'),
        (84, 'director_name'),
        (85, 'manager_name'),
        (86, 'officer_analyst_name'),
        (87, 'consultant_name'),
        (88, 'organizer_name'),
        (89, 'contract_number_name'),
        (90, 'service_provider_new_name'),
        (91, 'service_provider_name'),
        (92, 'site_visit_name'),
        (93, 'monitoring_ticket_name'),
        (94, 'application_no_name'),
        (95, 'individual_name'),
        (96, 'individual_application_name'),
        (97, 'establishment_application_name'),
        (98, 'bc_support_application_name'),
        (99, 'bc_support_payment_name'),
        (100, 'tws_enterprise_application_new_name'),
        (101, 'tws_enterprise_application_name'),
        (102, 'tws_employee_application_name'),
        (103, 'exhibition_name'),
        (104, 'from_department_name'),
        (105, 'bds_payment_name'),
        (106, 'e7trf_payment_name'),
        (107, 'ict_payment_name'),
        (108, 'gap_payment_name'),
        (109, 'mas_payment_name'),
        (110, 'qms_payment_name'),
        (111, 'tap_payment_name'),
        (112, 'es_payment_name_text'),
        (113, 'es_payment_request_name'),
        (114, 'bds_execution_record_name'),
        (115, 'gap_execution_record_name'),
        (116, 'ict_execution_record_name'),
        (117, 'qms_execution_record_name'),
        (118, 'mas_execution_record_name'),
        (119, 'tap_execution_record_name'),
        (120, 'cpp_client_name'),
        (121, 'cpp_beneficiary_name'),
        (122, 'cpp_pi_invoice_name'),
        (123, 'cpp_tsp_name'),
        (124, 'cpp_tsp_invoice_name'),
        (125, 'fvr_member_name'),
        (126, 'owner_name'),
        (127, 'created_by'),
        (128, 'modified_by'),
        (129, 'created_on'),
        (130, 'modified_on'),
        (131, 'report_date')
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`processid` AS STRING) AS `processid`,
        CAST(`entityidentifier` AS STRING) AS `entityidentifier`,
        CAST(`refnumber` AS STRING) AS `refnumber`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`tkchannelid` AS STRING) AS `tkchannelid`,
        CAST(`tickettypeid` AS STRING) AS `tickettypeid`,
        CAST(`ticketstatusid` AS STRING) AS `ticketstatusid`,
        CAST(`status` AS STRING) AS `status`,
        CAST(`type` AS STRING) AS `type`,
        CAST(`received_on` AS STRING) AS `received_on`,
        CAST(`actioned_on` AS STRING) AS `actioned_on`,
        CAST(`average_resolution_time_hour` AS STRING) AS `average_resolution_time_hour`,
        CAST(`average_resolution_time_days` AS STRING) AS `average_resolution_time_days`,
        CAST(`agent_name` AS STRING) AS `agent_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`violation_id` AS STRING) AS `violation_id`,
        CAST(`violation_no` AS STRING) AS `violation_no`,
        CAST(`reference_number` AS STRING) AS `reference_number`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`violation_types` AS STRING) AS `violation_types`,
        CAST(`assigned_violation_type` AS STRING) AS `assigned_violation_type`,
        CAST(`suggested_violation_type` AS STRING) AS `suggested_violation_type`,
        CAST(`suggested_violation_level` AS STRING) AS `suggested_violation_level`,
        CAST(`category` AS STRING) AS `category`,
        CAST(`scheme` AS STRING) AS `scheme`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`violations_workflow_status_list` AS STRING) AS `violations_workflow_status_list`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`action_send_warning_letter` AS STRING) AS `action_send_warning_letter`,
        CAST(`action_pay_organizer` AS STRING) AS `action_pay_organizer`,
        CAST(`action_send_to_ecd` AS STRING) AS `action_send_to_ecd`,
        CAST(`action_send_for_site_visit` AS STRING) AS `action_send_for_site_visit`,
        CAST(`action_send_to_cio` AS STRING) AS `action_send_to_cio`,
        CAST(`action_add_to_blacklist` AS STRING) AS `action_add_to_blacklist`,
        CAST(`action_other_decisions` AS STRING) AS `action_other_decisions`,
        CAST(`payment_batch_created` AS STRING) AS `payment_batch_created`,
        CAST(`action_support_bc` AS STRING) AS `action_support_bc`,
        CAST(`action_encash_guarantee_cheque` AS STRING) AS `action_encash_guarantee_cheque`,
        CAST(`other_decisions_text` AS STRING) AS `other_decisions_text`,
        CAST(`case_desc_bc_violation` AS STRING) AS `case_desc_bc_violation`,
        CAST(`case_desc_force_majeure` AS STRING) AS `case_desc_force_majeure`,
        CAST(`case_desc_bc_withdrawal` AS STRING) AS `case_desc_bc_withdrawal`,
        CAST(`case_desc_other` AS STRING) AS `case_desc_other`,
        CAST(`case_desc_cancellation_exhibition` AS STRING) AS `case_desc_cancellation_exhibition`,
        CAST(`case_desc_vendor_violation` AS STRING) AS `case_desc_vendor_violation`,
        CAST(`violation_fraud_date` AS STRING) AS `violation_fraud_date`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`amount_to_be_collected` AS STRING) AS `amount_to_be_collected`,
        CAST(`collected_amount` AS STRING) AS `collected_amount`,
        CAST(`collected_amount_state` AS STRING) AS `collected_amount_state`,
        CAST(`collected_amount_last_updated_on` AS STRING) AS `collected_amount_last_updated_on`,
        CAST(`balance` AS STRING) AS `balance`,
        CAST(`tamkeen_share` AS STRING) AS `tamkeen_share`,
        CAST(`bc_share` AS STRING) AS `bc_share`,
        CAST(`total_contract_cost` AS STRING) AS `total_contract_cost`,
        CAST(`exchange_rate` AS STRING) AS `exchange_rate`,
        CAST(`follow_up_result` AS STRING) AS `follow_up_result`,
        CAST(`manager_comment` AS STRING) AS `manager_comment`,
        CAST(`director_comment` AS STRING) AS `director_comment`,
        CAST(`initiator_comments` AS STRING) AS `initiator_comments`,
        CAST(`case_details` AS STRING) AS `case_details`,
        CAST(`tamkeens_decision` AS STRING) AS `tamkeens_decision`,
        CAST(`consultant_justification_remarks` AS STRING) AS `consultant_justification_remarks`,
        CAST(`grievance_decisions` AS STRING) AS `grievance_decisions`,
        CAST(`building` AS STRING) AS `building`,
        CAST(`road` AS STRING) AS `road`,
        CAST(`flat` AS STRING) AS `flat`,
        CAST(`block` AS STRING) AS `block`,
        CAST(`area` AS STRING) AS `area`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`mobile` AS STRING) AS `mobile`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`representative_name` AS STRING) AS `representative_name`,
        CAST(`supplier_name` AS STRING) AS `supplier_name`,
        CAST(`director_name` AS STRING) AS `director_name`,
        CAST(`manager_name` AS STRING) AS `manager_name`,
        CAST(`officer_analyst_name` AS STRING) AS `officer_analyst_name`,
        CAST(`consultant_name` AS STRING) AS `consultant_name`,
        CAST(`organizer_name` AS STRING) AS `organizer_name`,
        CAST(`contract_number_name` AS STRING) AS `contract_number_name`,
        CAST(`service_provider_new_name` AS STRING) AS `service_provider_new_name`,
        CAST(`service_provider_name` AS STRING) AS `service_provider_name`,
        CAST(`site_visit_name` AS STRING) AS `site_visit_name`,
        CAST(`monitoring_ticket_name` AS STRING) AS `monitoring_ticket_name`,
        CAST(`application_no_name` AS STRING) AS `application_no_name`,
        CAST(`individual_name` AS STRING) AS `individual_name`,
        CAST(`individual_application_name` AS STRING) AS `individual_application_name`,
        CAST(`establishment_application_name` AS STRING) AS `establishment_application_name`,
        CAST(`bc_support_application_name` AS STRING) AS `bc_support_application_name`,
        CAST(`bc_support_payment_name` AS STRING) AS `bc_support_payment_name`,
        CAST(`tws_enterprise_application_new_name` AS STRING) AS `tws_enterprise_application_new_name`,
        CAST(`tws_enterprise_application_name` AS STRING) AS `tws_enterprise_application_name`,
        CAST(`tws_employee_application_name` AS STRING) AS `tws_employee_application_name`,
        CAST(`exhibition_name` AS STRING) AS `exhibition_name`,
        CAST(`from_department_name` AS STRING) AS `from_department_name`,
        CAST(`bds_payment_name` AS STRING) AS `bds_payment_name`,
        CAST(`e7trf_payment_name` AS STRING) AS `e7trf_payment_name`,
        CAST(`ict_payment_name` AS STRING) AS `ict_payment_name`,
        CAST(`gap_payment_name` AS STRING) AS `gap_payment_name`,
        CAST(`mas_payment_name` AS STRING) AS `mas_payment_name`,
        CAST(`qms_payment_name` AS STRING) AS `qms_payment_name`,
        CAST(`tap_payment_name` AS STRING) AS `tap_payment_name`,
        CAST(`es_payment_name_text` AS STRING) AS `es_payment_name_text`,
        CAST(`es_payment_request_name` AS STRING) AS `es_payment_request_name`,
        CAST(`bds_execution_record_name` AS STRING) AS `bds_execution_record_name`,
        CAST(`gap_execution_record_name` AS STRING) AS `gap_execution_record_name`,
        CAST(`ict_execution_record_name` AS STRING) AS `ict_execution_record_name`,
        CAST(`qms_execution_record_name` AS STRING) AS `qms_execution_record_name`,
        CAST(`mas_execution_record_name` AS STRING) AS `mas_execution_record_name`,
        CAST(`tap_execution_record_name` AS STRING) AS `tap_execution_record_name`,
        CAST(`cpp_client_name` AS STRING) AS `cpp_client_name`,
        CAST(`cpp_beneficiary_name` AS STRING) AS `cpp_beneficiary_name`,
        CAST(`cpp_pi_invoice_name` AS STRING) AS `cpp_pi_invoice_name`,
        CAST(`cpp_tsp_name` AS STRING) AS `cpp_tsp_name`,
        CAST(`cpp_tsp_invoice_name` AS STRING) AS `cpp_tsp_invoice_name`,
        CAST(`fvr_member_name` AS STRING) AS `fvr_member_name`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`report_date` AS STRING) AS `report_date`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`processid` AS STRING) AS `processid`,
        CAST(`entityidentifier` AS STRING) AS `entityidentifier`,
        CAST(`refnumber` AS STRING) AS `refnumber`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`tkchannelid` AS STRING) AS `tkchannelid`,
        CAST(`tickettypeid` AS STRING) AS `tickettypeid`,
        CAST(`ticketstatusid` AS STRING) AS `ticketstatusid`,
        CAST(`status` AS STRING) AS `status`,
        CAST(`type` AS STRING) AS `type`,
        CAST(`received_on` AS STRING) AS `received_on`,
        CAST(`actioned_on` AS STRING) AS `actioned_on`,
        CAST(`average_resolution_time_hour` AS STRING) AS `average_resolution_time_hour`,
        CAST(`average_resolution_time_days` AS STRING) AS `average_resolution_time_days`,
        CAST(`agent_name` AS STRING) AS `agent_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`violation_id` AS STRING) AS `violation_id`,
        CAST(`violation_no` AS STRING) AS `violation_no`,
        CAST(`reference_number` AS STRING) AS `reference_number`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`violation_types` AS STRING) AS `violation_types`,
        CAST(`assigned_violation_type` AS STRING) AS `assigned_violation_type`,
        CAST(`suggested_violation_type` AS STRING) AS `suggested_violation_type`,
        CAST(`suggested_violation_level` AS STRING) AS `suggested_violation_level`,
        CAST(`category` AS STRING) AS `category`,
        CAST(`scheme` AS STRING) AS `scheme`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`violations_workflow_status_list` AS STRING) AS `violations_workflow_status_list`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`action_send_warning_letter` AS STRING) AS `action_send_warning_letter`,
        CAST(`action_pay_organizer` AS STRING) AS `action_pay_organizer`,
        CAST(`action_send_to_ecd` AS STRING) AS `action_send_to_ecd`,
        CAST(`action_send_for_site_visit` AS STRING) AS `action_send_for_site_visit`,
        CAST(`action_send_to_cio` AS STRING) AS `action_send_to_cio`,
        CAST(`action_add_to_blacklist` AS STRING) AS `action_add_to_blacklist`,
        CAST(`action_other_decisions` AS STRING) AS `action_other_decisions`,
        CAST(`payment_batch_created` AS STRING) AS `payment_batch_created`,
        CAST(`action_support_bc` AS STRING) AS `action_support_bc`,
        CAST(`action_encash_guarantee_cheque` AS STRING) AS `action_encash_guarantee_cheque`,
        CAST(`other_decisions_text` AS STRING) AS `other_decisions_text`,
        CAST(`case_desc_bc_violation` AS STRING) AS `case_desc_bc_violation`,
        CAST(`case_desc_force_majeure` AS STRING) AS `case_desc_force_majeure`,
        CAST(`case_desc_bc_withdrawal` AS STRING) AS `case_desc_bc_withdrawal`,
        CAST(`case_desc_other` AS STRING) AS `case_desc_other`,
        CAST(`case_desc_cancellation_exhibition` AS STRING) AS `case_desc_cancellation_exhibition`,
        CAST(`case_desc_vendor_violation` AS STRING) AS `case_desc_vendor_violation`,
        CAST(`violation_fraud_date` AS STRING) AS `violation_fraud_date`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`amount_to_be_collected` AS STRING) AS `amount_to_be_collected`,
        CAST(`collected_amount` AS STRING) AS `collected_amount`,
        CAST(`collected_amount_state` AS STRING) AS `collected_amount_state`,
        CAST(`collected_amount_last_updated_on` AS STRING) AS `collected_amount_last_updated_on`,
        CAST(`balance` AS STRING) AS `balance`,
        CAST(`tamkeen_share` AS STRING) AS `tamkeen_share`,
        CAST(`bc_share` AS STRING) AS `bc_share`,
        CAST(`total_contract_cost` AS STRING) AS `total_contract_cost`,
        CAST(`exchange_rate` AS STRING) AS `exchange_rate`,
        CAST(`follow_up_result` AS STRING) AS `follow_up_result`,
        CAST(`manager_comment` AS STRING) AS `manager_comment`,
        CAST(`director_comment` AS STRING) AS `director_comment`,
        CAST(`initiator_comments` AS STRING) AS `initiator_comments`,
        CAST(`case_details` AS STRING) AS `case_details`,
        CAST(`tamkeens_decision` AS STRING) AS `tamkeens_decision`,
        CAST(`consultant_justification_remarks` AS STRING) AS `consultant_justification_remarks`,
        CAST(`grievance_decisions` AS STRING) AS `grievance_decisions`,
        CAST(`building` AS STRING) AS `building`,
        CAST(`road` AS STRING) AS `road`,
        CAST(`flat` AS STRING) AS `flat`,
        CAST(`block` AS STRING) AS `block`,
        CAST(`area` AS STRING) AS `area`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`mobile` AS STRING) AS `mobile`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`representative_name` AS STRING) AS `representative_name`,
        CAST(`supplier_name` AS STRING) AS `supplier_name`,
        CAST(`director_name` AS STRING) AS `director_name`,
        CAST(`manager_name` AS STRING) AS `manager_name`,
        CAST(`officer_analyst_name` AS STRING) AS `officer_analyst_name`,
        CAST(`consultant_name` AS STRING) AS `consultant_name`,
        CAST(`organizer_name` AS STRING) AS `organizer_name`,
        CAST(`contract_number_name` AS STRING) AS `contract_number_name`,
        CAST(`service_provider_new_name` AS STRING) AS `service_provider_new_name`,
        CAST(`service_provider_name` AS STRING) AS `service_provider_name`,
        CAST(`site_visit_name` AS STRING) AS `site_visit_name`,
        CAST(`monitoring_ticket_name` AS STRING) AS `monitoring_ticket_name`,
        CAST(`application_no_name` AS STRING) AS `application_no_name`,
        CAST(`individual_name` AS STRING) AS `individual_name`,
        CAST(`individual_application_name` AS STRING) AS `individual_application_name`,
        CAST(`establishment_application_name` AS STRING) AS `establishment_application_name`,
        CAST(`bc_support_application_name` AS STRING) AS `bc_support_application_name`,
        CAST(`bc_support_payment_name` AS STRING) AS `bc_support_payment_name`,
        CAST(`tws_enterprise_application_new_name` AS STRING) AS `tws_enterprise_application_new_name`,
        CAST(`tws_enterprise_application_name` AS STRING) AS `tws_enterprise_application_name`,
        CAST(`tws_employee_application_name` AS STRING) AS `tws_employee_application_name`,
        CAST(`exhibition_name` AS STRING) AS `exhibition_name`,
        CAST(`from_department_name` AS STRING) AS `from_department_name`,
        CAST(`bds_payment_name` AS STRING) AS `bds_payment_name`,
        CAST(`e7trf_payment_name` AS STRING) AS `e7trf_payment_name`,
        CAST(`ict_payment_name` AS STRING) AS `ict_payment_name`,
        CAST(`gap_payment_name` AS STRING) AS `gap_payment_name`,
        CAST(`mas_payment_name` AS STRING) AS `mas_payment_name`,
        CAST(`qms_payment_name` AS STRING) AS `qms_payment_name`,
        CAST(`tap_payment_name` AS STRING) AS `tap_payment_name`,
        CAST(`es_payment_name_text` AS STRING) AS `es_payment_name_text`,
        CAST(`es_payment_request_name` AS STRING) AS `es_payment_request_name`,
        CAST(`bds_execution_record_name` AS STRING) AS `bds_execution_record_name`,
        CAST(`gap_execution_record_name` AS STRING) AS `gap_execution_record_name`,
        CAST(`ict_execution_record_name` AS STRING) AS `ict_execution_record_name`,
        CAST(`qms_execution_record_name` AS STRING) AS `qms_execution_record_name`,
        CAST(`mas_execution_record_name` AS STRING) AS `mas_execution_record_name`,
        CAST(`tap_execution_record_name` AS STRING) AS `tap_execution_record_name`,
        CAST(`cpp_client_name` AS STRING) AS `cpp_client_name`,
        CAST(`cpp_beneficiary_name` AS STRING) AS `cpp_beneficiary_name`,
        CAST(`cpp_pi_invoice_name` AS STRING) AS `cpp_pi_invoice_name`,
        CAST(`cpp_tsp_name` AS STRING) AS `cpp_tsp_name`,
        CAST(`cpp_tsp_invoice_name` AS STRING) AS `cpp_tsp_invoice_name`,
        CAST(`fvr_member_name` AS STRING) AS `fvr_member_name`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
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
        'ticket_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'ticket_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'ticket_base' AS table_name,
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
        'ticket_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'ticket_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
