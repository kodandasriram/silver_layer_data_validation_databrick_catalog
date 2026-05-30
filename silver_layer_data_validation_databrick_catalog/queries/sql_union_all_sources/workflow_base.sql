WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for workflow_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: workflow_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH workflow_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : workflow_base_os2
Description : This model consolidates and standardizes application support,
              amendment request, and assessment workflow-related attributes
              from OS2 source tables into a unified dataset.

              The model captures:
                - Application support lifecycle details
                - Amendment request information
                - Workflow and assessment statuses
                - Approval and rejection timestamps
                - Decision dates

              It derives detailed workflow status mappings for both
              applications and amendment requests using BPM assessment
              activities and status tables.

              The model is intended for downstream Silver Layer reporting,
              operational monitoring, and analytics use cases.

Source Tables : `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENT
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_PROCESS
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENTSTATUS
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORT
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORTSTATUS
                `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_AMENDMENTREQUEST

Target Table : APPLICATION_SUPPORT_BASE

Load Type    : Full Load (Table)
Materialized : table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-05-12 | Vignesh  | Initial version

================================================================================================= 
*/
WITH TEMPASSESSMENT2 AS (

    SELECT
        act.NAME                                            AS NAME,
        AssessmentStatus.LABEL                              AS LABEL,
        ass.APPLICATIONID                                   AS APPLICATION_ID,
        ass.AMENDMENTREQUESTID                              AS AMENDMENT_REQUEST_ID,
        act.tenant_id                                       AS TENANT_ID,
        act.id                                              AS ID,
        act.activity_def_id                                 AS ACTIVITY_DEF_ID,
        act.process_id                                      AS ACTIVITY_PROCESS_ID,
        act.user_id                                         AS USER_ID,
        act.created                                         AS CREATED,
        act.opened                                          AS OPENED,
        act.closed                                          AS CLOSED,
        act.status_id                                       AS STATUS_ID,
        act.is_running_since                                AS IS_RUNNING_SINCE,
        act.is_running_at                                   AS IS_RUNNING_AT,
        act.next_run                                        AS NEXT_RUN,
        act.precedent_activity_id                           AS PRECEDENT_ACTIVITY_ID,
        act.precedent_outcome                               AS PRECEDENT_OUTCOME,
        act.due_date                                        AS DUE_DATE,
        act.expired                                         AS EXPIRED,
        act.skipped                                         AS SKIPPED,
        act.error_count                                     AS ERROR_COUNT,
        act.inbox_detail                                    AS INBOX_DETAIL,
        act.group_id                                        AS GROUP_ID,
        act.last_error_id                                   AS LAST_ERROR_ID,
        act.last_modified                                   AS LAST_MODIFIED,

        pro.tenant_id                                       AS PROCESS_TENANT_ID,
        pro.id                                              AS PROCESS_ID,
        pro.label                                           AS PROCESS_LABEL,
        pro.process_def_id                                  AS PROCESS_DEF_ID,
        pro.parent_process_id                               AS PARENT_PROCESS_ID,
        pro.parent_activity_id                              AS PARENT_ACTIVITY_ID,
        pro.top_process_id                                  AS TOP_PROCESS_ID,
        pro.status_id                                       AS PROCESS_STATUS,
        pro.last_modified                                   AS PROCESS_LAST_MODIFIED,
        pro.last_modified_by                                AS PROCESS_LAST_MODIFIED_BY,
        pro.suspended_date                                  AS PROCESS_SUSPENDED_DATE,
        pro.suspended_by                                    AS PROCESS_SUSPENDED_BY,
                CASE
            WHEN actdef.LABEL IN ('MOL Agent', 'Applicant Review MOL')
                THEN AssessmentTeamMOL.NAME

            WHEN actdef.LABEL IN ('Assessor', 'Applicant Review Assessor')
                THEN AssessmentTeam.NAME

            WHEN actdef.LABEL IN ('Review AD', 'Applicant Review RM')
                THEN ReviewTeam.NAME

            WHEN actdef.LABEL IN ('Approve D', 'Approve AD', 'Approve ED')
                THEN ApproveTeam.NAME

            ELSE NULL
        END AS TEAM,
            actions.LABEL as ACTION,
        actdef.DESCRIPTION AS ACTIVITY_DESCRIPTION,
        actdef.LABEL AS ACTIVITY_NAME,

        ROW_NUMBER() OVER (
            PARTITION BY ass.APPLICATIONID, ass.AMENDMENTREQUESTID
            ORDER BY act.ID DESC
        )                                                   AS RN

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENT ass

    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_PROCESS pro
        ON pro.TOP_PROCESS_ID = ass.PROCESSID

    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY act
        ON act.PROCESS_ID = pro.ID

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENTSTATUS AssessmentStatus
        ON ass.ASSESSMENTSTATUSID = AssessmentStatus.CODE
            LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY_DEFINITION  actdef 
     on act.Activity_Def_Id = actdef.Id 
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_HMY_ACTIVITYEXTENDED   act_ext
    on act_ext.ID = act.Id
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2FH_APPLICATIONASSESSMENTACTIONS  actions
    on actions.KEY = act_ext.SELECTEDACTIONKEY
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_KUO_TEAM AssessmentTeamMOL
    ON AssessmentTeamMOL.ID = ass.ASSESSMENTTEAMMOL

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_KUO_TEAM AssessmentTeam
        ON AssessmentTeam.ID = ass.ASSESSMENTTEAM2

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_KUO_TEAM ReviewTeam
        ON ReviewTeam.ID = ass.REVIEWTEAM1

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_KUO_TEAM ApproveTeam
        ON ApproveTeam.ID = ass.APPROVETEAM1
)

SELECT

    CURRENT_DATE                                           AS extract_date,

    APP_SUP.ID                                             AS id_application_support,

    APP_SUP.APPLICATIONID                                  AS id_application,

    APP_SUP.CREATEDON                                      AS created_on_application_support,

    APP_SUP.AMENDMENTREQUESTID                             AS id_amendment_request,

    CASE
        WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
        THEN APP.SUBMITTEDON
        ELSE AmendReq.SUBMITTEDON
    END                                                    AS submitted_on,

    CASE
        WHEN APP_SUP_STA.LABEL = 'Approved'
        THEN
            CASE
                WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
                THEN APP.APPROVEDON
                ELSE AmendReq.APPROVEDON
            END
        ELSE NULL
    END                                                    AS approved_on,

    CASE
        WHEN APP_SUP_STA.LABEL = 'Rejected'
        THEN
            CASE
                WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
                THEN APP.APPROVEDON
                ELSE AmendReq.APPROVEDON
            END
        ELSE NULL
    END                                                    AS rejected_on,

    APP_SUP_STA.LABEL                                      AS workflow_status,

    APP_STA.LABEL                                          AS workflow_status_application,

    asses.LABEL                                            AS workflow_status_application_detailed,

    asses_amed.TENANT_ID                                   AS tenant_id,
    asses_amed.ID                                          AS activity_id,
    asses_amed.ACTIVITY_DEF_ID                             AS activity_def_id,
    asses_amed.ACTIVITY_PROCESS_ID                         AS activity_process_id,
    asses_amed.NAME                                        AS name,
    asses_amed.USER_ID                                     AS user_id,
    asses_amed.CREATED                                     AS created,
    asses_amed.OPENED                                      AS opened,
    asses_amed.CLOSED                                      AS closed,
    asses_amed.STATUS_ID                                   AS status_id,
    asses_amed.IS_RUNNING_SINCE                            AS is_running_since,
    asses_amed.IS_RUNNING_AT                               AS is_running_at,
    asses_amed.NEXT_RUN                                    AS next_run,
    asses_amed.PRECEDENT_ACTIVITY_ID                       AS precedent_activity_id,
    asses_amed.PRECEDENT_OUTCOME                           AS precedent_outcome,
    asses_amed.DUE_DATE                                    AS due_date,
    asses_amed.EXPIRED                                     AS expired,
    asses_amed.SKIPPED                                     AS skipped,
    asses_amed.ERROR_COUNT                                 AS error_count,
    asses_amed.INBOX_DETAIL                                AS inbox_detail,
    asses_amed.GROUP_ID                                    AS group_id,
    asses_amed.LAST_ERROR_ID                               AS last_error_id,
    asses_amed.LAST_MODIFIED                               AS last_modified,

    asses_amed.PROCESS_TENANT_ID                           AS process_tenant_id,
    asses_amed.PROCESS_ID                                  AS process_id,
    asses_amed.PROCESS_LABEL                               AS process_label,
    asses_amed.PROCESS_DEF_ID                              AS process_def_id,
    asses_amed.PARENT_PROCESS_ID                           AS parent_process_id,
    asses_amed.PARENT_ACTIVITY_ID                          AS parent_activity_id,
    asses_amed.TOP_PROCESS_ID                              AS top_process_id,
    asses_amed.PROCESS_STATUS                              AS process_status,
    asses_amed.PROCESS_LAST_MODIFIED                       AS process_last_modified,
    asses_amed.PROCESS_LAST_MODIFIED_BY                    AS process_last_modified_by,
    asses_amed.PROCESS_SUSPENDED_DATE                      AS process_suspended_date,
    asses_amed.PROCESS_SUSPENDED_BY                        AS process_suspended_by,

    CASE
        WHEN asses_amed.AMENDMENT_REQUEST_ID IS NULL
        THEN asses.LABEL
        ELSE asses_amed.LABEL
    END                                                    AS workflow_status_application_support_detailed,

    CASE
        WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
        THEN
            CASE
                WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
                ELSE APP.APPROVEDON
            END
        ELSE
            CASE
                WHEN AmendReq.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
                ELSE AmendReq.APPROVEDON
            END
    END                                                    AS decision_on,  
        asses.team,
    asses.action,
    asses.activity_description,
    asses.activity_name,
    asses.amendment_request_id,
      'NEO2' AS source_system_name,
    FALSE AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at 

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORT APP_SUP

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION APP
    ON APP_SUP.APPLICATIONID = APP.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS APP_STA
    ON APP_STA.CODE = APP.APPLICATIONSTATUSID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORTSTATUS APP_SUP_STA
    ON APP_SUP_STA.CODE = APP_SUP.APPLICATIONSUPPORTSTATUSID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_AMENDMENTREQUEST AmendReq
    ON APP_SUP.AMENDMENTREQUESTID = AmendReq.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS AmendReq_STA
    ON AmendReq.AMENDMENTSTATUSID = AmendReq_STA.CODE

LEFT JOIN TEMPASSESSMENT2 asses
    ON asses.APPLICATION_ID = APP.ID
    AND asses.RN = 1

LEFT JOIN TEMPASSESSMENT2 asses_amed
    ON asses_amed.AMENDMENT_REQUEST_ID = APP_SUP.AMENDMENTREQUESTID
    AND asses_amed.RN = 1

WHERE APP_SUP.ISACTIVE = TRUE
),
workflow_base_os1_source AS (
/*
============================================================================
silver_workflow_os1.sql
============================================================================
Per-source intermediate Silver model for the Workflow domain Ã¢â‚¬â€ OS1 only.

The Workflow domain captures status-history events for both applications
and payments. OS1 has only TWO event-log tables (much simpler than MIS
which had 17), so the UNION is small.

Sources (2 status-log tables):
  Ã¢Ëœâ€¦ OSUSR_PX1_APPLICATIONSTATUSLOG  Ã¢â€ â€™ application status changes
  Ã¢Ëœâ€¦ OSUSR_PX1_PAYMENTSTATUSLOG21    Ã¢â€ â€™ payment status changes

Reference SPs:
  - RPT-164_neoTamkeen_Status_History       Ã¢â€ â€™ pivots application SH to 13
                                                milestone columns. The pivot
                                                is a Gold/AGG concern; Silver
                                                preserves raw events.
  - RPT-178_neoTamkeen_Full_Status_History  Ã¢â€ â€™ raw application SH event log
  - RPT-187_neoTamkeen_Payment_Status_History
  - RPT-197_neoTamkeen_payment_status_history

Structure decision:
  - Both event log tables are UNIONed into a single Workflow Silver table.
  - Each row represents one status transition event.
  - Origin and Destination status IDs (and decoded labels) are preserved
    so downstream models can build any milestone pivot they want Ã¢â‚¬â€ that
    pivot logic does NOT belong in Silver.

Reference table OSUSR_PX1_APPLICATIONSTATUS holds all status labels for
applications. OSUSR_PX1_PAYMENTREQUESTSTATUS holds all status labels for
payments. Both are joined inline (twice each Ã¢â‚¬â€ once for origin, once for
destination) so the Silver row is self-describing.

Cross-domain note: parent_application_id and parent_payment_id are kept
as columns. The user FK is also preserved alongside the denormalised user
name (because both raw event-log SPs already denormalise it, and downstream
consumers may need either form).

Note on the customer-prefix pattern: RPT-178 and RPT-187 emit the user
name with a 'Customer: ' prefix when the user has an extension row. That
prefix decoration is a Gold/AGG concern; here we keep the raw user_name
plus a separate is_customer_user flag so the consumer can format as needed.
============================================================================
*/


-- ============================================================================
-- BRANCH 1: Application Status events (OSUSR_PX1_APPLICATIONSTATUSLOG)
-- ============================================================================
SELECT
    'APPLICATION_STATUS_LOG' AS workflow_subtype,
    'OSUSR_PX1_APPLICATIONSTATUSLOG' AS os1_source_table,

    -- Event identifier
    AppLog.ID                                                             AS event_id,

    -- Parent FKs (one will be NULL per event subtype)
    AppLog.APPLICATIONID                                                  AS parent_application_id,
    CAST(NULL AS BIGINT)                                                  AS parent_payment_id,

    -- Status transition: origin Ã¢â€ â€™ destination
    AppLog.ORIGINSTATUS                                                   AS origin_status_id,
    Org.LABEL                                                             AS origin_status_label,
    AppLog.DESTINATIONSTATUS                                              AS destination_status_id,
    Dst.LABEL                                                             AS destination_status_label,

    -- Current status of the parent entity (denormalised at event time)
    App.APPLICATIONSTATUSID                                               AS current_status_id,
    Cur.LABEL                                                             AS current_status_label,

    -- Event timing & actor
    AppLog.OPERATIONDATE                                                  AS event_on,
    AppLog.USERID                                                         AS user_id,
    usr.NAME                                                              AS user_name,
    CASE WHEN UsrExt.USERID IS NOT NULL THEN TRUE ELSE FALSE END          AS is_customer_user,

    -- Standard trailing audit columns
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONSTATUSLOG AppLog
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATION App
       ON App.ID = AppLog.APPLICATIONID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER usr
       ON usr.ID = AppLog.USERID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONSTATUS Cur
       ON Cur.ID = App.APPLICATIONSTATUSID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONSTATUS Org
       ON Org.ID = AppLog.ORIGINSTATUS
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONSTATUS Dst
       ON Dst.ID = AppLog.DESTINATIONSTATUS
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_MKZ_USEREXTENSION UsrExt
       ON UsrExt.USERID = usr.ID


UNION ALL


-- ============================================================================
-- BRANCH 2: Payment Status events (OSUSR_PX1_PAYMENTSTATUSLOG21)
-- ============================================================================
SELECT
    'PAYMENT_STATUS_LOG' AS workflow_subtype,
    'OSUSR_PX1_PAYMENTSTATUSLOG21' AS os1_source_table,

    -- Event identifier
    PayLog.ID                                                             AS event_id,

    -- Parent FKs
    pay.APPLICATIONID                                                     AS parent_application_id,
    PayLog.PAYMENTID                                                      AS parent_payment_id,

    -- Status transition
    -- OS1's payment log only carries DESTINATIONSTATUS-equivalent (PAYMENTREQUESTSTATUSID
    -- on the log row). Origin is implicit Ã¢â‚¬â€ the previous event's destination Ã¢â‚¬â€ so we
    -- leave origin NULL here. If origin tracking is needed downstream, derive via
    -- LAG() over the event stream at Gold/AGG level.
    CAST(NULL AS BIGINT)                                                  AS origin_status_id,
    CAST(NULL AS STRING)                                                 AS origin_status_label,
    PayLog.PAYMENTREQUESTSTATUSID                                         AS destination_status_id,
    PaySts.LABEL                                                          AS destination_status_label,

    -- Current status of the parent payment (denormalised at event time)
    pay.PAYMENTREQUESTSTATUSID                                            AS current_status_id,
    PaySts_Cur.LABEL                                                      AS current_status_label,

    -- Event timing & actor
    PayLog.CREATEDON                                                      AS event_on,
    PayLog.CREATEDBY                                                      AS user_id,
    usr_c.NAME                                                            AS user_name,
    CASE WHEN UsrExt.USERID IS NOT NULL THEN TRUE ELSE FALSE END          AS is_customer_user,

    -- Standard trailing audit columns
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_PAYMENTSTATUSLOG PayLog
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_PAYMENT pay
       ON pay.ID = PayLog.PAYMENTID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATION app
       ON app.ID = pay.APPLICATIONID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_PAYMENTREQUESTSTATUS PaySts
       ON PaySts.ID = PayLog.PAYMENTREQUESTSTATUSID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_PAYMENTREQUESTSTATUS PaySts_Cur
       ON PaySts_Cur.ID = pay.PAYMENTREQUESTSTATUSID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER usr_c
       ON usr_c.ID = PayLog.CREATEDBY
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_MKZ_USEREXTENSION UsrExt
       ON UsrExt.USERID = usr_c.ID
),
workflow_base_mis_source AS (
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
)
/*
============================================================================
silver_workflow_mis.sql
============================================================================
Per-source intermediate Silver model for the Workflow domain Ã¢â‚¬â€ MIS only.

The Workflow domain captures status-history events across all application
sub-types in MIS. Each application sub-type (Individual, BD, Wage Subsidy,
Training Enrollment, Job Application, BC Support, ES Monitoring, Site Visit,
etc.) has its own dedicated status-history table.

These 17 SH tables are PARALLEL entities Ã¢â‚¬â€ each application sub-type writes
to its own SH table independently. They are UNIONed (not joined) into a
single Workflow domain table.

Sources (17 status-history tables):
  Ã¢Ëœâ€¦ mis_individualapplicationstatushistory          Ã¢â€ â€™ Individual Applications
  Ã¢Ëœâ€¦ tmkn_appsh                                      Ã¢â€ â€™ Business Development
  Ã¢Ëœâ€¦ tws_employeeapplication_sh                      Ã¢â€ â€™ TWS Employee Application
  Ã¢Ëœâ€¦ tws_wagesubsidy_sh                              Ã¢â€ â€™ TWS Wage Subsidy
  Ã¢Ëœâ€¦ tws_wagesincrement_sh                           Ã¢â€ â€™ TWS Wage Increment
  Ã¢Ëœâ€¦ tws_wagepaymentrequest_sh                       Ã¢â€ â€™ TWS Wage Payment Request
  Ã¢Ëœâ€¦ tws_trainingenrollment_sh                       Ã¢â€ â€™ TWS Training Enrollment
  Ã¢Ëœâ€¦ tws_trainingenrollmentpaymentrequest_sh         Ã¢â€ â€™ TWS Training Payment Request
  Ã¢Ëœâ€¦ tws_twsapplicationformhistorystatus             Ã¢â€ â€™ TWS Application Form
  Ã¢Ëœâ€¦ tws_twsjobapplicationstatushistory              Ã¢â€ â€™ TWS Job Application
  Ã¢Ëœâ€¦ tmkn_amendreqsh                                 Ã¢â€ â€™ ES Amendment Request
  Ã¢Ëœâ€¦ tmkn_businesscontinuitysupportapplicationsh     Ã¢â€ â€™ BC Support Application
  Ã¢Ëœâ€¦ tmkn_monitoringstatushistory                    Ã¢â€ â€™ ES Monitoring
  Ã¢Ëœâ€¦ tmkn_svsh                                       Ã¢â€ â€™ Site Visit
  Ã¢Ëœâ€¦ tmkn_espayment_sh                               Ã¢â€ â€™ ES Payment
  Ã¢Ëœâ€¦ mis_paymentrequeststatushistory                 Ã¢â€ â€™ Individual Payment Request
  Ã¢Ëœâ€¦ mis_finshceme_sh                                Ã¢â€ â€™ Tamweel/Riyadat Financial Scheme

Canonical column shape (every UNION branch produces the same shape):
  - workflow_subtype          : identifier of which SH table the row came from
  - mis_source_table          : actual table name (denormalised metadata)
  - sh_id                     : PK of the SH row
  - parent_application_id     : FK back to the parent entity (different column
                                name in each table Ã¢â‚¬â€ aliased to a uniform name here)
  - status_report_id          : raw int code (option-set ID) for the status
  - status_report_name        : decoded option-set label
  - created_on / created_by   : when and who created the SH record
  - state                     : decoded statecode
  - workflow_status           : decoded workflowstatus where applicable

Every SH table provides at least the parent FK, status report, created_on,
and created_by. Some tables carry additional fields (transaction_date,
remarks, next_step) Ã¢â‚¬â€ preserved as branch-specific columns where present
and NULL-padded in branches that don't have them.

Cleansing only Ã¢â‚¬â€ no business logic. The unified Silver layer downstream
will use this for cross-domain workflow analytics (e.g., "how long did
each application spend in 'Send for Analysis' state" across all sub-types).
============================================================================
*/


-- ============================================================================
-- BRANCH 1: Individual Application status history
-- ============================================================================

SELECT
    'INDIVIDUAL_APPLICATION_SH' AS workflow_subtype,
    'mis_individualapplicationstatushistory' AS mis_source_table,

    CAST(sh.mis_individualapplicationstatushistoryid AS STRING)         AS sh_id,
    CAST(sh.mis_indiviualapplicationid AS STRING)                       AS parent_application_id,

    sh.mis_statusreport                                                  AS status_report_id,
    CASE WHEN sh.mis_statusreport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_individualapplicationstatushistory') || '|' || lower('mis_statusreport') || '|' || CAST(sh.mis_statusreport AS STRING)) END AS status_report_name,

    sh.createdon                                                         AS created_on,
    sh.createdby                                                  AS created_by,

    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_individualapplicationstatushistory') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END AS state,
    CAST(NULL AS STRING)                                                AS workflow_status,
    CAST(NULL AS TIMESTAMP)                                              AS transaction_date,
    CAST(NULL AS STRING)                                                AS remarks,
    CAST(NULL AS STRING)                                                AS next_step,
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_INDIVIDUALAPPLICATIONSTATUSHISTORYBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 2: Business Development status history (tmkn_appsh)
-- ============================================================================
SELECT
    'BUSINESS_DEVELOPMENT_SH' AS workflow_subtype,
    'tmkn_appsh' AS mis_source_table,

    CAST(sh.tmkn_appshid AS STRING)                                     AS sh_id,
    CAST(sh.tmkn_ref AS STRING)                                         AS parent_application_id,

    sh.tmkn_statusreport                                                 AS status_report_id,
    CASE WHEN sh.tmkn_statusreport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_appsh') || '|' || lower('tmkn_statusreport') || '|' || CAST(sh.tmkn_statusreport AS STRING)) END AS status_report_name,

    sh.createdon                                                         AS created_on,
    sh.createdby                                                     AS created_by,

    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_appsh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END    AS state,
    CAST(NULL AS STRING)                                                AS workflow_status,
    sh.tmkn_meetingdate                                                  AS transaction_date,
    CAST(NULL AS STRING)                                                AS remarks,
    CAST(NULL AS STRING)                                                AS next_step,
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_APPSHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 3: TWS Employee Application status history
-- ============================================================================
SELECT
    'TWS_EMPLOYEE_APPLICATION_SH', 'tws_employeeapplication_sh',

    CAST(sh.tws_employeeapplication_shid AS STRING),
    CAST(sh.tws_employee_application_reference AS STRING),

    sh.tws_status_report,
    CASE WHEN sh.tws_status_report IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_employeeapplication_sh') || '|' || lower('tws_status_report') || '|' || CAST(sh.tws_status_report AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_employeeapplication_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_EMPLOYEEAPPLICATION_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 4: TWS Wage Subsidy status history
-- ============================================================================
SELECT
    'TWS_WAGE_SUBSIDY_SH', 'tws_wagesubsidy_sh',

    CAST(sh.tws_wagesubsidy_shid AS STRING),
    CAST(sh.tws_wage_subsidy_reference AS STRING),

    sh.tws_status_report,
    CASE WHEN sh.tws_status_report IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy_sh') || '|' || lower('tws_status_report') || '|' || CAST(sh.tws_status_report AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesubsidy_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_WAGESUBSIDY_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 5: TWS Wage Increment status history
-- ============================================================================
SELECT
    'TWS_WAGE_INCREMENT_SH', 'tws_wagesincrement_sh',

    CAST(sh.tws_wagesincrement_shid AS STRING),
    CAST(sh.tws_wage_increment_reference AS STRING),

    sh.tws_status_report,
    CASE WHEN sh.tws_status_report IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesincrement_sh') || '|' || lower('tws_status_report') || '|' || CAST(sh.tws_status_report AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagesincrement_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_WAGESINCREMENT_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 6: TWS Wage Payment Request status history
-- ============================================================================
SELECT
    'TWS_WAGE_PAYMENT_REQUEST_SH', 'tws_wagepaymentrequest_sh',

    CAST(sh.tws_wagepaymentrequest_shid AS STRING),
    CAST(sh.tws_wage_payment_request_reference AS STRING),

    sh.tws_status_report,
    CASE WHEN sh.tws_status_report IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagepaymentrequest_sh') || '|' || lower('tws_status_report') || '|' || CAST(sh.tws_status_report AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_wagepaymentrequest_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING),
    sh.tmkn_transactiondate,
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_WAGEPAYMENTREQUEST_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 7: TWS Training Enrollment status history
-- ============================================================================
SELECT
    'TWS_TRAINING_ENROLLMENT_SH', 'tws_trainingenrollment_sh',

    CAST(sh.tws_trainingenrollment_shid AS STRING),
    CAST(sh.tws_training_enrollment_reference AS STRING),

    sh.tws_status_report,
    CASE WHEN sh.tws_status_report IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollment_sh') || '|' || lower('tws_status_report') || '|' || CAST(sh.tws_status_report AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollment_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_TRAININGENROLLMENT_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 8: TWS Training Enrollment Payment Request status history
-- ============================================================================
SELECT
    'TWS_TRAINING_ENROLLMENT_PAYMENT_REQUEST_SH', 'tws_trainingenrollmentpaymentrequest_sh',

    CAST(sh.tws_trainingenrollmentpaymentrequest_shid AS STRING),
    CAST(sh.tws_training_enrol_pay_req_reference AS STRING),

    sh.tws_status_report,
    CASE WHEN sh.tws_status_report IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollmentpaymentrequest_sh') || '|' || lower('tws_status_report') || '|' || CAST(sh.tws_status_report AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollmentpaymentrequest_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING),
    sh.tmkn_transactiondate,
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_TRAININGENROLLMENTPAYMENTREQUEST_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 9: TWS Application Form status history
-- ============================================================================
SELECT
    'TWS_APPLICATION_FORM_SH', 'tws_twsapplicationformhistorystatus',

    CAST(sh.tws_twsapplicationformhistorystatusid AS STRING),
    CAST(sh.tws_applicationform AS STRING),

    sh.tws_workflowstatus,
    CASE WHEN sh.tws_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_twsapplicationformhistorystatus') || '|' || lower('tws_WorkflowStatus') || '|' || CAST(sh.tws_workflowstatus AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_twsapplicationformhistorystatus') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_TWSAPPLICATIONFORMHISTORYSTATUSBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 10: TWS Job Application status history
-- ============================================================================
SELECT
    'TWS_JOB_APPLICATION_SH', 'tws_twsjobapplicationstatushistory',

    CAST(sh.tws_twsjobapplicationstatushistoryid AS STRING),
    CAST(sh.tws_jobapplication AS STRING),

    sh.tws_statusreport,
    CASE WHEN sh.tws_statusreport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_twsjobapplicationstatushistory') || '|' || lower('tws_StatusReport') || '|' || CAST(sh.tws_statusreport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_twsjobapplicationstatushistory') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_TWSJOBAPPLICATIONSTATUSHISTORYBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 11: ES Amendment Request status history
-- ============================================================================
SELECT
    'ES_AMENDMENT_REQUEST_SH', 'tmkn_amendreqsh',

    CAST(sh.tmkn_amendreqshid AS STRING),
    CAST(sh.tmkn_ref AS STRING),

    sh.tmkn_statusreport,
    CASE WHEN sh.tmkn_statusreport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_amendreqsh') || '|' || lower('tmkn_statusreport') || '|' || CAST(sh.tmkn_statusreport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_amendreqsh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP),
    sh.tmkn_remarks,
    CASE WHEN sh.tmkn_nextstep IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_amendreqsh') || '|' || lower('tmkn_nextstep') || '|' || CAST(sh.tmkn_nextstep AS STRING)) END,
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_AMENDREQSHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 12: BC Support Application status history
-- ============================================================================
SELECT
    'BC_SUPPORT_APPLICATION_SH', 'tmkn_businesscontinuitysupportapplicationsh',

    CAST(sh.tmkn_businesscontinuitysupportapplicationid AS STRING),
    CAST(null AS STRING), -- CAST(sh.tmkn_parentref AS STRING) not found 

    CAST(null AS INTEGER), --sh.tmkn_StatusReport not found 
    CAST(null AS STRING), 

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_businesscontinuitysupportapplicationsh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(sh.tmkn_businesssector AS STRING)       AS tmkn_businesssector,
    CAST(sh.tmkn_submittedon AS TIMESTAMP)        AS tmkn_submittedon,
    CAST(sh.tmkn_consumed AS DECIMAL(18,2))       AS tmkn_consumed,
    CAST(sh.tmkn_remaining AS DECIMAL(18,2))      AS tmkn_remaining,
    CAST(sh.tmkn_workflowstatus AS STRING)       AS tmkn_workflowstatus,
    CAST(sh.owneridname AS STRING)               AS owneridname,
    CAST(sh.tmkn_contractstartdate AS TIMESTAMP)  AS tmkn_contractstartdate,
    CAST(sh.tmkn_contractenddate AS TIMESTAMP)    AS tmkn_contractenddate,
    
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_BUSINESSCONTINUITYSUPPORTAPPLICATION sh

UNION ALL


-- ============================================================================
-- BRANCH 13: ES Monitoring status history
-- ============================================================================
SELECT
    'ES_MONITORING_SH', 'tmkn_monitoringstatushistory',

    CAST(sh.tmkn_monitoringstatushistoryid AS STRING),
    CAST(sh.tmkn_ref AS STRING),

    sh.tmkn_StatusReport,
    CASE WHEN sh.tmkn_StatusReport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_monitoringstatushistory') || '|' || lower('tmkn_StatusReport') || '|' || CAST(sh.tmkn_StatusReport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_monitoringstatushistory') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_MONITORINGSTATUSHISTORYBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 14: Site Visit status history
-- ============================================================================
SELECT
    'SITE_VISIT_SH', 'tmkn_svsh',

    CAST(sh.tmkn_svshid AS STRING),
    CAST(sh.tmkn_ref AS STRING),

    sh.tmkn_StatusReport,
    CASE WHEN sh.tmkn_StatusReport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_svsh') || '|' || lower('tmkn_StatusReport') || '|' || CAST(sh.tmkn_StatusReport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_svsh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_SVSHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 15: ES Payment Request status history
-- ============================================================================
SELECT
    'ES_PAYMENT_REQUEST_SH', 'tmkn_espayment_sh',

    CAST(sh.tmkn_espayment_shid AS STRING),
    CAST(sh.tmkn_parentref AS STRING),

    sh.tmkn_statusreport,
    CASE WHEN sh.tmkn_statusreport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_espayment_sh') || '|' || lower('tmkn_statusreport') || '|' || CAST(sh.tmkn_statusreport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_espayment_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING),
    sh.tmkn_transactiondate,
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_ESPAYMENT_SHBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 16: Individual Payment Request status history
-- ============================================================================
SELECT
    'INDIVIDUAL_PAYMENT_REQUEST_SH', 'mis_paymentrequeststatushistory',

    CAST(sh.mis_paymentrequeststatushistoryid AS STRING),
    CAST(sh.mis_PaymentRequestId AS STRING),

    sh.mis_statusreport,
    CASE WHEN sh.mis_statusreport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_paymentrequeststatushistory') || '|' || lower('mis_statusreport') || '|' || CAST(sh.mis_statusreport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_paymentrequeststatushistory') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_PAYMENTREQUESTSTATUSHISTORYBASE sh

UNION ALL


-- ============================================================================
-- BRANCH 17: Tamweel/Riyadat Financial Scheme status history
-- ============================================================================
SELECT
    'FINANCIAL_SCHEME_SH', 'mis_finshceme_sh',

    CAST(sh.mis_finshceme_shid AS STRING),
    CAST(sh.mis_Ref AS STRING),

    sh.mis_StatusReport,
    CASE WHEN sh.mis_StatusReport IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_finshceme_sh') || '|' || lower('mis_StatusReport') || '|' || CAST(sh.mis_StatusReport AS STRING)) END,

    sh.createdon, sh.createdby,
    CASE WHEN sh.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_finshceme_sh') || '|' || lower('statecode') || '|' || CAST(sh.statecode AS STRING)) END,
    CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING)        AS tmkn_businesssector,
    CAST(NULL AS TIMESTAMP)      AS tmkn_submittedon,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_consumed,
    CAST(NULL AS DECIMAL(18,2))  AS tmkn_remaining,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS owneridname,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_FINSHCEME_SH sh
)
select
 cast(id_application_support as STRING) id_application_support,
    cast(id_application as STRING)  as application_id,
cast(null as bigint)  as  parent_payment_id,
cast(null as bigint) as     destination_status_id,
cast(null as bigint) as     status_report_id,
cast(null as STRING) as     destination_status_label,
cast(null as STRING) as     status_report_name,
cast(null as bigint) as     origin_status_id,
cast(null as STRING) as     origin_status_label,
workflow_status_application_detailed,
workflow_status,
cast(null as timestamp) as     created_on,
cast(null as timestamp) as     event_on,
cast(null as timestamp) as     transaction_date,
TRY_CAST(NULLIF(CAST(user_id AS STRING), '') AS BIGINT) as user_id,
cast(null as STRING) as     user_name,
cast(null as STRING) as     created_by,
cast(null as boolean) as     is_customer_user,
cast(null as STRING) as     remarks,
cast(null as STRING) as     next_step,
cast(null as STRING) as     state,
cast(null as bigint) as     current_status_id,
cast(null as STRING) as     current_status_label,
cast(null as STRING) as     workflow_subtype,
cast(null as STRING) as     source_table,
source_system_name,
is_deleted,
cast(current_date as date) as     report_date,
dbt_updated_at,
extract_date,
created_on_application_support,
TRY_CAST(NULLIF(CAST(id_amendment_request AS STRING), '') AS BIGINT) as id_amendment_request,
submitted_on,
approved_on,
rejected_on,
decision_on,
workflow_status_application,
workflow_status_application_support_detailed,
TRY_CAST(NULLIF(CAST(tenant_id AS STRING), '') AS BIGINT) as tenant_id,
TRY_CAST(NULLIF(CAST(activity_id AS STRING), '') AS BIGINT) as activity_id,
TRY_CAST(NULLIF(CAST(activity_def_id AS STRING), '') AS BIGINT) as activity_def_id,
TRY_CAST(NULLIF(CAST(activity_process_id AS STRING), '') AS BIGINT) as activity_process_id,
name,
created,
opened,
closed,
TRY_CAST(NULLIF(CAST(status_id AS STRING), '') AS BIGINT) as status_id,
is_running_since,
is_running_at,
next_run,
TRY_CAST(NULLIF(CAST(precedent_activity_id AS STRING), '') AS BIGINT) as precedent_activity_id,
precedent_outcome,
due_date,
expired,
skipped,
TRY_CAST(NULLIF(CAST(error_count AS STRING), '') AS BIGINT) as error_count,
inbox_detail,
TRY_CAST(NULLIF(CAST(group_id AS STRING), '') AS BIGINT) as group_id,
last_error_id,
last_modified,
TRY_CAST(NULLIF(CAST(process_tenant_id AS STRING), '') AS BIGINT) as process_tenant_id,
TRY_CAST(NULLIF(CAST(process_id AS STRING), '') AS BIGINT) as process_id,
process_label,
TRY_CAST(NULLIF(CAST(process_def_id AS STRING), '') AS BIGINT) as process_def_id,
TRY_CAST(NULLIF(CAST(parent_process_id AS STRING), '') AS BIGINT) as parent_process_id,
TRY_CAST(NULLIF(CAST(parent_activity_id AS STRING), '') AS BIGINT) as parent_activity_id,
TRY_CAST(NULLIF(CAST(top_process_id AS STRING), '') AS BIGINT) as top_process_id,
TRY_CAST(NULLIF(CAST(process_status AS STRING), '') AS BIGINT) as process_status,
process_last_modified,
TRY_CAST(NULLIF(CAST(process_last_modified_by AS STRING), '') AS BIGINT) as process_last_modified_by,
process_suspended_date,
TRY_CAST(NULLIF(CAST(process_suspended_by AS STRING), '') AS BIGINT) as process_suspended_by,
action,
activity_description,
activity_name,
amendment_request_id,
team,
cast(null as STRING)       as tmkn_businesssector,
cast(null as timestamp)     as tmkn_submittedon,
cast(null as decimal(18,2)) as tmkn_consumed,
cast(null as decimal(18,2)) as tmkn_remaining,
cast(null as STRING)       as tmkn_workflowstatus,
cast(null as STRING)       as owneridname,
cast(null as timestamp)     as tmkn_contractstartdate,
cast(null as timestamp)     as tmkn_contractenddate
from workflow_base_os2_source

union all
-- =========================================================================
-- os1
-- =========================================================================
select
     cast(event_id as STRING) event_id,
    cast(parent_application_id as STRING)   as application_id,
TRY_CAST(NULLIF(CAST(parent_payment_id AS STRING), '') AS BIGINT) as parent_payment_id,
TRY_CAST(NULLIF(CAST(destination_status_id AS STRING), '') AS BIGINT) as destination_status_id,
cast(null as bigint) as     status_report_id,
destination_status_label,
cast(null as STRING) as     status_report_name,
TRY_CAST(NULLIF(CAST(origin_status_id AS STRING), '') AS BIGINT) as origin_status_id,
origin_status_label,
cast(null as STRING) as workflow_status_application_detailed,
cast(null as STRING) as workflow_status,
cast(null as timestamp) as     created_on,
event_on,
cast(null as timestamp) as     transaction_date,
TRY_CAST(NULLIF(CAST(user_id AS STRING), '') AS BIGINT) as user_id,
user_name,
cast(null as STRING) as     created_by,
is_customer_user,
cast(null as STRING) as     remarks,
cast(null as STRING) as     next_step,
cast(null as STRING) as     state,
TRY_CAST(NULLIF(CAST(current_status_id AS STRING), '') AS BIGINT) as current_status_id,
current_status_label,
workflow_subtype,
os1_source_table,
source_system_name,
is_deleted,
report_date,
dbt_updated_at,
cast(null as date)	as	extract_date,
cast(null as timestamp)	as	created_on_application_support,
cast(null as bigint)	as	id_amendment_request,
cast(null as timestamp)	as	submitted_on,
cast(null as timestamp)	as	approved_on,
cast(null as timestamp)	as	rejected_on,
cast(null as timestamp)	as	decision_on,
cast(null as STRING)	as	workflow_status_application,
cast(null as STRING)	as	workflow_status_application_support_detailed,
cast(null as bigint)	as	tenant_id,
cast(null as bigint)	as	activity_id,
cast(null as bigint)	as	activity_def_id,
cast(null as bigint)	as	activity_process_id,
cast(null as STRING)	as	name,
cast(null as timestamp)	as	created,
cast(null as timestamp)	as	opened,
cast(null as timestamp)	as	closed,
cast(null as bigint)	as	status_id,
cast(null as timestamp)	as	is_running_since,
cast(null as STRING)	as	is_running_at,
cast(null as timestamp)	as	next_run,
cast(null as bigint)	as	precedent_activity_id,
cast(null as STRING)	as	precedent_outcome,
cast(null as timestamp)	as	due_date,
cast(null as boolean)	as	expired,
cast(null as boolean)	as	skipped,
cast(null as bigint)	as	error_count,
cast(null as STRING)	as	inbox_detail,
cast(null as bigint)	as	group_id,
cast(null as STRING)	as	last_error_id,
cast(null as timestamp)	as	last_modified,
cast(null as bigint)	as	process_tenant_id,
cast(null as bigint)	as	process_id,
cast(null as STRING)	as	process_label,
cast(null as bigint)	as	process_def_id,
cast(null as bigint)	as	parent_process_id,
cast(null as bigint)	as	parent_activity_id,
cast(null as bigint)	as	top_process_id,
cast(null as bigint)	as	process_status,
cast(null as timestamp)	as	process_last_modified,
cast(null as bigint)	as	process_last_modified_by,
cast(null as timestamp)	as	process_suspended_date,
cast(null as bigint)	as	process_suspended_by,
cast(null as STRING) as action,
cast(null as STRING) as activity_description,
cast(null as STRING) as activity_name,
cast(null as bigint) as amendment_request_id,
cast(null as STRING) as team,
cast(null as STRING)       as tmkn_businesssector,
cast(null as timestamp)     as tmkn_submittedon,
cast(null as decimal(18,2)) as tmkn_consumed,
cast(null as decimal(18,2)) as tmkn_remaining,
cast(null as STRING)       as tmkn_workflowstatus,
cast(null as STRING)       as owneridname,
cast(null as timestamp)     as tmkn_contractstartdate,
cast(null as timestamp)     as tmkn_contractenddate

from workflow_base_os1_source

union all

-- =========================================================================
-- mis
-- =========================================================================
select
    cast(sh_id as STRING) sh_id,
     cast(parent_application_id as STRING)  as application_id,
cast(null as bigint),
cast(null as bigint),
TRY_CAST(NULLIF(CAST(status_report_id AS STRING), '') AS BIGINT) as status_report_id,
cast(null as STRING) as     destination_status_label,
status_report_name,
cast(null as bigint) as     origin_status_id,
cast(null as STRING) as     origin_status_label,
cast(null as STRING) as workflow_status_application_detailed,
workflow_status,
created_on,
cast(null as timestamp) as     event_on,
transaction_date,
cast(null as bigint) as user_id,
cast(null as STRING) as     user_name,
created_by,
cast(null as boolean) as     is_customer_user,
remarks,
next_step,
state,
cast(null as bigint)  as current_status_id,
cast(null as STRING) as     current_status_label,
workflow_subtype,
mis_source_table,
source_system_name,
is_deleted,
report_date,
dbt_updated_at,
cast(null as date)	as	extract_date,
cast(null as timestamp)	as	created_on_application_support,
cast(null as bigint)	as	id_amendment_request,
cast(null as timestamp)	as	submitted_on,
cast(null as timestamp)	as	approved_on,
cast(null as timestamp)	as	rejected_on,
cast(null as timestamp)	as	decision_on,
cast(null as STRING)	as	workflow_status_application,
cast(null as STRING)	as	workflow_status_application_support_detailed,
cast(null as bigint)	as	tenant_id,
cast(null as bigint)	as	activity_id,
cast(null as bigint)	as	activity_def_id,
cast(null as bigint)	as	activity_process_id,
cast(null as STRING)	as	name,
cast(null as timestamp)	as	created,
cast(null as timestamp)	as	opened,
cast(null as timestamp)	as	closed,
cast(null as bigint)	as	status_id,
cast(null as timestamp)	as	is_running_since,
cast(null as STRING)	as	is_running_at,
cast(null as timestamp)	as	next_run,
cast(null as bigint)	as	precedent_activity_id,
cast(null as STRING)	as	precedent_outcome,
cast(null as timestamp)	as	due_date,
cast(null as boolean)	as	expired,
cast(null as boolean)	as	skipped,
cast(null as bigint)	as	error_count,
cast(null as STRING)	as	inbox_detail,
cast(null as bigint)	as	group_id,
cast(null as STRING)	as	last_error_id,
cast(null as timestamp)	as	last_modified,
cast(null as bigint)	as	process_tenant_id,
cast(null as bigint)	as	process_id,
cast(null as STRING)	as	process_label,
cast(null as bigint)	as	process_def_id,
cast(null as bigint)	as	parent_process_id,
cast(null as bigint)	as	parent_activity_id,
cast(null as bigint)	as	top_process_id,
cast(null as bigint)	as	process_status,
cast(null as timestamp)	as	process_last_modified,
cast(null as bigint)	as	process_last_modified_by,
cast(null as timestamp)	as	process_suspended_date,
cast(null as bigint)	as	process_suspended_by,
cast(null as STRING) as action,
cast(null as STRING) as activity_description,
cast(null as STRING) as activity_name,
cast(null as bigint) as amendment_request_id,
cast(null as STRING) as team,
tmkn_businesssector,
tmkn_submittedon,
tmkn_consumed,
tmkn_remaining,
tmkn_workflowstatus,
owneridname,
tmkn_contractstartdate,
tmkn_contractenddate

from workflow_base_mis_source
),

silver_layer AS (
SELECT
    id_application_support,
    application_id,
    parent_payment_id,
    destination_status_id,
    status_report_id,
    destination_status_label,
    status_report_name,
    origin_status_id,
    origin_status_label,
    workflow_status_application_detailed,
    workflow_status,
    created_on,
    event_on,
    transaction_date,
    user_id,
    user_name,
    created_by,
    is_customer_user,
    remarks,
    next_step,
    state,
    current_status_id,
    current_status_label,
    workflow_subtype,
    source_table,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    extract_date,
    created_on_application_support,
    id_amendment_request,
    submitted_on,
    approved_on,
    rejected_on,
    decision_on,
    workflow_status_application,
    workflow_status_application_support_detailed,
    tenant_id,
    activity_id,
    activity_def_id,
    activity_process_id,
    name,
    created,
    opened,
    closed,
    status_id,
    is_running_since,
    is_running_at,
    next_run,
    precedent_activity_id,
    precedent_outcome,
    due_date,
    expired,
    skipped,
    error_count,
    inbox_detail,
    group_id,
    last_error_id,
    last_modified,
    process_tenant_id,
    process_id,
    process_label,
    process_def_id,
    parent_process_id,
    parent_activity_id,
    top_process_id,
    process_status,
    process_last_modified,
    process_last_modified_by,
    process_suspended_date,
    process_suspended_by,
    `action`,
    activity_description,
    activity_name,
    amendment_request_id,
    team,
    tmkn_businesssector,
    tmkn_submittedon,
    tmkn_consumed,
    tmkn_remaining,
    tmkn_workflowstatus,
    owneridname,
    tmkn_contractstartdate,
    tmkn_contractenddate
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.workflow_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id_application_support'),
        (2, 'application_id'),
        (3, 'parent_payment_id'),
        (4, 'destination_status_id'),
        (5, 'status_report_id'),
        (6, 'destination_status_label'),
        (7, 'status_report_name'),
        (8, 'origin_status_id'),
        (9, 'origin_status_label'),
        (10, 'workflow_status_application_detailed'),
        (11, 'workflow_status'),
        (12, 'created_on'),
        (13, 'event_on'),
        (14, 'transaction_date'),
        (15, 'user_id'),
        (16, 'user_name'),
        (17, 'created_by'),
        (18, 'is_customer_user'),
        (19, 'remarks'),
        (20, 'next_step'),
        (21, 'state'),
        (22, 'current_status_id'),
        (23, 'current_status_label'),
        (24, 'workflow_subtype'),
        (25, 'source_table'),
        (26, 'source_system_name'),
        (27, 'is_deleted'),
        (28, 'report_date'),
        (29, 'dbt_updated_at'),
        (30, 'extract_date'),
        (31, 'created_on_application_support'),
        (32, 'id_amendment_request'),
        (33, 'submitted_on'),
        (34, 'approved_on'),
        (35, 'rejected_on'),
        (36, 'decision_on'),
        (37, 'workflow_status_application'),
        (38, 'workflow_status_application_support_detailed'),
        (39, 'tenant_id'),
        (40, 'activity_id'),
        (41, 'activity_def_id'),
        (42, 'activity_process_id'),
        (43, 'name'),
        (44, 'created'),
        (45, 'opened'),
        (46, 'closed'),
        (47, 'status_id'),
        (48, 'is_running_since'),
        (49, 'is_running_at'),
        (50, 'next_run'),
        (51, 'precedent_activity_id'),
        (52, 'precedent_outcome'),
        (53, 'due_date'),
        (54, 'expired'),
        (55, 'skipped'),
        (56, 'error_count'),
        (57, 'inbox_detail'),
        (58, 'group_id'),
        (59, 'last_error_id'),
        (60, 'last_modified'),
        (61, 'process_tenant_id'),
        (62, 'process_id'),
        (63, 'process_label'),
        (64, 'process_def_id'),
        (65, 'parent_process_id'),
        (66, 'parent_activity_id'),
        (67, 'top_process_id'),
        (68, 'process_status'),
        (69, 'process_last_modified'),
        (70, 'process_last_modified_by'),
        (71, 'process_suspended_date'),
        (72, 'process_suspended_by'),
        (73, 'action'),
        (74, 'activity_description'),
        (75, 'activity_name'),
        (76, 'amendment_request_id'),
        (77, 'team'),
        (78, 'tmkn_businesssector'),
        (79, 'tmkn_submittedon'),
        (80, 'tmkn_consumed'),
        (81, 'tmkn_remaining'),
        (82, 'tmkn_workflowstatus'),
        (83, 'owneridname'),
        (84, 'tmkn_contractstartdate'),
        (85, 'tmkn_contractenddate')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id_application_support'),
        (2, 'application_id'),
        (3, 'parent_payment_id'),
        (4, 'destination_status_id'),
        (5, 'status_report_id'),
        (6, 'destination_status_label'),
        (7, 'status_report_name'),
        (8, 'origin_status_id'),
        (9, 'origin_status_label'),
        (10, 'workflow_status_application_detailed'),
        (11, 'workflow_status'),
        (12, 'created_on'),
        (13, 'event_on'),
        (14, 'transaction_date'),
        (15, 'user_id'),
        (16, 'user_name'),
        (17, 'created_by'),
        (18, 'is_customer_user'),
        (19, 'remarks'),
        (20, 'next_step'),
        (21, 'state'),
        (22, 'current_status_id'),
        (23, 'current_status_label'),
        (24, 'workflow_subtype'),
        (25, 'source_table'),
        (26, 'source_system_name'),
        (27, 'is_deleted'),
        (28, 'report_date'),
        (29, 'dbt_updated_at'),
        (30, 'extract_date'),
        (31, 'created_on_application_support'),
        (32, 'id_amendment_request'),
        (33, 'submitted_on'),
        (34, 'approved_on'),
        (35, 'rejected_on'),
        (36, 'decision_on'),
        (37, 'workflow_status_application'),
        (38, 'workflow_status_application_support_detailed'),
        (39, 'tenant_id'),
        (40, 'activity_id'),
        (41, 'activity_def_id'),
        (42, 'activity_process_id'),
        (43, 'name'),
        (44, 'created'),
        (45, 'opened'),
        (46, 'closed'),
        (47, 'status_id'),
        (48, 'is_running_since'),
        (49, 'is_running_at'),
        (50, 'next_run'),
        (51, 'precedent_activity_id'),
        (52, 'precedent_outcome'),
        (53, 'due_date'),
        (54, 'expired'),
        (55, 'skipped'),
        (56, 'error_count'),
        (57, 'inbox_detail'),
        (58, 'group_id'),
        (59, 'last_error_id'),
        (60, 'last_modified'),
        (61, 'process_tenant_id'),
        (62, 'process_id'),
        (63, 'process_label'),
        (64, 'process_def_id'),
        (65, 'parent_process_id'),
        (66, 'parent_activity_id'),
        (67, 'top_process_id'),
        (68, 'process_status'),
        (69, 'process_last_modified'),
        (70, 'process_last_modified_by'),
        (71, 'process_suspended_date'),
        (72, 'process_suspended_by'),
        (73, 'action'),
        (74, 'activity_description'),
        (75, 'activity_name'),
        (76, 'amendment_request_id'),
        (77, 'team'),
        (78, 'tmkn_businesssector'),
        (79, 'tmkn_submittedon'),
        (80, 'tmkn_consumed'),
        (81, 'tmkn_remaining'),
        (82, 'tmkn_workflowstatus'),
        (83, 'owneridname'),
        (84, 'tmkn_contractstartdate'),
        (85, 'tmkn_contractenddate')
),

bronze_normalized AS (
    SELECT
        CAST(`id_application_support` AS STRING) AS `id_application_support`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`parent_payment_id` AS STRING) AS `parent_payment_id`,
        CAST(`destination_status_id` AS STRING) AS `destination_status_id`,
        CAST(`status_report_id` AS STRING) AS `status_report_id`,
        CAST(`destination_status_label` AS STRING) AS `destination_status_label`,
        CAST(`status_report_name` AS STRING) AS `status_report_name`,
        CAST(`origin_status_id` AS STRING) AS `origin_status_id`,
        CAST(`origin_status_label` AS STRING) AS `origin_status_label`,
        CAST(`workflow_status_application_detailed` AS STRING) AS `workflow_status_application_detailed`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`event_on` AS STRING) AS `event_on`,
        CAST(`transaction_date` AS STRING) AS `transaction_date`,
        CAST(`user_id` AS STRING) AS `user_id`,
        CAST(`user_name` AS STRING) AS `user_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`is_customer_user` AS STRING) AS `is_customer_user`,
        CAST(`remarks` AS STRING) AS `remarks`,
        CAST(`next_step` AS STRING) AS `next_step`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`current_status_id` AS STRING) AS `current_status_id`,
        CAST(`current_status_label` AS STRING) AS `current_status_label`,
        CAST(`workflow_subtype` AS STRING) AS `workflow_subtype`,
        CAST(`source_table` AS STRING) AS `source_table`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`created_on_application_support` AS STRING) AS `created_on_application_support`,
        CAST(`id_amendment_request` AS STRING) AS `id_amendment_request`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`rejected_on` AS STRING) AS `rejected_on`,
        CAST(`decision_on` AS STRING) AS `decision_on`,
        CAST(`workflow_status_application` AS STRING) AS `workflow_status_application`,
        CAST(`workflow_status_application_support_detailed` AS STRING) AS `workflow_status_application_support_detailed`,
        CAST(`tenant_id` AS STRING) AS `tenant_id`,
        CAST(`activity_id` AS STRING) AS `activity_id`,
        CAST(`activity_def_id` AS STRING) AS `activity_def_id`,
        CAST(`activity_process_id` AS STRING) AS `activity_process_id`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`created` AS STRING) AS `created`,
        CAST(`opened` AS STRING) AS `opened`,
        CAST(`closed` AS STRING) AS `closed`,
        CAST(`status_id` AS STRING) AS `status_id`,
        CAST(`is_running_since` AS STRING) AS `is_running_since`,
        CAST(`is_running_at` AS STRING) AS `is_running_at`,
        CAST(`next_run` AS STRING) AS `next_run`,
        CAST(`precedent_activity_id` AS STRING) AS `precedent_activity_id`,
        CAST(`precedent_outcome` AS STRING) AS `precedent_outcome`,
        CAST(`due_date` AS STRING) AS `due_date`,
        CAST(`expired` AS STRING) AS `expired`,
        CAST(`skipped` AS STRING) AS `skipped`,
        CAST(`error_count` AS STRING) AS `error_count`,
        CAST(`inbox_detail` AS STRING) AS `inbox_detail`,
        CAST(`group_id` AS STRING) AS `group_id`,
        CAST(`last_error_id` AS STRING) AS `last_error_id`,
        CAST(`last_modified` AS STRING) AS `last_modified`,
        CAST(`process_tenant_id` AS STRING) AS `process_tenant_id`,
        CAST(`process_id` AS STRING) AS `process_id`,
        CAST(`process_label` AS STRING) AS `process_label`,
        CAST(`process_def_id` AS STRING) AS `process_def_id`,
        CAST(`parent_process_id` AS STRING) AS `parent_process_id`,
        CAST(`parent_activity_id` AS STRING) AS `parent_activity_id`,
        CAST(`top_process_id` AS STRING) AS `top_process_id`,
        CAST(`process_status` AS STRING) AS `process_status`,
        CAST(`process_last_modified` AS STRING) AS `process_last_modified`,
        CAST(`process_last_modified_by` AS STRING) AS `process_last_modified_by`,
        CAST(`process_suspended_date` AS STRING) AS `process_suspended_date`,
        CAST(`process_suspended_by` AS STRING) AS `process_suspended_by`,
        CAST(`action` AS STRING) AS `action`,
        CAST(`activity_description` AS STRING) AS `activity_description`,
        CAST(`activity_name` AS STRING) AS `activity_name`,
        CAST(`amendment_request_id` AS STRING) AS `amendment_request_id`,
        CAST(`team` AS STRING) AS `team`,
        CAST(`tmkn_businesssector` AS STRING) AS `tmkn_businesssector`,
        CAST(`tmkn_submittedon` AS STRING) AS `tmkn_submittedon`,
        CAST(`tmkn_consumed` AS STRING) AS `tmkn_consumed`,
        CAST(`tmkn_remaining` AS STRING) AS `tmkn_remaining`,
        CAST(`tmkn_workflowstatus` AS STRING) AS `tmkn_workflowstatus`,
        CAST(`owneridname` AS STRING) AS `owneridname`,
        CAST(`tmkn_contractstartdate` AS STRING) AS `tmkn_contractstartdate`,
        CAST(`tmkn_contractenddate` AS STRING) AS `tmkn_contractenddate`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id_application_support` AS STRING) AS `id_application_support`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`parent_payment_id` AS STRING) AS `parent_payment_id`,
        CAST(`destination_status_id` AS STRING) AS `destination_status_id`,
        CAST(`status_report_id` AS STRING) AS `status_report_id`,
        CAST(`destination_status_label` AS STRING) AS `destination_status_label`,
        CAST(`status_report_name` AS STRING) AS `status_report_name`,
        CAST(`origin_status_id` AS STRING) AS `origin_status_id`,
        CAST(`origin_status_label` AS STRING) AS `origin_status_label`,
        CAST(`workflow_status_application_detailed` AS STRING) AS `workflow_status_application_detailed`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`event_on` AS STRING) AS `event_on`,
        CAST(`transaction_date` AS STRING) AS `transaction_date`,
        CAST(`user_id` AS STRING) AS `user_id`,
        CAST(`user_name` AS STRING) AS `user_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`is_customer_user` AS STRING) AS `is_customer_user`,
        CAST(`remarks` AS STRING) AS `remarks`,
        CAST(`next_step` AS STRING) AS `next_step`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`current_status_id` AS STRING) AS `current_status_id`,
        CAST(`current_status_label` AS STRING) AS `current_status_label`,
        CAST(`workflow_subtype` AS STRING) AS `workflow_subtype`,
        CAST(`source_table` AS STRING) AS `source_table`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`created_on_application_support` AS STRING) AS `created_on_application_support`,
        CAST(`id_amendment_request` AS STRING) AS `id_amendment_request`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`rejected_on` AS STRING) AS `rejected_on`,
        CAST(`decision_on` AS STRING) AS `decision_on`,
        CAST(`workflow_status_application` AS STRING) AS `workflow_status_application`,
        CAST(`workflow_status_application_support_detailed` AS STRING) AS `workflow_status_application_support_detailed`,
        CAST(`tenant_id` AS STRING) AS `tenant_id`,
        CAST(`activity_id` AS STRING) AS `activity_id`,
        CAST(`activity_def_id` AS STRING) AS `activity_def_id`,
        CAST(`activity_process_id` AS STRING) AS `activity_process_id`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`created` AS STRING) AS `created`,
        CAST(`opened` AS STRING) AS `opened`,
        CAST(`closed` AS STRING) AS `closed`,
        CAST(`status_id` AS STRING) AS `status_id`,
        CAST(`is_running_since` AS STRING) AS `is_running_since`,
        CAST(`is_running_at` AS STRING) AS `is_running_at`,
        CAST(`next_run` AS STRING) AS `next_run`,
        CAST(`precedent_activity_id` AS STRING) AS `precedent_activity_id`,
        CAST(`precedent_outcome` AS STRING) AS `precedent_outcome`,
        CAST(`due_date` AS STRING) AS `due_date`,
        CAST(`expired` AS STRING) AS `expired`,
        CAST(`skipped` AS STRING) AS `skipped`,
        CAST(`error_count` AS STRING) AS `error_count`,
        CAST(`inbox_detail` AS STRING) AS `inbox_detail`,
        CAST(`group_id` AS STRING) AS `group_id`,
        CAST(`last_error_id` AS STRING) AS `last_error_id`,
        CAST(`last_modified` AS STRING) AS `last_modified`,
        CAST(`process_tenant_id` AS STRING) AS `process_tenant_id`,
        CAST(`process_id` AS STRING) AS `process_id`,
        CAST(`process_label` AS STRING) AS `process_label`,
        CAST(`process_def_id` AS STRING) AS `process_def_id`,
        CAST(`parent_process_id` AS STRING) AS `parent_process_id`,
        CAST(`parent_activity_id` AS STRING) AS `parent_activity_id`,
        CAST(`top_process_id` AS STRING) AS `top_process_id`,
        CAST(`process_status` AS STRING) AS `process_status`,
        CAST(`process_last_modified` AS STRING) AS `process_last_modified`,
        CAST(`process_last_modified_by` AS STRING) AS `process_last_modified_by`,
        CAST(`process_suspended_date` AS STRING) AS `process_suspended_date`,
        CAST(`process_suspended_by` AS STRING) AS `process_suspended_by`,
        CAST(`action` AS STRING) AS `action`,
        CAST(`activity_description` AS STRING) AS `activity_description`,
        CAST(`activity_name` AS STRING) AS `activity_name`,
        CAST(`amendment_request_id` AS STRING) AS `amendment_request_id`,
        CAST(`team` AS STRING) AS `team`,
        CAST(`tmkn_businesssector` AS STRING) AS `tmkn_businesssector`,
        CAST(`tmkn_submittedon` AS STRING) AS `tmkn_submittedon`,
        CAST(`tmkn_consumed` AS STRING) AS `tmkn_consumed`,
        CAST(`tmkn_remaining` AS STRING) AS `tmkn_remaining`,
        CAST(`tmkn_workflowstatus` AS STRING) AS `tmkn_workflowstatus`,
        CAST(`owneridname` AS STRING) AS `owneridname`,
        CAST(`tmkn_contractstartdate` AS STRING) AS `tmkn_contractstartdate`,
        CAST(`tmkn_contractenddate` AS STRING) AS `tmkn_contractenddate`
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
        'workflow_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'workflow_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'workflow_base' AS table_name,
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
        'workflow_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'workflow_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
