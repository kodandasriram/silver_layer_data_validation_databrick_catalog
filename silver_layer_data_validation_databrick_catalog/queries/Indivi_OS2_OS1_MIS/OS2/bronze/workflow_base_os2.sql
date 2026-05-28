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

        ROW_NUMBER() OVER (
            PARTITION BY ass.APPLICATIONID, ass.AMENDMENTREQUESTID
            ORDER BY act.ID DESC
        )                                                   AS RN

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ass

    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS pro
        ON pro.TOP_PROCESS_ID = ass.PROCESSID

    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
        ON act.PROCESS_ID = pro.ID

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS AssessmentStatus
        ON ass.ASSESSMENTSTATUSID = AssessmentStatus.CODE
)

SELECT

    CURRENT_DATE                                           AS "EXTRACT_DATE",

    APP_SUP.ID                                             AS "ID_APPLICATION_SUPPORT",

    APP_SUP.APPLICATIONID                                  AS "ID_APPLICATION",

    APP_SUP.CREATEDON                                      AS "CREATED_ON_APPLICATION_SUPPORT",

    APP_SUP.AMENDMENTREQUESTID                             AS "ID_AMENDMENT_REQUEST",

    CASE
        WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
        THEN APP.SUBMITTEDON
        ELSE AmendReq.SUBMITTEDON
    END                                                    AS "SUBMITTED_ON",

    CASE
        WHEN APP_SUP_STA.LABEL = 'Approved'
        THEN
            CASE
                WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
                THEN APP.APPROVEDON
                ELSE AmendReq.APPROVEDON
            END
        ELSE NULL
    END                                                    AS "APPROVED_ON",

    CASE
        WHEN APP_SUP_STA.LABEL = 'Rejected'
        THEN
            CASE
                WHEN APP_SUP.AMENDMENTREQUESTID IS NULL
                THEN APP.APPROVEDON
                ELSE AmendReq.APPROVEDON
            END
        ELSE NULL
    END                                                    AS "REJECTED_ON",

    APP_SUP_STA.LABEL                                      AS "WORKFLOW_STATUS",

    APP_STA.LABEL                                          AS "WORKFLOW_STATUS_APPLICATION",

    asses.LABEL                                            AS "WORKFLOW_STATUS_APPLICATION_DETAILED",

    asses_amed.TENANT_ID                                   AS "TENANT_ID",
    asses_amed.ID                                          AS "ACTIVITY_ID",
    asses_amed.ACTIVITY_DEF_ID                             AS "ACTIVITY_DEF_ID",
    asses_amed.ACTIVITY_PROCESS_ID                         AS "ACTIVITY_PROCESS_ID",
    asses_amed.NAME                                        AS "NAME",
    asses_amed.USER_ID                                     AS "USER_ID",
    asses_amed.CREATED                                     AS "CREATED",
    asses_amed.OPENED                                      AS "OPENED",
    asses_amed.CLOSED                                      AS "CLOSED",
    asses_amed.STATUS_ID                                   AS "STATUS_ID",
    asses_amed.IS_RUNNING_SINCE                            AS "IS_RUNNING_SINCE",
    asses_amed.IS_RUNNING_AT                               AS "IS_RUNNING_AT",
    asses_amed.NEXT_RUN                                    AS "NEXT_RUN",
    asses_amed.PRECEDENT_ACTIVITY_ID                       AS "PRECEDENT_ACTIVITY_ID",
    asses_amed.PRECEDENT_OUTCOME                           AS "PRECEDENT_OUTCOME",
    asses_amed.DUE_DATE                                    AS "DUE_DATE",
    asses_amed.EXPIRED                                     AS "EXPIRED",
    asses_amed.SKIPPED                                     AS "SKIPPED",
    asses_amed.ERROR_COUNT                                 AS "ERROR_COUNT",
    asses_amed.INBOX_DETAIL                                AS "INBOX_DETAIL",
    asses_amed.GROUP_ID                                    AS "GROUP_ID",
    asses_amed.LAST_ERROR_ID                               AS "LAST_ERROR_ID",
    asses_amed.LAST_MODIFIED                               AS "LAST_MODIFIED",

    asses_amed.PROCESS_TENANT_ID                           AS "PROCESS_TENANT_ID",
    asses_amed.PROCESS_ID                                  AS "PROCESS_ID",
    asses_amed.PROCESS_LABEL                               AS "PROCESS_LABEL",
    asses_amed.PROCESS_DEF_ID                              AS "PROCESS_DEF_ID",
    asses_amed.PARENT_PROCESS_ID                           AS "PARENT_PROCESS_ID",
    asses_amed.PARENT_ACTIVITY_ID                          AS "PARENT_ACTIVITY_ID",
    asses_amed.TOP_PROCESS_ID                              AS "TOP_PROCESS_ID",
    asses_amed.PROCESS_STATUS                              AS "PROCESS_STATUS",
    asses_amed.PROCESS_LAST_MODIFIED                       AS "PROCESS_LAST_MODIFIED",
    asses_amed.PROCESS_LAST_MODIFIED_BY                    AS "PROCESS_LAST_MODIFIED_BY",
    asses_amed.PROCESS_SUSPENDED_DATE                      AS "PROCESS_SUSPENDED_DATE",
    asses_amed.PROCESS_SUSPENDED_BY                        AS "PROCESS_SUSPENDED_BY",

    CASE
        WHEN asses_amed.AMENDMENT_REQUEST_ID IS NULL
        THEN asses.LABEL
        ELSE asses_amed.LABEL
    END                                                    AS "WORKFLOW_STATUS_APPLICATION_SUPPORT_DETAILED",

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
    END                                                    AS "DECISION_ON"

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APP_SUP

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
    ON APP_SUP.APPLICATIONID = APP.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS APP_STA
    ON APP_STA.CODE = APP.APPLICATIONSTATUSID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTSTATUS APP_SUP_STA
    ON APP_SUP_STA.CODE = APP_SUP.APPLICATIONSUPPORTSTATUSID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_AMENDMENTREQUEST AmendReq
    ON APP_SUP.AMENDMENTREQUESTID = AmendReq.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS AmendReq_STA
    ON AmendReq.AMENDMENTSTATUSID = AmendReq_STA.CODE

LEFT JOIN TEMPASSESSMENT2 asses
    ON asses.APPLICATION_ID = APP.ID
    AND asses.RN = 1

LEFT JOIN TEMPASSESSMENT2 asses_amed
    ON asses_amed.AMENDMENT_REQUEST_ID = APP_SUP.AMENDMENTREQUESTID
    AND asses_amed.RN = 1

WHERE APP_SUP.ISACTIVE = TRUE 