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
    tikstat.LABEL                                                                                    AS STATUS,
    tiktype.LABEL                                                                                    AS TYPE,
    MAX(CASE WHEN tik.CREATEDON = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
             ELSE date_add('hour', 3, tik.CREATEDON) END)                                           AS RECEIVED_ON,
    MAX(CASE WHEN tik.CLOSEDON = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
             ELSE date_add('hour', 3, tik.CLOSEDON) END)                                            AS ACTIONED_ON,
    AVG(CASE WHEN tik.CLOSEDON = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
             ELSE date_diff('hour', tik.CREATEDON, tik.CLOSEDON) END)                               AS AVERAGE_RESOLUTION_TIME_HOUR,
    AVG(CASE WHEN tik.CLOSEDON = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
             ELSE date_diff('day', tik.CREATEDON, tik.CLOSEDON) END)                                AS AVERAGE_RESOLUTION_TIME_DAYS,
    U.NAME                                                                                           AS AGENT_NAME,
    FALSE as IS_DELETED,
    'Neo2'                                                                                           AS SOURCE_SYSTEM_NAME,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS TIMESTAMP)                                          AS DBT_UPDATED_AT

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_TICKET tik
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_TICKETTYPE tiktype
        ON tik.TICKETTYPEID = tiktype.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_TICKETSTATUS tikstat
        ON tik.TICKETSTATUSID = tikstat.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY AC
        ON AC.PROCESS_ID = tik.PROCESSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER U
        ON AC.USER_ID = U.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEFINITION actdef
        ON AC.ACTIVITY_DEF_ID = actdef.ID

WHERE actdef.KIND = (
    SELECT ID
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_KIND
    WHERE NAME = 'Human Activity'
)

GROUP BY
    tik.ID,
    tik.GUID,
    tik.PROCESSID,
    tik.ENTITYIDENTIFIER,
    tik.REFNUMBER,
    tik.ISACTIVE,
    tik.TKCHANNELID,
    tik.TICKETTYPEID,
    tik.TICKETSTATUSID,
    tikstat.LABEL,
    tiktype.LABEL,
    U.NAME
