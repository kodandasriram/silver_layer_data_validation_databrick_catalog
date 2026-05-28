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
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_765_SPECIALCONDITIONFULFILMENT SCF
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS PRO
        ON PRO.ID = SCF.PROCESSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY ACT
        ON ACT.PROCESS_ID = PRO.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEFINITION ACTDEF
        ON ACT.ACTIVITY_DEF_ID = ACTDEF.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_HMY_ACTIVITYEXTENDED ACT_EXT
        ON ACT_EXT.ID = ACT.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2FH_APPLICATIONASSESSMENTACTIONS ACTIONS
        ON ACTIONS.KEY = ACT_EXT.SELECTEDACTIONKEY
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEF_ROLE ADR
        ON ACTDEF.ID = ADR.ACTIVITY_DEF_ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_ROLE R
        ON ADR.ROLE_ID = R.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER U
        ON ACT.USER_ID = U.ID
    WHERE ACTDEF.KIND = (
        SELECT ID
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_KIND
        WHERE NAME = 'Human Activity'
    )
),

CTE_SPECIAL_CONDITIONS AS
(
    SELECT
        CAST(date_add('hour', 3, current_timestamp) AS date)                AS EXTRACT_DATE,
        PROGVER.COMMERCIALNAME_EN                                           AS PROGRAM_NAME,
        CASE
            WHEN SPCONDTION.REFERENCENUMBER = '' THEN SCF.REFERENCENUMBER
            ELSE SPCONDTION.REFERENCENUMBER
        END                                                                  AS SPECIAL_CONDITION_REQUEST_NO,
        SPCONDTION.ID                                                       AS ID_SPECIAL_CONDITION,
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
            ELSE date_add('hour', 3, SCF.SUBMISSIONDATE)
        END                                                                 AS SUBMITTED_ON,
        date_add('hour', 3, PROCESS_ASSESSOR.CLOSED)                        AS VERIFIED_ON,
        PROCESS_ASSESSOR.OWNER                                              AS VERIFIED_BY,
        date_add('hour', 3, PROCESS_APPROVAL.CLOSED)                        AS APPROVED_ON,
        PROCESS_APPROVAL.OWNER                                              AS APPROVED_BY,
        CASE
            WHEN SCF_STAT.LABEL = 'Rejected' THEN date_add('hour', 3, SCF.UPDATEDON)
            ELSE NULL
        END                                                                  AS REJECTED_ON,
        CASE
            WHEN SCF_STAT.LABEL = 'Rejected' THEN PROCESS_ASSESSOR.OWNER
            ELSE NULL
        END                                                                  AS REJECTED_BY,
        date_add('hour', 3, SCF.UPDATEDON)                                     AS DECISION_DATE,
        CASE
            WHEN SPCONDTION.ISACTIVE THEN 'TRUE'
            ELSE 'FALSE'
        END                                                                  AS IS_ACTIVE,
        FALSE                                                                AS IS_DELETED,
        'Neo2'                                                               AS SOURCE_SYSTEM_NAME,
        SCF.BRONZE_CREATED_ON,
        SCF.BRONZE_UPDATED_ON,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)             AS DBT_UPDATED_AT
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_L68_SPECIALCONDITION SPCONDTION
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_765_SPECIALCONDITIONFULFILMENT SCF
        ON SCF.SPECIALCONDITIONID = SPCONDTION.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_765_MONITORINGSTATUS SCF_STAT
        ON SCF_STAT.CODE = SCF.MONITORINGSTATUSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_L68_SPECIALCONDITIONSTATUS SPCONDTION_STAT
        ON SPCONDTION.SPECIALCONDITIONSTATUSID = SPCONDTION_STAT.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
        ON SPCONDTION.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS APPWFS
        ON APPWFS.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION PROGVER
        ON PROGVER.ID = APP.PROGRAMVERSIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APP.ID = APPCUS.APPLICATIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
        ON CUS.ID = CMP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_SUPPORTAREA SUPPAREA
        ON SUPPAREA.CODE = SPCONDTION.SUPPORTAREAID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_L68_SPECIALCONDITIONTARGET SPCON_TAR
        ON SPCONDTION.SPECIALCONDITIONTARGETID = SPCON_TAR.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_T8J_SPECIALCONDITIONBO SPCONDTIONBO
        ON SPCONDTION.SPECIALCONDITIONBOID = SPCONDTIONBO.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_T8J_SPECIALCONDITIONTYPE SPECIALCONDITIONTYPE
        ON SPECIALCONDITIONTYPE.ID = SPCONDTIONBO.SPECIALCONDITIONTYPECODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_T8J_SPECIALCONDITIONLEVEL SPECIALCONDITIONLEVEL
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
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER U
        ON U.USERNAME = SCF.UPDATEDBY
)

SELECT
    CAST(EXTRACT_DATE AS date)  AS EXTRACT_DATE,
    PROGRAM_NAME AS PROGRAM_NAME,
    SPECIAL_CONDITION_REQUEST_NO AS SPECIAL_CONDITION_REQUEST_NO,
    CAST(ID_SPECIAL_CONDITION AS integer)                         AS ID_SPECIAL_CONDITION,
    APPLICATION_NO AS APPLICATION_NO,
    CAST(APPLICATION_ID AS integer)                               AS APPLICATION_ID,
    WORKFLOW_STATUS_APPLICATION AS WORKFLOW_STATUS_APPLICATION,
    WORKFLOW_STATUS_SPECIAL_CONDITION AS WORKFLOW_STATUS_SPECIAL_CONDITION,
    WORKFLOW_STATUS_SPECIAL_CONDITION_DETAILED AS WORKFLOW_STATUS_SPECIAL_CONDITION_DETAILED,
    CUSTOMER_TYPE AS CUSTOMER_TYPE,
    COMMERCIAL_NAME AS COMMERCIAL_NAME,
    CR_LICENSE_NO AS CR_LICENSE_NO,
    SPECIAL_CONDITION_TYPE AS SPECIAL_CONDITION_TYPE,
    SPECIAL_CONDITION_LEVEL AS SPECIAL_CONDITION_LEVEL,
    SPECIAL_CONDITION_DESCRIPTION AS SPECIAL_CONDITION_DESCRIPTION,
    SPECIAL_CONDITION_TARGET AS SPECIAL_CONDITION_TARGET,
    SPECIAL_CONDITION_TARGET_VALUE AS SPECIAL_CONDITION_TARGET_VALUE,
    SPECIAL_CONDITION_CUSTOMER_COMMENT AS SPECIAL_CONDITION_CUSTOMER_COMMENT,
    SPECIAL_CONDITION_TAMKEEN_COMMENT AS SPECIAL_CONDITION_TAMKEEN_COMMENT,
    SUPPORT_AREA AS SUPPORT_AREA,
    OWNER AS OWNER,
    CAST(SUBMITTED_ON AS timestamp)                               AS SUBMITTED_ON,
    CAST(VERIFIED_ON AS timestamp)                                AS VERIFIED_ON,
    VERIFIED_BY AS VERIFIED_BY,
    CAST(APPROVED_ON AS timestamp)                                AS APPROVED_ON,
    APPROVED_BY AS APPROVED_BY,
    CAST(REJECTED_ON AS timestamp)                                AS REJECTED_ON,
    REJECTED_BY AS REJECTED_BY,
    CAST(DECISION_DATE AS timestamp)                              AS DECISION_DATE,
    IS_ACTIVE AS IS_ACTIVE,
    IS_DELETED AS IS_DELETED,
    UPPER(TRIM(SOURCE_SYSTEM_NAME))                          AS SOURCE_SYSTEM_NAME,
    CAST(BRONZE_CREATED_ON AS timestamp)                AS BRONZE_CREATED_ON,
    CAST(BRONZE_UPDATED_ON AS timestamp)                AS BRONZE_UPDATED_ON,
    CAST(DBT_UPDATED_AT AS timestamp)                             AS DBT_UPDATED_AT
FROM CTE_SPECIAL_CONDITIONS
