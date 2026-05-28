WITH TEMPAssessment AS (

    SELECT 
        act.Name                                           AS NAME,
        AssessmentStatus.LABEL                             AS LABEL,
        ass.APPLICATIONID                                  AS APPLICATION_ID,
        ass.AMENDMENTREQUESTID                             AS AMENDMENT_REQUEST_ID,
        act.Closed                                         AS CLOSED,

        ROW_NUMBER() OVER (
            PARTITION BY ass.APPLICATIONID, ass.AMENDMENTREQUESTID
            ORDER BY act.id DESC
        )                                                  AS RN,

        CASE 
            WHEN act.Name LIKE 'Approve%'
                 AND AssessmentStatus.LABEL = 'Confirmed'
            THEN 'Yes'
            ELSE 'No'
        END                                                AS APPROVAL

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ass

    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS pro
        ON pro.TOP_PROCESS_ID = ass.PROCESSID

    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
        ON act.Process_Id = pro.Id

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS AssessmentStatus
        ON ass.ASSESSMENTSTATUSID = AssessmentStatus.CODE
)

SELECT DISTINCT

    withdraw.ID                                            AS "ID_WITHDRAW",
    withdraw.REFERENCENUMBER                               AS "WITHDRAW_REF",
    withdraw.REMARKS                                       AS "CUSTOMER_RESPONSE",
    withdraw.TKREMARKS                                     AS "TAMKEEN_RESPONSE",
    withdraw.PROCESSID                                     AS "PROCESS_ID",
    withdraw.STATUSID                                      AS "STATUS_ID",
    withdraw.INITIATORUSERID                               AS "INITIATOR_USER_ID",
    withdraw.STARTDATE                                     AS "START_DATE",
    withdraw.ATTRIBUTE1                                    AS "ATTRIBUTE",

    actdef.LABEL                                           AS "ACTIVITY_NAME",
    actdef.Description                                     AS "ACTIVITY_DESCRIPTION",
    actions.LABEL                                          AS "ACTION_NAME",
    R.NAME                                                 AS "ROLE_NAME",

    CASE 
        WHEN act.User_Id = 0
        THEN 'Activity not assigned yet'
        ELSE U.NAME
    END                                                    AS "ASSIGNED_TO",

    CASE 
        WHEN act.Closed = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE act.Closed + INTERVAL '3' HOUR
    END                                                    AS "CLOSED_DATE",

    CASE 
        WHEN act.Opened = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE act.Opened + INTERVAL '3' HOUR
    END                                                    AS "OPENED_DATE",

    withdrawstat.LABEL                                     AS "WORKFLOW_STATUS",
    withdrawstat.COLORID                                   AS "COLORID",

    'Withdraw'                                             AS "SLA_TYPE",
    'OS2'                                                  AS "SOURCE",

    ROW_NUMBER() OVER (
        PARTITION BY withdraw.ID
        ORDER BY act.id
    )                                                      AS "RNK",

    withdraw.REMARKS                                       AS "CUSTOMER_REMARKS",
    withdraw.TKREMARKS                                     AS "TAMKEEN_REMARKS",

    ACT.Created                                            AS "ASSIGNED_ON",

    APP.ID                                                 AS "APPLICATION_ID",
    APP.Referencenumber                                    AS "APPLICATION_REFERENCE_NUMBER",

    PV.COMMERCIALNAME_EN                                   AS "PROGRAM_NAME",

    CusIndApp.CPRNUMBER                                    AS "EMPLOYEE_CPR",

    UPPER(TRIM(CusApp.NAMEEN))                             AS "EMPLOYEE_NAME",

    CASE 
        WHEN EMP.JOBCURRENTWAGE IS NULL
        THEN CONCAT(APPCusIND.mobilecountryprefix, ' ', APPCusIND.MOBILENUMBER)
        ELSE CONCAT(Emp.mobilecountryprefix, ' ', Emp.MOBILENUMBER)
    END                                                    AS "MOBILE_NO",

    CASE 
        WHEN EMP.JOBCURRENTWAGE IS NULL
        THEN LOWER(TRIM(APPCusIND.EMAILADDRESS))
        ELSE LOWER(TRIM(Emp.EMAILADDRESS))
    END                                                    AS "INDIVIDUAL_EMAIL",

    UPPER(TRIM(Emp.JOBTITLE))                              AS "JOB_TITLE",

    CASE 
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        )
        THEN OJTCUS.NAMEEN

        ELSE CASE 
            WHEN APP.CUSTOMERTYPEID = 'IND'
            THEN NULL
            ELSE UPPER(TRIM(CUS.NAMEEN))
        END
    END                                                    AS "COMMERCIAL_NAME_EN",

    CASE 
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        )
        THEN OJTCUS.NAMEAR

        ELSE CASE 
            WHEN APP.CUSTOMERTYPEID = 'IND'
            THEN NULL
            ELSE UPPER(TRIM(CUS.NAMEAR))
        END
    END                                                    AS "COMMERCIAL_NAME_AR",

    CASE 
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        )
        THEN OJTCMP.CODE

        ELSE CASE 
            WHEN APP.CUSTOMERTYPEID = 'IND'
            THEN NULL
            ELSE TRIM(CMP.CODE)
        END
    END                                                    AS "CR_LICENSE_NO",

    CASE 
        WHEN Emp.JOBCURRENTWAGE > 0
        THEN Emp.JOBCURRENTWAGE
        ELSE APPCusIND.CURRENTWAGE
    END                                                    AS "WAGE_CURRENT",

    CASE 
        WHEN CAST(Emp.JOININGDATE AS DATE) = DATE '1900-01-01'
        THEN NULL
        ELSE CAST(Emp.JOININGDATE AS DATE)
    END                                                    AS "JOINING_DATE",

    CASE 
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE 
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
            ELSE APP.APPROVEDON + INTERVAL '3' HOUR
        END

        ELSE asses_amed.CLOSED
    END                                                    AS "APPROVED_ON_APPLICATION",

    withdrawstat.LABEL                                     AS "FINAL_WITHDRAW_STATUS",

    CASE 
        WHEN withdraw.STATUSID = 'ACC'
        THEN withdraw.UPDATEDON + INTERVAL '3' HOUR
        ELSE NULL
    END                                                    AS "WITHDRAWN_DATE"

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_WITHDRAWALREQUEST withdraw

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_WITHDRAWALSTATUS withdrawstat
    ON withdrawstat.CODE = withdraw.StatusId

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS pro
    ON pro.ID = withdraw.PROCESSID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
    ON act.Process_Id = pro.Id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEFINITION actdef
    ON act.Activity_Def_Id = actdef.Id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_HMY_ACTIVITYEXTENDED act_ext
    ON act_ext.ID = act.Id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2FH_APPLICATIONASSESSMENTACTIONS actions
    ON actions.KEY = act_ext.SELECTEDACTIONKEY

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEF_ROLE ADR
    ON actdef.Id = ADR.Activity_Def_Id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_ROLE R
    ON ADR.Role_Id = R.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER U
    ON act.User_Id = U.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
    ON APP.id = withdraw.APPLICATIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION PV
    ON PV.ID = APP.PROGRAMVERSIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
    ON APP.ID = APPCUS.APPLICATIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
    ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
    ON CUSPROF.CUSTOMERID = CUS.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
    ON CUS.ID = CMP.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APPSUP
    ON APPSUP.APPLICATIONID = APP.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEE Emp
    ON Emp.APPLICATIONSUPPORTID = APPSUP.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMERINDIVIDUAL APPCusIND
    ON APPCUS.ID = APPCusIND.APPLICATIONCUSTOMERID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION ProgVer
    ON ProgVer.ID = APP.PROGRAMVERSIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL CusIndApp
    ON APPSUP.INDIVIDUALID = CusIndApp.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CusApp
    ON APPSUP.INDIVIDUALID = CusApp.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER OJTCUS
    ON Emp.EMPLOYERID = OJTCUS.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY OJTCMP
    ON OJTCUS.ID = OJTCMP.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT AMT
    ON AMT.APPLICATIONID = APP.ID
    AND APP.ISACTIVE = TRUE

LEFT JOIN TEMPAssessment asses_amed
    ON asses_amed.AMENDMENT_REQUEST_ID = APPSUP.AMENDMENTREQUESTID
    AND asses_amed.APPROVAL = 'Yes'

WHERE actdef.Kind = (
    SELECT ID
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_KIND
    WHERE Name = 'Human Activity'
)

AND APPSUP.ISACTIVE = TRUE
AND APPSUP.INDIVIDUALID IS NOT NULL