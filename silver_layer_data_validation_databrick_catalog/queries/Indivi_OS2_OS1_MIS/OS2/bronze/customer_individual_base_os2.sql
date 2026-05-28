WITH tmkncap_cte AS (
    SELECT
        WSP.WAGEID                                                                        AS ID_APPLICATION_SUPPORT,
        SUM(CASE WHEN WSP.TKSHAREAMT IS NOT NULL THEN WSP.TKSHAREAMT ELSE 0 END)         AS TOTAL_TAMKEEN_CAP_AMOUNT,
        MIN(COALESCE(WSP.MONTHSTARTDATE, WSP.MONTHPAYMENTDATE))                          AS START_SUPPORT,
        MAX(WSP.MONTHENDDATE)                                                             AS END_SUPPORT,
        date_diff('month',
            MIN(COALESCE(WSP.MONTHSTARTDATE, WSP.MONTHPAYMENTDATE)),
            MAX(date_add('day', 1, WSP.MONTHENDDATE))
        )                                                                                 AS SUPPORT_DURATION
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VYW_WAGESUPPORTPLAN WSP
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APPSUP
            ON APPSUP.ID = WSP.WAGEID
    WHERE APPSUP.ACTIVESTATUSID = 'ACT'
    GROUP BY WSP.WAGEID
),

assessment_cte AS (
    SELECT
        act.NAME,
        AssessmentStatus.LABEL                                                            AS ASSESSMENT_STATUS_LABEL,
        ass.APPLICATIONID,
        ass.AMENDMENTREQUESTID,
        MAX(act.CLOSED)                                                                   AS CLOSED,
        ROW_NUMBER() OVER (
            PARTITION BY ass.APPLICATIONID, ass.AMENDMENTREQUESTID
            ORDER BY MAX(act.CLOSED) DESC
        )                                                                                 AS RN,
        CASE
            WHEN act.NAME LIKE 'Approve%' AND AssessmentStatus.LABEL = 'Confirmed' THEN 'Yes'
            ELSE 'No'
        END                                                                               AS APPROVAL
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ass
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS pro
            ON pro.TOP_PROCESS_ID = ass.PROCESSID
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
            ON act.PROCESS_ID = pro.ID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS AssessmentStatus
            ON ass.ASSESSMENTSTATUSID = AssessmentStatus.CODE
    GROUP BY
        act.NAME,
        AssessmentStatus.LABEL,
        ass.APPLICATIONID,
        ass.AMENDMENTREQUESTID,
        CASE
            WHEN act.NAME LIKE 'Approve%' AND AssessmentStatus.LABEL = 'Confirmed' THEN 'Yes'
            ELSE 'No'
        END
),

withdrawal_cte AS (
    SELECT
        withdrawstat.LABEL                                                                AS STATUS,
        act.CLOSED,
        withdraw.APPLICATIONID,
        ROW_NUMBER() OVER (
            PARTITION BY withdraw.APPLICATIONID
            ORDER BY act.ID DESC
        )                                                                                 AS RN
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_WITHDRAWALREQUEST withdraw
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_WITHDRAWALSTATUS withdrawstat
            ON withdrawstat.CODE = withdraw.STATUSID
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
            ON act.PROCESS_ID = withdraw.PROCESSID
),

ss_cte AS (
    SELECT
        ASP.APPLICATIONID,
        SUM(SS.REQUESTEDAMT)                                                              AS SUPPORT_STRUCTURE_REQUESTED_AMOUNT,
        SUM(SS.TKSHAREOVR)                                                                AS SUPPORT_STRUCTURE_TAMKEEN_SHARE_OVER,
        SUM(SS.TKSHARE)                                                                   AS SUPPORT_STRUCTURE_TAMKEEN_SHARE
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_SUPPORTSTRUCTURE SS
        JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT ASP
            ON ASP.ID = SS.APPLICATIONSUPPORTID
    GROUP BY ASP.APPLICATIONID
)

SELECT
    APPSUP.AMENDMENTREQUESTID                                                             AS AMENDMENTREQUESTID,
    APP.ID                                                                                AS APPLICATIONID,
    APPSUP.INDIVIDUALID                                                                   AS INDIVIDUALID,
    APPSUP.ID                                                                             AS APPLICATION_SUPPORT_ID,
    APP.GUID                                                                              AS GUID,
    APP.REFERENCENUMBER                                                                   AS APPLICATION_NO,
    ProgVer.COMMERCIALNAME_EN                                                             AS PROGRAM_NAME,
    ProgVer.COMMERCIALNAME_AR                                                             AS PROGRAM_NAME_AR,
    AppWFS.LABEL                                                                          AS WORKFLOW_STATUS,
    CASE WHEN APP.ISACTIVE THEN 'No' ELSE 'Yes' END                                      AS IS_ACTIVE,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE WHEN APP.CREATEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE date_add('hour', 3, APP.CREATEDON) END
        ELSE CASE WHEN amdment.CREATEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE date_add('hour', 3, amdment.CREATEDON) END
    END                                                                                   AS CREATED_ON,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE WHEN APP.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE date_add('hour', 3, APP.SUBMITTEDON) END
        ELSE CASE WHEN amdment.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE date_add('hour', 3, amdment.SUBMITTEDON) END
    END                                                                                   AS SUBMITTED_ON,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE WHEN APP.APPROVEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE date_add('hour', 3, APP.APPROVEDON) END
        ELSE asses_amed.CLOSED
    END                                                                                   AS APPROVED_ON,
    CASE WHEN APP.CUSTOMERTYPEID = 'CMP' THEN 'Enterprise' ELSE 'Individual' END         AS CUSTOMER_TYPE,
    CASE WHEN APP.STARTON <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.STARTON AS DATE) END                                     AS START_DATE,
    CASE WHEN APP.ENDON <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.ENDON AS DATE) END                                       AS END_DATE,
    CASE WHEN WAGE.STARTDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(WAGE.STARTDATE AS DATE) END                                  AS START_DATE_WAGE,
    CASE WHEN WAGE.ENDDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(WAGE.ENDDATE AS DATE) END                                    AS END_DATE_WAGE,
    WAGE.TOTALDURATION                                                                    AS DURATION_MONTHS_WAGE,
    APP.DURATION                                                                          AS DURATION_MONTHS_APPLICATION,
    CASE
        WHEN APP.ISHIPOOPTIONID = 1 THEN 'HiPo'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Non-HiPo'
        ELSE NULL
    END                                                                                   AS IS_HIPO,
    CASE WHEN APP.MONITORINGDUEDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.MONITORINGDUEDATE AS DATE) END                           AS MONITORING_DUE_DATE_APPLICATION,
    CASE WHEN APP.SPENDINGPERIODDUEDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.SPENDINGPERIODDUEDATE AS DATE) END                       AS SPENDING_PERIOD_END_DATE_APPLICATION,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCUS.NAMEEN
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE UPPER(TRIM(CUS.NAMEEN)) END
    END                                                                                   AS COMMERCIAL_NAME_EN,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCUS.NAMEAR
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE UPPER(TRIM(CUS.NAMEAR)) END
    END                                                                                   AS COMMERCIAL_NAME_AR,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCMP.CODE
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE TRIM(CMP.CODE) END
    END                                                                                   AS CR_LICENSE_NO,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCMP.MAINCODE
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE TRIM(CMP.MAINCODE) END
    END                                                                                   AS CR_LICENSE_NO_MAIN,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCMP.REGISTRATIONDATE
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE CAST(CMP.REGISTRATIONDATE AS DATE) END
    END                                                                                   AS REGISTRATION_DATE,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN CASE
            WHEN OJTCMP.COMPANYIDTYPEID = 1 THEN 'CR'
            WHEN OJTCMP.COMPANYIDTYPEID = 2 THEN 'License'
            ELSE NULL
        END
        ELSE CASE
            WHEN CMP.COMPANYIDTYPEID = 1 THEN 'CR'
            WHEN CMP.COMPANYIDTYPEID = 2 THEN 'License'
            ELSE NULL
        END
    END                                                                                   AS CR_LICENSE_TYPE,
    UPPER(TRIM(CusApp.NAMEEN))                                                            AS INDIVIDUAL_NAME,
    UPPER(TRIM(PORTUSR.NAME))                                                             AS PORTAL_USER_NAME,
    LOWER(TRIM(PORTUSR.EMAIL))                                                            AS EMAIL,
    CONCAT(
        COALESCE(PORTUSR.MOBILECOUNTRYPREFIX, ''),
        CASE WHEN PORTUSR.MOBILEPHONE IS NOT NULL AND PORTUSR.MOBILEPHONE <> '' THEN ' ' ELSE '' END,
        COALESCE(PORTUSR.MOBILEPHONE, '')
    )                                                                                     AS MOBILE_NO,
    CusIndApp.CPRNUMBER                                                                   AS CPR,
    CAST(CusIndApp.DATEOFBIRTH AS DATE)                                                   AS DATE_OF_BIRTH,
    CASE
        WHEN CusIndApp.GENDERID = 1 THEN 'Male'
        WHEN CusIndApp.GENDERID = 2 THEN 'Female'
        ELSE NULL
    END                                                                                   AS GENDER,
    SuppType.LABEL                                                                        AS SUPPORT_AREA,
    SuppArea.LABEL                                                                        AS SUPPORT_TYPE,
    WAGETRACK.LABEL                                                                       AS SUPPORT_TRACK_WAGE,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN LOWER(TRIM(APPCusIND.EMAILADDRESS))
         ELSE LOWER(TRIM(Emp.EMAILADDRESS))
    END                                                                                   AS CUSTOMER_CONTACT_INDIVIDUAL_EMAIL,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
        THEN CONCAT(COALESCE(APPCusIND.MOBILECOUNTRYPREFIX, ''), ' ', COALESCE(APPCusIND.MOBILENUMBER, ''))
        ELSE CONCAT(COALESCE(Emp.MOBILECOUNTRYPREFIX, ''), ' ', COALESCE(Emp.MOBILENUMBER, ''))
    END                                                                                   AS CUSTOMER_CONTACT_INDIVIDUAL_MOBILE_NO,
    CASE WHEN Emp.DATEDEGREEGRADUATION <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(Emp.DATEDEGREEGRADUATION AS DATE)
    END                                                                                   AS GRADUATION_DATE,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL OR EMP.JOBCURRENTWAGE = 0
         THEN UPPER(TRIM(APPCusIND.DEGREESPECIALIZATION))
         ELSE UPPER(TRIM(Emp.DEGREESPECIALIZATION))
    END                                                                                   AS HIGHEST_EDUCATIONAL_SPECIALIZATION,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN UPPER(TRIM(APPCusIND.EMPLOYERNAME))
         ELSE UPPER(TRIM(EMP.EMPLOYERNAME))
    END                                                                                   AS EMPLOYER_NAME,
    UPPER(TRIM(Emp.JOBTITLE))                                                             AS JOB_TITLE,
    CASE WHEN CAST(Emp.JOININGDATE AS DATE) = DATE '1900-01-01'
         THEN NULL ELSE CAST(Emp.JOININGDATE AS DATE)
    END                                                                                   AS JOINING_DATE,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL OR EMP.JOBCURRENTWAGE = 0
         THEN IndSeg.LABEL ELSE IndSegWage.LABEL
    END                                                                                   AS INDIVIDUAL_SEGMENT,
    TraTrack.LABEL                                                                        AS TRAINING_TRACK,
    Emp.CURRENTMONTHSEXPERIENCE                                                           AS MONTHS_OF_EXPERIENCE_CURRENT,
    Emp.TOTALMONTHSEXPERIENCE                                                             AS MONTHS_OF_EXPERIENCE_TOTAL,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN UPPER(TRIM(APPCusIND.UNIVERSITYNAME))
         ELSE UPPER(TRIM(Emp.UNIVERSITYNAME))
    END                                                                                   AS UNIVERSITY_NAME,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN Country.COUNTRYNAME ELSE CountryEmpUni.COUNTRYNAME
    END                                                                                   AS UNIVERSITY_LOCATION,
    CASE WHEN APPCusIND.ISENTREPRENEUR THEN 'Yes' ELSE 'No' END                          AS IS_ENTREPRENEUR,
    CASE WHEN Emp.JOBCURRENTWAGE > 0
         THEN Emp.JOBCURRENTWAGE ELSE APPCusIND.CURRENTWAGE
    END                                                                                   AS WAGE_CURRENT,
    WAGE.REQUESTEDINCREMENTAMOUNT                                                         AS REQUESTED_INCREMENT,
    WAGE.REQUESTEDSTIPEND                                                                 AS REQUESTED_STIPEND,
    WAGE.NEWWAGE                                                                          AS WAGE_NEW,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL OR EMP.JOBCURRENTWAGE = 0
         THEN AcaDegree.LABEL ELSE AcaDegreeWage.LABEL
    END                                                                                   AS HIGHEST_EDUCATIONAL_DEGREE,
    CASE WHEN APPSUP.ISACTIVE THEN 'Yes' ELSE 'No' END                                   AS IS_ACTIVE_INDIVIDUAL,
    ProvType.LABEL                                                                        AS TRAINING_PROVIDER_TYPE,
    ProvLocCR.CODE                                                                        AS TRAINING_PROVIDER_CR_LICENSE_NO,
    CASE
        WHEN APPSUP.PROVIDERID IS NOT NULL THEN ProvLoc.NAMEEN
        WHEN APPSUP.EXTERNALPROVIDERID IS NOT NULL THEN ProvOverseas.NAME
        ELSE NULL
    END                                                                                   AS TRAINING_PROVIDER,
    CASE
        WHEN APPSUP.PROVIDERID > 0 THEN 'Bahrain'
        WHEN APPSUP.EXTERNALPROVIDERID > 0 THEN CountryVendor.COUNTRYNAME
        ELSE NULL
    END                                                                                   AS TRAINING_PROVIDER_LOCATION,
    Trainingprogram.NAME                                                                  AS CERTIFICATE_NAME,
    CAST(TRA.TRAININGSTARTDATE AS DATE)                                                   AS TRAINING_START_DATE,
    CAST(
        CASE WHEN TRA.TRAININGENDDATE > TRA.TRAININGASSESSMENTDATE
             THEN TRA.TRAININGENDDATE ELSE TRA.TRAININGASSESSMENTDATE END
    AS DATE)                                                                              AS TRAINING_END_DATE,
    TRAMODE.LABEL                                                                         AS TRAINING_MODE_OF_DELIVERY,
    TRA.TKSHAREAMT                                                                        AS GRANT_APPROVED_TRAINING,
    TRA.CUSTOMERSHARETOTAL                                                                AS GRANT_APPROVED_TRAINING_CUSTOMER_SHARE,
    TRAPAYTYPE.LABEL                                                                      AS TRAINING_PAYMENT_TYPE,
    Trainingprogram.AWARDINGBODYNAME                                                      AS CERTIFICATE_AWARDING_BODY,
    TrnPrgPrv.TRAININGPROGRAMCAP                                                          AS CERTIFICATE_CAP,
    TrnPrgPrv.TRAININGHOURS                                                               AS CERTIFICATE_TRAINING_HOURS,
    Trainingprogram.TRAININGPROGAMSTATUSID                                                AS CERTIFICATE_STATUS,
    TRAAREA.LABEL                                                                         AS TRAINING_KNOWLEDGE_AREA,
    TRAAREADET.LABEL                                                                      AS TRAINING_KNOWLEDGE_AREA_DETAILED,
    JobLevelCur.LABEL                                                                     AS JOB_LEVEL_CURRENT,
    JobLevelNew.LABEL                                                                     AS JOB_LEVEL_NEW,
    CASE WHEN EMP.EMPLOYMENTTYPEID = 'FT' THEN 'Full-Time'
         WHEN EMPLOYMENTTYPEID = 'PT' THEN 'Part-Time'
         ELSE NULL
    END                                                                                   AS EMPLOYMENT_TYPE,
    CASE WHEN EMP.EMPLOYEECONTRACTTYPEID = 'PC' THEN 'Permanent'
         WHEN EMPLOYMENTTYPEID = 'TC' THEN 'Temporary'
         ELSE NULL
    END                                                                                   AS EMPLOYMENT_CONTRACT_TYPE,
    ''                                                                                    AS LATEST_ACTIVITY,
    ''                                                                                    AS WORKFLOW_STATUS_LAST_ACTIVITY,
    WAGE.TKSHAREAMT                                                                       AS GRANT_APPROVED_WAGE,
    tmkncap.TOTAL_TAMKEEN_CAP_AMOUNT,
    WAGE.CUSTOMERSHAREAMT                                                                 AS TOTAL_CUSTOMER_SHARE_AMOUNT,
    WAGE.TKSHAREAMT                                                                       AS TOTAL_TAMKEEN_SHARE_AMOUNT_WAGE,
    CASE
        WHEN APP.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
        ELSE date_diff(
            'year',
            CAST(CusIndApp.DATEOFBIRTH AS DATE),
            CAST(APP.SUBMITTEDON AS DATE)
        )
    END                                                                                   AS INDIVIDUAL_AGE,
    CASE
        WHEN APP.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
        ELSE date_diff(
            'year',
            CAST(CusIndApp.DATEOFBIRTH AS DATE),
            CAST(current_timestamp AS DATE)
        )
    END                                                                                   AS INDIVIDUAL_AGE_LIVE,
    AppSuppWFS.LABEL                                                                      AS WORKFLOW_STATUS_APPLICATION_SUPPORT,
    asses.ASSESSMENT_STATUS_LABEL                                                         AS WORKFLOW_STATUS_APPLICATION_DETAILED,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL THEN asses.ASSESSMENT_STATUS_LABEL
        ELSE asses_amed2.ASSESSMENT_STATUS_LABEL
    END                                                                                   AS WORKFLOW_STATUS_APPLICATION_SUPPORT_DETAILED,
    CASE
        WHEN AppWFS.LABEL IN ('Rejected') THEN asses.CLOSED
        ELSE NULL
    END                                                                                   AS REJECTED_ON,
    TrainingProgramType.NAME                                                              AS CERTIFICATE_TYPE,
    CASE
        WHEN APPSUP.ACTIVESTATUSID = 'INA' THEN 'No'
        WHEN APPSUP.ACTIVESTATUSID = 'ACT' THEN 'Yes'
        ELSE NULL
    END                                                                                   AS IS_ACTIVE_APPLICATION_SUPPORT,
    CASE
        WHEN ACK.EMPLOYEESUBMISSIONDATE <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
        ELSE date_add('hour', 3, ACK.EMPLOYEESUBMISSIONDATE)
    END                                                                                   AS CONFIRMED_ON,
    CASE
        WHEN Withdrawal.STATUS = 'Accepted'
        THEN date_add('hour', 3, Withdrawal.CLOSED)
        ELSE NULL
    END                                                                                   AS WITHDRAWN_ON,
    AuthEnt.NAME                                                                          AS AUTHORIZED_TRAINING_PROVIDER,
    Trainingprogram.NAME                                                                  AS CERTIFICATION,
    AppSuppAction.LABEL                                                                   AS APPLICATION_SUPPORT_ACTION_ID,
    TRA.ITEMCOSTTOTAL                                                                     AS COST_OF_TRAINING,
    APP.DURATION                                                                          AS TRAINING_DURATION,
    APPCONT.PRIMARYEMAIL                                                                  AS PRIMARY_EMAIL_CONTACT_DETAILS,
    CONCAT(
        COALESCE(APPCONT.PRIMARYMOBILECOUNTRYPREFIX, ''),
        ' ',
        COALESCE(APPCONT.PRIMARYMOBILENUMBER, '')
    )                                                                                     AS PRIMARY_MOBILE_NUMBER_CONTACT_DETAILS,
    APPCONT.PRIMARYNAME                                                                   AS PRIMARY_CONTACT_NAME_CONTACT_DETAILS,
    APP.CALCULATEDGRANTAMOUNT                                                             AS CALCULATED_GRANT_AMOUNT,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL THEN
            CASE
                WHEN APP.APPROVEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                ELSE date_add('hour', 3, APP.APPROVEDON)
            END
        ELSE
            CASE
                WHEN amdment.APPROVEDON <= TIMESTAMP '1900-01-01 00:00:00'
                     AND asses_amed.CLOSED = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
                WHEN amdment.APPROVEDON <> TIMESTAMP '1900-01-01 00:00:00'
                THEN date_add('hour', 3, amdment.APPROVEDON)
                ELSE date_add('hour', 3, asses_amed.CLOSED)
            END
    END                                                                                   AS APPROVED_ON_NEW,
    APPSUP.REFERENCENUMBER                                                                AS APPLICATION_SUPPORT_REF,
    ss.SUPPORT_STRUCTURE_REQUESTED_AMOUNT                                                 AS APPROVED_OTHER,
    APS.LABEL                                                                             AS AMENDMENT_STATUS,
    CASE WHEN emp.EMPLOYERNAME = ''
         THEN APPCusIND.EMPLOYERNAME ELSE emp.EMPLOYERNAME
    END                                                                                   AS EMPLOYER_NAME_EMPLOYEE_DETAILS,
    WAGE.STARTDATE                                                                        AS START_SUPPORT,
    WAGE.ENDDATE                                                                          AS END_SUPPORT,
    WAGE.TOTALDURATION                                                                    AS SUPPORT_DURATION,
    FALSE as IS_DELETED,
    'Neo2'                                                                                AS SOURCE_SYSTEM_NAME,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS TIMESTAMP)                              AS DBT_UPDATED_AT

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APPSUP
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
        ON APP.ID = APPSUP.APPLICATIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS AppWFS
        ON AppWFS.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APP.ID = APPCUS.APPLICATIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
        ON CUS.ID = CMP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_PORTALUSER PORTUSR
        ON PORTUSR.ID = APP.PORTALUSERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL IND
        ON CUS.ID = IND.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CusApp
        ON APPSUP.INDIVIDUALID = CusApp.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL CusIndApp
        ON APPSUP.INDIVIDUALID = CusIndApp.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION ProgVer
        ON ProgVer.ID = APP.PROGRAMVERSIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_SUPPORTTYPE SuppType
        ON APPSUP.SUPPORTTYPEID = SuppType.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_SUPPORTAREA SuppArea
        ON SuppType.SUPPORTAREAID = SuppArea.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTSTATUS AppSuppWFS
        ON AppSuppWFS.CODE = APPSUP.APPLICATIONSUPPORTSTATUSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMERINDIVIDUAL APPCusIND
        ON APPCUS.ID = APPCusIND.APPLICATIONCUSTOMERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_APPLICANTSEGMENT IndSeg
        ON IndSeg.CODE = APPCusIND.APPLICANTSEGMENTID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_TRAININGTRACK TraTrack
        ON TraTrack.ID = APPCusIND.TRAININGTRACKID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY Country
        ON Country.ID = APPCusIND.UNIVERSITYLOCATIONS
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_ACADEMICDEGREE AcaDegree
        ON AcaDegree.CODE = APPCusIND.ACADEMICDEGREEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_PROVIDERTYPE ProvType
        ON ProvType.ID = APPSUP.PROVIDERTYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER ProvLoc
        ON ProvLoc.ID = APPSUP.PROVIDERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY ProvLocCR
        ON ProvLocCR.ID = ProvLoc.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EXTERNALPROVIDER ProvOverseas
        ON ProvOverseas.ID = APPSUP.EXTERNALPROVIDERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY CountryVendor
        ON CountryVendor.ID = ProvOverseas.COUNTRYID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_TRAINING TRA
        ON TRA.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_CERTIFICATION TRACertif
        ON TRACertif.ID = TRA.CERTIFICATIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_TRAININGPROGRAM Trainingprogram
        ON TRACertif.TRAININGPROGRAMID = Trainingprogram.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_GUR_AUTHORIZEDENTITIES AuthEnt
        ON AuthEnt.CUSTOMERID = APPSUP.PROVIDERID AND AuthEnt.PROFILETYPEID = 'TRP'
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_TRAININGPROGRAMPROVIDER TrnPrgPrv
        ON TrnPrgPrv.TRAININGPROGRAMID = Trainingprogram.ID AND TrnPrgPrv.AUTHORIZEDPROVIDERID = AuthEnt.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_TRAININGPROGRAMTYPE1 TrainingProgramType
        ON TrainingProgramType.ID = Trainingprogram.TRAININGTYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_TRAININGDELIVERYTYPE TRAMODE
        ON TRAMODE.CODE = TRA.TRAININGDELIVERYTYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_TRAININGPAYMENTTYPE TRAPAYTYPE
        ON TRAPAYTYPE.CODE = TRA.TRAININGPAYMENTTYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_TRAININGDETAILAREA TRAAREADET
        ON TRAAREADET.ID = Trainingprogram.TRAININGDETAILAREAID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_TRAININGKNOWLEDGEAREA TRAAREA
        ON TRAAREA.ID = Trainingprogram.TRAININGKNOWLEDGEAREAID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEE Emp
        ON Emp.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY CountryEmpUni
        ON CountryEmpUni.ID = Emp.UNIVERSITYLOCATIONS
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_ACADEMICDEGREE AcaDegreeWage
        ON AcaDegreeWage.CODE = Emp.ACADEMICDEGREEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_APPLICANTSEGMENT IndSegWage
        ON IndSegWage.CODE = Emp.SEGMENTTYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_JOBLEVEL JobLevelCur
        ON JobLevelCur.CODE = Emp.JOBLEVELID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VYW_WAGE WAGE
        ON WAGE.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_JOBLEVEL JobLevelNew
        ON JobLevelNew.CODE = WAGE.NEWJOBLEVELID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_WAGETRACK WAGETRACK
        ON WAGETRACK.CODE = WAGE.WAGETRACKID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_AMENDMENTREQUEST amdment
        ON amdment.ID = APPSUP.AMENDMENTREQUESTID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER OJTCUS
        ON Emp.EMPLOYERID = OJTCUS.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY OJTCMP
        ON OJTCUS.ID = OJTCMP.ID
    LEFT JOIN assessment_cte asses
        ON asses.APPLICATIONID = APP.ID AND (asses.RN = 1 OR asses.RN IS NULL)
    LEFT JOIN assessment_cte asses_amed
        ON asses_amed.AMENDMENTREQUESTID = APPSUP.AMENDMENTREQUESTID AND asses_amed.APPROVAL = 'Yes'
    LEFT JOIN assessment_cte asses_amed2
        ON asses_amed2.AMENDMENTREQUESTID = APPSUP.AMENDMENTREQUESTID AND (asses_amed2.RN = 1 OR asses_amed2.RN IS NULL)
    LEFT JOIN withdrawal_cte Withdrawal
        ON Withdrawal.APPLICATIONID = APP.ID AND Withdrawal.RN = 1 AND Withdrawal.STATUS <> 'Draft'
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEEACKNOWLEDGMENT ACK
        ON APPSUP.ID = ACK.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTACTION AppSuppAction
        ON AppSuppAction.CODE = APPSUP.APPLICATIONSUPPORTACTIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCONTACTDETAILS APPCONT
        ON APPCONT.APPLICATIONID = APP.ID
    LEFT JOIN ss_cte ss
        ON APP.ID = ss.APPLICATIONID
    LEFT JOIN tmkncap_cte tmkncap
        ON tmkncap.ID_APPLICATION_SUPPORT = APPSUP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS APS
        ON APS.CODE = amdment.AMENDMENTSTATUSID

WHERE APPSUP.INDIVIDUALID IS NOT NULL
  AND (ProgVer.PROGRAMID <> 46 OR ProgVer.PROGRAMID IS NULL)
  AND (APP.APPLICATIONSTATUSID <> 'PM' OR APP.APPLICATIONSTATUSID IS NULL)
  AND NOT (
      SuppArea.LABEL = 'Support Structure'
      AND ProgVer.COMMERCIALNAME_EN = 'Medical Fellowship Program'
  )
  OR SuppArea.LABEL IS NULL;
