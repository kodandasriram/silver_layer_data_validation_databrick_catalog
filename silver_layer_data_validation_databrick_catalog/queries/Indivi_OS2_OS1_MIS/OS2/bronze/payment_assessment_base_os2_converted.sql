WITH PAYASS AS (
    SELECT
        PayAss.PAYMENTREQUESTID                                        AS PAYMENTREQUESTID,
        act.USER_ID                                                    AS USER_ID,
        CASE
            WHEN act.USER_ID = 0
                THEN 'Activity not assigned yet'
            ELSE U.NAME
        END                                                            AS ASSIGNED_TO,
        ROW_NUMBER() OVER (
            PARTITION BY PayAss.PAYMENTREQUESTID
            ORDER BY act.ID
        )                                                              AS RNK
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTASSESSMENT          PayAss
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTASSESSMENTSTATUS PayAssStat
           ON PayAss.ASSESSMENTSTATUSID = PayAssStat.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY              act
           ON act.PROCESS_ID = PayAss.PROCESSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER                      U
           ON act.USER_ID = U.ID
    WHERE act.USER_ID IS NOT NULL
),

SEND_TO_INFO AS (
    SELECT
        PayAss.PAYMENTREQUESTID                                        AS PAYMENTREQUESTID,
        MAX(act.CLOSED)                                                AS MAX_EXECUTE_PAYMENT_DATE,
        COUNT(*)                                                       AS NUMBER_OF_EXECUTE_PAYMENT_TRIES
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTASSESSMENT          PayAss
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS               pro
           ON pro.TOP_PROCESS_ID = PayAss.PROCESSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY              act
           ON act.PROCESS_ID = pro.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY_DEFINITION   actdef
           ON act.ACTIVITY_DEF_ID = actdef.ID
    WHERE actdef.LABEL = 'Execute Payment'
    GROUP BY
        PayAss.PAYMENTREQUESTID,
        actdef.LABEL
),

CTE_PAYMENT_REQUEST AS (
    SELECT DISTINCT
        PayReq.ID,
        PayReq.PAYMENTSTATUSID,
        PayReq.PAYMENTREQUESTTYPEID,
        PayReq.IBANID,
        PayReq.IBANID2,
        PayReq.APPLICATIONID,
        PayReq.SUPPORTAREAID,
        PayReq.PORTALUSERID,
        PayReq.PAYEETYPEID,
        PayReq.PROCESSREFERENCE,
        PayReq.TOTALCOSTVALUE,
        PayReq.CUSTOMERSHAREVALUE,
        PayReq.UPDATEDON,
        PayReq.UPDATEDBY,
        PayReq.GUID,
        PayReq.CUSTOMERPROFILEID,
        PayReq.SUBMITTEDON,
        PayReq.ISBEINGPROCESSED,
        PayReq.RECONCILIATIONDATE,
        PayReq.PAYMENTIDENTIFIER,
        PayReq.CASHREQUIREMENTNUMBER,
        PayReq.LASTSUBMISSIONDATE,
        PayReq.SENDTOINFORON,
        PayReq.PROCESSEDON,
        PayReq.ELIGIBILITYSTATUSID,
        PayReq.AITOOLRESULT,
        PayReq.FROMBANK,
        PayReq.ISSAMPLING,
        PayReq.CREATEDON,
        CURRENT_DATE                                                   AS EXTRACT_DATE,
        APP.REFERENCENUMBER                                            AS APPLICATION_REFERENCE,
        PayReq.PROCESSREFERENCE                                        AS PAYMENT_REQUEST_REFERENCE,
        PayReq_Stat.LABEL                                              AS PAYMENT_REQUEST_STATUS,
        PayReq.TAMKEENSHAREVALUE                                       AS TOTAL_AMOUNT_TAMKEEN_SHARE,
        PayReq.FAWATEERREFERENCE                                       AS FAWATEER_REFERENCE,
        CASE
            WHEN PayReq.FAWATEERREFERENCE IS NULL
                 OR PayReq.FAWATEERREFERENCE = ''
                THEN 'No'
            ELSE 'Yes'
        END                                                            AS FAWATEER_FLAG,
        CASE
            WHEN PayReq.CREATEDON = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
            ELSE PayReq.CREATEDON + INTERVAL '3' HOUR
        END                                                            AS CREATED_ON_PAYMENT_REQUEST_GENERATED,
        CASE
            WHEN PayReq.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
            ELSE PayReq.SUBMITTEDON + INTERVAL '3' HOUR
        END                                                            AS SUBMITTED_ON_PAYMENT_REQUEST_SUBMITTED,
        PayReq_Type.LABEL                                              AS PAYMENT_TYPE,
        IBAN.IBANNUMBER                                                AS IBAN,
        IBST.LABEL                                                     AS IBAN_STATUS,
        PayeeType.LABEL                                                AS PAYEE_TYPE,
        Cus.NAMEEN                                                     AS PAYEE,
        CASE
            WHEN PayeeType.CODE = 'CST'
                THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END                                                            AS PAYEE_CPR_CR_LICENSE,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
            ELSE APP.APPROVEDON + INTERVAL '3' HOUR
        END                                                            AS APPROVED_ON_APPLICATION,
        APST.LABEL                                                     AS WORKFLOW_STATUS_APPLICATION,
        AppSuppWFS.LABEL                                               AS WORKFLOW_STATUS_EMPLOYEE,
        PAYASS.ASSIGNED_TO                                             AS ASSIGNED_TO,
        Vendor_CMP.CODE                                                AS VENDOR_CR_LICENSE,
        Vendor_CUS.NAMEEN                                              AS VENDOR_NAME_TRAINING_PROVIDER_NAME,
        PV.COMMERCIALNAME_EN                                           AS PROGRAM_NAME,
        DIM.DIMENSION4                                                 AS DIMENSION4,
        PayReq.UPDATEDBY                                               AS UPDATED_BY,
        CASE
            WHEN SEND_TO_INFO.MAX_EXECUTE_PAYMENT_DATE = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
            ELSE SEND_TO_INFO.MAX_EXECUTE_PAYMENT_DATE + INTERVAL '3' HOUR
        END                                                            AS MAX_EXECUTE_PAYMENT_DATE,
        SEND_TO_INFO.NUMBER_OF_EXECUTE_PAYMENT_TRIES                   AS NUMBER_OF_EXECUTE_PAYMENT_TRIES,
        IBAN.ACCOUNTNAME                                               AS ACCOUNT_NAME,
        FALSE                                                          AS IS_DELETED,
        'Neo2'                                                         AS SOURCE_SYSTEM_NAME,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)        AS DBT_UPDATED_AT

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTREQUEST             PayReq
    LEFT JOIN PAYASS
           ON PAYASS.PAYMENTREQUESTID = PayReq.ID
          AND PAYASS.RNK = 1
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION           APP
           ON PayReq.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS     APST
           ON APP.APPLICATIONSTATUSID = APST.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION        PV
           ON APP.PROGRAMVERSIONID = PV.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM               P
           ON P.ID = PV.PROGRAMID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_INFORDIMENSIONMAPPING3 DIM
           ON P.GUID = DIM.PROGRAMGUID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTREQUESTSTATUS  PayReq_Stat
           ON PayReq.PAYMENTSTATUSID = PayReq_Stat.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_PAYMENTREQUESTTYPES   PayReq_Type
           ON PayReq.PAYMENTREQUESTTYPEID = PayReq_Type.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_PAYEETYPE             PayeeType
           ON PayeeType.CODE = PayReq.PAYEETYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_TLV_IBAN                  IBAN
           ON PayReq.IBANID2 = IBAN.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_TLV_IBANSTATUS            IBST
           ON IBAN.IBANSTATUSID = IBST.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE       CusProf
           ON IBAN.CUSTOMERPROFILEID = CusProf.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER              Cus
           ON CusProf.CUSTOMERID = Cus.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL            IND
           ON CusProf.CUSTOMERID = IND.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY               CMP
           ON CusProf.CUSTOMERID = CMP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT    APPSUP
           ON APPSUP.APPLICATIONID = PayReq.APPLICATIONID
          AND APPSUP.ACTIVESTATUSID = 'ACT'
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTSTATUS AppSuppWFS
           ON AppSuppWFS.CODE = APPSUP.APPLICATIONSUPPORTSTATUSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEE              Emp
           ON APPSUP.ID = Emp.APPLICATIONSUPPORTID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER              Vendor_CUS
           ON Vendor_CUS.ID = Emp.EMPLOYERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY               Vendor_CMP
           ON Vendor_CUS.ID = Vendor_CMP.ID
    LEFT JOIN SEND_TO_INFO
           ON SEND_TO_INFO.PAYMENTREQUESTID = PayReq.ID
)

SELECT
    ID,
    PAYMENTSTATUSID,
    PAYMENTREQUESTTYPEID,
    IBANID,
    IBANID2,
    APPLICATIONID,
    SUPPORTAREAID,
    PORTALUSERID,
    PAYEETYPEID,
    PROCESSREFERENCE,
    TOTALCOSTVALUE,
    CUSTOMERSHAREVALUE,
    UPDATEDON,
    CREATEDON,
    UPDATEDBY,
    GUID,
    CUSTOMERPROFILEID,
    SUBMITTEDON,
    ISBEINGPROCESSED,
    RECONCILIATIONDATE,
    PAYMENTIDENTIFIER,
    CASHREQUIREMENTNUMBER,
    LASTSUBMISSIONDATE,
    SENDTOINFORON,
    PROCESSEDON,
    ELIGIBILITYSTATUSID,
    AITOOLRESULT,
    FROMBANK,
    ISSAMPLING,
    EXTRACT_DATE,
    APPLICATION_REFERENCE,
    PAYMENT_REQUEST_REFERENCE,
    PAYMENT_REQUEST_STATUS,
    TOTAL_AMOUNT_TAMKEEN_SHARE,
    FAWATEER_REFERENCE,
    FAWATEER_FLAG,
    CREATED_ON_PAYMENT_REQUEST_GENERATED,
    SUBMITTED_ON_PAYMENT_REQUEST_SUBMITTED,
    PAYMENT_TYPE,
    IBAN,
    IBAN_STATUS,
    PAYEE_TYPE,
    PAYEE,
    PAYEE_CPR_CR_LICENSE,
    APPROVED_ON_APPLICATION,
    WORKFLOW_STATUS_APPLICATION,
    WORKFLOW_STATUS_EMPLOYEE,
    ASSIGNED_TO,
    VENDOR_CR_LICENSE,
    VENDOR_NAME_TRAINING_PROVIDER_NAME,
    PROGRAM_NAME,
    DIMENSION4,
    UPDATED_BY,
    MAX_EXECUTE_PAYMENT_DATE,
    NUMBER_OF_EXECUTE_PAYMENT_TRIES,
    ACCOUNT_NAME,
    IS_DELETED,
    UPPER(TRIM(SOURCE_SYSTEM_NAME))                     AS SOURCE_SYSTEM_NAME,
    CAST(DBT_UPDATED_AT AS TIMESTAMP)                        AS DBT_UPDATED_AT

FROM CTE_PAYMENT_REQUEST

/*
Incremental filter from dbt model (enable manually if needed):
WHERE
    GREATEST(
        CREATEDON,
        COALESCE(UPDATEDON, CREATEDON)
    ) >
    (
        SELECT COALESCE(
            MAX(
                GREATEST(
                    CREATEDON,
                    COALESCE(UPDATEDON, CREATEDON)
                )
            ),
            CAST('1900-01-01' AS TIMESTAMP)
        )
        FROM <target_table>
    )

*/