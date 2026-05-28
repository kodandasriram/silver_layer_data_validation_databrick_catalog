WITH RANKED_MOIC AS (
    SELECT
        COMNERCIALNAMEEN                                                    AS COMNERCIAL_NAME_EN,
        APPLICATIONID                                                       AS APPLICATION_ID,
        STATUS,
        CRNUMBER,
        ROW_NUMBER() OVER (
            PARTITION BY CRNUMBER
            ORDER BY UPDATEDON DESC
        ) AS ROW_NUM
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CRDETAILS
),

MOIC AS (
    SELECT *
    FROM RANKED_MOIC
    WHERE ROW_NUM = 1
),

PAYMENT_REQ AS (
    SELECT
        ID                                                                  AS PAYMENT_REQUEST_ID,
        PROCESSREFERENCE                                                    AS PROCESS_REFERENCE,
        CREATEDON,
        SUBMITTEDON,
        FAWATEERREFERENCE,
        TAMKEENSHAREVALUE,
        CUSTOMERSHAREVALUE,
        TOTALCOSTVALUE,
        APPLICATIONID,
        PAYMENTSTATUSID,
        PAYMENTREQUESTTYPEID,
        PAYEETYPEID,
        IBANID2,
        SUPPORTAREAID,
        CREATEDBY,
        UPDATEDON
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTREQUEST
),

CTE_PAYMENT_REQUEST AS (
    SELECT
        -- Extract date
        CURRENT_TIMESTAMP + INTERVAL '3' HOUR                              AS EXTRACT_DATE,

        -- Identifiers
        A.ID                                                                AS APPLICATION_ID,
        PR.PAYMENT_REQUEST_ID                                               AS PAYMENT_REQUEST_ID,
        PR.PROCESS_REFERENCE                                                AS PAYMENT_REQUEST_REFERENCE,
        A.REFERENCENUMBER                                                   AS APPLICATION_REFERENCE,

        -- Program
        P.NAME                                                              AS PROGRAM_NAME,

        -- Payment request status
        PRS.LABEL                                                           AS PAYMENT_REQUEST_STATUS,

        -- Dates (sentinel-1900 handling + timezone offset)
        CASE
            WHEN PR.CREATEDON = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE PR.CREATEDON + INTERVAL '3' HOUR
        END                                                                 AS CREATED_ON_PAYMENT_REQUEST,

        CASE
            WHEN PR.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE PR.SUBMITTEDON + INTERVAL '3' HOUR
        END                                                                 AS SUBMITTED_ON_PAYMENT_REQUEST,

        CASE
            WHEN PRS.LABEL = 'Paid' THEN PR.UPDATEDON + INTERVAL '3' HOUR
            ELSE NULL
        END                                                                 AS PAID_ON,

        -- Payment classification
        PRT.LABEL                                                           AS PAYMENT_TYPE,

        -- IBAN
        I.IBANNUMBER                                                        AS IBAN,
        IBST.LABEL                                                          AS IBAN_STATUS,

        -- Payee
        PT.LABEL                                                            AS PAYEE_TYPE,
        C.NAMEEN                                                            AS PAYEE,

        -- Amounts
        PR.TOTALCOSTVALUE                                                   AS TOTAL_COST_VALUE,
        PR.TAMKEENSHAREVALUE                                                AS TAMKEEN_SHARE_VALUE,
        PR.CUSTOMERSHAREVALUE                                               AS CUSTOMER_SHARE_VALUE,

        -- Fawateer
        PR.FAWATEERREFERENCE                                                AS FAWATEER_REFERENCE,

        -- Origin system classification
        CASE
            WHEN PR.CREATEDBY LIKE '%MIS Migration User%' THEN 'MIS'
            WHEN LENGTH(CAST(PR.CREATEDBY AS VARCHAR)) IN (4, 5)  THEN 'NEOT_1_0'
            ELSE 'NEOT_2_0'
        END                                                                 AS ORIGIN_SYSTEM,

        -- Audit
        PR.CREATEDBY                                                        AS CREATED_BY,
        PR.UPDATEDON                                                        AS UPDATEDON,
        PR.CREATEDON                                                        AS CREATEDON,

        -- Soft delete + metadata
        FALSE                                                               AS IS_DELETED,
        UPPER(TRIM('Neo2'))                                AS SOURCE_SYSTEM_NAME,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)            AS DBT_UPDATED_AT

    FROM PAYMENT_REQ PR
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION                A
           ON PR.APPLICATIONID = A.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM                    P
           ON P.ID = A.PROGRAMVERSIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTREQUESTSTATUS       PRS
           ON PR.PAYMENTSTATUSID = PRS.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_PAYMENTREQUESTTYPES        PRT
           ON PR.PAYMENTREQUESTTYPEID = PRT.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_PAYEETYPE                  PT
           ON PT.CODE = PR.PAYEETYPEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_TLV_IBAN                       I
           ON PR.IBANID2 = I.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_TLV_IBANSTATUS                 IBST
           ON I.IBANSTATUSID = IBST.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE            CP
           ON I.CUSTOMERPROFILEID = CP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER                   C
           ON CP.CUSTOMERID = C.ID
    LEFT JOIN MOIC
           ON MOIC.APPLICATION_ID = A.ID
)


SELECT *
FROM CTE_PAYMENT_REQUEST
WHERE 1 = 1
