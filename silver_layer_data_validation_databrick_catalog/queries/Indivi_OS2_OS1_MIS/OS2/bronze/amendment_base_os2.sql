WITH COMBINED_APP AS (

    SELECT
        PROGVER.COMMERCIALNAME_EN                        AS PROGRAM_NAME,
        PROGRAM.PROFILETYPEID                            AS PROGRAM_TYPE,
        APP.REFERENCENUMBER                              AS REFERENCE,
        APP.ID                                           AS APPLICATION_ID,
        APST.LABEL                                       AS APPLICATION_STATUS,
        CAST(NULL AS BIGINT)                             AS AMENDMENTNO,
		CAST(NULL AS BIGINT)                             AS UTILIZEDAMOUNT,
		CAST(NULL AS BIGINT)                             AS UNUTILIZEDAMOUNT,
		CAST(NULL AS BIGINT)                             AS TOTALAPPROVEDAMOUNT,
		CAST(NULL AS BIGINT)                             AS TOTALAVAILABLEAMT,
		CAST(NULL AS BIGINT)                             AS UTILIZEDAMT,
		CAST(NULL AS BIGINT)                             AS UNUTILIZEDAMT,
        APP.CUSTOMERSHAREAMT,
		APP.HASWAGESUPPORTMOLEMPLOYEES,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'IND' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END                                               AS CPR_NUMBER,
        CUS.NAMEEN                                      AS CUSTOMER_ENTERPRISE_NAME,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.APPROVEDON + INTERVAL '3' HOUR
        END                                               AS APPROVED_ON_DATE,
        CASE
            WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.STARTON AS DATE)
        END                                               AS CONTRACT_START_DATE,
        CASE
            WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.MONITORINGDUEDATE AS DATE)
        END                                               AS MONITORING_DUE_DATE,
        CASE
            WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.ENDON AS DATE)
        END                                               AS CONTRACT_END_DATE,
        APP.TKSHAREAMT                                  AS TOTAL_APPROVED_AMOUNT_TAMKEEN_SHARE,
        CASE
            WHEN APP.CREATEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.CREATEDON + INTERVAL '3' HOUR
        END                                               AS CREATED_ON,
        CASE
            WHEN APP.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SUBMITTEDON + INTERVAL '3' HOUR
        END                                               AS SUBMITTED_ON,
        CASE
            WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL '3' HOUR 
        END                                               AS SPENDING_PERIOD_END_DATE,
--        CASE
--            WHEN APP.APPROVALLETTERACCEPTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
--            ELSE APP.APPROVALLETTERACCEPTEDON + INTERVAL '3' HOUR 
--        END                                               AS APPROVAL_LETTER_CONFIRMED,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION PROGVER
        ON APP.PROGRAMVERSIONID = PROGVER.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM PROGRAM
        ON PROGVER.PROGRAMID = PROGRAM.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS APST
        ON APP.APPLICATIONSTATUSID = APST.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL IND
        ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
        ON CUSPROF.CUSTOMERID = CMP.ID

    UNION ALL

    SELECT
        PROGVER.COMMERCIALNAME_EN                        AS PROGRAM_NAME,
        PROGRAM.PROFILETYPEID                            AS PROGRAM_TYPE,
        AMED.REFERENCENUMBER                             AS REFERENCE,
        APP.ID                                           AS APPLICATION_ID,
        APST.LABEL                                      AS APPLICATION_STATUS,
        AMED.AMENDMENTNO,
		AMED.UTILIZEDAMOUNT,
		AMED.UNUTILIZEDAMOUNT,
		AMED.TOTALAPPROVEDAMOUNT,
		AMED.TOTALAVAILABLEAMT,
		AMED.UTILIZEDAMT,
		AMED.UNUTILIZEDAMT,
		AMED.CUSTOMERSHAREAMT,
		AMED.HASWAGESUPPORTMOLEMPLOYEES,
        CASE
            WHEN APP.CUSTOMERTYPEID = 'IND' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END                                               AS CPR_NUMBER,
        CUS.NAMEEN                                      AS CUSTOMER_ENTERPRISE_NAME,
        CASE
            WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.APPROVEDON + INTERVAL '3' HOUR
        END                                               AS APPROVED_ON_DATE,
        CASE
            WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.STARTON AS DATE)
        END                                               AS CONTRACT_START_DATE,
        CASE
            WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.MONITORINGDUEDATE AS DATE)
        END                                               AS MONITORING_DUE_DATE,
        CASE
            WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE CAST(APP.ENDON AS DATE)
        END                                               AS CONTRACT_END_DATE,
        AMED.TKSHAREAMT                                 AS TOTAL_APPROVED_AMOUNT_TAMKEEN_SHARE,
        CASE
            WHEN AMED.CREATEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.CREATEDON + INTERVAL '3' HOUR
        END                                               AS CREATED_ON,
        CASE
            WHEN AMED.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE AMED.SUBMITTEDON + INTERVAL '3' HOUR
        END                                               AS SUBMITTED_ON,
        CASE
            WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
            ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL '3' HOUR
        END                                               AS SPENDING_PERIOD_END_DATE,
--        CASE
--            WHEN APP.APPROVALLETTERACCEPTEDON = TIMESTAMP '1900-01-01 00:00:00.000' THEN NULL
--            ELSE APP.APPROVALLETTERACCEPTEDON + INTERVAL '3' HOUR
--        END                                               AS APPROVAL_LETTER_CONFIRMED,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_AMENDMENTREQUEST AMED
        ON AMED.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION PROGVER
        ON APP.PROGRAMVERSIONID = PROGVER.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM PROGRAM
        ON PROGVER.PROGRAMID = PROGRAM.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL IND
        ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
        ON CUSPROF.CUSTOMERID = CMP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS APST
        ON AMED.AMENDMENTSTATUSID = APST.CODE
    WHERE APST.LABEL = 'Active'

),

RANKED_DATA AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY APPLICATION_ID
               ORDER BY CREATED_ON DESC
           ) AS RNK
    FROM COMBINED_APP

)

SELECT  DISTINCT
    PROGRAM_NAME,
    PROGRAM_TYPE,
    REFERENCE,
    APPLICATION_ID,
    APPLICATION_STATUS,
    AMENDMENTNO,
    UTILIZEDAMOUNT,
    UNUTILIZEDAMOUNT,
    TOTALAPPROVEDAMOUNT,
    TOTALAVAILABLEAMT,
    UTILIZEDAMT,
    UNUTILIZEDAMT,
    CUSTOMERSHAREAMT,
    HASWAGESUPPORTMOLEMPLOYEES,
    CPR_NUMBER,
    CUSTOMER_ENTERPRISE_NAME,
    CAST(APPROVED_ON_DATE AS DATE) AS APPROVED_ON_DATE,
    CAST(CONTRACT_START_DATE AS DATE) AS CONTRACT_START_DATE,
    CAST(MONITORING_DUE_DATE AS DATE) AS MONITORING_DUE_DATE,
    CAST(CONTRACT_END_DATE AS DATE) AS CONTRACT_END_DATE,
    TOTAL_APPROVED_AMOUNT_TAMKEEN_SHARE,
    CAST(CREATED_ON AS TIMESTAMP) AS CREATED_ON,
    CAST(SUBMITTED_ON AS TIMESTAMP) AS SUBMITTED_ON,
    CAST(SPENDING_PERIOD_END_DATE AS TIMESTAMP) AS SPENDING_PERIOD_END_DATE
   -- DBT_UPDATED_AT,
    --CASE WHEN RNK = 1 THEN 'Yes' ELSE 'No' END AS LATEST
FROM RANKED_DATA
