-- Optional wrapper:
-- CREATE TABLE silver.customer_enterprice_base_os2 AS

WITH FinancingProgram_cte AS (
    SELECT
        APP.ID,
        CASE
            WHEN CUS.CUSTOMERTYPEID = 'CMP'
            THEN UPPER(TRIM(CUS.NAMEEN))
            ELSE NULL
        END AS BANK_NAME,
        FIN.FINANCINGAMTREQUESTED                                                AS AMOUNT_REQUESTED,
        FIN.GRACEPERIOD                                                          AS GRACE_PERIOD_MONTHS,
        FINACEBREAK.MACHINERYANDEQUIPMENT                                        AS BREAKUP_FACILITY_MACHINERY_EQUIPMENT_BHD,
        FINACEBREAK.TECHNOLOGY                                                   AS BREAKUP_FACILITY_TECHNOLOGY_BHD,
        FINACEBREAK.MARKETINGANDBRANDING                                         AS BREAKUP_FACILITY_MARKETING_BRANDING_BHD,
        FINACEBREAK.FIXTURESANDFITTINGS                                          AS BREAKUP_FACILITY_FIXTURES_FITTINGS_BHD,
        FINACEBREAK.FACILITYBREAKUPOTHERAMOUNT                                   AS BREAKUP_FACILITY_OTHER_BHD,
        DISTYPE.LABEL                                                            AS DISBURSEMENT_TYPE,
        OPTI.LABEL                                                               AS IS_REVOLVING_FACILITY,
        FIN.AVAILABILITYPERIOD                                                   AS AVAILABILITY_PERIOD_MONTHS,
        PAYFREQ.LABEL                                                            AS REPAYMENT_PERIOD,
        PRTYPE.LABEL                                                             AS FINANCING_PRODUCT_TYPE,
        PROFIT.PROFITRATE * 100                                                  AS PROFIT_RATE_PCT,
        PROFITTYPE.LABEL                                                         AS PROFIT_RATE_TYPE,
        PROFIT.TOTALPROFIT                                                       AS TOTAL_PROFIT_BHD,
        PROFIT.TOTALPROFITRECALCULATED                                           AS TOTAL_PROFIT_AMOUNT_CALCULATED_BHD,
        FinType.LABEL                                                            AS FACILITY_TYPE,
        FIN.TENOR                                                                AS TENOR_MONTHS,
        FIN.BANKAPPROVALDATE                                                     AS APPROVED_ON_BANK
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APPSUP_FIN
            ON APP.ID = APPSUP_FIN.APPLICATIONID
            AND APPSUP_FIN.SUPPORTTYPEID = 'FIN'
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_FINANCING FIN
            ON FIN.ID = APPSUP_FIN.ID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APPSUP_PRO
            ON APP.ID = APPSUP_PRO.APPLICATIONID
            AND APPSUP_PRO.SUPPORTTYPEID = 'PRF'
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_PROFIT PROFIT
            ON PROFIT.APPLICATIONSUPPORTID = APPSUP_PRO.ID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_PROFITRATETYPE PROFITTYPE
            ON PROFITTYPE.ID = PROFIT.PROFITRATETYPEID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_FINANCINGBREAKUP FINACEBREAK
            ON FINACEBREAK.APPLICATIONSUPPORTID = APPSUP_FIN.ID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_DISBURSEMENTTYPE DISTYPE
            ON DISTYPE.ID = FIN.DISBURSEMENTTYPEID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_FINANCINGPRODUCTTYPE PRTYPE
            ON PRTYPE.ID = FIN.FINANCINGPRODUCTTYPEID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
            ON APP.ID = APPCUS.APPLICATIONID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
            ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
            ON CUSPROF.CUSTOMERID = CUS.ID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_FACILITYTYPE FinType
            ON FinType.ID = FIN.FACILITYTYPEID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_PAYMENTFREQUENCY PAYFREQ
            ON PAYFREQ.CODE = FIN.PAYMENTFREQUENCYID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_YESNOOPTION OPTI
            ON OPTI.ID = FIN.REVOLVINGLOAN
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT APPSUP_GUA
            ON APP.ID = APPSUP_GUA.APPLICATIONID
            AND APPSUP_GUA.SUPPORTTYPEID = 'GUA'
    WHERE CUSPROF.PROFILETYPEID = 'BNK'
),

CROwner_CTE AS (
    SELECT
        APPLICATIONID,
        CREATEDON,
        CRNUMBER,
        SUM(CASE WHEN GENDERID = 1 THEN 1 ELSE 0 END)                           AS TOTAL_MALE_SHAREHOLDERS,
        SUM(CASE WHEN GENDERID = 2 THEN 1 ELSE 0 END)                           AS TOTAL_FEMALE_SHAREHOLDERS,
        SUM(CASE WHEN NATIONALITY = 'BAHRAIN' THEN 1 ELSE 0 END)                AS TOTAL_BAHRAINI_SHAREHOLDERS,
        SUM(CASE WHEN NATIONALITY IN (
            'KUWAIT', 'SAUDI ARABIA', 'QATAR', 'OMAN', 'UNITED ARAB EMIRATES'
        ) THEN 1 ELSE 0 END)                                                     AS TOTAL_GCC_SHAREHOLDERS,
        SUM(CASE WHEN NATIONALITY NOT IN (
            'BAHRAIN', 'KUWAIT', 'SAUDI ARABIA', 'QATAR', 'OMAN',
            'UNITED ARAB EMIRATES', 'UNSPECIFIED', ''
        ) THEN 1 ELSE 0 END)                                                     AS TOTAL_NON_BAHRAINI_SHAREHOLDERS,
        SUM(CASE WHEN NATIONALITY IN ('', 'UNSPECIFIED') THEN 1 ELSE 0 END)     AS TOTAL_UNSPECIFIED_SHAREHOLDERS,
        ROW_NUMBER() OVER (
            PARTITION BY APPLICATIONID
            ORDER BY APPLICATIONID
        )                                                                         AS BATCH_NUMBER
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CROWNERSHIP
    GROUP BY
        APPLICATIONID,
        CREATEDON,
        CRNUMBER
),

Assessment_cte AS (
    SELECT
        act.NAME,
        AssessmentStatus.LABEL                                                   AS LABEL,
        ass.APPLICATIONID,
        ass.AMENDMENTREQUESTID,
        ROW_NUMBER() OVER (
            PARTITION BY ass.APPLICATIONID
            ORDER BY act.ID DESC
        )                                                                         AS RN
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ass
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS pro
            ON pro.TOP_PROCESS_ID = ass.PROCESSID
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
            ON act.PROCESS_ID = pro.ID
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS AssessmentStatus
            ON ass.ASSESSMENTSTATUSID = AssessmentStatus.CODE
),

Withdrawal_cte AS (
    SELECT
        withdraw.APPLICATIONID,
        withdrawstat.LABEL                                                        AS STATUS,
        act.CLOSED,
        ROW_NUMBER() OVER (
            PARTITION BY withdraw.APPLICATIONID
            ORDER BY act.ID DESC
        )                                                                         AS RN
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_WITHDRAWALREQUEST withdraw
        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_WITHDRAWALSTATUS withdrawstat
            ON withdrawstat.CODE = withdraw.STATUSID
        INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY act
            ON act.PROCESS_ID = withdraw.PROCESSID
)

SELECT DISTINCT
    APP.ID                                                                        AS APPLICATION_ID,
    APP.GUID                                                                      AS GUID,
    APP.REFERENCENUMBER                                                           AS APPLICATION_NO,
    APP.programversionid                                                          AS programversionid,
    APP.decisionprogramversionid                                                  AS decisionprogramversionid,
    APP.portaluserid                                                              AS portaluserid,
    APP.beneficiaryid                                                             AS beneficiaryid,
    APP.applicationstatusid                                                       AS applicationstatusid,
    APP.profilinginstanceguid                                                     AS profilinginstanceguid,
    APP.applicationinstanceformguid                                               AS applicationinstanceformguid,
    APP.applicationinstancedocguid                                                AS applicationinstancedocguid,
    APP.customerinstanceformguid                                                  AS customerinstanceformguid,
    APP.findatainstanceformguid                                                   AS findatainstanceformguid,
    APP.customerinstancedocguid                                                   AS customerinstancedocguid,
    APP.bindinginstancedocguid                                                    AS bindinginstancedocguid,
    APP.amendapprovalinstancedocguid                                              AS amendapprovalinstancedocguid,
    APP.hipoinstanceformguid                                                      AS hipoinstanceformguid,
    APP.analysisinstanceformguid                                                  AS analysisinstanceformguid,
    APP.ishipooptionid                                                            AS ishipooptionid,
    APP.programcap                                                                AS programcap,
    APP.programcapid                                                              AS programcapid,
    AppWFS.LABEL                                                                  AS WORKFLOW_STATUS,
    CASE WHEN APP.ISACTIVE THEN 'No' ELSE 'Yes' END                              AS IS_ACTIVE,
    APP.CREATEDON                                                                 AS CREATED_ON,
    CASE WHEN APP.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE APP.SUBMITTEDON + INTERVAL '3' HOUR END                  AS SUBMITTED_ON,
    CASE WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE APP.APPROVEDON + INTERVAL '3' HOUR END                   AS APPROVED_ON,
    CASE WHEN APP.CUSTOMERTYPEID = 'CMP'
         THEN 'Enterprise' ELSE 'Individual' END                                 AS CUSTOMER_TYPE,
    APP.APPLICATIONCAP                                                            AS CAP,
    APP.TKSHAREAMT                                                                AS TKSHARE_APPROVED,
    FinancingProgram.AMOUNT_REQUESTED                                             AS AMOUNT_REQUESTED,
    APP.APPLICATIONCAPUNUTILIZED                                                  AS REMAINING,
    APP.CUSTOMERSHAREAMT                                                          AS CUSTOMER_SHARE,
    APP.TOTALCOSTWVAT                                                             AS TOTAL_COST,
    CASE WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.STARTON AS DATE) END                            AS START_DATE,
    CASE WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.ENDON AS DATE) END                              AS END_DATE,
    APP.DURATION                                                                  AS CONTRACT_DURATION_MONTHS,
    CASE
        WHEN APP.ISHIPOOPTIONID = 1 THEN 'HiPo'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Non-HiPo'
        ELSE NULL
    END                                                                           AS IS_HIPO,
    CASE WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.MONITORINGDUEDATE AS DATE) END                  AS MONITORING_DUE_DATE,
    CASE WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.SPENDINGPERIODDUEDATE AS DATE) END              AS SPENDING_PERIOD_END_DATE,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN UPPER(TRIM(CUS.NAMEEN)) ELSE NULL END                              AS COMMERCIAL_NAME_EN,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN UPPER(TRIM(CUS.NAMEAR)) ELSE NULL END                              AS COMMERCIAL_NAME_AR,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN TRIM(CMP.CODE) ELSE NULL END                                       AS CR_LICENSE_NO,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN TRIM(CMP.MAINCODE) ELSE NULL END                                   AS CR_LICENSE_NO_MAIN,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN CAST(CMP.REGISTRATIONDATE AS DATE) ELSE NULL END                   AS REGISTRATION_DATE,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
        THEN CASE
            WHEN CMP.COMPANYIDTYPEID = 1 THEN 'CR'
            WHEN CMP.COMPANYIDTYPEID = 2 THEN 'License'
        END
        ELSE NULL
    END                                                                           AS CR_LICENSE_TYPE,
    UPPER(TRIM(PORTUSR.NAME))                                                    AS PORTAL_USER_NAME,
    LOWER(TRIM(PORTUSR.EMAIL))                                                   AS EMAIL,
    CONCAT(PORTUSR.MOBILECOUNTRYPREFIX, ' ', PORTUSR.MOBILEPHONE)               AS MOBILE_NO,
    TRIM(ProgVer.COMMERCIALNAME_EN)                                              AS PROGRAM_NAME,
    CROwner.TOTAL_MALE_SHAREHOLDERS,
    CROwner.TOTAL_FEMALE_SHAREHOLDERS,
    CROwner.TOTAL_BAHRAINI_SHAREHOLDERS,
    CROwner.TOTAL_GCC_SHAREHOLDERS,
    CROwner.TOTAL_NON_BAHRAINI_SHAREHOLDERS,
    CROwner.TOTAL_UNSPECIFIED_SHAREHOLDERS,
    FinancingProgram.TENOR_MONTHS                                                AS FINANCING_TENOR,
    FinancingProgram.APPROVED_ON_BANK,
    Assessment.LABEL                                                             AS WORKFLOW_STATUS_DETAILED,
    CASE
        WHEN APP.ISELIGIBLEOPTIONID = 1 THEN 'Eligible'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Not Eligible'
        ELSE NULL
    END                                                                           AS IS_ELIGIBLE,
    CASE WHEN Withdrawal.STATUS = 'Accepted'
         THEN Withdrawal.CLOSED + INTERVAL '3' HOUR ELSE NULL END               AS WITHDRAWN_ON,
    CASE WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE APP.STARTON + INTERVAL '3' HOUR END                     AS CONFIRMED_ON,
    MEMDES.LABEL                                                                 AS CONTACT_DESIGNATION,
    FinancingProgram.BANK_NAME,
    FinancingProgram.GRACE_PERIOD_MONTHS,
    FinancingProgram.BREAKUP_FACILITY_MACHINERY_EQUIPMENT_BHD,
    FinancingProgram.BREAKUP_FACILITY_TECHNOLOGY_BHD,
    FinancingProgram.BREAKUP_FACILITY_MARKETING_BRANDING_BHD,
    FinancingProgram.BREAKUP_FACILITY_FIXTURES_FITTINGS_BHD,
    FinancingProgram.BREAKUP_FACILITY_OTHER_BHD,
    FinancingProgram.DISBURSEMENT_TYPE,
    FinancingProgram.IS_REVOLVING_FACILITY,
    FinancingProgram.AVAILABILITY_PERIOD_MONTHS,
    FinancingProgram.REPAYMENT_PERIOD,
    FinancingProgram.FINANCING_PRODUCT_TYPE,
    FinancingProgram.PROFIT_RATE_PCT,
    FinancingProgram.PROFIT_RATE_TYPE,
    FinancingProgram.TOTAL_PROFIT_BHD,
    FinancingProgram.TOTAL_PROFIT_AMOUNT_CALCULATED_BHD,
    FALSE as IS_DELETED,
    'dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze"'                                                                       AS SOURCE_SYSTEM_NAME,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS TIMESTAMP)                      AS DBT_UPDATED_AT

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS AppWFS
        ON AppWFS.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APP.ID = APPCUS.APPLICATIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCONTACTDETAILS APPCON
        ON APPCON.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_MEMBERDESIGNATION MEMDES
        ON MEMDES.CODE = APPCON.PRIMARYMEMBERDESIGNATIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
        ON CUS.ID = CMP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_PORTALUSER PORTUSR
        ON PORTUSR.ID = APP.PORTALUSERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION ProgVer
        ON ProgVer.ID = APP.PROGRAMVERSIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT AppSupp
        ON AppSupp.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_FINANCING Fin
        ON Fin.ID = AppSupp.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_FACILITYTYPE FinType
        ON FinType.ID = Fin.FACILITYTYPEID
    LEFT JOIN CROwner_CTE CROwner
        ON CROwner.APPLICATIONID = APP.ID
    LEFT JOIN Assessment_cte Assessment
        ON Assessment.APPLICATIONID = APP.ID
        AND (Assessment.RN = 1 OR Assessment.RN IS NULL)
    LEFT JOIN Withdrawal_cte Withdrawal
        ON Withdrawal.APPLICATIONID = APP.ID
        AND Withdrawal.RN = 1
        AND Withdrawal.STATUS <> 'Draft'
    LEFT JOIN FinancingProgram_cte FinancingProgram
        ON FinancingProgram.ID = APP.ID

WHERE APP.CUSTOMERTYPEID = 'CMP'
  AND APP.APPLICATIONSTATUSID <> 'PM'

ORDER BY APP.ID
