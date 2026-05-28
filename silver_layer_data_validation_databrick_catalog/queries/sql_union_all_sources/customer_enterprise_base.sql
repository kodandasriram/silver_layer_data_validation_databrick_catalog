-- Compare bronze-layer query output with silver-layer table output for customer_enterprise_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\union of os2_os1_mis\customer_enterprise_base_os2_os1_mis_union_bronze_layer.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\customer_enterprise_base_silver_layer.sql

WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for customer_enterprise_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: customer_enterprise_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH customer_enterprise_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================
Name        : APPLICATION_FINANCING_BASE_OS2
Description : This model extracts and transforms application and financing-related attributes
              from the NEO2 (OS2) source system Bronze Layer and loads into the
              OSUSR_NTP_APPLICATION target table as part of the Silver Layer
              data pipeline.
Source Tables : neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_398_APPLICATIONSTATUS
                neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                neo2.OSUSR_NTP_APPLICATIONCONTACTDETAILS
                neo2.OSUSR_NTP_MEMBERDESIGNATION
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_COMPANY
                neo2.OSUSR_QM6_PORTALUSER
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_2DA_APPLICATIONSUPPORT
                neo2.OSUSR_2DA_FINANCING
                neo2.OSUSR_H95_FACILITYTYPE
                neo2.OSUSR_MYA_MOIC_CROWNERSHIP
                neo2.OSUSR_1AT_ASSESSMENT
                neo2.OSUSR_1AT_ASSESSMENTSTATUS
                neo2.OSSYS_BPM_PROCESS
                neo2.OSSYS_BPM_ACTIVITY
                neo2.OSUSR_NTP_WITHDRAWALREQUEST
                neo2.OSUSR_NTP_WITHDRAWALSTATUS
                neo2.OSUSR_2DA_PROFIT
                neo2.OSUSR_H95_PROFITRATETYPE
                neo2.OSUSR_2DA_FINANCINGBREAKUP
                neo2.OSUSR_2DA_DISBURSEMENTTYPE
                neo2.OSUSR_2DA_FINANCINGPRODUCTTYPE
                neo2.OSUSR_H95_PAYMENTFREQUENCY
                neo2.OSUSR_398_YESNOOPTION
Target Table : OSUSR_NTP_APPLICATION_FINANCING
Load Type    : Full Load
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily
Revision History:
--------------------------------------------------------------
Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-05-12 |   Kaviya       | Initial version
================================================================================================= 
*/
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
, cte_base as (
SELECT DISTINCT
    APP.ID                                                                        AS application_id,
    APP.GUID                                                                      AS guid,
    APP.REFERENCENUMBER                                                           AS application_no,
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
    AppWFS.LABEL                                                                  AS workflow_status,
    CASE WHEN APP.ISACTIVE THEN 'No' ELSE 'Yes' END                              AS is_active,
    APP.CREATEDON                                                                 AS created_on,
    CASE WHEN APP.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE APP.SUBMITTEDON + INTERVAL '3' HOUR END                  AS submitted_on,
    CASE WHEN APP.APPROVEDON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE APP.APPROVEDON + INTERVAL '3' HOUR END                   AS approved_on,
    CASE WHEN APP.CUSTOMERTYPEID = 'CMP'
         THEN 'Enterprise' ELSE 'Individual' END                                 AS customer_type,
    APP.APPLICATIONCAP                                                            AS cap,
    APP.TKSHAREAMT                                                                AS tkshare_approved,
    FinancingProgram.AMOUNT_REQUESTED                                             AS amount_requested,
    APP.APPLICATIONCAPUNUTILIZED                                                  AS remaining,
    APP.CUSTOMERSHAREAMT                                                          AS customer_share,
    APP.TOTALCOSTWVAT                                                             AS total_cost,
    CASE WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.STARTON AS DATE) END                            AS start_date,
    CASE WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.ENDON AS DATE) END                              AS end_date,
    APP.DURATION                                                                  AS contract_duration_months,
    CASE
        WHEN APP.ISHIPOOPTIONID = 1 THEN 'HiPo'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Non-HiPo'
        ELSE NULL
    END                                                                           AS is_hipo,
    CASE WHEN APP.MONITORINGDUEDATE = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.MONITORINGDUEDATE AS DATE) END                  AS monitoring_due_date,
    CASE WHEN APP.SPENDINGPERIODDUEDATE = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.SPENDINGPERIODDUEDATE AS DATE) END              AS spending_period_end_date,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN UPPER(TRIM(CUS.NAMEEN)) ELSE NULL END                              AS commercial_name_en,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN UPPER(TRIM(CUS.NAMEAR)) ELSE NULL END                              AS commercial_name_ar,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN TRIM(CMP.CODE) ELSE NULL END                                       AS cr_license_no,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN TRIM(CMP.MAINCODE) ELSE NULL END                                   AS cr_license_no_main,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
         THEN CAST(CMP.REGISTRATIONDATE AS DATE) ELSE NULL END                   AS registration_date,
    CASE WHEN CUS.CUSTOMERTYPEID = 'CMP'
        THEN CASE
            WHEN CMP.COMPANYIDTYPEID = 1 THEN 'CR'
            WHEN CMP.COMPANYIDTYPEID = 2 THEN 'License'
        END
        ELSE NULL
    END                                                                           AS cr_license_type,
    UPPER(TRIM(PORTUSR.NAME))                                                    AS portal_user_name,
    LOWER(TRIM(PORTUSR.EMAIL))                                                   AS email,
    CONCAT(PORTUSR.MOBILECOUNTRYPREFIX, ' ', PORTUSR.MOBILEPHONE)               AS mobile_no,
    TRIM(ProgVer.COMMERCIALNAME_EN)                                              AS program_name,
    CROwner.total_male_shareholders,
    CROwner.total_female_shareholders,
    CROwner.total_bahraini_shareholders,
    CROwner.total_gcc_shareholders,
    CROwner.total_non_bahraini_shareholders,
    CROwner.total_unspecified_shareholders,
    FinancingProgram.TENOR_MONTHS                                                AS financing_tenor,
    FinancingProgram.approved_on_bank,
    Assessment.LABEL                                                             AS workflow_status_detailed,
    CASE
        WHEN APP.ISELIGIBLEOPTIONID = 1 THEN 'Eligible'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Not Eligible'
        ELSE NULL
    END                                                                           AS is_eligible,
    CASE WHEN Withdrawal.STATUS = 'Accepted'
         THEN Withdrawal.CLOSED + INTERVAL '3' HOUR ELSE NULL END               AS withdrawn_on,
    CASE WHEN APP.STARTON = TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE APP.STARTON + INTERVAL '3' HOUR END                     AS confirmed_on,
    MEMDES.LABEL                                                                 AS contact_designation,
    FinancingProgram.bank_name,
    FinancingProgram.grace_period_months,
    FinancingProgram.breakup_facility_machinery_equipment_bhd,
    FinancingProgram.breakup_facility_technology_bhd,
    FinancingProgram.breakup_facility_marketing_branding_bhd,
    FinancingProgram.breakup_facility_fixtures_fittings_bhd,
    FinancingProgram.breakup_facility_other_bhd,
    FinancingProgram.disbursement_type,
    FinancingProgram.is_revolving_facility,
    FinancingProgram.availability_period_months,
    FinancingProgram.repayment_period,
    FinancingProgram.financing_product_type,
    FinancingProgram.profit_rate_pct,
    FinancingProgram.profit_rate_type,
    FinancingProgram.total_profit_bhd,
    FinancingProgram.total_profit_amount_calculated_bhd,
    CAST (NULL AS VARCHAR) AS finalapprovedtkshareamt,
    FALSE as is_deleted, 
    'NEO2'                                                                       AS source_system_name,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS TIMESTAMP)                      AS dbt_updated_at,
    APP.createdon,
    APP.updatedon,
    ROW_NUMBER() OVER (PARTITION BY APP.ID ORDER BY APP.UPDATEDON DESC NULLS LAST, APP.CREATEDON DESC NULLS LAST) AS rnk

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

)
SELECT
    application_id,
    guid,
    application_no,
    programversionid,
    decisionprogramversionid,
    portaluserid,
    beneficiaryid,
    applicationstatusid,
    profilinginstanceguid,
    applicationinstanceformguid,
    applicationinstancedocguid,
    customerinstanceformguid,
    findatainstanceformguid,
    customerinstancedocguid,
    bindinginstancedocguid,
    amendapprovalinstancedocguid,
    hipoinstanceformguid,
    analysisinstanceformguid,
    ishipooptionid,
    programcap,
    programcapid,
    workflow_status,
    is_active,
    created_on,
    submitted_on,
    approved_on,
    customer_type,
    cap,
    tkshare_approved,
    amount_requested,
    remaining,
    customer_share,
    total_cost,
    start_date,
    end_date,
    contract_duration_months,
    is_hipo,
    monitoring_due_date,
    spending_period_end_date,
    commercial_name_en,
    commercial_name_ar,
    cr_license_no,
    cr_license_no_main,
    registration_date,
    cr_license_type,
    portal_user_name,
    email,
    mobile_no,
    program_name,
    total_male_shareholders,
    total_female_shareholders,
    total_bahraini_shareholders,
    total_gcc_shareholders,
    total_non_bahraini_shareholders,
    total_unspecified_shareholders,
    financing_tenor,
    approved_on_bank,
    workflow_status_detailed,
    is_eligible,
    withdrawn_on,
    confirmed_on,
    contact_designation,
    bank_name,
    grace_period_months,
    breakup_facility_machinery_equipment_bhd,
    breakup_facility_technology_bhd,
    breakup_facility_marketing_branding_bhd,
    breakup_facility_fixtures_fittings_bhd,
    breakup_facility_other_bhd,
    disbursement_type,
    is_revolving_facility,
    availability_period_months,
    repayment_period,
    financing_product_type,
    profit_rate_pct,
    profit_rate_type,
    total_profit_bhd,
    total_profit_amount_calculated_bhd,
    finalapprovedtkshareamt,
    is_deleted,
    source_system_name,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS VARCHAR), '') AS TIMESTAMP) AS dbt_updated_at,
    TRY_CAST(NULLIF(CAST(CREATEDON AS VARCHAR), '') AS TIMESTAMP) AS createdon,
    TRY_CAST(NULLIF(CAST(updatedon AS VARCHAR), '') AS TIMESTAMP) AS updatedon
from cte_base app
where rnk=1
),
customer_enterprise_base_os1_source AS (
/*
============================================================================
silver_customer_enterprise_os1.sql
============================================================================
Per-source intermediate Silver model for the Customer Enterprise domain â€” OS1 only.

Architectural note:
  In OS1, "Customer Enterprise" is NOT a separate entity (unlike MIS where
  it has its own table tmkn_company). Instead, it's a FILTERED PROJECTION
  of OSUSR_PX1_APPLICATION focused on enterprise programs (i.e., programs
  other than Train Me / individual programs).

  RPT-168 builds a wide enterprise-application view by:
    1. Filtering applications: WHERE prog.ID <> 3 (excludes Train Me)
    2. Joining application support details (RM, Assessor, Approver scoring,
       financing, grant amounts, banks, HiPo classifications, etc.)
    3. Pulling ~30 enterprise-specific fields from the 5W2 questionnaire
       system (CR registration date, expiration, company type, investment
       capital, shareholder counts, address fields, etc.)
    4. Pulling rejection reasons via ROW_NUMBER pivot (Reasons 1, 2, 3)

This Silver model captures items 1, 2, and 4 (the non-EAV portions). The
EAV-derived enterprise fields (item 3) are NOT pre-pivoted here â€” they
are available via silver_application_questions_os1.sql at Gold/AGG.

Why this split is correct:
  - The EAV pivots in RPT-168 use ~30 different EXTERNALID lists, each
    containing 7-12 alternate IDs (one per applicable program type). This
    is genuinely a report-level concern: which EXTERNALID list is "Bahraini
    Shareholders count" depends on which application type the row is for.
  - Pre-pivoting all 30 fields here would make this Silver model very wide
    (~30 EAV-derived columns) AND would couple this Silver to a specific
    set of program-type EXTERNALID conventions that may change.
  - Per the agreed Silver design (Option A), questionnaire data lives in
    silver_application_questions_os1.sql in EAV form. Gold/AGG layer reads
    both this domain Silver and the questions Silver, then assembles the
    full report.

Sources (non-EAV portion):
  â˜… OSUSR_PX1_APPLICATION                          â†’ anchor (filtered to
                                                       enterprise programs)
    OSUSR_PX1_APPLICATIONSUPPORTDETAILS            â†’ 1:1 financial/support
    OSUSR_PX1_APPLICATIONSYB                       â†’ 1:1 SYB-specific
    OSUSR_PX1_APPLICATIONINTERNALSTATUSUPDATES21   â†’ many:1 â€” latest internal
                                                       status (CTE)
    OSUSR_5W2_APPROVERREJECTIONREMARKS             â†’ many:1 â€” rejection
                                                       reasons (CTE)
    ossys_User (Ã—3)                                â†’ RM, Assessor, Approver

  Lookup tables joined inline:
    OSUSR_PX1_PROGRAM, OSUSR_PX1_PROGRAMTYPE,
    OSUSR_PX1_APPLICATIONSTATUS, OSUSR_PX1_APPLICANTTYPE,
    OSUSR_PX1_TURNOVERVALUE, OSUSR_PX1_SECTOR,
    OSUSR_PX1_BANKNAMES, OSUSR_PX1_HIGHPOTENTIALCLASSIFICATION (Ã—3),
    OSUSR_PX1_SUPPORTCAPDYNAMIC21,
    OSUSR_PX1_APPLICATIONLOANSTATUS,
    OSUSR_PX1_APPLICATIONINTERNALSTATUSES21

Reference SP:
  - RPT-168_neoTamkeen_Enterprise_Applications

Cross-domain note on rejection reasons:
  RPT-168 uses the same ROW_NUMBER + 3-column pivot as RPT-163. Mirrored
  here as a LISTAGG into a pipe-delimited string, same approach as in
  silver_customer_individual_employee_os1. Downstream Gold can re-pivot
  to top-N if needed.

Cross-domain note on duplicate scoring fields:
  Many of these scoring fields (RECOMMENDEDGRANTBYRM, etc.) are also
  surfaced on silver_application_os1.sql. They appear here too because
  RPT-168 surfaces them with enterprise-specific aliasing and for
  completeness of the enterprise view. In the unified Silver layer
  downstream, deduplication should be addressed.

Filter applied:
  WHERE prog.ID <> 3 â€” mirrors RPT-168's exclusion of Train Me. This IS
  applied in Silver because it defines the entity scope (enterprise vs
  non-enterprise applications). Confirm with team.

Cross-domain note on Youth Bahraini shareholders:
  RPT-168 has a complex CPR-validate + DOB-from-CPR + age-gate calculation
  that depends on dbo.fn_Validate_CPR (UDF) and SUBSTRING-based DOB
  derivation. Same pattern flagged in silver_customer_enterprise_mis.sql.
  Implemented as NULL with TODO until the team builds a validate_cpr +
  extract_dob_from_cpr macro pair.
============================================================================
*/


-- ============================================================================
-- Latest internal status per application
-- ============================================================================
-- WITH latest_internal_status AS (
--     SELECT
--         application_id,
--         internal_status_id
--     FROM (
--         SELECT
--             APPLICATIONID                                   AS application_id,
--             STATUS                                          AS internal_status_id,
--             ROW_NUMBER() OVER (
--                 PARTITION BY APPLICATIONID
--                 ORDER BY CREATEDON DESC
--             ) AS rn
--         FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONINTERNALSTATUSUPDATES21
--     ) ranked
--     WHERE rn = 1
-- ),


-- ============================================================================
-- All rejection reasons per application, joined as a pipe-delimited string
-- ============================================================================
-- rejection_reasons_agg AS (
--     SELECT
--         AppRejRmk.APPLICATIONID                                          AS application_id,
--         LISTAGG(RejRsn.LABEL, ' | ')
--             WITHIN GROUP (ORDER BY RejRsn.ID)                            AS rejection_reasons,
--         LISTAGG(RejRsn.MESSAGE, ' | ')
--             WITHIN GROUP (ORDER BY RejRsn.ID)                            AS rejection_messages,
--         COUNT(*)                                                          AS rejection_reason_count
--     FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5W2_APPROVERREJECTIONREMARKS AppRejRmk
--     LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5W2_APPROVERREJECTIONREMARKSREASONS AppRejRmkRsn
--            ON AppRejRmkRsn.APPROVERREJECTIONREMARKSID = AppRejRmk.ID
--     LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5W2_REJECTREASONS RejRsn
--            ON RejRsn.ID = AppRejRmkRsn.APPROVERREJECTIONREASONID
--     WHERE RejRsn.LABEL IS NOT NULL
--     GROUP BY AppRejRmk.APPLICATIONID
-- )


SELECT
    'OSUSR_PX1_APPLICATION_ENTERPRISE' AS os1_source_table,

    -- Identifiers
    app.ID                                                                AS application_id,
    app.IDENTIFIER                                                        AS application_no,
    app.CR                                                                AS cr_license_no,
    app.COMMERCIALNAME                                                    AS commercial_name,

    -- Foreign keys preserved for cross-domain re-joining
    app.PROGRAMID                                                         AS program_id,
    app.APPLICATIONSTATUSID                                               AS application_status_id,
    app.APPLICATIONLOANSTATUSID                                           AS application_loan_status_id,
    app.RMID                                                              AS rm_user_id,
    app.ASSESSORID                                                        AS assessor_user_id,
    app.APPROVERID                                                        AS approver_user_id,
    app.USERID                                                            AS customer_user_id,

    -- Decoded labels
    prog.PROGRAMNAME                                                      AS program_name,
    prog_typ.LABEL                                                        AS program_type_name,
    --typ.LABEL                                                             AS applicant_type,
    CAST(NULL AS VARCHAR) AS applicant_type,
    stus.LABEL                                                            AS workflow_status,
    --loanStat.LABEL                                                        AS financing_loan_status,
    --Int_ST.LABEL                                                          AS internal_status,
    --sec.SECTOR                                                            AS sector,
    --trnovr.LABEL                                                          AS turnover,
    CAST(NULL AS VARCHAR) AS financing_loan_status,
    CAST(NULL AS VARCHAR) AS internal_status,
    CAST(NULL AS VARCHAR) AS sector,
    CAST(NULL AS VARCHAR) AS turnover,
    bank.LABEL                                                            AS bank_name,

    -- HiPo classification (per role: RM, Assessor, Approver)
    HPC_RM.LABEL                                                          AS hipo_classification_rm,
    HPC_ASSESSO.LABEL                                                     AS hipo_classification_assessor,
    HPC_APPROVE.LABEL                                                     AS hipo_classification_approver,

    -- Application sector / objective (HiPo dimension)
    --SupCapDyn.TITLE                                                       AS application_sector_objective,
    CAST(NULL AS VARCHAR) AS application_sector_objective,

    -- People (denormalised user names)
    RM.NAME                                                               AS rm_name,
    Assessor.NAME                                                         AS assessor_name,
    Approver.NAME                                                         AS approver_name,

    -- Application timeline (sentinel-1900 nulled)
    app.CREATEDON                                                         AS created_on,
    CASE WHEN app.APPROVEDON                  = DATE '1900-01-01' THEN NULL ELSE app.APPROVEDON                  END  AS approved_on,
    CASE WHEN app.CONTRACTSTARTDATE           = DATE '1900-01-01' THEN NULL ELSE app.CONTRACTSTARTDATE           END  AS contract_start_date,
    CASE WHEN app.CONTRACTENDDATE             = DATE '1900-01-01' THEN NULL ELSE app.CONTRACTENDDATE             END  AS contract_end_date,
    CASE WHEN app.DATEAPPROVEDPENDINGCUSTOMER = DATE '1900-01-01' THEN NULL ELSE app.DATEAPPROVEDPENDINGCUSTOMER END  AS approved_pending_customer_on,
    CASE WHEN app.SAVEDON                     = DATE '1900-01-01' THEN NULL ELSE app.SAVEDON                     END  AS saved_on,
    CASE WHEN app.SPENDINGPERIODENDDATE       = DATE '1900-01-01' THEN NULL ELSE app.SPENDINGPERIODENDDATE       END  AS spending_period_end_date,

    -- Support details (caps, financing, grant)
    detls.PROGRAMSUPPORTCAP                                               AS program_support_cap,
    detls.FINANCINGCAP                                                    AS financing_cap,
    detls.FINANCINGGUARANTEECAP                                           AS financing_guarantee_cap,

    -- Recommended grant by role
    detls.RECOMMENDEDGRANTBYRM                                            AS recommended_grant_rm,
    detls.RECOMMENDEDGRANTBYASSESSOR                                      AS recommended_grant_assessor,

    -- Assessment support cap by role
    detls.ASSESSMENTSUPPORTCAPBYRM                                        AS assessment_support_cap_rm,
    detls.ASSESSMENTSUPPORTCAPASSESSOR                                    AS assessment_support_cap_assessor,
    detls.ASSESSMENTSUPPORTCAPAPPROVER                                    AS assessment_support_cap_approver,

    -- Approved amounts
    detls.APPROVEDGRANT                                                   AS approved_grant,
    detls.APPROVEDGRANT_MAXIMUM                                           AS approved_grant_maximum,
    detls.APPROVEDFINANCINGAMOUNT                                         AS approved_financing_amount,
    detls.APPROVEDGUARANTEEAMOUNT                                         AS approved_guarantee_amount,

    -- Requested amounts
    detls.TOTALREQUESTED                                                  AS total_requested_grant,
    detls.TOTALFINANCINGAMTREQUESTED                                      AS total_requested_financing,
    detls.CONSUMEDAMOUNT                                                  AS consumed_amount,
    detls.REMAININGAMOUNT                                                 AS remaining_amount,

    -- Recommended financing by role
    detls.RECOMMENDEDFINANCINGBYRM                                        AS recommended_financing_rm,
    detls.RECOMMENDEDFINANCINGBYASSESS                                    AS recommended_financing_assessor,
    detls.RECOMMENDEDFINANCINGBYAPPR                                      AS recommended_financing_approver,

    -- Loan terms
    detls.LOANTENOR                                                       AS loan_tenor,
    detls.INTERESTRATE                                                    AS loan_profit_rate,
    detls.TOTALINTERESTAMOUNT                                             AS loan_total_profit_amount,
    detls.MONTHLYINSTALLMENT                                              AS loan_monthly_installment,
    detls.GRACEPERIOD                                                     AS loan_grace_period,
    detls.LOANSTARTDATE                                                   AS loan_start_date,
    detls.LOANENDDATE                                                     AS loan_end_date,

    -- Workforce / employees
    detls.TOTALNUMBEREMPLOYEES                                            AS requested_support_no_of_employees,

    -- Remarks
    detls.RMREMARKS                                                       AS remarks_rm,
    detls.ASSESSORREMARKS                                                 AS remarks_assessor,
    detls.APPROVERREMARKS                                                 AS remarks_approver,

    -- Rejection reasons (preserved as pipe-delimited string)
    --rej.rejection_reasons                                                 AS rejection_reasons,
    --rej.rejection_messages                                                AS rejection_messages,
    --rej.rejection_reason_count                                            AS rejection_reason_count,

    CAST(NULL AS VARCHAR) AS rejection_reasons,
    CAST(NULL AS VARCHAR) AS rejection_messages,
    CAST(NULL AS VARCHAR) AS rejection_reason_count,

    -- Youth Bahraini shareholders count
    -- TODO: implement validate_cpr + extract_dob_from_cpr macro pair.
    -- The original RPT-168 logic uses dbo.fn_Validate_CPR + SUBSTRING-based
    -- DOB derivation from CPR digits, then age-gate to >=35 years from
    -- shareholder list-entry rows where VALUE3 = 'BAHRAIN'. This needs
    -- the column can be calculated correctly.
    -- Same TODO as silver_customer_enterprise_mis.sql.
    CAST(NULL AS INTEGER)                                                 AS youth_bahraini_shareholders_count,

    -- Standard trailing audit columns
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATION app
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_PROGRAM                        prog       ON prog.ID       = app.PROGRAMID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_PROGRAMTYPE                    prog_typ   ON prog_typ.ID   = prog.PROGRAMTYPEID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONSTATUS              stus       ON stus.ID       = app.APPLICATIONSTATUSID
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICANTTYPE                  typ        ON typ.ID        = prog.APPLICANTTYPE

-- SYB / sector / turnover (1:1 with application)
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONSYB                 p          ON p.ID          = app.ID
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_TURNOVERVALUE                  trnovr     ON trnovr.ID     = p.TURNOVERVALUEID
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_SECTOR                         sec        ON sec.ID        = p.SECTORID

-- Loan status + latest internal status
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONLOANSTATUS          loanStat   ON loanStat.ID   = app.APPLICATIONLOANSTATUSID
--LEFT JOIN latest_internal_status                                                  lis        ON lis.application_id = app.ID
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONINTERNALSTATUSES21  Int_ST     ON Int_ST.ID     = lis.internal_status_id

-- Application support details + HiPo classifications
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_APPLICATIONSUPPORTDETAILS      detls      ON detls.APPLICATIONID = app.ID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_HIGHPOTENTIALCLASSIFICATION    HPC_RM      ON HPC_RM.ID       = detls.HIPOCLASSIFICATIONID_RM
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_HIGHPOTENTIALCLASSIFICATION    HPC_ASSESSO ON HPC_ASSESSO.ID  = detls.HIPOCLASSIFICATIONID_ASSESSO
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_HIGHPOTENTIALCLASSIFICATION    HPC_APPROVE ON HPC_APPROVE.ID  = detls.HIPOCLASSIFICATIONID_APPROVE
--LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_SUPPORTCAPDYNAMIC21            SupCapDyn   ON SupCapDyn.ID    = detls.SUPPORTCAPDYNAMICID

-- People
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER                               Approver    ON Approver.ID     = app.APPROVERID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER                               Assessor    ON Assessor.ID     = app.ASSESSORID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER                               RM          ON RM.ID           = app.RMID
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_PX1_BANKNAMES                      bank        ON bank.ID         = detls.BANKID

-- Rejection reasons CTE
--LEFT JOIN rejection_reasons_agg                                                   rej         ON rej.application_id  = app.ID

-- Filter: enterprise programs only (excludes Train Me, ID = 3)
WHERE prog.ID <> 3
),
customer_enterprise_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR) AS option_key,
        max(sm.value) AS option_value
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".STRINGMAP sm
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR)
),
option_set_map AS (
    SELECT map_agg(option_key, option_value) AS option_values
    FROM option_set_values
),
/*
============================================================================
silver_customer_enterprise_mis.sql
============================================================================
Per-source intermediate Silver model for the Customer Enterprise domain â€” MIS only.

Sources (Customer Enterprise domain entities):
  â˜… tmkn_company                                  â€” anchor: enterprise/company entity
    tmkn_shareholderspartners                     â€” child: shareholders & partners
    tmkn_tmkn_company_tmkn_businessactivity       â€” M2M bridge: company â†” activities
    tmkn_businessactivity                         â€” lookup: business activity names

Reference SP: RPT-029_Company (most comprehensive enterprise view)

The Customer Enterprise domain uses tmkn_company as its anchor â€” 17 SPs join
TO tmkn_company. RPT-029 is the canonical company-level report, which:
  - Anchors on tmkn_company
  - Aggregates business activities via M2M bridge (LISTAGG of activity names)
  - Aggregates shareholder counts and ownership percentages by nationality
  - Flags Bahrain Government as shareholder
  - Joins iban for the company's primary IBAN

Filters applied (mirroring RPT-029):
  - WHERE c.statecode = 0       (active companies only)
  - AND c.tmkn_MainRecord IS NULL  (exclude duplicate/merged company records)

Cross-domain note: tmkn_iban is NOT joined here. IBAN is its own Silver
domain (silver_iban_mis); the iban_id FK is preserved on company rows for
downstream re-joining at unified Silver / Gold level. This avoids
duplicating IBAN data across multiple domain Silver models.

The UDF dbo.fn_Validate_CPR is used in RPT-029 for "Youth Bahraini
shareholders" calculation. That logic is preserved as a comment below
but commented out â€” it should be implemented as a DBT macro
( validate_cpr() ) before the column is uncommented.
============================================================================
*/


-- ============================================================================
-- Pre-aggregate business activities via M2M bridge (LISTAGG replaces STUFF)
-- ============================================================================
company_business_activities AS (
    SELECT
        comact.tmkn_companyid                                                AS company_id,
        COUNT(*)                                                             AS activity_count,
        LISTAGG(act.tmkn_name,                                       ' | ')
            WITHIN GROUP (ORDER BY act.tmkn_name)                            AS isic4_codes,
        LISTAGG(act.tmkn_businessactivitynameenglish,                ' | ')
            WITHIN GROUP (ORDER BY act.tmkn_businessactivitynameenglish)     AS activities
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_TMKN_COMPANY_TMKN_BUSINESSACTIVITYBASE comact
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_BUSINESSACTIVITYBASE act
           ON act.tmkn_businessactivityid = comact.tmkn_businessactivityid
          AND act.statecode = 0
    GROUP BY comact.tmkn_companyid
),


-- ============================================================================
-- Shareholders aggregation: nationality & gender breakdowns
-- ============================================================================
shareholders_agg AS (
    SELECT
        share.tmkn_company                                                   AS company_id,

        -- Nationality counts
        SUM(CASE
            WHEN share.tmkn_nationality = '753939C9-D64E-E411-942E-E61F13D0BD43'  -- Bahrain
            THEN 1 ELSE 0 END)                                               AS bahraini_shareholders_count,
        SUM(CASE
            WHEN share.tmkn_nationality IN (
                'CF3A39C9-D64E-E411-942E-E61F13D0BD43',  -- Saudi Arabia
                '3B3A39C9-D64E-E411-942E-E61F13D0BD43',  -- Kuwait
                'B53A39C9-D64E-E411-942E-E61F13D0BD43',  -- Qatar
                '193B39C9-D64E-E411-942E-E61F13D0BD43',  -- UAE
                '9B3A39C9-D64E-E411-942E-E61F13D0BD43'   -- Saudi Arabia (alt)
            ) THEN 1 ELSE 0 END)                                             AS gcc_shareholders_count,
        SUM(CASE
            WHEN share.tmkn_nationality NOT IN (
                '753939C9-D64E-E411-942E-E61F13D0BD43',
                'CF3A39C9-D64E-E411-942E-E61F13D0BD43',
                '3B3A39C9-D64E-E411-942E-E61F13D0BD43',
                'B53A39C9-D64E-E411-942E-E61F13D0BD43',
                '193B39C9-D64E-E411-942E-E61F13D0BD43',
                '9B3A39C9-D64E-E411-942E-E61F13D0BD43'
            ) THEN 1 ELSE 0 END)                                             AS non_gcc_shareholders_count,

        -- Ownership percentages
        SUM(CASE
            WHEN share.tmkn_nationality = '753939C9-D64E-E411-942E-E61F13D0BD43'
            THEN share.tmkn_ownership ELSE 0 END)                            AS bahraini_ownership_pct,
        SUM(CASE
            WHEN share.tmkn_nationality IN (
                'CF3A39C9-D64E-E411-942E-E61F13D0BD43',
                '3B3A39C9-D64E-E411-942E-E61F13D0BD43',
                'B53A39C9-D64E-E411-942E-E61F13D0BD43',
                '193B39C9-D64E-E411-942E-E61F13D0BD43',
                '9B3A39C9-D64E-E411-942E-E61F13D0BD43'
            ) THEN share.tmkn_ownership ELSE 0 END)                          AS gcc_ownership_pct,
        SUM(CASE
            WHEN share.tmkn_nationality NOT IN (
                '753939C9-D64E-E411-942E-E61F13D0BD43',
                'CF3A39C9-D64E-E411-942E-E61F13D0BD43',
                '3B3A39C9-D64E-E411-942E-E61F13D0BD43',
                'B53A39C9-D64E-E411-942E-E61F13D0BD43',
                '193B39C9-D64E-E411-942E-E61F13D0BD43',
                '9B3A39C9-D64E-E411-942E-E61F13D0BD43'
            ) THEN share.tmkn_ownership ELSE 0 END)                          AS non_gcc_ownership_pct,

        -- Gender counts
        SUM(CASE WHEN share.tmkn_gender = 810800000 THEN 1 ELSE 0 END)       AS shareholders_male_count,
        SUM(CASE WHEN share.tmkn_gender = 810800001 THEN 1 ELSE 0 END)       AS shareholders_female_count

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_SHAREHOLDERSPARTNERSBASE share
    WHERE share.statecode = 0
    GROUP BY share.tmkn_company
),


-- ============================================================================
-- Government shareholder flag (Bahrain Government as a recognised shareholder)
-- ============================================================================
gov_shareholders AS (
    SELECT
        share.tmkn_company                                                   AS company_id,
        MAX(share.tmkn_nameenglish)                                          AS gov_name,
        SUM(share.tmkn_ownership)                                            AS gov_ownership_pct
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_SHAREHOLDERSPARTNERSBASE share
    WHERE share.statecode = 0
      AND share.tmkn_nameenglish IN (
          'BAHRAIN GOVERNMENT',
          'The Government of the Kingdom of Bahrain',
          'GOVERNMENT OF KINGDOM OF BAHRAIN'
      )
    GROUP BY share.tmkn_company
)


-- ============================================================================
-- Main SELECT: company anchor with aggregations joined in
-- ============================================================================
SELECT
    'tmkn_company' AS mis_source_table,

    -- Identifiers
    CAST(c.tmkn_companyid AS VARCHAR)                    AS company_id,
    c.tmkn_cr                                            AS cr_license_number,
    c.tmkn_commercialnameenglish                         AS commercial_name_english,
    c.tmkn_commercialnamearabic                          AS commercial_name_arabic,
    c.tmkn_mainrecord                                AS main_record_name,
    c.tmkn_registrationdate                              AS registration_date,

    -- Foreign keys preserved for cross-domain re-joining
    CAST(c.tmkn_iban AS VARCHAR)                         AS iban_id,

    -- Classification (option-set decoded)
     CASE WHEN c.tmkn_companytype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_CompanyType') || '|' || CAST(c.tmkn_companytype AS VARCHAR)) END  AS company_type,
     CASE WHEN c.tmkn_crstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_CRStatus') || '|' || CAST(c.tmkn_crstatus AS VARCHAR)) END  AS cr_status,
     CASE WHEN c.tmkn_auditedstatement IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_AuditedStatement') || '|' || CAST(c.tmkn_auditedstatement AS VARCHAR)) END  AS have_audited_statement,
     CASE WHEN c.tmkn_subjecttobahrainization IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_SubjecttoBahrainization') || '|' || CAST(c.tmkn_subjecttobahrainization AS VARCHAR)) END AS subject_to_bahrainization,
     CASE WHEN c.tmkn_isvirtual IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_IsVirtual') || '|' || CAST(c.tmkn_isvirtual AS VARCHAR)) END  AS is_virtual,
     CASE WHEN c.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('statecode') || '|' || CAST(c.statecode AS VARCHAR)) END  AS state,              
    c.tmkn_activitysector                            AS activity_sector,
    c.tmkn_tamkeencompanycategory                    AS tamkeen_company_category,
    c.tmkn_tamkeencompanymaincategory                AS tamkeen_company_main_category,

    -- Financial
    c.tmkn_annualrevenue                                 AS annual_revenue,
    c.tmkn_auditduration                                 AS audit_duration_years,
    c.tmkn_issuedcaptial                                 AS issued_capital,
    c.tmkn_totalbahrainisalaries                         AS total_bahraini_salaries,
    c.tmkn_totalexpatriatessalaries                      AS total_expatriates_salaries,

    -- Workforce / Bahrainization
    c.tmkn_totalnumberofbahrainiworkers                  AS total_bahraini_workers_sio,
    c.tmkn_totalnumberofdisabledbahrainiworkers          AS total_disabled_bahraini_workers_sio,
    c.tmkn_totalnumberofnonbahrainiworkers               AS total_non_bahraini_workers_lmra,
    c.tmkn_currentbahrainizationrate                     AS current_bahrainization_rate_pct,
    c.tmkn_targetbahrainizationrate                      AS target_bahrainization_rate_pct,
    c.tmkn_bahrainizationratedifference                  AS bahrainization_rate_difference_pct,
    c.tmkn_inprogressrequests                            AS in_progress_requests,
    c.tmkn_hwtowork                                      AS hw_to_work,
    c.tmkn_activeworkers                                 AS active_workers,
    c.tmkn_parallelexpat                                 AS parallel_expat,

    -- Address
    c.tmkn_addressblock                              AS address_block,
    c.tmkn_addressbuilding                               AS address_building,
    c.tmkn_addressflat                                   AS address_flat,
    c.tmkn_addressroadstreet                             AS address_road_street,
    c.tmkn_addresstown                                  as tmkn_addresstown,

    -- Primary contact
    c.tmkn_contactfirstname                              AS contact_first_name,
    c.tmkn_contactlastname                               AS contact_last_name,
    c.tmkn_contactcpr                                    AS contact_cpr,
    c.tmkn_contactdesignation                            AS contact_designation,
    c.tmkn_contactemail                                  AS contact_email,
    c.tmkn_contactmobilenumber                           AS contact_mobile_number,
    c.tmkn_contactofficenumber                           AS contact_office_number,
    c.tmkn_contactnationality                        AS contact_nationality,
     CASE WHEN c.tmkn_contactgender IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_ContactGender') || '|' || CAST(c.tmkn_contactgender AS VARCHAR)) END AS contact_gender,

    -- Secondary contact
    c.tmkn_secondarycontactfirstname                     AS secondary_contact_first_name,
    c.tmkn_secondarycontactlastname                      AS secondary_contact_last_name,
    c.tmkn_secondarycontactcpr                           AS secondary_contact_cpr,
    c.tmkn_secondarycontactdesignation                   AS secondary_contact_designation,
    c.tmkn_secondarycontactemail                         AS secondary_contact_email,
    c.tmkn_secondarycontactmobilenumber                  AS secondary_contact_mobile_number,
    c.tmkn_secondarycontactofficenumber                  AS secondary_contact_office_number,
    c.tmkn_secondarycontactnationality               AS secondary_contact_nationality,
     CASE WHEN c.tmkn_secondarycontactgender IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_company') || '|' || lower('tmkn_SecondaryContactGender') || '|' || CAST(c.tmkn_secondarycontactgender AS VARCHAR)) END AS secondary_contact_gender,

    -- Aggregated business activities
    cba.activity_count                                   AS business_activity_count,
    cba.isic4_codes                                      AS isic4_codes,
    cba.activities                                       AS business_activities,

    -- Aggregated shareholders
    sh.bahraini_shareholders_count                       AS bahraini_shareholders_count,
    sh.gcc_shareholders_count                            AS gcc_shareholders_count,
    sh.non_gcc_shareholders_count                        AS non_gcc_shareholders_count,
    sh.bahraini_ownership_pct                            AS bahraini_ownership_pct,
    sh.gcc_ownership_pct                                 AS gcc_ownership_pct,
    sh.non_gcc_ownership_pct                             AS non_gcc_ownership_pct,
    sh.shareholders_male_count                           AS shareholders_male_count,
    sh.shareholders_female_count                         AS shareholders_female_count,

    -- Government shareholder flag
    CASE WHEN gs.company_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS bahrain_government_shareholder,
    COALESCE(gs.gov_ownership_pct, 0)                    AS bahrain_government_ownership_pct,

    -- Youth Bahraini shareholders count
    -- TODO: implement validate_cpr macro and uncomment.
    -- The original RPT-029 logic uses dbo.fn_Validate_CPR + DOB extraction
    -- from CPR digits. This needs to become a  validate_cpr(cpr)  +
    --  extract_dob_from_cpr(cpr)  macro pair before this column can
    -- be calculated correctly.
    CAST(NULL AS INTEGER)                                AS youth_bahraini_shareholders_count,
    tmkn_cr,
    tmkn_activitysector,
    tmkn_totalbahrainino,
    tmkn_contactmobilenumber,
    tmkn_contactemail,
    tmkn_addressblock,

    -- Audit
    c.createdon                                          AS created_on,
    c.modifiedon                                         AS modified_on,
    

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_COMPANYBASE c
LEFT JOIN company_business_activities cba ON cba.company_id = c.tmkn_companyid
LEFT JOIN shareholders_agg            sh  ON sh.company_id  = c.tmkn_companyid
LEFT JOIN gov_shareholders            gs  ON gs.company_id  = c.tmkn_companyid

WHERE c.statecode = 0
  AND c.tmkn_mainrecord IS NULL
)
SELECT
    application_id,
    guid,
    finalapprovedtkshareamt,
    application_no,
    programversionid,
    decisionprogramversionid,
    portaluserid,
    beneficiaryid,
    applicationstatusid,
    profilinginstanceguid,
    applicationinstanceformguid,
    applicationinstancedocguid,
    customerinstanceformguid,
    findatainstanceformguid,
    customerinstancedocguid,
    bindinginstancedocguid,
    amendapprovalinstancedocguid,
    hipoinstanceformguid,
    analysisinstanceformguid,
    ishipooptionid,
    programcap,
    programcapid,
    workflow_status,
    is_active,
    created_on,
    submitted_on,
    approved_on,
    customer_type,
    cap,
    tkshare_approved,
    amount_requested,
    remaining,
    customer_share,
    total_cost,
    start_date,
    end_date,
    contract_duration_months,
    is_hipo,
    monitoring_due_date,
    spending_period_end_date,
    cr_license_no_main,
    registration_date,
    cr_license_type,
    portal_user_name,
    program_name,
    total_male_shareholders,
    total_female_shareholders,
    total_bahraini_shareholders,
    total_gcc_shareholders,
    total_non_bahraini_shareholders,
    total_unspecified_shareholders,
    financing_tenor,
    approved_on_bank,
    workflow_status_detailed,
    is_eligible,
    withdrawn_on,
    confirmed_on,
    contact_designation,
    bank_name,
    breakup_facility_machinery_equipment_bhd,
    breakup_facility_technology_bhd,
    breakup_facility_marketing_branding_bhd,
    breakup_facility_fixtures_fittings_bhd,
    breakup_facility_other_bhd,
    disbursement_type,
    is_revolving_facility,
    availability_period_months,
    repayment_period,
    financing_product_type,
    profit_rate_pct,
    profit_rate_type,
    total_profit_bhd,
    total_profit_amount_calculated_bhd,
    commercial_name,
    program_id,
    application_status_id,
    application_loan_status_id,
    rm_user_id,
    assessor_user_id,
    approver_user_id,
    customer_user_id,
    program_type_name,
    hipo_classification_rm,
    hipo_classification_assessor,
    hipo_classification_approver,
    rm_name,
    assessor_name,
    approver_name,
    approved_pending_customer_on,
    saved_on,
    program_support_cap,
    financing_cap,
    financing_guarantee_cap,
    recommended_grant_rm,
    recommended_grant_assessor,
    assessment_support_cap_rm,
    assessment_support_cap_assessor,
    assessment_support_cap_approver,
    approved_grant,
    approved_grant_maximum,
    approved_financing_amount,
    approved_guarantee_amount,
    total_requested_grant,
    total_requested_financing,
    consumed_amount,
    remaining_amount,
    recommended_financing_rm,
    recommended_financing_assessor,
    recommended_financing_approver,
    loan_tenor,
    loan_profit_rate,
    loan_total_profit_amount,
    loan_monthly_installment,
    loan_grace_period,
    loan_start_date,
    loan_end_date,
    requested_support_no_of_employees,
    remarks_rm,
    remarks_assessor,
    remarks_approver,
    company_id,
    cr_license_number,
    commercial_name_english,
    commercial_name_arabic,
    main_record_name,
    iban_id,
    company_type,
    cr_status,
    have_audited_statement,
    subject_to_bahrainization,
    is_virtual,
    state,
    activity_sector,
    tamkeen_company_category,
    tamkeen_company_main_category,
    annual_revenue,
    audit_duration_years,
    issued_capital,
    total_bahraini_salaries,
    total_expatriates_salaries,
    total_bahraini_workers_sio,
    total_disabled_bahraini_workers_sio,
    total_non_bahraini_workers_lmra,
    current_bahrainization_rate_pct,
    target_bahrainization_rate_pct,
    bahrainization_rate_difference_pct,
    in_progress_requests,
    hw_to_work,
    active_workers,
    parallel_expat,
    address_block,
    address_building,
    address_flat,
    address_road_street,
    contact_first_name,
    contact_last_name,
    contact_cpr,
    contact_email,
    contact_mobile_number,
    contact_office_number,
    contact_nationality,
    contact_gender,
    secondary_contact_first_name,
    secondary_contact_last_name,
    secondary_contact_cpr,
    secondary_contact_designation,
    secondary_contact_email,
    secondary_contact_mobile_number,
    secondary_contact_office_number,
    secondary_contact_nationality,
    secondary_contact_gender,
    business_activity_count,
    isic4_codes,
    business_activities,
    bahraini_ownership_pct,
    gcc_ownership_pct,
    non_gcc_ownership_pct,
    bahrain_government_shareholder,
    bahrain_government_ownership_pct,
    youth_bahraini_shareholders_count,
    tmkn_cr,
    tmkn_activitysector,
    tmkn_TotalBahrainiNo,
    tmkn_ContactMobileNumber,
    tmkn_ContactEMail,
    tmkn_AddressBlock,
    modified_on,
    source_table,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    createdon,
    updatedon

FROM (

-- OS2
SELECT
    os2.application_id,
    os2.guid,
    os2.finalapprovedtkshareamt,
    os2.application_no,
    os2.programversionid,
    os2.decisionprogramversionid,
    os2.portaluserid,
    os2.beneficiaryid,
    os2.applicationstatusid,
    os2.profilinginstanceguid,
    os2.applicationinstanceformguid,
    os2.applicationinstancedocguid,
    os2.customerinstanceformguid,
    os2.findatainstanceformguid,
    os2.customerinstancedocguid,
    os2.bindinginstancedocguid,
    os2.amendapprovalinstancedocguid,
    os2.hipoinstanceformguid,
    os2.analysisinstanceformguid,
    os2.ishipooptionid,
    os2.programcap,
    os2.programcapid,
    os2.workflow_status,
    os2.is_active,
    os2.created_on,
    os2.submitted_on,
    os2.approved_on,
    os2.customer_type,
    os2.cap,
    os2.tkshare_approved,
    os2.amount_requested,
    os2.remaining,
    os2.customer_share,
    os2.total_cost,
    os2.start_date,
    os2.end_date,
    os2.contract_duration_months,
    os2.is_hipo,
    os2.monitoring_due_date,
    os2.spending_period_end_date,
    os2.cr_license_no_main,
    os2.registration_date,
    os2.cr_license_type,
    os2.portal_user_name,
    os2.program_name,
    os2.total_male_shareholders,
    os2.total_female_shareholders,
    os2.total_bahraini_shareholders,
    os2.total_gcc_shareholders,
    os2.total_non_bahraini_shareholders,
    os2.total_unspecified_shareholders,
    os2.financing_tenor,
    os2.approved_on_bank,
    os2.workflow_status_detailed,
    os2.is_eligible,
    os2.withdrawn_on,
    os2.confirmed_on,
    os2.contact_designation,
    os2.bank_name,
    os2.breakup_facility_machinery_equipment_bhd,
    os2.breakup_facility_technology_bhd,
    os2.breakup_facility_marketing_branding_bhd,
    os2.breakup_facility_fixtures_fittings_bhd,
    os2.breakup_facility_other_bhd,
    os2.disbursement_type,
    os2.is_revolving_facility,
    os2.availability_period_months,
    os2.repayment_period,
    os2.financing_product_type,
    os2.profit_rate_pct,
    os2.profit_rate_type,
    os2.total_profit_bhd,
    os2.total_profit_amount_calculated_bhd,
    CAST(NULL AS VARCHAR)       AS commercial_name,
    CAST(NULL AS BIGINT)        AS program_id,
    CAST(NULL AS BIGINT)        AS application_status_id,
    CAST(NULL AS BIGINT)        AS application_loan_status_id,
    CAST(NULL AS BIGINT)        AS rm_user_id,
    CAST(NULL AS BIGINT)        AS assessor_user_id,
    CAST(NULL AS BIGINT)        AS approver_user_id,
    CAST(NULL AS BIGINT)        AS customer_user_id,
    CAST(NULL AS VARCHAR)       AS program_type_name,
    CAST(NULL AS VARCHAR)       AS hipo_classification_rm,
    CAST(NULL AS VARCHAR)       AS hipo_classification_assessor,
    CAST(NULL AS VARCHAR)       AS hipo_classification_approver,
    CAST(NULL AS VARCHAR)       AS rm_name,
    CAST(NULL AS VARCHAR)       AS assessor_name,
    CAST(NULL AS VARCHAR)       AS approver_name,
    CAST(NULL AS TIMESTAMP)     AS approved_pending_customer_on,
    CAST(NULL AS TIMESTAMP)     AS saved_on,
    CAST(NULL AS DECIMAL(38,8)) AS program_support_cap,
    CAST(NULL AS DECIMAL(38,8)) AS financing_cap,
    CAST(NULL AS DECIMAL(38,8)) AS financing_guarantee_cap,
    CAST(NULL AS DECIMAL(38,8)) AS recommended_grant_rm,
    CAST(NULL AS DECIMAL(38,8)) AS recommended_grant_assessor,
    CAST(NULL AS DECIMAL(38,8)) AS assessment_support_cap_rm,
    CAST(NULL AS DECIMAL(38,8)) AS assessment_support_cap_assessor,
    CAST(NULL AS DECIMAL(38,8)) AS assessment_support_cap_approver,
    CAST(NULL AS DECIMAL(38,8)) AS approved_grant,
    CAST(NULL AS DECIMAL(38,8)) AS approved_grant_maximum,
    CAST(NULL AS DECIMAL(38,8)) AS approved_financing_amount,
    CAST(NULL AS DECIMAL(38,8)) AS approved_guarantee_amount,
    CAST(NULL AS DECIMAL(38,8)) AS total_requested_grant,
    CAST(NULL AS DECIMAL(38,8)) AS total_requested_financing,
    CAST(NULL AS DECIMAL(38,8)) AS consumed_amount,
    CAST(NULL AS DECIMAL(38,8)) AS remaining_amount,
    CAST(NULL AS DECIMAL(38,8)) AS recommended_financing_rm,
    CAST(NULL AS DECIMAL(38,8)) AS recommended_financing_assessor,
    CAST(NULL AS DECIMAL(38,8)) AS recommended_financing_approver,
    CAST(NULL AS INTEGER)       AS loan_tenor,
    CAST(NULL AS DECIMAL(38,8)) AS loan_profit_rate,
    CAST(NULL AS DECIMAL(38,8)) AS loan_total_profit_amount,
    CAST(NULL AS DECIMAL(38,8)) AS loan_monthly_installment,
    os2.grace_period_months     AS loan_grace_period,
    CAST(NULL AS DATE)          AS loan_start_date,
    CAST(NULL AS DATE)          AS loan_end_date,
    CAST(NULL AS INTEGER)       AS requested_support_no_of_employees,
    CAST(NULL AS VARCHAR)       AS remarks_rm,
    CAST(NULL AS VARCHAR)       AS remarks_assessor,
    CAST(NULL AS VARCHAR)       AS remarks_approver,
    CAST(NULL AS VARCHAR)       AS company_id,
    os2.cr_license_no           AS cr_license_number,
    os2.commercial_name_en      AS commercial_name_english,
    os2.commercial_name_ar      AS commercial_name_arabic,
    CAST(NULL AS VARCHAR)       AS main_record_name,
    CAST(NULL AS VARCHAR)       AS iban_id,
    CAST(NULL AS VARCHAR)       AS company_type,
    CAST(NULL AS VARCHAR)       AS cr_status,
    CAST(NULL AS VARCHAR)       AS have_audited_statement,
    CAST(NULL AS VARCHAR)       AS subject_to_bahrainization,
    CAST(NULL AS VARCHAR)       AS is_virtual,
    CAST(NULL AS VARCHAR)       AS state,
    CAST(NULL AS VARCHAR)       AS activity_sector,
    CAST(NULL AS VARCHAR)       AS tamkeen_company_category,
    CAST(NULL AS VARCHAR)       AS tamkeen_company_main_category,
    CAST(NULL AS DECIMAL(38,8)) AS annual_revenue,
    CAST(NULL AS INTEGER)       AS audit_duration_years,
    CAST(NULL AS DECIMAL(38,8)) AS issued_capital,
    CAST(NULL AS DECIMAL(38,8)) AS total_bahraini_salaries,
    CAST(NULL AS DECIMAL(38,8)) AS total_expatriates_salaries,
    CAST(NULL AS INTEGER)       AS total_bahraini_workers_sio,
    CAST(NULL AS INTEGER)       AS total_disabled_bahraini_workers_sio,
    CAST(NULL AS INTEGER)       AS total_non_bahraini_workers_lmra,
    CAST(NULL AS DECIMAL(38,8)) AS current_bahrainization_rate_pct,
    CAST(NULL AS DECIMAL(38,8)) AS target_bahrainization_rate_pct,
    CAST(NULL AS DECIMAL(38,8)) AS bahrainization_rate_difference_pct,
    CAST(NULL AS INTEGER)       AS in_progress_requests,
    CAST(NULL AS INTEGER)       AS hw_to_work,
    CAST(NULL AS INTEGER)       AS active_workers,
    CAST(NULL AS INTEGER)       AS parallel_expat,
    CAST(NULL AS VARCHAR)       AS address_block,
    CAST(NULL AS VARCHAR)       AS address_building,
    CAST(NULL AS VARCHAR)       AS address_flat,
    CAST(NULL AS VARCHAR)       AS address_road_street,
    CAST(NULL AS VARCHAR)       AS contact_first_name,
    CAST(NULL AS VARCHAR)       AS contact_last_name,
    CAST(NULL AS VARCHAR)       AS contact_cpr,
    os2.email                   AS contact_email,
    os2.mobile_no               AS contact_mobile_number,
    CAST(NULL AS VARCHAR)       AS contact_office_number,
    CAST(NULL AS VARCHAR)       AS contact_nationality,
    CAST(NULL AS VARCHAR)       AS contact_gender,
    CAST(NULL AS VARCHAR)       AS secondary_contact_first_name,
    CAST(NULL AS VARCHAR)       AS secondary_contact_last_name,
    CAST(NULL AS VARCHAR)       AS secondary_contact_cpr,
    CAST(NULL AS VARCHAR)       AS secondary_contact_designation,
    CAST(NULL AS VARCHAR)       AS secondary_contact_email,
    CAST(NULL AS VARCHAR)       AS secondary_contact_mobile_number,
    CAST(NULL AS VARCHAR)       AS secondary_contact_office_number,
    CAST(NULL AS VARCHAR)       AS secondary_contact_nationality,
    CAST(NULL AS VARCHAR)       AS secondary_contact_gender,
    CAST(NULL AS BIGINT)        AS business_activity_count,
    CAST(NULL AS VARCHAR)       AS isic4_codes,
    CAST(NULL AS VARCHAR)       AS business_activities,
    CAST(NULL AS DECIMAL(38,8)) AS bahraini_ownership_pct,
    CAST(NULL AS DECIMAL(38,8)) AS gcc_ownership_pct,
    CAST(NULL AS DECIMAL(38,8)) AS non_gcc_ownership_pct,
    CAST(NULL AS VARCHAR)       AS bahrain_government_shareholder,
    CAST(NULL AS DECIMAL(38,8)) AS bahrain_government_ownership_pct,
    CAST(NULL AS BIGINT)        AS youth_bahraini_shareholders_count,
    CAST(NULL AS VARCHAR)       AS tmkn_cr, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_activitysector, --NEW
    CAST(NULL AS BIGINT)        AS tmkn_TotalBahrainiNo, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_ContactMobileNumber, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_ContactEMail, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_AddressBlock, --NEW
    -- CAST(NULL AS VARCHAR)       AS application_sector_objective ,
    CAST(NULL AS TIMESTAMP)     AS modified_on,
    -- CAST(NULL AS VARCHAR)    as tmkn_addresstown,
    CAST(NULL AS VARCHAR)       AS source_table,
    os2.source_system_name,
    os2.is_deleted,
    CAST(NULL AS DATE)          AS report_date,
    os2.dbt_updated_at,
    os2.createdon,
    os2.updatedon

from customer_enterprise_base_os2_source os2

UNION ALL

-- OS1
SELECT
    os1.application_id,
    CAST(NULL AS VARCHAR)       AS guid,
     CAST(NULL AS VARCHAR)  as finalapprovedtkshareamt,
    os1.application_no,
    CAST(NULL AS BIGINT)        AS programversionid,
    CAST(NULL AS BIGINT)        AS decisionprogramversionid,
    CAST(NULL AS BIGINT)        AS portaluserid,
    CAST(NULL AS BIGINT)        AS beneficiaryid,
    CAST(NULL AS VARCHAR)       AS applicationstatusid,
    CAST(NULL AS VARCHAR)       AS profilinginstanceguid,
    CAST(NULL AS VARCHAR)       AS applicationinstanceformguid,
    CAST(NULL AS VARCHAR)       AS applicationinstancedocguid,
    CAST(NULL AS VARCHAR)       AS customerinstanceformguid,
    CAST(NULL AS VARCHAR)       AS findatainstanceformguid,
    CAST(NULL AS VARCHAR)       AS customerinstancedocguid,
    CAST(NULL AS VARCHAR)       AS bindinginstancedocguid,
    CAST(NULL AS VARCHAR)       AS amendapprovalinstancedocguid,
    CAST(NULL AS VARCHAR)       AS hipoinstanceformguid,
    CAST(NULL AS VARCHAR)       AS analysisinstanceformguid,
    CAST(NULL AS INTEGER)       AS ishipooptionid,
    CAST(NULL AS DECIMAL(38,8)) AS programcap,
    CAST(NULL AS BIGINT)        AS programcapid,
    os1.workflow_status,
    CAST(NULL AS VARCHAR)       AS is_active,
    os1.created_on,
    CAST(NULL AS TIMESTAMP)     AS submitted_on,
    os1.approved_on,
    CAST(NULL AS VARCHAR)       AS customer_type,
    CAST(NULL AS DECIMAL(38,8)) AS cap,
    CAST(NULL AS DECIMAL(38,8)) AS tkshare_approved,
    CAST(NULL AS DECIMAL(38,8)) AS amount_requested,
    CAST(NULL AS DECIMAL(38,8)) AS remaining,
    CAST(NULL AS DECIMAL(38,8)) AS customer_share,
    CAST(NULL AS DECIMAL(38,8)) AS total_cost,
    os1.contract_start_date     AS start_date,
    os1.contract_end_date       AS end_date,
    CAST(NULL AS INTEGER)       AS contract_duration_months,
    CAST(NULL AS VARCHAR)       AS is_hipo,
    CAST(NULL AS DATE)          AS monitoring_due_date,
    os1.spending_period_end_date,
    CAST(NULL AS VARCHAR)       AS cr_license_no_main,
    CAST(NULL AS DATE)          AS registration_date,
    CAST(NULL AS VARCHAR)       AS cr_license_type,
    CAST(NULL AS VARCHAR)       AS portal_user_name,
    os1.program_name,
    CAST(NULL AS BIGINT)        AS total_male_shareholders,
    CAST(NULL AS BIGINT)        AS total_female_shareholders,
    CAST(NULL AS BIGINT)        AS total_bahraini_shareholders,
    CAST(NULL AS BIGINT)        AS total_gcc_shareholders,
    CAST(NULL AS BIGINT)        AS total_non_bahraini_shareholders,
    CAST(NULL AS BIGINT)        AS total_unspecified_shareholders,
    CAST(NULL AS INTEGER)       AS financing_tenor,
    CAST(NULL AS TIMESTAMP)     AS approved_on_bank,
    CAST(NULL AS VARCHAR)       AS workflow_status_detailed,
    CAST(NULL AS VARCHAR)       AS is_eligible,
    CAST(NULL AS TIMESTAMP)     AS withdrawn_on,
    CAST(NULL AS TIMESTAMP)     AS confirmed_on,
    CAST(NULL AS VARCHAR)       AS contact_designation,
    os1.bank_name,
    CAST(NULL AS VARCHAR)       AS breakup_facility_machinery_equipment_bhd,
    CAST(NULL AS VARCHAR)       AS breakup_facility_technology_bhd,
    CAST(NULL AS VARCHAR)       AS breakup_facility_marketing_branding_bhd,
    CAST(NULL AS VARCHAR)       AS breakup_facility_fixtures_fittings_bhd,
    CAST(NULL AS VARCHAR)       AS breakup_facility_other_bhd,
    CAST(NULL AS VARCHAR)       AS disbursement_type,
    CAST(NULL AS VARCHAR)       AS is_revolving_facility,
    CAST(NULL AS INTEGER)       AS availability_period_months,
    CAST(NULL AS VARCHAR)       AS repayment_period,
    CAST(NULL AS VARCHAR)       AS financing_product_type,
    os1.loan_profit_rate        AS profit_rate_pct,
    CAST(NULL AS VARCHAR)       AS profit_rate_type,
    os1.loan_total_profit_amount AS total_profit_bhd,
    CAST(NULL AS DECIMAL(38,8)) AS total_profit_amount_calculated_bhd,
    os1.commercial_name,
    os1.program_id,
    os1.application_status_id,
    os1.application_loan_status_id,
    os1.rm_user_id,
    os1.assessor_user_id,
    os1.approver_user_id,
    os1.customer_user_id,
    os1.program_type_name,
    os1.hipo_classification_rm,
    os1.hipo_classification_assessor,
    os1.hipo_classification_approver,
    os1.rm_name,
    os1.assessor_name,
    os1.approver_name,
    os1.approved_pending_customer_on,
    os1.saved_on,
    os1.program_support_cap,
    os1.financing_cap,
    os1.financing_guarantee_cap,
    os1.recommended_grant_rm,
    os1.recommended_grant_assessor,
    os1.assessment_support_cap_rm,
    os1.assessment_support_cap_assessor,
    os1.assessment_support_cap_approver,
    os1.approved_grant,
    os1.approved_grant_maximum,
    os1.approved_financing_amount,
    os1.approved_guarantee_amount,
    os1.total_requested_grant,
    os1.total_requested_financing,
    os1.consumed_amount,
    os1.remaining_amount,
    os1.recommended_financing_rm,
    os1.recommended_financing_assessor,
    os1.recommended_financing_approver,
    os1.loan_tenor,
    os1.loan_profit_rate,
    os1.loan_total_profit_amount,
    os1.loan_monthly_installment,
    os1.loan_grace_period,
    os1.loan_start_date,
    os1.loan_end_date,
    os1.requested_support_no_of_employees,
    os1.remarks_rm,
    os1.remarks_assessor,
    os1.remarks_approver,
    CAST(NULL AS VARCHAR)       AS company_id,
    os1.cr_license_no           AS cr_license_number,
    os1.commercial_name         AS commercial_name_english,
    CAST(NULL AS VARCHAR)       AS commercial_name_arabic,
    CAST(NULL AS VARCHAR)       AS main_record_name,
    CAST(NULL AS VARCHAR)       AS iban_id,
    CAST(NULL AS VARCHAR)       AS company_type,
    CAST(NULL AS VARCHAR)       AS cr_status,
    CAST(NULL AS VARCHAR)       AS have_audited_statement,
    CAST(NULL AS VARCHAR)       AS subject_to_bahrainization,
    CAST(NULL AS VARCHAR)       AS is_virtual,
    CAST(NULL AS VARCHAR)       AS state,
    CAST(NULL AS VARCHAR)       AS activity_sector,
    CAST(NULL AS VARCHAR)       AS tamkeen_company_category,
    CAST(NULL AS VARCHAR)       AS tamkeen_company_main_category,
    CAST(NULL AS DECIMAL(38,8)) AS annual_revenue,
    CAST(NULL AS INTEGER)       AS audit_duration_years,
    CAST(NULL AS DECIMAL(38,8)) AS issued_capital,
    CAST(NULL AS DECIMAL(38,8)) AS total_bahraini_salaries,
    CAST(NULL AS DECIMAL(38,8)) AS total_expatriates_salaries,
    CAST(NULL AS INTEGER)       AS total_bahraini_workers_sio,
    CAST(NULL AS INTEGER)       AS total_disabled_bahraini_workers_sio,
    CAST(NULL AS INTEGER)       AS total_non_bahraini_workers_lmra,
    CAST(NULL AS DECIMAL(38,8)) AS current_bahrainization_rate_pct,
    CAST(NULL AS DECIMAL(38,8)) AS target_bahrainization_rate_pct,
    CAST(NULL AS DECIMAL(38,8)) AS bahrainization_rate_difference_pct,
    CAST(NULL AS INTEGER)       AS in_progress_requests,
    CAST(NULL AS INTEGER)       AS hw_to_work,
    CAST(NULL AS INTEGER)       AS active_workers,
    CAST(NULL AS INTEGER)       AS parallel_expat,
    CAST(NULL AS VARCHAR)       AS address_block,
    CAST(NULL AS VARCHAR)       AS address_building,
    CAST(NULL AS VARCHAR)       AS address_flat,
    CAST(NULL AS VARCHAR)       AS address_road_street,
    CAST(NULL AS VARCHAR)       AS contact_first_name,
    CAST(NULL AS VARCHAR)       AS contact_last_name,
    CAST(NULL AS VARCHAR)       AS contact_cpr,
    CAST(NULL AS VARCHAR)       AS contact_email,
    CAST(NULL AS VARCHAR)       AS contact_mobile_number,
    CAST(NULL AS VARCHAR)       AS contact_office_number,
    CAST(NULL AS VARCHAR)       AS contact_nationality,
    CAST(NULL AS VARCHAR)       AS contact_gender,
    CAST(NULL AS VARCHAR)       AS secondary_contact_first_name,
    CAST(NULL AS VARCHAR)       AS secondary_contact_last_name,
    CAST(NULL AS VARCHAR)       AS secondary_contact_cpr,
    CAST(NULL AS VARCHAR)       AS secondary_contact_designation,
    CAST(NULL AS VARCHAR)       AS secondary_contact_email,
    CAST(NULL AS VARCHAR)       AS secondary_contact_mobile_number,
    CAST(NULL AS VARCHAR)       AS secondary_contact_office_number,
    CAST(NULL AS VARCHAR)       AS secondary_contact_nationality,
    CAST(NULL AS VARCHAR)       AS secondary_contact_gender,
    CAST(NULL AS BIGINT)        AS business_activity_count,
    CAST(NULL AS VARCHAR)       AS isic4_codes,
    CAST(NULL AS VARCHAR)       AS business_activities,
    CAST(NULL AS DECIMAL(38,8)) AS bahraini_ownership_pct,
    CAST(NULL AS DECIMAL(38,8)) AS gcc_ownership_pct,
    CAST(NULL AS DECIMAL(38,8)) AS non_gcc_ownership_pct,
    CAST(NULL AS VARCHAR)       AS bahrain_government_shareholder,
    CAST(NULL AS DECIMAL(38,8)) AS bahrain_government_ownership_pct,
    os1.youth_bahraini_shareholders_count,
    CAST(NULL AS VARCHAR)       AS tmkn_cr, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_activitysector, --NEW
    CAST(NULL AS BIGINT)        AS tmkn_TotalBahrainiNo, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_ContactMobileNumber, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_ContactEMail, --NEW
    CAST(NULL AS VARCHAR)       AS tmkn_AddressBlock, --NEW
    -- os1.application_sector_objective ,
    CAST(NULL AS TIMESTAMP)     AS modified_on,
    -- CAST(NULL AS VARCHAR)    as tmkn_addresstown,
    os1.os1_source_table        AS source_table,
    os1.source_system_name,
    os1.is_deleted,
    os1.report_date,
    os1.dbt_updated_at,
    os1.created_on AS createdon,
    CAST(NULL AS TIMESTAMP) AS updatedon

from customer_enterprise_base_os1_source os1

UNION ALL

-- MIS
SELECT
    CAST(NULL AS BIGINT)        AS application_id,
    CAST(NULL AS VARCHAR)       AS guid,
     CAST(NULL AS VARCHAR)  as finalapprovedtkshareamt,
    CAST(NULL AS VARCHAR)       AS application_no,
    CAST(NULL AS BIGINT)        AS programversionid,
    CAST(NULL AS BIGINT)        AS decisionprogramversionid,
    CAST(NULL AS BIGINT)        AS portaluserid,
    CAST(NULL AS BIGINT)        AS beneficiaryid,
    CAST(NULL AS VARCHAR)       AS applicationstatusid,
    CAST(NULL AS VARCHAR)       AS profilinginstanceguid,
    CAST(NULL AS VARCHAR)       AS applicationinstanceformguid,
    CAST(NULL AS VARCHAR)       AS applicationinstancedocguid,
    CAST(NULL AS VARCHAR)       AS customerinstanceformguid,
    CAST(NULL AS VARCHAR)       AS findatainstanceformguid,
    CAST(NULL AS VARCHAR)       AS customerinstancedocguid,
    CAST(NULL AS VARCHAR)       AS bindinginstancedocguid,
    CAST(NULL AS VARCHAR)       AS amendapprovalinstancedocguid,
    CAST(NULL AS VARCHAR)       AS hipoinstanceformguid,
    CAST(NULL AS VARCHAR)       AS analysisinstanceformguid,
    CAST(NULL AS INTEGER)       AS ishipooptionid,
    CAST(NULL AS DECIMAL(38,8)) AS programcap,
    CAST(NULL AS BIGINT)        AS programcapid,
    CAST(NULL AS VARCHAR)       AS workflow_status,
    CAST(NULL AS VARCHAR)       AS is_active,
    mis.created_on,
    CAST(NULL AS TIMESTAMP)     AS submitted_on,
    CAST(NULL AS TIMESTAMP)     AS approved_on,
    CAST(NULL AS VARCHAR)       AS customer_type,
    CAST(NULL AS DECIMAL(38,8)) AS cap,
    CAST(NULL AS DECIMAL(38,8)) AS tkshare_approved,
    CAST(NULL AS DECIMAL(38,8)) AS amount_requested,
    CAST(NULL AS DECIMAL(38,8)) AS remaining,
    CAST(NULL AS DECIMAL(38,8)) AS customer_share,
    CAST(NULL AS DECIMAL(38,8)) AS total_cost,
    CAST(NULL AS DATE)          AS start_date,
    CAST(NULL AS DATE)          AS end_date,
    CAST(NULL AS INTEGER)       AS contract_duration_months,
    CAST(NULL AS VARCHAR)       AS is_hipo,
    CAST(NULL AS DATE)          AS monitoring_due_date,
    CAST(NULL AS DATE)          AS spending_period_end_date,
    CAST(NULL AS VARCHAR)       AS cr_license_no_main,
    mis.registration_date,
    CAST(NULL AS VARCHAR)       AS cr_license_type,
    CAST(NULL AS VARCHAR)       AS portal_user_name,
    CAST(NULL AS VARCHAR)       AS program_name,
    mis.shareholders_male_count   AS total_male_shareholders,
    mis.shareholders_female_count AS total_female_shareholders,
    mis.bahraini_shareholders_count AS total_bahraini_shareholders,
    mis.gcc_shareholders_count    AS total_gcc_shareholders,
    mis.non_gcc_shareholders_count AS total_non_bahraini_shareholders,
    CAST(NULL AS BIGINT)          AS total_unspecified_shareholders,
    CAST(NULL AS INTEGER)         AS financing_tenor,
    CAST(NULL AS TIMESTAMP)       AS approved_on_bank,
    CAST(NULL AS VARCHAR)         AS workflow_status_detailed,
    CAST(NULL AS VARCHAR)         AS is_eligible,
    CAST(NULL AS TIMESTAMP)       AS withdrawn_on,
    CAST(NULL AS TIMESTAMP)       AS confirmed_on,
    mis.contact_designation,
    CAST(NULL AS VARCHAR)         AS bank_name,
    CAST(NULL AS VARCHAR)         AS breakup_facility_machinery_equipment_bhd,
    CAST(NULL AS VARCHAR)         AS breakup_facility_technology_bhd,
    CAST(NULL AS VARCHAR)         AS breakup_facility_marketing_branding_bhd,
    CAST(NULL AS VARCHAR)         AS breakup_facility_fixtures_fittings_bhd,
    CAST(NULL AS VARCHAR)         AS breakup_facility_other_bhd,
    CAST(NULL AS VARCHAR)         AS disbursement_type,
    CAST(NULL AS VARCHAR)         AS is_revolving_facility,
    CAST(NULL AS INTEGER)         AS availability_period_months,
    CAST(NULL AS VARCHAR)         AS repayment_period,
    CAST(NULL AS VARCHAR)         AS financing_product_type,
    CAST(NULL AS DECIMAL(38,8))   AS profit_rate_pct,
    CAST(NULL AS VARCHAR)         AS profit_rate_type,
    CAST(NULL AS DECIMAL(38,8))   AS total_profit_bhd,
    CAST(NULL AS DECIMAL(38,8))   AS total_profit_amount_calculated_bhd,
    CAST(NULL AS VARCHAR)         AS commercial_name,
    CAST(NULL AS BIGINT)          AS program_id,
    CAST(NULL AS BIGINT)          AS application_status_id,
    CAST(NULL AS BIGINT)          AS application_loan_status_id,
    CAST(NULL AS BIGINT)          AS rm_user_id,
    CAST(NULL AS BIGINT)          AS assessor_user_id,
    CAST(NULL AS BIGINT)          AS approver_user_id,
    CAST(NULL AS BIGINT)          AS customer_user_id,
    CAST(NULL AS VARCHAR)         AS program_type_name,
    CAST(NULL AS VARCHAR)         AS hipo_classification_rm,
    CAST(NULL AS VARCHAR)         AS hipo_classification_assessor,
    CAST(NULL AS VARCHAR)         AS hipo_classification_approver,
    CAST(NULL AS VARCHAR)         AS rm_name,
    CAST(NULL AS VARCHAR)         AS assessor_name,
    CAST(NULL AS VARCHAR)         AS approver_name,
    CAST(NULL AS TIMESTAMP)       AS approved_pending_customer_on,
    CAST(NULL AS TIMESTAMP)       AS saved_on,
    CAST(NULL AS DECIMAL(38,8))   AS program_support_cap,
    CAST(NULL AS DECIMAL(38,8))   AS financing_cap,
    CAST(NULL AS DECIMAL(38,8))   AS financing_guarantee_cap,
    CAST(NULL AS DECIMAL(38,8))   AS recommended_grant_rm,
    CAST(NULL AS DECIMAL(38,8))   AS recommended_grant_assessor,
    CAST(NULL AS DECIMAL(38,8))   AS assessment_support_cap_rm,
    CAST(NULL AS DECIMAL(38,8))   AS assessment_support_cap_assessor,
    CAST(NULL AS DECIMAL(38,8))   AS assessment_support_cap_approver,
    CAST(NULL AS DECIMAL(38,8))   AS approved_grant,
    CAST(NULL AS DECIMAL(38,8))   AS approved_grant_maximum,
    CAST(NULL AS DECIMAL(38,8))   AS approved_financing_amount,
    CAST(NULL AS DECIMAL(38,8))   AS approved_guarantee_amount,
    CAST(NULL AS DECIMAL(38,8))   AS total_requested_grant,
    CAST(NULL AS DECIMAL(38,8))   AS total_requested_financing,
    CAST(NULL AS DECIMAL(38,8))   AS consumed_amount,
    CAST(NULL AS DECIMAL(38,8))   AS remaining_amount,
    CAST(NULL AS DECIMAL(38,8))   AS recommended_financing_rm,
    CAST(NULL AS DECIMAL(38,8))   AS recommended_financing_assessor,
    CAST(NULL AS DECIMAL(38,8))   AS recommended_financing_approver,
    CAST(NULL AS INTEGER)         AS loan_tenor,
    CAST(NULL AS DECIMAL(38,8))   AS loan_profit_rate,
    CAST(NULL AS DECIMAL(38,8))   AS loan_total_profit_amount,
    CAST(NULL AS DECIMAL(38,8))   AS loan_monthly_installment,
    CAST(NULL AS INTEGER)         AS loan_grace_period,
    CAST(NULL AS DATE)            AS loan_start_date,
    CAST(NULL AS DATE)            AS loan_end_date,
    CAST(NULL AS INTEGER)         AS requested_support_no_of_employees,
    CAST(NULL AS VARCHAR)         AS remarks_rm,
    CAST(NULL AS VARCHAR)         AS remarks_assessor,
    CAST(NULL AS VARCHAR)         AS remarks_approver,
    mis.company_id,
    mis.cr_license_number,
    mis.commercial_name_english,
    mis.commercial_name_arabic,
    mis.main_record_name,
    mis.iban_id,
    mis.company_type,
    mis.cr_status,
    mis.have_audited_statement,
    mis.subject_to_bahrainization,
    mis.is_virtual,
    mis.state,
    mis.activity_sector,
    mis.tamkeen_company_category,
    mis.tamkeen_company_main_category,
    mis.annual_revenue,
    mis.audit_duration_years,
    mis.issued_capital,
    mis.total_bahraini_salaries,
    mis.total_expatriates_salaries,
    mis.total_bahraini_workers_sio,
    mis.total_disabled_bahraini_workers_sio,
    mis.total_non_bahraini_workers_lmra,
    mis.current_bahrainization_rate_pct,
    mis.target_bahrainization_rate_pct,
    mis.bahrainization_rate_difference_pct,
    mis.in_progress_requests,
    mis.hw_to_work,
    mis.active_workers,
    mis.parallel_expat,
    mis.address_block,
    mis.address_building,
    mis.address_flat,
    mis.address_road_street,
    mis.contact_first_name,
    mis.contact_last_name,
    mis.contact_cpr,
    mis.contact_email,
    mis.contact_mobile_number,
    mis.contact_office_number,
    mis.contact_nationality,
    mis.contact_gender,
    mis.secondary_contact_first_name,
    mis.secondary_contact_last_name,
    mis.secondary_contact_cpr,
    mis.secondary_contact_designation,
    mis.secondary_contact_email,
    mis.secondary_contact_mobile_number,
    mis.secondary_contact_office_number,
    mis.secondary_contact_nationality,
    mis.secondary_contact_gender,
    mis.business_activity_count,
    mis.isic4_codes,
    mis.business_activities,
    mis.bahraini_ownership_pct,
    mis.gcc_ownership_pct,
    mis.non_gcc_ownership_pct,
    mis.bahrain_government_shareholder,
    mis.bahrain_government_ownership_pct,
    mis.youth_bahraini_shareholders_count,
    mis.tmkn_cr, --NEW
    mis.tmkn_activitysector, --NEW
    mis.tmkn_TotalBahrainiNo, --NEW
    mis.tmkn_ContactMobileNumber, --NEW
    mis.tmkn_ContactEMail, --NEW
    mis.tmkn_AddressBlock, --NEW
    -- CAST(NULL AS VARCHAR)       AS application_sector_objective ,
    mis.modified_on,
    -- mis.tmkn_addresstown,
    mis.mis_source_table          AS source_table,
    mis.source_system_name,
    mis.is_deleted,
    mis.report_date,
    mis.dbt_updated_at,
    mis.created_on as createdon,
    CAST(NULL AS TIMESTAMP) AS updatedon

from customer_enterprise_base_mis_source mis

) final_data
),

silver_layer AS (
SELECT
    application_id,
    guid,
    finalapprovedtkshareamt,
    application_no,
    programversionid,
    decisionprogramversionid,
    portaluserid,
    beneficiaryid,
    applicationstatusid,
    profilinginstanceguid,
    applicationinstanceformguid,
    applicationinstancedocguid,
    customerinstanceformguid,
    findatainstanceformguid,
    customerinstancedocguid,
    bindinginstancedocguid,
    amendapprovalinstancedocguid,
    hipoinstanceformguid,
    analysisinstanceformguid,
    ishipooptionid,
    programcap,
    programcapid,
    workflow_status,
    is_active,
    created_on,
    submitted_on,
    approved_on,
    customer_type,
    cap,
    tkshare_approved,
    amount_requested,
    remaining,
    customer_share,
    total_cost,
    start_date,
    end_date,
    contract_duration_months,
    is_hipo,
    monitoring_due_date,
    spending_period_end_date,
    cr_license_no_main,
    registration_date,
    cr_license_type,
    portal_user_name,
    program_name,
    total_male_shareholders,
    total_female_shareholders,
    total_bahraini_shareholders,
    total_gcc_shareholders,
    total_non_bahraini_shareholders,
    total_unspecified_shareholders,
    financing_tenor,
    approved_on_bank,
    workflow_status_detailed,
    is_eligible,
    withdrawn_on,
    confirmed_on,
    contact_designation,
    bank_name,
    breakup_facility_machinery_equipment_bhd,
    breakup_facility_technology_bhd,
    breakup_facility_marketing_branding_bhd,
    breakup_facility_fixtures_fittings_bhd,
    breakup_facility_other_bhd,
    disbursement_type,
    is_revolving_facility,
    availability_period_months,
    repayment_period,
    financing_product_type,
    profit_rate_pct,
    profit_rate_type,
    total_profit_bhd,
    total_profit_amount_calculated_bhd,
    commercial_name,
    program_id,
    application_status_id,
    application_loan_status_id,
    rm_user_id,
    assessor_user_id,
    approver_user_id,
    customer_user_id,
    program_type_name,
    hipo_classification_rm,
    hipo_classification_assessor,
    hipo_classification_approver,
    rm_name,
    assessor_name,
    approver_name,
    approved_pending_customer_on,
    saved_on,
    program_support_cap,
    financing_cap,
    financing_guarantee_cap,
    recommended_grant_rm,
    recommended_grant_assessor,
    assessment_support_cap_rm,
    assessment_support_cap_assessor,
    assessment_support_cap_approver,
    approved_grant,
    approved_grant_maximum,
    approved_financing_amount,
    approved_guarantee_amount,
    total_requested_grant,
    total_requested_financing,
    consumed_amount,
    remaining_amount,
    recommended_financing_rm,
    recommended_financing_assessor,
    recommended_financing_approver,
    loan_tenor,
    loan_profit_rate,
    loan_total_profit_amount,
    loan_monthly_installment,
    loan_grace_period,
    loan_start_date,
    loan_end_date,
    requested_support_no_of_employees,
    remarks_rm,
    remarks_assessor,
    remarks_approver,
    company_id,
    cr_license_number,
    commercial_name_english,
    commercial_name_arabic,
    main_record_name,
    iban_id,
    company_type,
    cr_status,
    have_audited_statement,
    subject_to_bahrainization,
    is_virtual,
    state,
    activity_sector,
    tamkeen_company_category,
    tamkeen_company_main_category,
    annual_revenue,
    audit_duration_years,
    issued_capital,
    total_bahraini_salaries,
    total_expatriates_salaries,
    total_bahraini_workers_sio,
    total_disabled_bahraini_workers_sio,
    total_non_bahraini_workers_lmra,
    current_bahrainization_rate_pct,
    target_bahrainization_rate_pct,
    bahrainization_rate_difference_pct,
    in_progress_requests,
    hw_to_work,
    active_workers,
    parallel_expat,
    address_block,
    address_building,
    address_flat,
    address_road_street,
    contact_first_name,
    contact_last_name,
    contact_cpr,
    contact_email,
    contact_mobile_number,
    contact_office_number,
    contact_nationality,
    contact_gender,
    secondary_contact_first_name,
    secondary_contact_last_name,
    secondary_contact_cpr,
    secondary_contact_designation,
    secondary_contact_email,
    secondary_contact_mobile_number,
    secondary_contact_office_number,
    secondary_contact_nationality,
    secondary_contact_gender,
    business_activity_count,
    isic4_codes,
    business_activities,
    bahraini_ownership_pct,
    gcc_ownership_pct,
    non_gcc_ownership_pct,
    bahrain_government_shareholder,
    bahrain_government_ownership_pct,
    youth_bahraini_shareholders_count,
    tmkn_cr,
    tmkn_activitysector,
    tmkn_totalbahrainino,
    tmkn_contactmobilenumber,
    tmkn_contactemail,
    tmkn_addressblock,
    modified_on,
    source_table,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    createdon,
    updatedon
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".customer_enterprise_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'application_id'),
        (2, 'guid'),
        (3, 'finalapprovedtkshareamt'),
        (4, 'application_no'),
        (5, 'programversionid'),
        (6, 'decisionprogramversionid'),
        (7, 'portaluserid'),
        (8, 'beneficiaryid'),
        (9, 'applicationstatusid'),
        (10, 'profilinginstanceguid'),
        (11, 'applicationinstanceformguid'),
        (12, 'applicationinstancedocguid'),
        (13, 'customerinstanceformguid'),
        (14, 'findatainstanceformguid'),
        (15, 'customerinstancedocguid'),
        (16, 'bindinginstancedocguid'),
        (17, 'amendapprovalinstancedocguid'),
        (18, 'hipoinstanceformguid'),
        (19, 'analysisinstanceformguid'),
        (20, 'ishipooptionid'),
        (21, 'programcap'),
        (22, 'programcapid'),
        (23, 'workflow_status'),
        (24, 'is_active'),
        (25, 'created_on'),
        (26, 'submitted_on'),
        (27, 'approved_on'),
        (28, 'customer_type'),
        (29, 'cap'),
        (30, 'tkshare_approved'),
        (31, 'amount_requested'),
        (32, 'remaining'),
        (33, 'customer_share'),
        (34, 'total_cost'),
        (35, 'start_date'),
        (36, 'end_date'),
        (37, 'contract_duration_months'),
        (38, 'is_hipo'),
        (39, 'monitoring_due_date'),
        (40, 'spending_period_end_date'),
        (41, 'cr_license_no_main'),
        (42, 'registration_date'),
        (43, 'cr_license_type'),
        (44, 'portal_user_name'),
        (45, 'program_name'),
        (46, 'total_male_shareholders'),
        (47, 'total_female_shareholders'),
        (48, 'total_bahraini_shareholders'),
        (49, 'total_gcc_shareholders'),
        (50, 'total_non_bahraini_shareholders'),
        (51, 'total_unspecified_shareholders'),
        (52, 'financing_tenor'),
        (53, 'approved_on_bank'),
        (54, 'workflow_status_detailed'),
        (55, 'is_eligible'),
        (56, 'withdrawn_on'),
        (57, 'confirmed_on'),
        (58, 'contact_designation'),
        (59, 'bank_name'),
        (60, 'breakup_facility_machinery_equipment_bhd'),
        (61, 'breakup_facility_technology_bhd'),
        (62, 'breakup_facility_marketing_branding_bhd'),
        (63, 'breakup_facility_fixtures_fittings_bhd'),
        (64, 'breakup_facility_other_bhd'),
        (65, 'disbursement_type'),
        (66, 'is_revolving_facility'),
        (67, 'availability_period_months'),
        (68, 'repayment_period'),
        (69, 'financing_product_type'),
        (70, 'profit_rate_pct'),
        (71, 'profit_rate_type'),
        (72, 'total_profit_bhd'),
        (73, 'total_profit_amount_calculated_bhd'),
        (74, 'commercial_name'),
        (75, 'program_id'),
        (76, 'application_status_id'),
        (77, 'application_loan_status_id'),
        (78, 'rm_user_id'),
        (79, 'assessor_user_id'),
        (80, 'approver_user_id'),
        (81, 'customer_user_id'),
        (82, 'program_type_name'),
        (83, 'hipo_classification_rm'),
        (84, 'hipo_classification_assessor'),
        (85, 'hipo_classification_approver'),
        (86, 'rm_name'),
        (87, 'assessor_name'),
        (88, 'approver_name'),
        (89, 'approved_pending_customer_on'),
        (90, 'saved_on'),
        (91, 'program_support_cap'),
        (92, 'financing_cap'),
        (93, 'financing_guarantee_cap'),
        (94, 'recommended_grant_rm'),
        (95, 'recommended_grant_assessor'),
        (96, 'assessment_support_cap_rm'),
        (97, 'assessment_support_cap_assessor'),
        (98, 'assessment_support_cap_approver'),
        (99, 'approved_grant'),
        (100, 'approved_grant_maximum'),
        (101, 'approved_financing_amount'),
        (102, 'approved_guarantee_amount'),
        (103, 'total_requested_grant'),
        (104, 'total_requested_financing'),
        (105, 'consumed_amount'),
        (106, 'remaining_amount'),
        (107, 'recommended_financing_rm'),
        (108, 'recommended_financing_assessor'),
        (109, 'recommended_financing_approver'),
        (110, 'loan_tenor'),
        (111, 'loan_profit_rate'),
        (112, 'loan_total_profit_amount'),
        (113, 'loan_monthly_installment'),
        (114, 'loan_grace_period'),
        (115, 'loan_start_date'),
        (116, 'loan_end_date'),
        (117, 'requested_support_no_of_employees'),
        (118, 'remarks_rm'),
        (119, 'remarks_assessor'),
        (120, 'remarks_approver'),
        (121, 'company_id'),
        (122, 'cr_license_number'),
        (123, 'commercial_name_english'),
        (124, 'commercial_name_arabic'),
        (125, 'main_record_name'),
        (126, 'iban_id'),
        (127, 'company_type'),
        (128, 'cr_status'),
        (129, 'have_audited_statement'),
        (130, 'subject_to_bahrainization'),
        (131, 'is_virtual'),
        (132, 'state'),
        (133, 'activity_sector'),
        (134, 'tamkeen_company_category'),
        (135, 'tamkeen_company_main_category'),
        (136, 'annual_revenue'),
        (137, 'audit_duration_years'),
        (138, 'issued_capital'),
        (139, 'total_bahraini_salaries'),
        (140, 'total_expatriates_salaries'),
        (141, 'total_bahraini_workers_sio'),
        (142, 'total_disabled_bahraini_workers_sio'),
        (143, 'total_non_bahraini_workers_lmra'),
        (144, 'current_bahrainization_rate_pct'),
        (145, 'target_bahrainization_rate_pct'),
        (146, 'bahrainization_rate_difference_pct'),
        (147, 'in_progress_requests'),
        (148, 'hw_to_work'),
        (149, 'active_workers'),
        (150, 'parallel_expat'),
        (151, 'address_block'),
        (152, 'address_building'),
        (153, 'address_flat'),
        (154, 'address_road_street'),
        (155, 'contact_first_name'),
        (156, 'contact_last_name'),
        (157, 'contact_cpr'),
        (158, 'contact_email'),
        (159, 'contact_mobile_number'),
        (160, 'contact_office_number'),
        (161, 'contact_nationality'),
        (162, 'contact_gender'),
        (163, 'secondary_contact_first_name'),
        (164, 'secondary_contact_last_name'),
        (165, 'secondary_contact_cpr'),
        (166, 'secondary_contact_designation'),
        (167, 'secondary_contact_email'),
        (168, 'secondary_contact_mobile_number'),
        (169, 'secondary_contact_office_number'),
        (170, 'secondary_contact_nationality'),
        (171, 'secondary_contact_gender'),
        (172, 'business_activity_count'),
        (173, 'isic4_codes'),
        (174, 'business_activities'),
        (175, 'bahraini_ownership_pct'),
        (176, 'gcc_ownership_pct'),
        (177, 'non_gcc_ownership_pct'),
        (178, 'bahrain_government_shareholder'),
        (179, 'bahrain_government_ownership_pct'),
        (180, 'youth_bahraini_shareholders_count'),
        (181, 'tmkn_cr'),
        (182, 'tmkn_activitysector'),
        (183, 'tmkn_totalbahrainino'),
        (184, 'tmkn_contactmobilenumber'),
        (185, 'tmkn_contactemail'),
        (186, 'tmkn_addressblock'),
        (187, 'modified_on'),
        (188, 'source_table'),
        (189, 'source_system_name'),
        (190, 'is_deleted'),
        (191, 'report_date'),
        (192, 'dbt_updated_at'),
        (193, 'createdon'),
        (194, 'updatedon')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'application_id'),
        (2, 'guid'),
        (3, 'finalapprovedtkshareamt'),
        (4, 'application_no'),
        (5, 'programversionid'),
        (6, 'decisionprogramversionid'),
        (7, 'portaluserid'),
        (8, 'beneficiaryid'),
        (9, 'applicationstatusid'),
        (10, 'profilinginstanceguid'),
        (11, 'applicationinstanceformguid'),
        (12, 'applicationinstancedocguid'),
        (13, 'customerinstanceformguid'),
        (14, 'findatainstanceformguid'),
        (15, 'customerinstancedocguid'),
        (16, 'bindinginstancedocguid'),
        (17, 'amendapprovalinstancedocguid'),
        (18, 'hipoinstanceformguid'),
        (19, 'analysisinstanceformguid'),
        (20, 'ishipooptionid'),
        (21, 'programcap'),
        (22, 'programcapid'),
        (23, 'workflow_status'),
        (24, 'is_active'),
        (25, 'created_on'),
        (26, 'submitted_on'),
        (27, 'approved_on'),
        (28, 'customer_type'),
        (29, 'cap'),
        (30, 'tkshare_approved'),
        (31, 'amount_requested'),
        (32, 'remaining'),
        (33, 'customer_share'),
        (34, 'total_cost'),
        (35, 'start_date'),
        (36, 'end_date'),
        (37, 'contract_duration_months'),
        (38, 'is_hipo'),
        (39, 'monitoring_due_date'),
        (40, 'spending_period_end_date'),
        (41, 'cr_license_no_main'),
        (42, 'registration_date'),
        (43, 'cr_license_type'),
        (44, 'portal_user_name'),
        (45, 'program_name'),
        (46, 'total_male_shareholders'),
        (47, 'total_female_shareholders'),
        (48, 'total_bahraini_shareholders'),
        (49, 'total_gcc_shareholders'),
        (50, 'total_non_bahraini_shareholders'),
        (51, 'total_unspecified_shareholders'),
        (52, 'financing_tenor'),
        (53, 'approved_on_bank'),
        (54, 'workflow_status_detailed'),
        (55, 'is_eligible'),
        (56, 'withdrawn_on'),
        (57, 'confirmed_on'),
        (58, 'contact_designation'),
        (59, 'bank_name'),
        (60, 'breakup_facility_machinery_equipment_bhd'),
        (61, 'breakup_facility_technology_bhd'),
        (62, 'breakup_facility_marketing_branding_bhd'),
        (63, 'breakup_facility_fixtures_fittings_bhd'),
        (64, 'breakup_facility_other_bhd'),
        (65, 'disbursement_type'),
        (66, 'is_revolving_facility'),
        (67, 'availability_period_months'),
        (68, 'repayment_period'),
        (69, 'financing_product_type'),
        (70, 'profit_rate_pct'),
        (71, 'profit_rate_type'),
        (72, 'total_profit_bhd'),
        (73, 'total_profit_amount_calculated_bhd'),
        (74, 'commercial_name'),
        (75, 'program_id'),
        (76, 'application_status_id'),
        (77, 'application_loan_status_id'),
        (78, 'rm_user_id'),
        (79, 'assessor_user_id'),
        (80, 'approver_user_id'),
        (81, 'customer_user_id'),
        (82, 'program_type_name'),
        (83, 'hipo_classification_rm'),
        (84, 'hipo_classification_assessor'),
        (85, 'hipo_classification_approver'),
        (86, 'rm_name'),
        (87, 'assessor_name'),
        (88, 'approver_name'),
        (89, 'approved_pending_customer_on'),
        (90, 'saved_on'),
        (91, 'program_support_cap'),
        (92, 'financing_cap'),
        (93, 'financing_guarantee_cap'),
        (94, 'recommended_grant_rm'),
        (95, 'recommended_grant_assessor'),
        (96, 'assessment_support_cap_rm'),
        (97, 'assessment_support_cap_assessor'),
        (98, 'assessment_support_cap_approver'),
        (99, 'approved_grant'),
        (100, 'approved_grant_maximum'),
        (101, 'approved_financing_amount'),
        (102, 'approved_guarantee_amount'),
        (103, 'total_requested_grant'),
        (104, 'total_requested_financing'),
        (105, 'consumed_amount'),
        (106, 'remaining_amount'),
        (107, 'recommended_financing_rm'),
        (108, 'recommended_financing_assessor'),
        (109, 'recommended_financing_approver'),
        (110, 'loan_tenor'),
        (111, 'loan_profit_rate'),
        (112, 'loan_total_profit_amount'),
        (113, 'loan_monthly_installment'),
        (114, 'loan_grace_period'),
        (115, 'loan_start_date'),
        (116, 'loan_end_date'),
        (117, 'requested_support_no_of_employees'),
        (118, 'remarks_rm'),
        (119, 'remarks_assessor'),
        (120, 'remarks_approver'),
        (121, 'company_id'),
        (122, 'cr_license_number'),
        (123, 'commercial_name_english'),
        (124, 'commercial_name_arabic'),
        (125, 'main_record_name'),
        (126, 'iban_id'),
        (127, 'company_type'),
        (128, 'cr_status'),
        (129, 'have_audited_statement'),
        (130, 'subject_to_bahrainization'),
        (131, 'is_virtual'),
        (132, 'state'),
        (133, 'activity_sector'),
        (134, 'tamkeen_company_category'),
        (135, 'tamkeen_company_main_category'),
        (136, 'annual_revenue'),
        (137, 'audit_duration_years'),
        (138, 'issued_capital'),
        (139, 'total_bahraini_salaries'),
        (140, 'total_expatriates_salaries'),
        (141, 'total_bahraini_workers_sio'),
        (142, 'total_disabled_bahraini_workers_sio'),
        (143, 'total_non_bahraini_workers_lmra'),
        (144, 'current_bahrainization_rate_pct'),
        (145, 'target_bahrainization_rate_pct'),
        (146, 'bahrainization_rate_difference_pct'),
        (147, 'in_progress_requests'),
        (148, 'hw_to_work'),
        (149, 'active_workers'),
        (150, 'parallel_expat'),
        (151, 'address_block'),
        (152, 'address_building'),
        (153, 'address_flat'),
        (154, 'address_road_street'),
        (155, 'contact_first_name'),
        (156, 'contact_last_name'),
        (157, 'contact_cpr'),
        (158, 'contact_email'),
        (159, 'contact_mobile_number'),
        (160, 'contact_office_number'),
        (161, 'contact_nationality'),
        (162, 'contact_gender'),
        (163, 'secondary_contact_first_name'),
        (164, 'secondary_contact_last_name'),
        (165, 'secondary_contact_cpr'),
        (166, 'secondary_contact_designation'),
        (167, 'secondary_contact_email'),
        (168, 'secondary_contact_mobile_number'),
        (169, 'secondary_contact_office_number'),
        (170, 'secondary_contact_nationality'),
        (171, 'secondary_contact_gender'),
        (172, 'business_activity_count'),
        (173, 'isic4_codes'),
        (174, 'business_activities'),
        (175, 'bahraini_ownership_pct'),
        (176, 'gcc_ownership_pct'),
        (177, 'non_gcc_ownership_pct'),
        (178, 'bahrain_government_shareholder'),
        (179, 'bahrain_government_ownership_pct'),
        (180, 'youth_bahraini_shareholders_count'),
        (181, 'tmkn_cr'),
        (182, 'tmkn_activitysector'),
        (183, 'tmkn_totalbahrainino'),
        (184, 'tmkn_contactmobilenumber'),
        (185, 'tmkn_contactemail'),
        (186, 'tmkn_addressblock'),
        (187, 'modified_on'),
        (188, 'source_table'),
        (189, 'source_system_name'),
        (190, 'is_deleted'),
        (191, 'report_date'),
        (192, 'dbt_updated_at'),
        (193, 'createdon'),
        (194, 'updatedon')
),

bronze_normalized AS (
    SELECT
        CAST("application_id" AS VARCHAR) AS "application_id",
        CAST("guid" AS VARCHAR) AS "guid",
        CAST("finalapprovedtkshareamt" AS VARCHAR) AS "finalapprovedtkshareamt",
        CAST("application_no" AS VARCHAR) AS "application_no",
        CAST("programversionid" AS VARCHAR) AS "programversionid",
        CAST("decisionprogramversionid" AS VARCHAR) AS "decisionprogramversionid",
        CAST("portaluserid" AS VARCHAR) AS "portaluserid",
        CAST("beneficiaryid" AS VARCHAR) AS "beneficiaryid",
        CAST("applicationstatusid" AS VARCHAR) AS "applicationstatusid",
        CAST("profilinginstanceguid" AS VARCHAR) AS "profilinginstanceguid",
        CAST("applicationinstanceformguid" AS VARCHAR) AS "applicationinstanceformguid",
        CAST("applicationinstancedocguid" AS VARCHAR) AS "applicationinstancedocguid",
        CAST("customerinstanceformguid" AS VARCHAR) AS "customerinstanceformguid",
        CAST("findatainstanceformguid" AS VARCHAR) AS "findatainstanceformguid",
        CAST("customerinstancedocguid" AS VARCHAR) AS "customerinstancedocguid",
        CAST("bindinginstancedocguid" AS VARCHAR) AS "bindinginstancedocguid",
        CAST("amendapprovalinstancedocguid" AS VARCHAR) AS "amendapprovalinstancedocguid",
        CAST("hipoinstanceformguid" AS VARCHAR) AS "hipoinstanceformguid",
        CAST("analysisinstanceformguid" AS VARCHAR) AS "analysisinstanceformguid",
        CAST("ishipooptionid" AS VARCHAR) AS "ishipooptionid",
        CAST("programcap" AS VARCHAR) AS "programcap",
        CAST("programcapid" AS VARCHAR) AS "programcapid",
        CAST("workflow_status" AS VARCHAR) AS "workflow_status",
        CAST("is_active" AS VARCHAR) AS "is_active",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("submitted_on" AS VARCHAR) AS "submitted_on",
        CAST("approved_on" AS VARCHAR) AS "approved_on",
        CAST("customer_type" AS VARCHAR) AS "customer_type",
        CAST("cap" AS VARCHAR) AS "cap",
        CAST("tkshare_approved" AS VARCHAR) AS "tkshare_approved",
        CAST("amount_requested" AS VARCHAR) AS "amount_requested",
        CAST("remaining" AS VARCHAR) AS "remaining",
        CAST("customer_share" AS VARCHAR) AS "customer_share",
        CAST("total_cost" AS VARCHAR) AS "total_cost",
        CAST("start_date" AS VARCHAR) AS "start_date",
        CAST("end_date" AS VARCHAR) AS "end_date",
        CAST("contract_duration_months" AS VARCHAR) AS "contract_duration_months",
        CAST("is_hipo" AS VARCHAR) AS "is_hipo",
        CAST("monitoring_due_date" AS VARCHAR) AS "monitoring_due_date",
        CAST("spending_period_end_date" AS VARCHAR) AS "spending_period_end_date",
        CAST("cr_license_no_main" AS VARCHAR) AS "cr_license_no_main",
        CAST("registration_date" AS VARCHAR) AS "registration_date",
        CAST("cr_license_type" AS VARCHAR) AS "cr_license_type",
        CAST("portal_user_name" AS VARCHAR) AS "portal_user_name",
        CAST("program_name" AS VARCHAR) AS "program_name",
        CAST("total_male_shareholders" AS VARCHAR) AS "total_male_shareholders",
        CAST("total_female_shareholders" AS VARCHAR) AS "total_female_shareholders",
        CAST("total_bahraini_shareholders" AS VARCHAR) AS "total_bahraini_shareholders",
        CAST("total_gcc_shareholders" AS VARCHAR) AS "total_gcc_shareholders",
        CAST("total_non_bahraini_shareholders" AS VARCHAR) AS "total_non_bahraini_shareholders",
        CAST("total_unspecified_shareholders" AS VARCHAR) AS "total_unspecified_shareholders",
        CAST("financing_tenor" AS VARCHAR) AS "financing_tenor",
        CAST("approved_on_bank" AS VARCHAR) AS "approved_on_bank",
        CAST("workflow_status_detailed" AS VARCHAR) AS "workflow_status_detailed",
        CAST("is_eligible" AS VARCHAR) AS "is_eligible",
        CAST("withdrawn_on" AS VARCHAR) AS "withdrawn_on",
        CAST("confirmed_on" AS VARCHAR) AS "confirmed_on",
        CAST("contact_designation" AS VARCHAR) AS "contact_designation",
        CAST("bank_name" AS VARCHAR) AS "bank_name",
        CAST("breakup_facility_machinery_equipment_bhd" AS VARCHAR) AS "breakup_facility_machinery_equipment_bhd",
        CAST("breakup_facility_technology_bhd" AS VARCHAR) AS "breakup_facility_technology_bhd",
        CAST("breakup_facility_marketing_branding_bhd" AS VARCHAR) AS "breakup_facility_marketing_branding_bhd",
        CAST("breakup_facility_fixtures_fittings_bhd" AS VARCHAR) AS "breakup_facility_fixtures_fittings_bhd",
        CAST("breakup_facility_other_bhd" AS VARCHAR) AS "breakup_facility_other_bhd",
        CAST("disbursement_type" AS VARCHAR) AS "disbursement_type",
        CAST("is_revolving_facility" AS VARCHAR) AS "is_revolving_facility",
        CAST("availability_period_months" AS VARCHAR) AS "availability_period_months",
        CAST("repayment_period" AS VARCHAR) AS "repayment_period",
        CAST("financing_product_type" AS VARCHAR) AS "financing_product_type",
        CAST("profit_rate_pct" AS VARCHAR) AS "profit_rate_pct",
        CAST("profit_rate_type" AS VARCHAR) AS "profit_rate_type",
        CAST("total_profit_bhd" AS VARCHAR) AS "total_profit_bhd",
        CAST("total_profit_amount_calculated_bhd" AS VARCHAR) AS "total_profit_amount_calculated_bhd",
        CAST("commercial_name" AS VARCHAR) AS "commercial_name",
        CAST("program_id" AS VARCHAR) AS "program_id",
        CAST("application_status_id" AS VARCHAR) AS "application_status_id",
        CAST("application_loan_status_id" AS VARCHAR) AS "application_loan_status_id",
        CAST("rm_user_id" AS VARCHAR) AS "rm_user_id",
        CAST("assessor_user_id" AS VARCHAR) AS "assessor_user_id",
        CAST("approver_user_id" AS VARCHAR) AS "approver_user_id",
        CAST("customer_user_id" AS VARCHAR) AS "customer_user_id",
        CAST("program_type_name" AS VARCHAR) AS "program_type_name",
        CAST("hipo_classification_rm" AS VARCHAR) AS "hipo_classification_rm",
        CAST("hipo_classification_assessor" AS VARCHAR) AS "hipo_classification_assessor",
        CAST("hipo_classification_approver" AS VARCHAR) AS "hipo_classification_approver",
        CAST("rm_name" AS VARCHAR) AS "rm_name",
        CAST("assessor_name" AS VARCHAR) AS "assessor_name",
        CAST("approver_name" AS VARCHAR) AS "approver_name",
        CAST("approved_pending_customer_on" AS VARCHAR) AS "approved_pending_customer_on",
        CAST("saved_on" AS VARCHAR) AS "saved_on",
        CAST("program_support_cap" AS VARCHAR) AS "program_support_cap",
        CAST("financing_cap" AS VARCHAR) AS "financing_cap",
        CAST("financing_guarantee_cap" AS VARCHAR) AS "financing_guarantee_cap",
        CAST("recommended_grant_rm" AS VARCHAR) AS "recommended_grant_rm",
        CAST("recommended_grant_assessor" AS VARCHAR) AS "recommended_grant_assessor",
        CAST("assessment_support_cap_rm" AS VARCHAR) AS "assessment_support_cap_rm",
        CAST("assessment_support_cap_assessor" AS VARCHAR) AS "assessment_support_cap_assessor",
        CAST("assessment_support_cap_approver" AS VARCHAR) AS "assessment_support_cap_approver",
        CAST("approved_grant" AS VARCHAR) AS "approved_grant",
        CAST("approved_grant_maximum" AS VARCHAR) AS "approved_grant_maximum",
        CAST("approved_financing_amount" AS VARCHAR) AS "approved_financing_amount",
        CAST("approved_guarantee_amount" AS VARCHAR) AS "approved_guarantee_amount",
        CAST("total_requested_grant" AS VARCHAR) AS "total_requested_grant",
        CAST("total_requested_financing" AS VARCHAR) AS "total_requested_financing",
        CAST("consumed_amount" AS VARCHAR) AS "consumed_amount",
        CAST("remaining_amount" AS VARCHAR) AS "remaining_amount",
        CAST("recommended_financing_rm" AS VARCHAR) AS "recommended_financing_rm",
        CAST("recommended_financing_assessor" AS VARCHAR) AS "recommended_financing_assessor",
        CAST("recommended_financing_approver" AS VARCHAR) AS "recommended_financing_approver",
        CAST("loan_tenor" AS VARCHAR) AS "loan_tenor",
        CAST("loan_profit_rate" AS VARCHAR) AS "loan_profit_rate",
        CAST("loan_total_profit_amount" AS VARCHAR) AS "loan_total_profit_amount",
        CAST("loan_monthly_installment" AS VARCHAR) AS "loan_monthly_installment",
        CAST("loan_grace_period" AS VARCHAR) AS "loan_grace_period",
        CAST("loan_start_date" AS VARCHAR) AS "loan_start_date",
        CAST("loan_end_date" AS VARCHAR) AS "loan_end_date",
        CAST("requested_support_no_of_employees" AS VARCHAR) AS "requested_support_no_of_employees",
        CAST("remarks_rm" AS VARCHAR) AS "remarks_rm",
        CAST("remarks_assessor" AS VARCHAR) AS "remarks_assessor",
        CAST("remarks_approver" AS VARCHAR) AS "remarks_approver",
        CAST("company_id" AS VARCHAR) AS "company_id",
        CAST("cr_license_number" AS VARCHAR) AS "cr_license_number",
        CAST("commercial_name_english" AS VARCHAR) AS "commercial_name_english",
        CAST("commercial_name_arabic" AS VARCHAR) AS "commercial_name_arabic",
        CAST("main_record_name" AS VARCHAR) AS "main_record_name",
        CAST("iban_id" AS VARCHAR) AS "iban_id",
        CAST("company_type" AS VARCHAR) AS "company_type",
        CAST("cr_status" AS VARCHAR) AS "cr_status",
        CAST("have_audited_statement" AS VARCHAR) AS "have_audited_statement",
        CAST("subject_to_bahrainization" AS VARCHAR) AS "subject_to_bahrainization",
        CAST("is_virtual" AS VARCHAR) AS "is_virtual",
        CAST("state" AS VARCHAR) AS "state",
        CAST("activity_sector" AS VARCHAR) AS "activity_sector",
        CAST("tamkeen_company_category" AS VARCHAR) AS "tamkeen_company_category",
        CAST("tamkeen_company_main_category" AS VARCHAR) AS "tamkeen_company_main_category",
        CAST("annual_revenue" AS VARCHAR) AS "annual_revenue",
        CAST("audit_duration_years" AS VARCHAR) AS "audit_duration_years",
        CAST("issued_capital" AS VARCHAR) AS "issued_capital",
        CAST("total_bahraini_salaries" AS VARCHAR) AS "total_bahraini_salaries",
        CAST("total_expatriates_salaries" AS VARCHAR) AS "total_expatriates_salaries",
        CAST("total_bahraini_workers_sio" AS VARCHAR) AS "total_bahraini_workers_sio",
        CAST("total_disabled_bahraini_workers_sio" AS VARCHAR) AS "total_disabled_bahraini_workers_sio",
        CAST("total_non_bahraini_workers_lmra" AS VARCHAR) AS "total_non_bahraini_workers_lmra",
        CAST("current_bahrainization_rate_pct" AS VARCHAR) AS "current_bahrainization_rate_pct",
        CAST("target_bahrainization_rate_pct" AS VARCHAR) AS "target_bahrainization_rate_pct",
        CAST("bahrainization_rate_difference_pct" AS VARCHAR) AS "bahrainization_rate_difference_pct",
        CAST("in_progress_requests" AS VARCHAR) AS "in_progress_requests",
        CAST("hw_to_work" AS VARCHAR) AS "hw_to_work",
        CAST("active_workers" AS VARCHAR) AS "active_workers",
        CAST("parallel_expat" AS VARCHAR) AS "parallel_expat",
        CAST("address_block" AS VARCHAR) AS "address_block",
        CAST("address_building" AS VARCHAR) AS "address_building",
        CAST("address_flat" AS VARCHAR) AS "address_flat",
        CAST("address_road_street" AS VARCHAR) AS "address_road_street",
        CAST("contact_first_name" AS VARCHAR) AS "contact_first_name",
        CAST("contact_last_name" AS VARCHAR) AS "contact_last_name",
        CAST("contact_cpr" AS VARCHAR) AS "contact_cpr",
        CAST("contact_email" AS VARCHAR) AS "contact_email",
        CAST("contact_mobile_number" AS VARCHAR) AS "contact_mobile_number",
        CAST("contact_office_number" AS VARCHAR) AS "contact_office_number",
        CAST("contact_nationality" AS VARCHAR) AS "contact_nationality",
        CAST("contact_gender" AS VARCHAR) AS "contact_gender",
        CAST("secondary_contact_first_name" AS VARCHAR) AS "secondary_contact_first_name",
        CAST("secondary_contact_last_name" AS VARCHAR) AS "secondary_contact_last_name",
        CAST("secondary_contact_cpr" AS VARCHAR) AS "secondary_contact_cpr",
        CAST("secondary_contact_designation" AS VARCHAR) AS "secondary_contact_designation",
        CAST("secondary_contact_email" AS VARCHAR) AS "secondary_contact_email",
        CAST("secondary_contact_mobile_number" AS VARCHAR) AS "secondary_contact_mobile_number",
        CAST("secondary_contact_office_number" AS VARCHAR) AS "secondary_contact_office_number",
        CAST("secondary_contact_nationality" AS VARCHAR) AS "secondary_contact_nationality",
        CAST("secondary_contact_gender" AS VARCHAR) AS "secondary_contact_gender",
        CAST("business_activity_count" AS VARCHAR) AS "business_activity_count",
        CAST("isic4_codes" AS VARCHAR) AS "isic4_codes",
        CAST("business_activities" AS VARCHAR) AS "business_activities",
        CAST("bahraini_ownership_pct" AS VARCHAR) AS "bahraini_ownership_pct",
        CAST("gcc_ownership_pct" AS VARCHAR) AS "gcc_ownership_pct",
        CAST("non_gcc_ownership_pct" AS VARCHAR) AS "non_gcc_ownership_pct",
        CAST("bahrain_government_shareholder" AS VARCHAR) AS "bahrain_government_shareholder",
        CAST("bahrain_government_ownership_pct" AS VARCHAR) AS "bahrain_government_ownership_pct",
        CAST("youth_bahraini_shareholders_count" AS VARCHAR) AS "youth_bahraini_shareholders_count",
        CAST("tmkn_cr" AS VARCHAR) AS "tmkn_cr",
        CAST("tmkn_activitysector" AS VARCHAR) AS "tmkn_activitysector",
        CAST("tmkn_totalbahrainino" AS VARCHAR) AS "tmkn_totalbahrainino",
        CAST("tmkn_contactmobilenumber" AS VARCHAR) AS "tmkn_contactmobilenumber",
        CAST("tmkn_contactemail" AS VARCHAR) AS "tmkn_contactemail",
        CAST("tmkn_addressblock" AS VARCHAR) AS "tmkn_addressblock",
        CAST("modified_on" AS VARCHAR) AS "modified_on",
        CAST("source_table" AS VARCHAR) AS "source_table",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("report_date" AS VARCHAR) AS "report_date",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("updatedon" AS VARCHAR) AS "updatedon"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("application_id" AS VARCHAR) AS "application_id",
        CAST("guid" AS VARCHAR) AS "guid",
        CAST("finalapprovedtkshareamt" AS VARCHAR) AS "finalapprovedtkshareamt",
        CAST("application_no" AS VARCHAR) AS "application_no",
        CAST("programversionid" AS VARCHAR) AS "programversionid",
        CAST("decisionprogramversionid" AS VARCHAR) AS "decisionprogramversionid",
        CAST("portaluserid" AS VARCHAR) AS "portaluserid",
        CAST("beneficiaryid" AS VARCHAR) AS "beneficiaryid",
        CAST("applicationstatusid" AS VARCHAR) AS "applicationstatusid",
        CAST("profilinginstanceguid" AS VARCHAR) AS "profilinginstanceguid",
        CAST("applicationinstanceformguid" AS VARCHAR) AS "applicationinstanceformguid",
        CAST("applicationinstancedocguid" AS VARCHAR) AS "applicationinstancedocguid",
        CAST("customerinstanceformguid" AS VARCHAR) AS "customerinstanceformguid",
        CAST("findatainstanceformguid" AS VARCHAR) AS "findatainstanceformguid",
        CAST("customerinstancedocguid" AS VARCHAR) AS "customerinstancedocguid",
        CAST("bindinginstancedocguid" AS VARCHAR) AS "bindinginstancedocguid",
        CAST("amendapprovalinstancedocguid" AS VARCHAR) AS "amendapprovalinstancedocguid",
        CAST("hipoinstanceformguid" AS VARCHAR) AS "hipoinstanceformguid",
        CAST("analysisinstanceformguid" AS VARCHAR) AS "analysisinstanceformguid",
        CAST("ishipooptionid" AS VARCHAR) AS "ishipooptionid",
        CAST("programcap" AS VARCHAR) AS "programcap",
        CAST("programcapid" AS VARCHAR) AS "programcapid",
        CAST("workflow_status" AS VARCHAR) AS "workflow_status",
        CAST("is_active" AS VARCHAR) AS "is_active",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("submitted_on" AS VARCHAR) AS "submitted_on",
        CAST("approved_on" AS VARCHAR) AS "approved_on",
        CAST("customer_type" AS VARCHAR) AS "customer_type",
        CAST("cap" AS VARCHAR) AS "cap",
        CAST("tkshare_approved" AS VARCHAR) AS "tkshare_approved",
        CAST("amount_requested" AS VARCHAR) AS "amount_requested",
        CAST("remaining" AS VARCHAR) AS "remaining",
        CAST("customer_share" AS VARCHAR) AS "customer_share",
        CAST("total_cost" AS VARCHAR) AS "total_cost",
        CAST("start_date" AS VARCHAR) AS "start_date",
        CAST("end_date" AS VARCHAR) AS "end_date",
        CAST("contract_duration_months" AS VARCHAR) AS "contract_duration_months",
        CAST("is_hipo" AS VARCHAR) AS "is_hipo",
        CAST("monitoring_due_date" AS VARCHAR) AS "monitoring_due_date",
        CAST("spending_period_end_date" AS VARCHAR) AS "spending_period_end_date",
        CAST("cr_license_no_main" AS VARCHAR) AS "cr_license_no_main",
        CAST("registration_date" AS VARCHAR) AS "registration_date",
        CAST("cr_license_type" AS VARCHAR) AS "cr_license_type",
        CAST("portal_user_name" AS VARCHAR) AS "portal_user_name",
        CAST("program_name" AS VARCHAR) AS "program_name",
        CAST("total_male_shareholders" AS VARCHAR) AS "total_male_shareholders",
        CAST("total_female_shareholders" AS VARCHAR) AS "total_female_shareholders",
        CAST("total_bahraini_shareholders" AS VARCHAR) AS "total_bahraini_shareholders",
        CAST("total_gcc_shareholders" AS VARCHAR) AS "total_gcc_shareholders",
        CAST("total_non_bahraini_shareholders" AS VARCHAR) AS "total_non_bahraini_shareholders",
        CAST("total_unspecified_shareholders" AS VARCHAR) AS "total_unspecified_shareholders",
        CAST("financing_tenor" AS VARCHAR) AS "financing_tenor",
        CAST("approved_on_bank" AS VARCHAR) AS "approved_on_bank",
        CAST("workflow_status_detailed" AS VARCHAR) AS "workflow_status_detailed",
        CAST("is_eligible" AS VARCHAR) AS "is_eligible",
        CAST("withdrawn_on" AS VARCHAR) AS "withdrawn_on",
        CAST("confirmed_on" AS VARCHAR) AS "confirmed_on",
        CAST("contact_designation" AS VARCHAR) AS "contact_designation",
        CAST("bank_name" AS VARCHAR) AS "bank_name",
        CAST("breakup_facility_machinery_equipment_bhd" AS VARCHAR) AS "breakup_facility_machinery_equipment_bhd",
        CAST("breakup_facility_technology_bhd" AS VARCHAR) AS "breakup_facility_technology_bhd",
        CAST("breakup_facility_marketing_branding_bhd" AS VARCHAR) AS "breakup_facility_marketing_branding_bhd",
        CAST("breakup_facility_fixtures_fittings_bhd" AS VARCHAR) AS "breakup_facility_fixtures_fittings_bhd",
        CAST("breakup_facility_other_bhd" AS VARCHAR) AS "breakup_facility_other_bhd",
        CAST("disbursement_type" AS VARCHAR) AS "disbursement_type",
        CAST("is_revolving_facility" AS VARCHAR) AS "is_revolving_facility",
        CAST("availability_period_months" AS VARCHAR) AS "availability_period_months",
        CAST("repayment_period" AS VARCHAR) AS "repayment_period",
        CAST("financing_product_type" AS VARCHAR) AS "financing_product_type",
        CAST("profit_rate_pct" AS VARCHAR) AS "profit_rate_pct",
        CAST("profit_rate_type" AS VARCHAR) AS "profit_rate_type",
        CAST("total_profit_bhd" AS VARCHAR) AS "total_profit_bhd",
        CAST("total_profit_amount_calculated_bhd" AS VARCHAR) AS "total_profit_amount_calculated_bhd",
        CAST("commercial_name" AS VARCHAR) AS "commercial_name",
        CAST("program_id" AS VARCHAR) AS "program_id",
        CAST("application_status_id" AS VARCHAR) AS "application_status_id",
        CAST("application_loan_status_id" AS VARCHAR) AS "application_loan_status_id",
        CAST("rm_user_id" AS VARCHAR) AS "rm_user_id",
        CAST("assessor_user_id" AS VARCHAR) AS "assessor_user_id",
        CAST("approver_user_id" AS VARCHAR) AS "approver_user_id",
        CAST("customer_user_id" AS VARCHAR) AS "customer_user_id",
        CAST("program_type_name" AS VARCHAR) AS "program_type_name",
        CAST("hipo_classification_rm" AS VARCHAR) AS "hipo_classification_rm",
        CAST("hipo_classification_assessor" AS VARCHAR) AS "hipo_classification_assessor",
        CAST("hipo_classification_approver" AS VARCHAR) AS "hipo_classification_approver",
        CAST("rm_name" AS VARCHAR) AS "rm_name",
        CAST("assessor_name" AS VARCHAR) AS "assessor_name",
        CAST("approver_name" AS VARCHAR) AS "approver_name",
        CAST("approved_pending_customer_on" AS VARCHAR) AS "approved_pending_customer_on",
        CAST("saved_on" AS VARCHAR) AS "saved_on",
        CAST("program_support_cap" AS VARCHAR) AS "program_support_cap",
        CAST("financing_cap" AS VARCHAR) AS "financing_cap",
        CAST("financing_guarantee_cap" AS VARCHAR) AS "financing_guarantee_cap",
        CAST("recommended_grant_rm" AS VARCHAR) AS "recommended_grant_rm",
        CAST("recommended_grant_assessor" AS VARCHAR) AS "recommended_grant_assessor",
        CAST("assessment_support_cap_rm" AS VARCHAR) AS "assessment_support_cap_rm",
        CAST("assessment_support_cap_assessor" AS VARCHAR) AS "assessment_support_cap_assessor",
        CAST("assessment_support_cap_approver" AS VARCHAR) AS "assessment_support_cap_approver",
        CAST("approved_grant" AS VARCHAR) AS "approved_grant",
        CAST("approved_grant_maximum" AS VARCHAR) AS "approved_grant_maximum",
        CAST("approved_financing_amount" AS VARCHAR) AS "approved_financing_amount",
        CAST("approved_guarantee_amount" AS VARCHAR) AS "approved_guarantee_amount",
        CAST("total_requested_grant" AS VARCHAR) AS "total_requested_grant",
        CAST("total_requested_financing" AS VARCHAR) AS "total_requested_financing",
        CAST("consumed_amount" AS VARCHAR) AS "consumed_amount",
        CAST("remaining_amount" AS VARCHAR) AS "remaining_amount",
        CAST("recommended_financing_rm" AS VARCHAR) AS "recommended_financing_rm",
        CAST("recommended_financing_assessor" AS VARCHAR) AS "recommended_financing_assessor",
        CAST("recommended_financing_approver" AS VARCHAR) AS "recommended_financing_approver",
        CAST("loan_tenor" AS VARCHAR) AS "loan_tenor",
        CAST("loan_profit_rate" AS VARCHAR) AS "loan_profit_rate",
        CAST("loan_total_profit_amount" AS VARCHAR) AS "loan_total_profit_amount",
        CAST("loan_monthly_installment" AS VARCHAR) AS "loan_monthly_installment",
        CAST("loan_grace_period" AS VARCHAR) AS "loan_grace_period",
        CAST("loan_start_date" AS VARCHAR) AS "loan_start_date",
        CAST("loan_end_date" AS VARCHAR) AS "loan_end_date",
        CAST("requested_support_no_of_employees" AS VARCHAR) AS "requested_support_no_of_employees",
        CAST("remarks_rm" AS VARCHAR) AS "remarks_rm",
        CAST("remarks_assessor" AS VARCHAR) AS "remarks_assessor",
        CAST("remarks_approver" AS VARCHAR) AS "remarks_approver",
        CAST("company_id" AS VARCHAR) AS "company_id",
        CAST("cr_license_number" AS VARCHAR) AS "cr_license_number",
        CAST("commercial_name_english" AS VARCHAR) AS "commercial_name_english",
        CAST("commercial_name_arabic" AS VARCHAR) AS "commercial_name_arabic",
        CAST("main_record_name" AS VARCHAR) AS "main_record_name",
        CAST("iban_id" AS VARCHAR) AS "iban_id",
        CAST("company_type" AS VARCHAR) AS "company_type",
        CAST("cr_status" AS VARCHAR) AS "cr_status",
        CAST("have_audited_statement" AS VARCHAR) AS "have_audited_statement",
        CAST("subject_to_bahrainization" AS VARCHAR) AS "subject_to_bahrainization",
        CAST("is_virtual" AS VARCHAR) AS "is_virtual",
        CAST("state" AS VARCHAR) AS "state",
        CAST("activity_sector" AS VARCHAR) AS "activity_sector",
        CAST("tamkeen_company_category" AS VARCHAR) AS "tamkeen_company_category",
        CAST("tamkeen_company_main_category" AS VARCHAR) AS "tamkeen_company_main_category",
        CAST("annual_revenue" AS VARCHAR) AS "annual_revenue",
        CAST("audit_duration_years" AS VARCHAR) AS "audit_duration_years",
        CAST("issued_capital" AS VARCHAR) AS "issued_capital",
        CAST("total_bahraini_salaries" AS VARCHAR) AS "total_bahraini_salaries",
        CAST("total_expatriates_salaries" AS VARCHAR) AS "total_expatriates_salaries",
        CAST("total_bahraini_workers_sio" AS VARCHAR) AS "total_bahraini_workers_sio",
        CAST("total_disabled_bahraini_workers_sio" AS VARCHAR) AS "total_disabled_bahraini_workers_sio",
        CAST("total_non_bahraini_workers_lmra" AS VARCHAR) AS "total_non_bahraini_workers_lmra",
        CAST("current_bahrainization_rate_pct" AS VARCHAR) AS "current_bahrainization_rate_pct",
        CAST("target_bahrainization_rate_pct" AS VARCHAR) AS "target_bahrainization_rate_pct",
        CAST("bahrainization_rate_difference_pct" AS VARCHAR) AS "bahrainization_rate_difference_pct",
        CAST("in_progress_requests" AS VARCHAR) AS "in_progress_requests",
        CAST("hw_to_work" AS VARCHAR) AS "hw_to_work",
        CAST("active_workers" AS VARCHAR) AS "active_workers",
        CAST("parallel_expat" AS VARCHAR) AS "parallel_expat",
        CAST("address_block" AS VARCHAR) AS "address_block",
        CAST("address_building" AS VARCHAR) AS "address_building",
        CAST("address_flat" AS VARCHAR) AS "address_flat",
        CAST("address_road_street" AS VARCHAR) AS "address_road_street",
        CAST("contact_first_name" AS VARCHAR) AS "contact_first_name",
        CAST("contact_last_name" AS VARCHAR) AS "contact_last_name",
        CAST("contact_cpr" AS VARCHAR) AS "contact_cpr",
        CAST("contact_email" AS VARCHAR) AS "contact_email",
        CAST("contact_mobile_number" AS VARCHAR) AS "contact_mobile_number",
        CAST("contact_office_number" AS VARCHAR) AS "contact_office_number",
        CAST("contact_nationality" AS VARCHAR) AS "contact_nationality",
        CAST("contact_gender" AS VARCHAR) AS "contact_gender",
        CAST("secondary_contact_first_name" AS VARCHAR) AS "secondary_contact_first_name",
        CAST("secondary_contact_last_name" AS VARCHAR) AS "secondary_contact_last_name",
        CAST("secondary_contact_cpr" AS VARCHAR) AS "secondary_contact_cpr",
        CAST("secondary_contact_designation" AS VARCHAR) AS "secondary_contact_designation",
        CAST("secondary_contact_email" AS VARCHAR) AS "secondary_contact_email",
        CAST("secondary_contact_mobile_number" AS VARCHAR) AS "secondary_contact_mobile_number",
        CAST("secondary_contact_office_number" AS VARCHAR) AS "secondary_contact_office_number",
        CAST("secondary_contact_nationality" AS VARCHAR) AS "secondary_contact_nationality",
        CAST("secondary_contact_gender" AS VARCHAR) AS "secondary_contact_gender",
        CAST("business_activity_count" AS VARCHAR) AS "business_activity_count",
        CAST("isic4_codes" AS VARCHAR) AS "isic4_codes",
        CAST("business_activities" AS VARCHAR) AS "business_activities",
        CAST("bahraini_ownership_pct" AS VARCHAR) AS "bahraini_ownership_pct",
        CAST("gcc_ownership_pct" AS VARCHAR) AS "gcc_ownership_pct",
        CAST("non_gcc_ownership_pct" AS VARCHAR) AS "non_gcc_ownership_pct",
        CAST("bahrain_government_shareholder" AS VARCHAR) AS "bahrain_government_shareholder",
        CAST("bahrain_government_ownership_pct" AS VARCHAR) AS "bahrain_government_ownership_pct",
        CAST("youth_bahraini_shareholders_count" AS VARCHAR) AS "youth_bahraini_shareholders_count",
        CAST("tmkn_cr" AS VARCHAR) AS "tmkn_cr",
        CAST("tmkn_activitysector" AS VARCHAR) AS "tmkn_activitysector",
        CAST("tmkn_totalbahrainino" AS VARCHAR) AS "tmkn_totalbahrainino",
        CAST("tmkn_contactmobilenumber" AS VARCHAR) AS "tmkn_contactmobilenumber",
        CAST("tmkn_contactemail" AS VARCHAR) AS "tmkn_contactemail",
        CAST("tmkn_addressblock" AS VARCHAR) AS "tmkn_addressblock",
        CAST("modified_on" AS VARCHAR) AS "modified_on",
        CAST("source_table" AS VARCHAR) AS "source_table",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("report_date" AS VARCHAR) AS "report_date",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("updatedon" AS VARCHAR) AS "updatedon"
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
        'customer_enterprise_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'customer_enterprise_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'customer_enterprise_base' AS table_name,
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
        'customer_enterprise_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'customer_enterprise_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
