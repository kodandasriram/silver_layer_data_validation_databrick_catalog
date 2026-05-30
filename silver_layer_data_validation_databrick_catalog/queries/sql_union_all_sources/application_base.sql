WITH
bronze_layer AS (
WITH bronze_raw AS (
-- Bronze-layer UNION ALL for application_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: application_base_union all.sql.
-- Source CTEs preserve the standalone OS2, OS1, and MIS joins/functionality,
-- then the dbt union mapping supplies typed NULLs where a source does not carry a column.

WITH os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : APPLICATION_BASE
   Description   : This model extracts and transforms Neo Tamkeen application
                   data from the NEO2 (OS2) Bronze Layer and loads it into the
                   APPLICATION_BASE target table as part of the Silver Layer
                   data pipeline.

                   The model captures application-level details including
                   program information, customer and enterprise details,
                   application status, contract lifecycle dates, financial
                   metrics, monitoring and claiming periods, Tamkeen share
                   amounts, approval information, and HiPo classification.

                   The model enriches application data by joining multiple
                   reference and master tables including program version,
                   program master, customer profile, customer details,
                   application status, individual customer information,
                   and company information.

                   Timestamp fields are standardized by:
                   - Converting invalid sentinel dates
                     (1900-01-01 00:00:00) to NULL
                   - Applying +3 hour timezone adjustment to align with
                     Bahrain local time.

                   Draft applications are excluded from the final dataset.

   Source Tables : neo2.OSUSR_NTP_APPLICATION
                   neo2.OSUSR_3QQ_PROGRAMVERSION
                   neo2.OSUSR_3QQ_PROGRAM
                   neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                   neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                   neo2.OSUSR_ZMZ_CUSTOMER
                   neo2.OSUSR_398_APPLICATIONSTATUS
                   neo2.OSUSR_ZMZ_INDIVIDUAL
                   neo2.OSUSR_ZMZ_COMPANY

   Target Table  : APPLICATION_BASE

   Load Type     : Full Load
   Materialized  : Table
   Format        : PARQUET
   Tags          : silver, neo_tamkeen, application

   Business Rules:
   ---------------------------------------------------------------------------
   1. Applications with status = 'Draft' are excluded.
   2. Sentinel timestamps (1900-01-01 00:00:00) are converted to NULL.
   3. All valid timestamps are adjusted by +3 hours.
   4. CPR Number is derived for Individuals.
   5. CR/License Code is derived for Companies.
   6. HiPo option values are translated as:
        1 = HiPo
        2 = Non-HiPo

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author        | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-05-12   | Siva       | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/

WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATEDON DESC NULLS LAST, CREATEDON DESC NULLS LAST) AS RNK
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION
)

SELECT APP.id,
    ProgVer.COMMERCIALNAME_EN                              AS program_name,
    Program.PROFILETYPEID                                 AS program_type,
    APP.REFERENCENUMBER                                   AS reference_number,
    APST.LABEL                                            AS application_status,

    CASE 
        WHEN APP.CUSTOMERTYPEID = 'IND' 
            THEN IND.CPRNUMBER
        ELSE CMP.CODE
    END                                                   AS cr_license_cpr,

    CUS.NAMEEN                                            AS customer_enterprise_name,

    CASE 
        WHEN CAST(APP.APPROVEDON AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.APPROVEDON + INTERVAL '3' HOUR
    END                                                   AS approved_on_date,

    CASE 
        WHEN CAST(APP.STARTON AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.STARTON + INTERVAL '3' HOUR
    END                                                   AS contract_start_date,

    CASE 
        WHEN CAST(APP.MONITORINGDUEDATE AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.MONITORINGDUEDATE + INTERVAL '3' HOUR
    END                                                   AS monitoring_due_date,

    CASE 
        WHEN CAST(APP.ENDON AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.ENDON + INTERVAL '3' HOUR
    END                                                   AS contract_end_date,

    APP.TKSHAREAMT                                        AS total_approved_amount_tamkeen_share_old,

   --- APP.FINALAPPROVEDTKSHAREAMT                           AS final_amount_tamkeen_share,
        CAST (NULL AS STRING) AS finalapprovedtkshareamt,
            CAST (NULL AS STRING) AS final_amount_tamkeen_share,

    CASE 
        WHEN CAST(APP.CREATEDON AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.CREATEDON + INTERVAL '3' HOUR
    END                                                   AS created_on,

    CASE 
        WHEN CAST(APP.SUBMITTEDON AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.SUBMITTEDON + INTERVAL '3' HOUR
    END                                                   AS submitted_on,

    CASE 
        WHEN CAST(APP.SPENDINGPERIODDUEDATE AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
            THEN NULL
        ELSE APP.SPENDINGPERIODDUEDATE + INTERVAL '3' HOUR
    END                                                   AS spending_period_end_date,

    -- CASE 
    --     WHEN CAST(APP.APPROVALLETTERACCEPTEDON AS TIMESTAMP) = TIMESTAMP '1900-01-01 00:00:00'
    --         THEN NULL
    --     ELSE APP.APPROVALLETTERACCEPTEDON + INTERVAL '3' HOUR
    -- END                                                   AS approval_letter_confirmed,
    CAST (NULL AS STRING) AS approval_letter_confirmed,
    CASE 
        WHEN APP.ISHIPOOPTIONID = 1 THEN 'HiPo'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Non-HiPo'
        ELSE NULL
    END                                                   AS is_hipo_application,
    app.programcap                                              AS programcap,

    app.applicationcap                                          AS applicationcap,

    app.tkshareamt                                              AS tkshareamt,

    app.applicationcapunutilized                                AS applicationcapunutilized,

    app.customershareamt                                        AS customershareamt,

    app.totalcostwvat                                           AS totalcostwvat,
    CASE
        WHEN app.starton = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE app.starton + INTERVAL '3' HOUR
    END                                                         AS starton,

    CASE
        WHEN app.endon = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE app.endon + INTERVAL '3' HOUR
    END                                                         AS endon,

    CASE
        WHEN app.monitoringduedate = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE app.monitoringduedate + INTERVAL '3' HOUR
    END                                                         AS monitoringduedate,

    CASE
        WHEN app.spendingperiodduedate = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE app.spendingperiodduedate + INTERVAL '3' HOUR    
    END                                                         AS spendingperiodduedate,    
    CASE        
        WHEN app.claimingperiodduedate = TIMESTAMP '1900-01-01 00:00:00'        
        THEN NULL        ELSE app.claimingperiodduedate + INTERVAL '3' HOUR    
    END                                                         AS claimingperiodduedate,    
    app.duration                                                AS duration,    
    app.isactive                                                AS isactive,    
    app.createdby                                               AS createdby,    
    CASE        
        WHEN app.createdon = TIMESTAMP '1900-01-01 00:00:00'        
        THEN NULL        ELSE app.createdon + INTERVAL '3' HOUR    
    END                                                         AS createdon,    
    app.updatedby                                               AS updatedby,    
    CASE        
        WHEN app.updatedon = TIMESTAMP '1900-01-01 00:00:00'        
        THEN NULL        ELSE app.updatedon + INTERVAL '3' HOUR    
    END                                                         AS updatedon,    
    CASE        
        WHEN app.submittedon = TIMESTAMP '1900-01-01 00:00:00'        
        THEN NULL        ELSE app.submittedon + INTERVAL '3' HOUR    
    END                                                         AS submittedon,    
    CASE        
        WHEN app.approvedon = TIMESTAMP '1900-01-01 00:00:00'        
        THEN NULL        ELSE app.approvedon + INTERVAL '3' HOUR    
    END    AS approvedon,    
        app.amendappinstancedocgudi_ar                              AS amendappinstancedocgudi_ar,    
        app.haswagesupportmolemployees                              AS haswagesupportmolemployees,    
        app.calculatedeconomicvalue                                 AS calculatedeconomicvalue,    
        app.calculatedgrantamount                                   AS calculatedgrantamount,    
    'NEO2' AS source_system_name,
     FALSE AS is_deleted,
     CURRENT_DATE AS report_date,
     CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at

FROM CTE AS APP

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION ProgVer
    ON APP.PROGRAMVERSIONID = ProgVer.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAM Program
    ON ProgVer.PROGRAMID = Program.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
    ON APPCUS.APPLICATIONID = APP.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
    ON APPCUS.CUSTOMERPROFILEID = CUSPROF.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER CUS
    ON CUSPROF.CUSTOMERID = CUS.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS APST
    ON APP.APPLICATIONSTATUSID = APST.CODE

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_INDIVIDUAL IND
    ON CUSPROF.CUSTOMERID = IND.ID

LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_COMPANY CMP
    ON CUSPROF.CUSTOMERID = CMP.ID

WHERE APST.LABEL <> 'Draft' and APP.RNK=1
),
os1_source AS (
/*
============================================================================
silver_application_os1.sql
============================================================================
Per-source intermediate Silver model for the Application domain ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â OS1 only.

Sources (Application domain):
  ÃƒÂ¢Ã‹Å“Ã¢â‚¬Â¦ OSUSR_PX1_APPLICATION                 ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ anchor: the application entity
    OSUSR_PX1_APPLICATIONSUPPORTDETAILS   ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 1:1 wide attribute extension
                                              (RM/Assessor/Approver scoring,
                                              financing details, grant amounts ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â
                                              ~40 columns)
    OSUSR_PX1_APPLICATIONSYB              ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 1:1 SYB-specific attributes
                                              (turnover, sector ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â used for
                                              Start-Your-Business applications)
    OSUSR_PX1_APPLICATIONINTERNALSTATUSUPDATES21 ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ many:1 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â internal status
                                              audit trail; here we pick the
                                              LATEST per application
    OSUSR_PX1_CUSTOMERTRAINING            ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 1:1 customer-training details
    ossys_User (ÃƒÆ’Ã¢â‚¬â€3)                       ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ RM, Assessor, Approver people

  Lookup tables joined inline:
    OSUSR_PX1_PROGRAM, OSUSR_PX1_PROGRAMTYPE,
    OSUSR_PX1_APPLICATIONTYPE, OSUSR_PX1_APPLICATIONSTATUS,
    OSUSR_PX1_APPLICANTTYPE, OSUSR_PX1_TURNOVERVALUE, OSUSR_PX1_SECTOR,
    OSUSR_PX1_ASSESSORRECOMENDATION, OSUSR_PX1_BANKNAMES,
    OSUSR_PX1_MODEOFDELIVERY, OSUSR_PX1_TRAININGPROGRAM,
    OSUSR_PX1_TRAININGPROGRAMTYPE, OSUSR_PX1_TYPEOFTRAININGPROVIDER,
    OSUSR_PX1_APPLICATIONLOANSTATUS, OSUSR_PX1_APPLICATIONINTERNALSTATUSES21

Reference SPs:
  - RPT-152_neoTamkeen_Applications        (the canonical wide application view)
  - RPT-156_neoTamkeen_Customer_Contact    (slice ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â application + user + ext)

Notes:
  - The customer-contact data (RPT-156) is folded into this same Silver model
    rather than being a separate file. It's the same entity, just a thinner
    slice. The user / extension columns are added inline.
  - The 40 support-details columns make this file wide but they're all 1:1
    with the application ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â no duplication risk.
  - Internal status: the source SP uses a temp-table to find the latest update
    per application via MAX(CREATEDON). Here we replace that with a CTE using
    QUALIFY ROW_NUMBER() ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â same logic, set-based, idiomatic for Trino.
  - Cross-domain joins to Payment / Items / etc. are NOT performed here;
    those domains have their own Silver models that preserve APPLICATIONID.
============================================================================
*/


-- ============================================================================
-- Latest internal status update per application
-- Replaces the SP's #Int_St temp table + correlated re-join
-- ============================================================================
WITH latest_internal_status AS (
    SELECT
        APPLICATIONID                                   AS application_id,
        STATUS                                          AS internal_status_id
    FROM (
        SELECT
            APPLICATIONID,
            STATUS,
            ROW_NUMBER() OVER (
                PARTITION BY APPLICATIONID
                ORDER BY CREATEDON DESC
            ) AS rn
        FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONINTERNALSTATUSUPDATES
    ) ranked
    WHERE rn = 1
)


SELECT
    'OSUSR_PX1_APPLICATION' AS os1_source_table,

    -- Identifiers
    app.ID                                                                AS application_id,
    app.IDENTIFIER                                                        AS application_no,
    app.CR                                                                AS cr_license_no,
    app.COMMERCIALNAME                                                    AS commercial_name,

    -- Foreign keys preserved for cross-domain re-joining
    app.PROGRAMID                                                         AS program_id,
    app.APPLICATIONSTATUSID                                               AS application_status_id,
    app.APPLICATIONTYPEID                                                 AS application_type_id,
    app.ASSESSORRECOMENDATION                                             AS assessor_recommendation_id,
    app.APPLICATIONLOANSTATUSID                                           AS application_loan_status_id,
    app.USERID                                                            AS customer_user_id,
    app.RMID                                                              AS rm_user_id,
    app.ASSESSORID                                                        AS assessor_user_id,
    app.APPROVERID                                                        AS approver_user_id,

    -- Decoded labels (program / type / status / classification)
    prog.PROGRAMNAME                                                      AS program_name,
    prog_typ.LABEL                                                        AS program_type_name,
    --typ.LABEL                                                             AS application_type,
    CAST(NULL AS STRING) AS application_type,
    stus.LABEL                                                            AS application_status,
    --AR.LABEL                                                              AS assessor_recommendation,
    CAST(NULL AS STRING) AS assessor_recommendation,
    CAST(NULL AS STRING) AS financing_approved_loan_status,
    --loanStat.LABEL                                                        AS financing_approved_loan_status,
    Int_ST.LABEL                                                          AS internal_status,

    -- SYB / Sector / Turnover (1:1 with application)
    ---sec.SECTOR                                                            AS sector,
    --trnovr.LABEL                                                          AS turnover,
    CAST(NULL AS STRING) AS sector,
    CAST(NULL AS STRING) AS turnover,
    -- Application timeline
    app.CREATEDON                                                         AS created_on,
    app.APPROVEDON                                                        AS approved_on,
    app.CONTRACTSTARTDATE                                                 AS contract_start_date,
    app.CONTRACTENDDATE                                                   AS contract_end_date,
    app.DATEAPPROVEDPENDINGCUSTOMER                                       AS approve_pending_customer_confirmation_on,
    app.SAVEDON                                                           AS saved_on,
    app.SPENDINGPERIODENDDATE                                             AS spending_period_end_date,

    -- Financing flags
    COALESCE(app.ISFINANCINGAPPROVED,      FALSE)  AS is_financing_approved,

    -- Support details (40 columns from OSUSR_PX1_APPLICATIONSUPPORTDETAILS)
    -- These are 1:1 with the application
    detls.PROGRAMSUPPORTCAP                                               AS program_support_cap,
    detls.FINANCINGCAP                                                    AS financing_cap,
    detls.FINANCINGGUARANTEECAP                                           AS financing_guarantee_cap,
    detls.APPROVEDFINANCINGAMOUNT                                         AS approved_financing_amount,
    detls.APPROVEDGUARANTEEAMOUNT                                         AS approved_guarantee_amount,
    detls.LOANTENOR                                                       AS loan_tenor,
    detls.INTERESTRATE                                                    AS interest_rate,
    detls.LOANSTARTDATE                                                   AS loan_start_date,
    detls.LOANENDDATE                                                     AS loan_end_date,
    detls.TOTALINTERESTAMOUNT                                             AS total_interest_amount,
    detls.GRACEPERIOD                                                     AS grace_period,
    detls.MONTHLYINSTALLMENT                                              AS monthly_installment,
    detls.TOTALREQUESTED                                                  AS total_requested,
    detls.APPROVEDGRANT_MAXIMUM                                           AS approved_grant_maximum,
    detls.APPROVEDGRANT                                                   AS approved_grant,
    detls.CONSUMEDAMOUNT                                                  AS consumed_amount,
    detls.REMAININGAMOUNT                                                 AS remaining_amount,
    detls.CAP                                                             AS cap,
    detls.TAMKEENSHARE                                                    AS tamkeen_share,
    detls.TOTALNUMBEREMPLOYEES                                            AS total_number_employees,
    bank.LABEL                                                            AS bank_name,

    -- RM-side scoring
    RM.NAME                                                               AS rm_name,
    detls.RMREMARKS                                                       AS rm_remarks,
    detls.SCORERM                                                         AS rm_score,
    detls.RECOMMENDEDFINANCINGBYRM                                        AS rm_recommended_financing,
    detls.RECOMMENDEDGRANTBYRM                                            AS rm_recommended_grant,
    detls.ASSESSMENTSUPPORTCAPBYRM                                        AS rm_assessment_support_cap,

    -- Assessor-side scoring
    Assessor.NAME                                                         AS assessor_name,
    detls.ASSESSORREMARKS                                                 AS assessor_remarks,
    detls.SCOREASSESSOR                                                   AS assessor_score,
    detls.RECOMMENDEDFINANCINGBYASSESS                                    AS assessor_recommended_financing,
    detls.RECOMMENDEDGRANTBYASSESSOR                                      AS assessor_recommended_grant,
    detls.ASSESSMENTSUPPORTCAPASSESSOR                                    AS assessor_assessment_support_cap,

    -- Approver-side scoring
    Approver.NAME                                                         AS approver_name,
    detls.APPROVERREMARKS                                                 AS approver_remarks,
    detls.ASSESSMENTSUPPORTCAPAPPROVER                                    AS approver_assessment_support_cap,

    -- Customer training (1:1 with application via APPLICATIONID)
    CusTrn.NAMEOFCOURSECERTIFICATION                                      AS training_name_of_course_certification,
    TrnPrgTyp.LABEL                                                       AS training_program_type,
    TrnPrg.TRAININGPROGRAMNAME                                            AS training_program,
    CusTrn.TRAININGOVERVIEW                                               AS training_overview,
    CusTrn.COSTOFTRAINING                                                 AS training_cost,
    MofD.LABEL                                                            AS training_mode_of_delivery,
    CusTrn.ESTIMATEDSTARTDATE                                             AS training_estimated_start_date,
    CusTrn.ESTIMATEDENDDATE                                               AS training_estimated_end_date,
    CusTrn.TOTALHOURSOFTRAINING                                           AS training_total_hours,
    COALESCE(CusTrn.INCLUDEPRATICALHOURS,  FALSE)  AS training_include_practical_hours,
    COALESCE(CusTrn.INCLUDEJOBTRAINING,    FALSE)  AS training_include_job_training,
    TypTrnProv.LABEL                                                      AS training_provider_type,
    CusTrn.TRAININGPROVIDERNAME                                           AS training_provider_name,
    CusTrn.AWARDINGBODYNAME                                               AS training_awarding_body_name,
    CusTrn.AWARDINGBODYDETAILS                                            AS training_awarding_body_details,
    CusTrn.COUNTRY                                                        AS training_country,

    -- Customer contact (slice from RPT-156 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â joined via app.USERID ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ ossys_user)
    Cust.NAME                                                             AS customer_name,
    Cust.MobilePhone                                                      AS customer_mobile,
    Cust.EMAIL                                                            AS customer_email,
    CustExt.NATIONALITY                                                   AS customer_nationality,
    CustExt.GENDER                                                        AS customer_gender,
    CASE
        WHEN CustExt.DATEOFBIRTH = DATE '1900-01-01'   THEN NULL
        WHEN EXTRACT(YEAR FROM CustExt.DATEOFBIRTH) <= 1900 THEN NULL
        ELSE CustExt.DATEOFBIRTH
    END                                                                   AS customer_date_of_birth,
    CustExt.CPR_NUMBER                                                    AS customer_cpr_number,

    -- Customer entity classification (CR if commercial, otherwise CPR)
    CASE WHEN app.CR <> '' THEN app.CR
         ELSE CustExt.CPR_NUMBER 
    END                                                                   AS customer_cr_or_cpr,
    CASE WHEN app.CR <> '' THEN app.COMMERCIALNAME
         ELSE Cust.NAME
    END                                                                   AS customer_application_name,

    -- Standard trailing audit columns
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATION app
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_PROGRAM                        prog       ON prog.ID       = app.PROGRAMID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_PROGRAMTYPE                    prog_typ   ON prog_typ.ID   = prog.PROGRAMTYPEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONSTATUS              stus       ON stus.ID       = app.APPLICATIONSTATUSID
--LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICANTTYPE                  typ        ON typ.ID        = app.ID

-- SYB / sector / turnover 
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONTYPE                 p          ON p.ID          = app.ID
--LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_TURNOVERVALUE                  trnovr     ON trnovr.ID     = p.TURNOVERVALUEID
--LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_SECTOR                         sec        ON sec.ID        = p.SECTORID

--LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_ASSESSORRECOMENDATION          AR         ON AR.ID         = app.ASSESSORRECOMENDATION
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONSUPPORTDETAILS      detls      ON detls.APPLICATIONID = app.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER                               Approver   ON Approver.ID   = app.APPROVERID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER                               Assessor   ON Assessor.ID   = app.ASSESSORID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER                               RM         ON RM.ID         = app.RMID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_BANKNAMES                      bank       ON bank.ID       = detls.APPLICATIONID

-- Customer training
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_CUSTOMERTRAINING               CusTrn     ON CusTrn.APPLICATIONID = app.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_MODEOFDELIVERY                 MofD       ON MofD.ID       = CusTrn.MODEOFDELIVERYID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_TRAININGPROGRAM                TrnPrg     ON TrnPrg.ID     = CusTrn.TRAININGPROGRAMID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_TRAININGPROGRAMTYPE            TrnPrgTyp  ON TrnPrgTyp.ID  = CusTrn.TRAININGPROGRAMTYPEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_TYPEOFTRAININGPROVIDER         TypTrnProv ON TypTrnProv.ID = CusTrn.TRAININGPROVIDERTYPEID

-- Loan status + latest internal status
--LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONLOANSTATUS          loanStat   ON loanStat.ID   = app.APPLICATIONLOANSTATUSID
LEFT JOIN latest_internal_status                                                  lis        ON lis.application_id = app.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_APPLICATIONINTERNALSTATUSES  Int_ST     ON Int_ST.ID     = lis.internal_status_id

-- Customer (the applying user) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â folded in from RPT-156
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER                               Cust       ON Cust.ID       = app.USERID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_MKZ_USEREXTENSION                  CustExt    ON CustExt.USERID = Cust.ID
),
mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING) AS option_key,
        max(sm.value) AS option_value
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.STRINGMAP sm
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING)
),
option_set_map AS (
    SELECT map_from_entries(collect_list(named_struct('key', option_key, 'value', option_value))) AS option_values
    FROM option_set_values
),
ind_status_history AS (
    SELECT
        sh.mis_indiviualapplicationid                          AS application_id,
        sh.mis_statusreport                                    AS status_report_id,
        COUNT(sh.mis_individualapplicationstatushistoryid)     AS occurrence_count,
        MIN(sh.createdon)                                      AS first_created_on,
        MAX(sh.createdon)                                      AS last_created_on
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_INDIVIDUALAPPLICATIONSTATUSHISTORYBASE sh
    WHERE sh.mis_indiviualapplicationid IS NOT NULL
      AND sh.statecode = 0
    GROUP BY
        sh.mis_indiviualapplicationid,
        sh.mis_statusreport
),

tmkn_status_history AS (
    SELECT
        sh.tmkn_appshid                                        AS application_id,
        sh.tmkn_statusreport                                   AS status_report_id,
        COUNT(sh.tmkn_appshid)                                 AS occurrence_count,
        MIN(sh.createdon)                                      AS first_created_on,
        MAX(sh.createdon)                                      AS last_created_on,
        sh.tmkn_ref
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_APPSHBASE sh
    WHERE sh.tmkn_appshid IS NOT NULL
      AND sh.statecode = 0
    GROUP BY
        sh.tmkn_appshid,
        sh.tmkn_statusreport,
        sh.tmkn_ref
)

-- ============================================================================
-- SUB-TYPE 1: Individual Applications (RPT-058, RPT-059)
-- Anchor: MIS_individualapplication
-- ============================================================================
SELECT
    'INDIVIDUAL_APPLICATION' AS application_subtype,
    'MIS_individualapplication' AS mis_source_table,

    -- Identifiers
    CAST(app.mis_individualapplicationid AS STRING) AS application_id,
    app.mis_id                                       AS application_no,

    -- Foreign keys
    CAST(app.mis_individualid AS STRING)            AS individual_id,
    CAST(app.mis_certificatename AS STRING)         AS certificate_id,
    CAST(app.tmkn_pid AS STRING)                    AS pid,
    CAST(app.mis_productid AS STRING)               AS product_id,

    -- Display names (denormalised at source)
    app.mis_individualapplicationid                         AS individual_name,
    app.mis_certificatename                          AS certificate_name,
    app.tmkn_pid                                     AS product_pid_name,
    app.mis_productid                                AS product_name,
    app.mis_training_provider                        AS training_provider,
    app.tmkn_sponsorship                             AS sponsorship,
    app.tmkn_certificateapproval                     AS certificate_approval,
    app.mis_assessmentanalyst                        AS assessment_analyst,
    CASE WHEN EXTRACT(YEAR FROM app.mis_tpcs_startdate) > 1900
         THEN app.mis_tpcs_startdate END             AS tpcs_start_date,
    CASE WHEN EXTRACT(YEAR FROM app.mis_tpcs_enddate) > 1900
         THEN app.mis_tpcs_enddate END               AS tpcs_end_date,
    CASE WHEN EXTRACT(YEAR FROM app.mis_tpcs_examdate) > 1900
         THEN app.mis_tpcs_examdate END              AS tpcs_exam_date,
    app.mis_submitteddate                            AS submitted_date,
    app.mis_approvedon                               AS approved_on,
    app.mis_applicationdate                          AS application_date,
    CASE WHEN EXTRACT(YEAR FROM app.mis_enddate) > 1900
         THEN app.mis_enddate END                    AS end_date,
    app.mis_tpcs_totalfees                           AS total_fees,
    app.tmkn_salary                                  AS salary,
    app.mis_cert_cap                                 AS certificate_cap,
    app.tmkn_tamkeenshare                            AS tamkeen_share,
    CASE WHEN app.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('statecode') || '|' || CAST(app.statecode AS STRING)) END                  AS state,
    CASE WHEN app.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('statuscode') || '|' || CAST(app.statuscode AS STRING)) END               AS status_reason,
    CASE WHEN app.mis_hasnopayment IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('mis_hasnopayment') || '|' || CAST(app.mis_hasnopayment AS STRING)) END    AS has_no_payment,
    CASE WHEN app.mis_pass IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('mis_pass') || '|' || CAST(app.mis_pass AS STRING)) END                 AS pass,
    CASE WHEN app.mis_studytype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('mis_studytype') || '|' || CAST(app.mis_studytype AS STRING)) END          AS study_type,
    CASE WHEN app.mis_tpcs_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('mis_tpcs_workflowstatus') || '|' || CAST(app.mis_tpcs_workflowstatus AS STRING)) END  AS workflow_status,
    CASE WHEN app.tmkn_withdrawreason IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('tmkn_withdrawreason') || '|' || CAST(app.tmkn_withdrawreason AS STRING)) END      AS withdraw_reason,
    CASE WHEN app.tmkn_violationstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('tmkn_violationstatus') || '|' || CAST(app.tmkn_violationstatus AS STRING)) END     AS violation_status,
    CASE WHEN app.tmkn_segment IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individualapplication') || '|' || lower('tmkn_segment') || '|' || CAST(app.tmkn_segment AS STRING)) END             AS segment,
    app.mis_employmentdetails                        AS employment_details,
    app.mis_justificationfcertification              AS justification_for_certification,
    app.modifiedby                               AS modified_by,
    app.modifiedon                                   AS modified_on,
    ind.mis_cpr                                      AS individual_cpr,
    CASE WHEN EXTRACT(YEAR FROM ind.mis_dateofbirth) > 1900
         THEN ind.mis_dateofbirth END                AS individual_date_of_birth,
    CASE WHEN EXTRACT(YEAR FROM ind.tmkn_graduation_date) > 1900
         THEN ind.tmkn_graduation_date END           AS individual_graduation_date,
    ind.mis_email                                    AS individual_email,
    ind.mis_mobile                                   AS individual_mobile,
    ind.mis_addr_flat                                AS individual_addr_flat,
    ind.mis_addr_building                            AS individual_addr_building,
    ind.mis_addr_road                                AS individual_addr_road,
    ind.mis_addr_block                               AS individual_addr_block,
    ind.mis_addr_area                                AS individual_addr_area,
    CASE WHEN ind.mis_schoollevel IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('mis_schoollevel') || '|' || CAST(ind.mis_schoollevel AS STRING)) END              AS individual_school_level,
    CASE WHEN ind.mis_unversitylevel IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('mis_unversitylevel') || '|' || CAST(ind.mis_unversitylevel AS STRING)) END           AS individual_university_level,
    CASE WHEN ind.mis_universityspecialization IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('mis_universityspecialization') || '|' || CAST(ind.mis_universityspecialization AS STRING)) END AS individual_university_specialization,
    CASE WHEN ind.tmkn_highest_degree_obtained IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('tmkn_highest_degree_obtained') || '|' || CAST(ind.tmkn_highest_degree_obtained AS STRING)) END AS individual_highest_degree,
    CASE WHEN ind.mis_gender IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('mis_gender') || '|' || CAST(ind.mis_gender AS STRING)) END                   AS individual_gender,
    CASE WHEN ind.mis_qualification IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('mis_qualification') || '|' || CAST(ind.mis_qualification AS STRING)) END            AS individual_qualification,
    CASE WHEN ind.mis_nationality IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('MIS_Nationality') || '|' || CAST(ind.mis_nationality AS STRING)) END              AS individual_nationality,
    CAST(cert.tmkn_id AS STRING)                    AS certificate_external_id,
    cert.mis_category                                AS certificate_old_category,
    cert.mis_broad                                   AS certificate_broad,
    cert.mis_detailed                                AS certificate_detailed,
    cert.mis_narrow                                  AS certificate_narrow,
    cert.mis_awardingbody                            AS certificate_awarding_body,
    CASE WHEN cert.mis_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_type') || '|' || CAST(cert.mis_type AS STRING)) END              AS certificate_type,
    CASE WHEN cert.tmkn_certificatetype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_CertificateType') || '|' || CAST(cert.tmkn_certificatetype AS STRING)) END  AS certificate_category,
    sh_submit_analysis.first_created_on              AS first_submit_for_analysis_date,
    sh_submit_analysis.last_created_on               AS last_submit_for_analysis_date,
    sh_send_back.first_created_on                    AS first_send_back_date,
    sh_send_back.last_created_on                     AS last_send_back_date,
    sh_send_interview.last_created_on                AS last_send_for_interview_date,
    sh_send_manager.last_created_on                  AS last_send_to_manager_date,
    sh_approved.last_created_on                      AS last_approved_by_manager_date,
    sh_rejected.last_created_on                      AS last_rejected_date,
    sh_withdraw.last_created_on                      AS last_withdraw_date,
    CAST(NULL AS STRING)                            AS company_id,
    CAST(NULL AS STRING)                            AS company_name,
    CAST(NULL AS STRING)                            AS cr_number,
    CAST(NULL AS STRING)                            AS enterprise_application_id,
    CAST(NULL AS STRING)                            AS employee_application_id,
    CAST(NULL AS DECIMAL(18, 2))                     AS approved_amount,
    CAST(NULL AS DECIMAL(18, 2))                     AS paid_amount,
    CAST(NULL AS STRING)                            AS tmkn_area,             
    CAST(NULL AS STRING)                            AS tmkn_crregistrationdate,   
    CAST(NULL AS STRING)                            AS  tws_job_title,
    CAST(NULL AS STRING)                            AS   tws_job,  
    CAST(NULL AS STRING)        AS tmkn_maincr,
    CAST(NULL AS STRING)        AS tmkn_industry,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS tmkn_isapproved,
    CAST(NULL AS DECIMAL(18, 2)) AS tmkn_humancapitalgrant,
    CAST(NULL AS DECIMAL(18, 2)) AS tmkn_totalcommitted,
    CAST(NULL AS TIMESTAMP)      AS tmkn_monitoringduedate,
    CAST(NULL AS DECIMAL(18, 2)) AS tmkn_revenuey1,
    CAST(NULL AS STRING)        AS tmkn_objectiverouting,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    CAST(NULL AS STRING)        AS tmkn_primaryname,
    CAST(NULL AS STRING)        AS tmkn_primarymobilenumber,
    CAST(NULL AS STRING)        AS tmkn_primaryemail,
    CAST(NULL AS STRING)        AS tmkn_blocknumber,  
       CAST(NULL AS STRING)    as tmkn_commercialname,
       CAST(NULL AS decimal)   as tmkn_humancapitalactionplanmaxapproved,     
       CAST(NULL AS decimal)    as tmkn_plangrant,
       CAST(NULL AS decimal)   as tmkn_profilegrantmax,
       CAST(NULL AS decimal)    as tmkn_actplangrant,
       CAST(NULL AS decimal)   as tmkn_humancapitaltotalcommitted     ,                                
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_INDIVIDUALAPPLICATIONBASE app
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_INDIVIDUALBASE ind
       ON ind.mis_individualid = app.mis_individualid
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_CERTIFICATEBASE cert
       ON cert.mis_certificateid = app.mis_certificatename
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_PIDBASE pid
       ON pid.tmkn_pidid = app.tmkn_pid
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_PRODUCTBASE prod
       ON prod.mis_productid = app.mis_productid

-- Status history milestones ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â one LEFT JOIN per status of interest
LEFT JOIN  ind_status_history sh_submit_analysis
       ON sh_submit_analysis.application_id = app.mis_individualapplicationid
      AND sh_submit_analysis.status_report_id = 100000006  -- Submit For Analysis
LEFT JOIN  ind_status_history sh_send_back
       ON sh_send_back.application_id = app.mis_individualapplicationid
      AND sh_send_back.status_report_id = 100000003  -- Send back for missing documents/feedback
LEFT JOIN  ind_status_history sh_send_interview
       ON sh_send_interview.application_id = app.mis_individualapplicationid
      AND sh_send_interview.status_report_id = 100000002  -- Send for Interview
LEFT JOIN  ind_status_history sh_send_manager
       ON sh_send_manager.application_id = app.mis_individualapplicationid
      AND sh_send_manager.status_report_id = 100000009  -- Send to Manager for Approval
LEFT JOIN  ind_status_history sh_approved
       ON sh_approved.application_id = app.mis_individualapplicationid
      AND sh_approved.status_report_id = 100000010  -- Mark As Approved by Manager
LEFT JOIN  ind_status_history sh_rejected
       ON sh_rejected.application_id = app.mis_individualapplicationid
      AND sh_rejected.status_report_id = 100000004  -- Mark as Rejected
LEFT JOIN  ind_status_history sh_withdraw
       ON sh_withdraw.application_id = app.mis_individualapplicationid
      AND sh_withdraw.status_report_id = 100000005  -- Mark as Withdraw


UNION ALL


-- ============================================================================
-- SUB-TYPE 2: Business Development Applications (RPT-030)
-- Anchor: tmkn_application (filtered to BD)
-- ============================================================================
SELECT
    'BUSINESS_DEVELOPMENT' AS application_subtype, 
    'tmkn_application' AS mis_source_table,
    CAST(app.tmkn_applicationid AS STRING)          AS application_id,
    app.tmkn_name                                    AS application_no,
    CAST(NULL AS STRING)                            AS individual_id,
    CAST(NULL AS STRING)                            AS certificate_id,
    CAST(app.tmkn_pid AS STRING)                    AS pid,
    CAST(NULL AS STRING)                            AS product_id,
    CAST(NULL AS STRING)                            AS individual_name,
    CAST(NULL AS STRING)                            AS certificate_name,
    app.tmkn_pid                                 AS product_pid_name,
    CAST(NULL AS STRING)                            AS product_name,
    CAST(NULL AS STRING)                            AS training_provider,
    CAST(NULL AS STRING)                            AS sponsorship,
    CAST(NULL AS STRING)                            AS certificate_approval,
    CAST(NULL AS STRING)                            AS assessment_analyst,
    CAST(NULL AS DATE)                               AS tpcs_start_date,
    CAST(NULL AS DATE)                               AS tpcs_end_date,
    CAST(NULL AS DATE)                               AS tpcs_exam_date,
    app.tmkn_submittedon                             AS submitted_date,
    app.tmkn_approvedon                              AS approved_on,
    app.tmkn_applicationdate                         AS application_date,
--    CASE WHEN EXTRACT(YEAR FROM app.tmkn_enddate) > 1900 THEN app.tmkn_enddate END AS end_date,
    CAST(NULL AS TIMESTAMP)                          AS end_date,
    CAST(NULL AS DECIMAL(18, 2))                     AS total_fees,
    CAST(NULL AS DECIMAL(18, 2))                     AS salary,
    CAST(NULL AS DECIMAL(18, 2))                     AS certificate_cap,
    --app.tmkn_tamkeenshare                            AS tamkeen_share,
    app.tmkn_totaltamkeenshare                       AS tamkeen_share,  
    CASE WHEN app.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_application') || '|' || lower('statecode') || '|' || CAST(app.statecode AS STRING)) END  AS state,
    CASE WHEN app.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_application') || '|' || lower('statuscode') || '|' || CAST(app.statuscode AS STRING)) END AS status_reason,
    CAST(NULL AS STRING)                            AS has_no_payment,
    CAST(NULL AS STRING)                            AS pass,
    CAST(NULL AS STRING)                            AS study_type,
    CASE WHEN app.tmkn_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_application') || '|' || lower('tmkn_workflowstatus') || '|' || CAST(app.tmkn_workflowstatus AS STRING)) END   AS workflow_status,
    CAST(NULL AS STRING)                            AS withdraw_reason,
    CAST(NULL AS STRING)                            AS violation_status,
    CAST(NULL AS STRING)                            AS segment,
    CAST(NULL AS STRING)                            AS employment_details,
    CAST(NULL AS STRING)                            AS justification_for_certification,
    app.modifiedby                                   AS modified_by,
    app.modifiedon                                   AS modified_on,
    CAST(NULL AS STRING)                            AS individual_cpr,
    CAST(NULL AS DATE)                               AS individual_date_of_birth,
    CAST(NULL AS DATE)                               AS individual_graduation_date,
    CAST(NULL AS STRING)                            AS individual_email,
    CAST(NULL AS STRING)                            AS individual_mobile,
    CAST(NULL AS STRING)                            AS individual_addr_flat,
    CAST(NULL AS STRING)                            AS individual_addr_building,
    CAST(NULL AS STRING)                            AS individual_addr_road,
    CAST(NULL AS STRING)                            AS individual_addr_block,
    CAST(NULL AS STRING)                            AS individual_addr_area,
    CAST(NULL AS STRING)                            AS individual_school_level,
    CAST(NULL AS STRING)                            AS individual_university_level,
    CAST(NULL AS STRING)                            AS individual_university_specialization,
    CAST(NULL AS STRING)                            AS individual_highest_degree,
    CAST(NULL AS STRING)                            AS individual_gender,
    CAST(NULL AS STRING)                            AS individual_qualification,
    CAST(NULL AS STRING)                            AS individual_nationality,
    CAST(NULL AS STRING)                            AS certificate_external_id,
    CAST(NULL AS STRING)                            AS certificate_old_category,
    CAST(NULL AS STRING)                            AS certificate_broad,
    CAST(NULL AS STRING)                            AS certificate_detailed,
    CAST(NULL AS STRING)                            AS certificate_narrow,
    CAST(NULL AS STRING)                            AS certificate_awarding_body,
    CAST(NULL AS STRING)                            AS certificate_type,
    CAST(NULL AS STRING)                            AS certificate_category,
    sh_submit.first_created_on                       AS first_submit_for_analysis_date,
    sh_submit.last_created_on                        AS last_submit_for_analysis_date,
    sh_sendback.first_created_on                     AS first_send_back_date,
    sh_sendback.last_created_on                      AS last_send_back_date,
    CAST(NULL AS TIMESTAMP)                          AS last_send_for_interview_date,
    sh_manager.last_created_on                       AS last_send_to_manager_date,
    sh_approved.last_created_on                      AS last_approved_by_manager_date,
    sh_rejected.last_created_on                      AS last_rejected_date,
    sh_withdraw.last_created_on                      AS last_withdraw_date,
    CAST(app.tmkn_maincompany AS STRING)            AS company_id,
    --app.tmkn_companyname                             AS company_name,
    CAST(NULL AS STRING)                           AS company_name,  -- BD doesn't have company name at this layer; would need to join to account or similar
    CAST(NULL AS STRING)                            AS cr_number,
    CAST(NULL AS STRING)                            AS enterprise_application_id,
    CAST(NULL AS STRING)                            AS employee_application_id,
    CAST(NULL AS DECIMAL(18, 2))                     AS approved_amount,
    CAST(NULL AS DECIMAL(18, 2))                     AS paid_amount,
    CAST(tmkn_area AS STRING)                       AS tmkn_area,       
    CAST(tmkn_crregistrationdate AS STRING)         AS tmkn_crregistrationdate,
    CAST(NULL AS STRING)                            AS  tws_job_title,
    CAST(NULL AS STRING)                            AS   tws_job,
    app.tmkn_maincr, --NEW
    CAST(app.tmkn_industry AS STRING) AS tmkn_industry, --NEW
    CAST(app.tmkn_workflowstatus AS STRING) as tmkn_workflowstatus, --NEW
    CAST(app.tmkn_isapproved AS STRING) as tmkn_isapproved, --NEW
    CAST(app.tmkn_humancapitalgrant AS DECIMAL) as tmkn_humancapitalgrant, --NEW
    CAST(app.tmkn_totalcommitted AS DECIMAL) as tmkn_totalcommitted, --NEW
    CAST(app.tmkn_monitoringduedate AS TIMESTAMP) as tmkn_monitoringduedate, --NEW
    CAST(app.tmkn_revenuey1 AS INTEGER) as tmkn_revenuey1, --NEW
    CAST(app.tmkn_objectiverouting AS STRING) AS tmkn_objectiverouting, --NEW
    app.tmkn_contractstartdate, --NEW
    app.tmkn_contractenddate, --NEW
    app.tmkn_primaryname, --NEW
    app.tmkn_primarymobilenumber, --NEW
    app.tmkn_primaryemail, --NEW
    app.tmkn_blocknumber, --NEW       
    app.tmkn_commercialname,
    app.tmkn_humancapitalactionplanmaxapproved,
       tmkn_plangrant,
       tmkn_profilegrantmax,
       tmkn_actplangrant,
       tmkn_humancapitaltotalcommitted,
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_APPLICATIONBASE app
LEFT JOIN tmkn_status_history sh_submit
       ON sh_submit.application_id = app.tmkn_applicationid
      AND sh_submit.status_report_id = 100000006
LEFT JOIN tmkn_status_history sh_sendback
       ON sh_sendback.application_id = app.tmkn_applicationid
      AND sh_sendback.status_report_id = 100000003
LEFT JOIN tmkn_status_history sh_manager
       ON sh_manager.application_id = app.tmkn_applicationid
      AND sh_manager.status_report_id = 100000009
LEFT JOIN tmkn_status_history sh_approved
       ON sh_approved.application_id = app.tmkn_applicationid
      AND sh_approved.status_report_id = 100000010
LEFT JOIN tmkn_status_history sh_rejected
       ON sh_rejected.application_id = app.tmkn_applicationid
      AND sh_rejected.status_report_id = 100000004
LEFT JOIN tmkn_status_history sh_withdraw
       ON sh_withdraw.application_id = app.tmkn_applicationid
      AND sh_withdraw.status_report_id = 100000005

--WHERE app.tmkn_bd = 1  -- Filter to Business Development applications 


UNION ALL


-- ============================================================================
-- SUB-TYPE 3: TWS Wage Subsidy / Employee Applications (RPT-044, RPT-045, RPT-046)
-- Anchors: tws_employeeapplication (joined to tws_enterpriseapplication)
-- ============================================================================
SELECT
    'TWS_WAGE_SUBSIDY' AS application_subtype,
    'tws_employeeapplication' AS mis_source_table,

    -- Identifiers
    CAST(emp.tws_employeeapplicationid AS STRING)   AS application_id,
    emp.tws_name                                     AS application_no,

    -- Foreign keys
    --CAST(emp.tws_individualid AS STRING)            AS individual_id,
    CAST(NULL AS STRING)                            AS individual_id,  -- TWS Wage doesn't join to MIS_individual at this layer; would need to join to TWS Individual and then to MIS_individual
    CAST(NULL AS STRING)                            AS certificate_id,
    CAST(NULL AS STRING)                            AS pid,
  --  CAST(emp.tws_productid AS STRING)               AS product_id,
    CAST(NULL AS STRING)                            AS product_id, -- Product isn't a required field in TWS Wage, and the join to MIS_product causes significant performance issues due to the size of the MIS_product table. Omit product_id for now; can add back if needed with a more performant join strategy (e.g. pre-joining to a smaller subset of MIS_product or joining to a TWS Wage-specific product dimension if available)
    -- Display names
    --emp.tws_individualidname                         AS individual_name, 
    CAST(NULL AS STRING)                            AS individual_name, -- TWS Wage doesn't join to MIS_individual at this layer; would need to join to TWS Individual and then to MIS_individual for the name
    CAST(NULL AS STRING)                            AS certificate_name,
    CAST(NULL AS STRING)                            AS product_pid_name,
    --emp.tws_productidname                          AS product_name,
    emp.tws_product                                  AS product_name, -- Product isn't a required field in TWS Wage, and the join to MIS_product causes significant performance issues due to the size of the MIS_product table. Omit product_name for now; can add back if needed with a more performant join strategy (e.g. pre-joining to a smaller subset of MIS_product or joining to a TWS Wage-specific product dimension if available)
    CAST(NULL AS STRING)                            AS training_provider,
    CAST(NULL AS STRING)                            AS sponsorship,
    CAST(NULL AS STRING)                            AS certificate_approval,
    CAST(NULL AS STRING)                            AS assessment_analyst,

    -- Dates
    CAST(NULL AS DATE)                               AS tpcs_start_date,
    CAST(NULL AS DATE)                               AS tpcs_end_date,
    CAST(NULL AS DATE)                               AS tpcs_exam_date,
    emp.tws_submitted_on                              AS submitted_date,
    --emp.tws_approved_on                               AS approved_on,
    CAST(NULL AS DATE)                               AS approved_on,  -- TWS Employee Application doesn't have an "approved on" date field; would need to infer from status history
    emp.tws_application_date                          AS application_date,
    CAST(NULL AS TIMESTAMP)                          AS end_date, 
    CAST(NULL AS DECIMAL(18, 2))                     AS total_fees,
    --emp.tws_salary                                   AS salary,
    CAST(NULL AS DECIMAL(18, 2))                     AS salary,  -- Salary isn't a required field in TWS Wage, and the join to MIS_individual for the salary causes significant performance issues due to the size of the MIS_individual table. Omit salary for now; can add back if needed with a more performant join strategy (e.g. pre-joining to a smaller subset of MIS_individual or joining to a TWS Wage-specific individual dimension if available)
    CAST(NULL AS DECIMAL(18, 2))                     AS certificate_cap,
   -- emp.tws_tamkeenshare                             AS tamkeen_share,
    CAST(NULL AS DECIMAL(18, 2))                     AS tamkeen_share,  -- Tamkeen share isn't a required field in TWS Wage, and the join to MIS_individual for the tamkeen share causes significant performance issues due to the size of the MIS_individual table. Omit tamkeen share for now; can add back if needed with a more performant join strategy (e.g. pre-joining to a smaller subset of MIS_individual or joining to a TWS Wage-specific individual dimension if available)

    -- Status
    CASE WHEN emp.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_employeeapplication') || '|' || lower('statecode') || '|' || CAST(emp.statecode AS STRING)) END  AS state,
    CASE WHEN emp.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_employeeapplication') || '|' || lower('statuscode') || '|' || CAST(emp.statuscode AS STRING)) END AS status_reason,
    CAST(NULL AS STRING)                            AS has_no_payment,
    CAST(NULL AS STRING)                            AS pass,
    CAST(NULL AS STRING)                            AS study_type,
    CAST(NULL AS STRING)                            AS workflow_status,
    CAST(NULL AS STRING)                            AS withdraw_reason,
    CAST(NULL AS STRING)                            AS violation_status,
    CAST(NULL AS STRING)                            AS segment,

    -- Other
    CAST(NULL AS STRING)                            AS employment_details,
    CAST(NULL AS STRING)                            AS justification_for_certification,

    -- Audit
    emp.modifiedby                                   AS modified_by,
    emp.modifiedon                                   AS modified_on,

    -- Individual placeholders (TWS Wage doesn't join to MIS_individual at this layer)
    CAST(NULL AS STRING)                            AS individual_cpr,
    CAST(NULL AS DATE)                               AS individual_date_of_birth,
    CAST(NULL AS DATE)                               AS individual_graduation_date,
    CAST(NULL AS STRING)                            AS individual_email,
    CAST(NULL AS STRING)                            AS individual_mobile,
    CAST(NULL AS STRING)                            AS individual_addr_flat,
    CAST(NULL AS STRING)                            AS individual_addr_building,
    CAST(NULL AS STRING)                            AS individual_addr_road,
    CAST(NULL AS STRING)                            AS individual_addr_block,
    CAST(NULL AS STRING)                            AS individual_addr_area,
    CAST(NULL AS STRING)                            AS individual_school_level,
    CAST(NULL AS STRING)                            AS individual_university_level,
    CAST(NULL AS STRING)                            AS individual_university_specialization,
    CAST(NULL AS STRING)                            AS individual_highest_degree,
    CAST(NULL AS STRING)                            AS individual_gender,
    CAST(NULL AS STRING)                            AS individual_qualification,
    CAST(NULL AS STRING)                            AS individual_nationality,
    CAST(NULL AS STRING)                            AS certificate_external_id,
    CAST(NULL AS STRING)                            AS certificate_old_category,
    CAST(NULL AS STRING)                            AS certificate_broad,
    CAST(NULL AS STRING)                            AS certificate_detailed,
    CAST(NULL AS STRING)                            AS certificate_narrow,
    CAST(NULL AS STRING)                            AS certificate_awarding_body,
    CAST(NULL AS STRING)                            AS certificate_type,
    CAST(NULL AS STRING)                            AS certificate_category,

    -- Status history (TWS uses different SH table ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â left null until confirmed)
    CAST(NULL AS TIMESTAMP)                          AS first_submit_for_analysis_date,
    CAST(NULL AS TIMESTAMP)                          AS last_submit_for_analysis_date,
    CAST(NULL AS TIMESTAMP)                          AS first_send_back_date,
    CAST(NULL AS TIMESTAMP)                          AS last_send_back_date,
    CAST(NULL AS TIMESTAMP)                          AS last_send_for_interview_date,
    CAST(NULL AS TIMESTAMP)                          AS last_send_to_manager_date,
    CAST(NULL AS TIMESTAMP)                          AS last_approved_by_manager_date,
    CAST(NULL AS TIMESTAMP)                          AS last_rejected_date,
    CAST(NULL AS TIMESTAMP)                          AS last_withdraw_date,
    CAST(ent.tws_enterpriseapplicationid AS STRING) AS company_id,
    --ent.tws_companyname                              AS company_name,
    CAST(NULL AS STRING)                            AS company_name,  -- TWS Wage doesn't have company name at this layer; would need to join to account or similar
    ent.tws_crnumber                                 AS cr_number,
    CAST(ent.tws_enterpriseapplicationid AS STRING) AS enterprise_application_id,
    CAST(emp.tws_employeeapplicationid AS STRING)   AS employee_application_id,
    CAST(NULL AS DECIMAL(18, 2))                     AS approved_amount,
    CAST(NULL AS DECIMAL(18, 2))                     AS paid_amount,
    CAST(NULL AS STRING)                            AS tmkn_area, 
    CAST(NULL AS STRING)                            AS tmkn_crregistrationdate,    
    CAST(TWS_JOB_TITLE AS STRING),
    CAST(TWS_JOB AS STRING),
    CAST(NULL AS STRING)        AS tmkn_maincr,
    CAST(NULL AS STRING)        AS tmkn_industry,
    CAST(NULL AS STRING)        AS tmkn_workflowstatus,
    CAST(NULL AS STRING)        AS tmkn_isapproved,
    CAST(NULL AS DECIMAL(18, 2)) AS tmkn_humancapitalgrant,
    CAST(NULL AS DECIMAL(18, 2)) AS tmkn_totalcommitted,
    CAST(NULL AS TIMESTAMP)      AS tmkn_monitoringduedate,
    CAST(NULL AS DECIMAL(18, 2)) AS tmkn_revenuey1,
    CAST(NULL AS STRING)        AS tmkn_objectiverouting,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractstartdate,
    CAST(NULL AS TIMESTAMP)      AS tmkn_contractenddate,
    CAST(NULL AS STRING)        AS tmkn_primaryname,
    CAST(NULL AS STRING)        AS tmkn_primarymobilenumber,
    CAST(NULL AS STRING)        AS tmkn_primaryemail,
    CAST(NULL AS STRING)        AS tmkn_blocknumber,          
       CAST(NULL AS STRING)    as tmkn_commercialname,
       CAST(NULL AS decimal)   as tmkn_humancapitalactionplanmaxapproved, 
       CAST(NULL AS decimal)    as tmkn_plangrant,
       CAST(NULL AS decimal)   as tmkn_profilegrantmax,
       CAST(NULL AS decimal)    as tmkn_actplangrant,
       CAST(NULL AS decimal)   as tmkn_humancapitaltotalcommitted,             
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_EMPLOYEEAPPLICATIONBASE emp
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_ENTERPRISEAPPLICATIONBASE ent
       ON ent.tws_enterpriseapplicationid = emp.tws_enterprise_application
),
os1 as (
    select
        -- =========================
        -- COMMON / OS1 COLUMNS
        -- =========================
        os1_source_table,
        cast(application_id as STRING) as application_id,
        application_no,
        cr_license_no,
        commercial_name,
        program_id,
        application_status_id,
        application_type_id,
        assessor_recommendation_id,
        application_loan_status_id,
        customer_user_id,
        rm_user_id,
        assessor_user_id,
        approver_user_id,
        application_type,
        assessor_recommendation,
        financing_approved_loan_status,
        sector,
        turnover,
        program_name,
        program_type_name,
        application_status,
        internal_status,
        created_on,
        approved_on,
        contract_start_date,
        contract_end_date,
        approve_pending_customer_confirmation_on,
        saved_on,
        spending_period_end_date,
        is_financing_approved,
        program_support_cap,
        financing_cap,
        financing_guarantee_cap,
        approved_financing_amount,
        approved_guarantee_amount,
        loan_tenor,
        interest_rate,
        loan_start_date,
        loan_end_date,
        total_interest_amount,
        grace_period,
        monthly_installment,
        total_requested,
        approved_grant_maximum,
        approved_grant,
        consumed_amount,
        remaining_amount,
        cap,
        tamkeen_share,
        total_number_employees,
        bank_name,
        rm_name,
        rm_remarks,
        rm_score,
        rm_recommended_financing,
        rm_recommended_grant,
        rm_assessment_support_cap,
        assessor_name,
        assessor_remarks,
        assessor_score,
        assessor_recommended_financing,
        assessor_recommended_grant,
        assessor_assessment_support_cap,
        approver_name,
        approver_remarks,
        approver_assessment_support_cap,
        training_name_of_course_certification,
        training_program_type,
        training_program,
        training_overview,
        training_cost,
        training_mode_of_delivery,
        training_estimated_start_date,
        training_estimated_end_date,
        training_total_hours,
        training_include_practical_hours,
        training_include_job_training,
        training_provider_type,
        training_provider_name,
        training_awarding_body_name,
        training_awarding_body_details,
        training_country,
        customer_name,
        customer_mobile,
        customer_email,
        customer_nationality,
        customer_gender,
        customer_date_of_birth,
        customer_cpr_number,
        customer_cr_or_cpr,
        customer_application_name,
        source_system_name,
        is_deleted,
        report_date,
        dbt_updated_at,

        -- =========================
        -- OS2 COLUMNS
        -- =========================
        cast(null as STRING)     as reference_number,
        cast(null as STRING)     as program_type,
        cast(null as STRING)     as cr_license_cpr,
        cast(null as STRING)     as customer_enterprise_name,
        cast(null as timestamp)   as approved_on_date,
        cast(null as timestamp)   as monitoring_due_date,
        cast(null as decimal(18,2)) as total_approved_amount_tamkeen_share_old,
        cast(null as timestamp)   as submitted_on,
        cast(null as STRING)     as is_hipo_application,
        cast(null as decimal(18,2)) as programcap,
        cast(null as decimal(18,2)) as applicationcap,
        cast(null as decimal(18,2)) as tkshareamt,
        cast(null as decimal(18,2)) as applicationcapunutilized,
        cast(null as decimal(18,2)) as customershareamt,
        cast(null as decimal(18,2)) as totalcostwvat,
        cast(null as timestamp)   as starton,
        cast(null as timestamp)   as endon,
        cast(null as timestamp)   as monitoringduedate,
        cast(null as timestamp)   as spendingperiodduedate,
        cast(null as timestamp)   as claimingperiodduedate,
        cast(null as integer)     as duration,
        cast(null as boolean)     as isactive,
        cast(null as STRING)     as createdby,
        cast(null as timestamp)   as createdon,
        cast(null as STRING)     as updatedby,
        cast(null as timestamp)   as updatedon,
        cast(null as timestamp)   as submittedon,
        cast(null as timestamp)   as approvedon,
        cast(null as STRING)     as amendappinstancedocgudi_ar,
        cast(null as boolean)     as haswagesupportmolemployees,
        cast(null as decimal(18,2)) as calculatedeconomicvalue,
        cast(null as decimal(18,2)) as calculatedgrantamount,

        -- =========================
        -- MIS COLUMNS
        -- =========================
        cast(null as STRING)     as application_subtype,
        cast(null as STRING)     as mis_source_table,
        cast(null as STRING)     as individual_id,
        cast(null as STRING)     as certificate_id,
        cast(null as STRING)     as pid,
        cast(null as STRING)     as product_id,
        cast(null as STRING)     as individual_name,
        cast(null as STRING)     as certificate_name,
        cast(null as STRING)     as product_pid_name,
        cast(null as STRING)     as product_name,
        cast(null as STRING)     as training_provider,
        cast(null as STRING)     as sponsorship,
        cast(null as STRING)     as certificate_approval,
        cast(null as STRING)     as assessment_analyst,
        cast(null as timestamp)   as tpcs_start_date,
        cast(null as timestamp)   as tpcs_end_date,
        cast(null as timestamp)   as tpcs_exam_date,
        cast(null as timestamp)   as submitted_date,
        cast(null as timestamp)   as application_date,
        cast(null as timestamp)   as end_date,
        cast(null as decimal(18,2)) as total_fees,
        cast(null as decimal(18,2)) as salary,
        cast(null as decimal(18,2)) as certificate_cap,
        cast(null as STRING)     as state,
        cast(null as STRING)     as status_reason,
        cast(null as STRING)     as has_no_payment,
        cast(null as STRING)     as pass,
        cast(null as STRING)     as study_type,
        cast(null as STRING)     as workflow_status,
        cast(null as STRING)     as withdraw_reason,
        cast(null as STRING)     as violation_status,
        cast(null as STRING)     as segment,
        cast(null as STRING)     as employment_details,
        cast(null as STRING)     as justification_for_certification,
        cast(null as STRING)     as modified_by,
        cast(null as timestamp)   as modified_on,
        cast(null as STRING)     as individual_cpr,
        cast(null as timestamp)   as individual_date_of_birth,
        cast(null as timestamp)   as individual_graduation_date,
        cast(null as STRING)     as individual_email,
        cast(null as STRING)     as individual_mobile,
        cast(null as STRING)     as individual_addr_flat,
        cast(null as STRING)     as individual_addr_building,
        cast(null as STRING)     as individual_addr_road,
        cast(null as STRING)     as individual_addr_block,
        cast(null as STRING)     as individual_addr_area,
        cast(null as STRING)     as individual_school_level,
        cast(null as STRING)     as individual_university_level,
        cast(null as STRING)     as individual_university_specialization,
        cast(null as STRING)     as individual_highest_degree,
        cast(null as STRING)     as individual_gender,
        cast(null as STRING)     as individual_qualification,
        cast(null as STRING)     as individual_nationality,
        cast(null as STRING)     as certificate_external_id,
        cast(null as STRING)     as certificate_old_category,
        cast(null as STRING)     as certificate_broad,
        cast(null as STRING)     as certificate_detailed,
        cast(null as STRING)     as certificate_narrow,
        cast(null as STRING)     as certificate_awarding_body,
        cast(null as STRING)     as certificate_type,
        cast(null as STRING)     as certificate_category,
        cast(null as timestamp)   as first_submit_for_analysis_date,
        cast(null as timestamp)   as last_submit_for_analysis_date,
        cast(null as timestamp)   as first_send_back_date,
        cast(null as timestamp)   as last_send_back_date,
        cast(null as timestamp)   as last_send_for_interview_date,
        cast(null as timestamp)   as last_send_to_manager_date,
        cast(null as timestamp)   as last_approved_by_manager_date,
        cast(null as timestamp)   as last_rejected_date,
        cast(null as timestamp)   as last_withdraw_date,
        cast(null as STRING)     as company_id,
        cast(null as STRING)     as company_name,
        cast(null as STRING)     as cr_number,
        cast(null as STRING)     as enterprise_application_id,
        cast(null as STRING)     as employee_application_id,
        cast(null as decimal(18,2)) as approved_amount,
        cast(null as decimal(18,2)) as paid_amount,
		cast(null as STRING)     as tmkn_area,
        cast(null as STRING)   as tmkn_crregistrationdate,
        cast(null as STRING)     as tws_job_title,
        cast(null as STRING)     as tws_job,
        cast(null as STRING)     as tmkn_maincr,
        cast(null as STRING)     as tmkn_industry,
        cast(null as STRING)     as tmkn_workflowstatus,
        cast(null as STRING)     as tmkn_isapproved,
        cast(null as decimal(18,2)) as tmkn_humancapitalgrant,
        cast(null as decimal(18,2)) as tmkn_totalcommitted,
        cast(null as timestamp)   as tmkn_monitoringduedate,
        cast(null as decimal(18,2)) as tmkn_revenuey1,
        cast(null as STRING)     as tmkn_objectiverouting,
        cast(null as timestamp)   as tmkn_contractstartdate,
        cast(null as timestamp)   as tmkn_contractenddate,
        cast(null as STRING)     as tmkn_primaryname,
        cast(null as STRING)     as tmkn_primarymobilenumber,
        cast(null as STRING)     as tmkn_primaryemail,
        cast(null as STRING)     as tmkn_blocknumber,
        CAST(NULL AS STRING)    as tmkn_commercialname,
        CAST(NULL AS decimal)   as tmkn_humancapitalactionplanmaxapproved,
        CAST(NULL AS decimal)    as tmkn_plangrant,
        CAST(NULL AS decimal)   as tmkn_profilegrantmax,
        CAST(NULL AS decimal)    as tmkn_actplangrant,
        CAST(NULL AS decimal)   as tmkn_humancapitaltotalcommitted

    from os1_source
),

os2 as (

    select
        -- =========================
        -- COMMON / OS1 COLUMNS
        -- =========================
        cast(null as STRING)     as os1_source_table,
        cast(null as STRING)      as application_id,
        cast(null as STRING)     as application_no,
        cast(null as STRING)     as cr_license_no,
        cast(null as STRING)     as commercial_name,
        cast(null as bigint)      as program_id,
        cast(null as integer)     as application_status_id,
        cast(null as integer)     as application_type_id,
        cast(null as integer)     as assessor_recommendation_id,
        cast(null as integer)     as application_loan_status_id,
        cast(null as integer)     as customer_user_id,
        cast(null as integer)     as rm_user_id,
        cast(null as integer)     as assessor_user_id,
        cast(null as integer)     as approver_user_id,        
        cast(null as STRING)     as application_type,
        cast(null as STRING)     as assessor_recommendation,
        cast(null as STRING)     as financing_approved_loan_status,
        cast(null as STRING)     as sector,
        cast(null as STRING)     as turnover,
        program_name,
        cast(program_type as STRING) as program_type_name,

        application_status,

        cast(null as STRING)     as internal_status,

        created_on,

        cast(null as timestamp)   as approved_on,

        contract_start_date,
        contract_end_date,

        cast(null as timestamp)   as approve_pending_customer_confirmation_on,
        cast(null as timestamp)   as saved_on,

        spending_period_end_date,

        cast(null as boolean)     as is_financing_approved,
        cast(null as decimal(18,2)) as program_support_cap,
        cast(null as decimal(18,2)) as financing_cap,
        cast(null as decimal(18,2)) as financing_guarantee_cap,
        cast(null as decimal(18,2)) as approved_financing_amount,
        cast(null as decimal(18,2)) as approved_guarantee_amount,
        cast(null as integer)     as loan_tenor,
        cast(null as decimal(18,2)) as interest_rate,
        cast(null as timestamp)   as loan_start_date,
        cast(null as timestamp)   as loan_end_date,
        cast(null as decimal(18,2)) as total_interest_amount,
        cast(null as integer)     as grace_period,
        cast(null as decimal(18,2)) as monthly_installment,
        cast(null as decimal(18,2)) as total_requested,
        cast(null as decimal(18,2)) as approved_grant_maximum,
        cast(null as decimal(18,2)) as approved_grant,
        cast(null as decimal(18,2)) as consumed_amount,
        cast(null as decimal(18,2)) as remaining_amount,
        cast(null as integer)     as cap,
        cast(tkshareamt as decimal(18,2)) as tamkeen_share,
        cast(null as integer)     as total_number_employees,
        cast(null as STRING)     as bank_name,
        cast(null as STRING)     as rm_name,
        cast(null as STRING)     as rm_remarks,
        cast(null as integer)     as rm_score,
        cast(null as decimal(18,2)) as rm_recommended_financing,
        cast(null as decimal(18,2)) as rm_recommended_grant,
        cast(null as decimal(18,2)) as rm_assessment_support_cap,
        cast(null as STRING)     as assessor_name,
        cast(null as STRING)     as assessor_remarks,
        cast(null as integer)     as assessor_score,
        cast(null as decimal(18,2)) as assessor_recommended_financing,
        cast(null as decimal(18,2)) as assessor_recommended_grant,
        cast(null as decimal(18,2)) as assessor_assessment_support_cap,
        cast(null as STRING)     as approver_name,
        cast(null as STRING)     as approver_remarks,
        cast(null as decimal(18,2)) as approver_assessment_support_cap,
        cast(null as STRING)     as training_name_of_course_certification,
        cast(null as STRING)     as training_program_type,
        cast(null as STRING)     as training_program,
        cast(null as STRING)     as training_overview,
        cast(null as integer)     as training_cost,
        cast(null as STRING)     as training_mode_of_delivery,
        cast(null as timestamp)   as training_estimated_start_date,
        cast(null as timestamp)   as training_estimated_end_date,
        cast(null as integer)     as training_total_hours,
        cast(null as boolean)     as training_include_practical_hours,
        cast(null as boolean)     as training_include_job_training,
        cast(null as STRING)     as training_provider_type,
        cast(null as STRING)     as training_provider_name,
        cast(null as STRING)     as training_awarding_body_name,
        cast(null as STRING)     as training_awarding_body_details,
        cast(null as STRING)     as training_country,
        cast(null as STRING)     as customer_name,
        cast(null as STRING)     as customer_mobile,
        cast(null as STRING)     as customer_email,
        cast(null as STRING)     as customer_nationality,
        cast(null as STRING)     as customer_gender,
        cast(null as timestamp)   as customer_date_of_birth,
        cast(null as STRING)     as customer_cpr_number,
        customer_enterprise_name  as customer_cr_or_cpr,
        customer_enterprise_name  as customer_application_name,
        source_system_name,
        is_deleted,
        report_date,
        dbt_updated_at,

        -- =========================
        -- OS2 COLUMNS
        -- =========================
        reference_number,
        program_type,
        cr_license_cpr,
        customer_enterprise_name,
        approved_on_date,
        monitoring_due_date,
        total_approved_amount_tamkeen_share_old,
        submitted_on,
        is_hipo_application,
        programcap,
        applicationcap,
        tkshareamt,
        applicationcapunutilized,
        customershareamt,
        totalcostwvat,
        starton,
        endon,
        monitoringduedate,
        spendingperiodduedate,
        claimingperiodduedate,
        duration,
        isactive,
        createdby,
        createdon,
        updatedby,
        updatedon,
        submittedon,
        approvedon,
        amendappinstancedocgudi_ar,
        haswagesupportmolemployees,
        calculatedeconomicvalue,
        calculatedgrantamount,

        -- =========================
        -- MIS COLUMNS
        -- =========================
        cast(null as STRING)     as application_subtype,
        cast(null as STRING)     as mis_source_table,
        cast(null as STRING)     as individual_id,
        cast(null as STRING)     as certificate_id,
        cast(null as STRING)     as pid,
        cast(null as STRING)     as product_id,
        cast(null as STRING)     as individual_name,
        cast(null as STRING)     as certificate_name,
        cast(null as STRING)     as product_pid_name,
        cast(null as STRING)     as product_name,
        cast(null as STRING)     as training_provider,
        cast(null as STRING)     as sponsorship,
        cast(null as STRING)     as certificate_approval,
        cast(null as STRING)     as assessment_analyst,
        cast(null as timestamp)   as tpcs_start_date,
        cast(null as timestamp)   as tpcs_end_date,
        cast(null as timestamp)   as tpcs_exam_date,
        cast(null as timestamp)   as submitted_date,
        cast(null as timestamp)   as application_date,
        cast(null as timestamp)   as end_date,
        cast(null as decimal(18,2)) as total_fees,
        cast(null as decimal(18,2)) as salary,
        cast(null as decimal(18,2)) as certificate_cap,
        cast(null as STRING)     as state,
        cast(null as STRING)     as status_reason,
        cast(null as STRING)     as has_no_payment,
        cast(null as STRING)     as pass,
        cast(null as STRING)     as study_type,
        cast(null as STRING)     as workflow_status,
        cast(null as STRING)     as withdraw_reason,
        cast(null as STRING)     as violation_status,
        cast(null as STRING)     as segment,
        cast(null as STRING)     as employment_details,
        cast(null as STRING)     as justification_for_certification,
        cast(null as STRING)     as modified_by,
        cast(null as timestamp)   as modified_on,
        cast(null as STRING)     as individual_cpr,
        cast(null as timestamp)   as individual_date_of_birth,
        cast(null as timestamp)   as individual_graduation_date,
        cast(null as STRING)     as individual_email,
        cast(null as STRING)     as individual_mobile,
        cast(null as STRING)     as individual_addr_flat,
        cast(null as STRING)     as individual_addr_building,
        cast(null as STRING)     as individual_addr_road,
        cast(null as STRING)     as individual_addr_block,
        cast(null as STRING)     as individual_addr_area,
        cast(null as STRING)     as individual_school_level,
        cast(null as STRING)     as individual_university_level,
        cast(null as STRING)     as individual_university_specialization,
        cast(null as STRING)     as individual_highest_degree,
        cast(null as STRING)     as individual_gender,
        cast(null as STRING)     as individual_qualification,
        cast(null as STRING)     as individual_nationality,
        cast(null as STRING)     as certificate_external_id,
        cast(null as STRING)     as certificate_old_category,
        cast(null as STRING)     as certificate_broad,
        cast(null as STRING)     as certificate_detailed,
        cast(null as STRING)     as certificate_narrow,
        cast(null as STRING)     as certificate_awarding_body,
        cast(null as STRING)     as certificate_type,
        cast(null as STRING)     as certificate_category,
        cast(null as timestamp)   as first_submit_for_analysis_date,
        cast(null as timestamp)   as last_submit_for_analysis_date,
        cast(null as timestamp)   as first_send_back_date,
        cast(null as timestamp)   as last_send_back_date,
        cast(null as timestamp)   as last_send_for_interview_date,
        cast(null as timestamp)   as last_send_to_manager_date,
        cast(null as timestamp)   as last_approved_by_manager_date,
        cast(null as timestamp)   as last_rejected_date,
        cast(null as timestamp)   as last_withdraw_date,
        cast(null as STRING)     as company_id,
        cast(null as STRING)     as company_name,
        cast(null as STRING)     as cr_number,
        cast(null as STRING)     as enterprise_application_id,
        cast(null as STRING)     as employee_application_id,
        cast(null as decimal(18,2)) as approved_amount,
        cast(null as decimal(18,2)) as paid_amount,
		cast(null as STRING)     as tmkn_area,
        cast(null as STRING)   as tmkn_crregistrationdate,
        cast(null as STRING)     as tws_job_title,
        cast(null as STRING)     as tws_job,
        cast(null as STRING)     as tmkn_maincr,
        cast(null as STRING)     as tmkn_industry,
        cast(null as STRING)     as tmkn_workflowstatus,
        cast(null as STRING)     as tmkn_isapproved,
        cast(null as decimal(18,2)) as tmkn_humancapitalgrant,
        cast(null as decimal(18,2)) as tmkn_totalcommitted,
        cast(null as timestamp)   as tmkn_monitoringduedate,
        cast(null as decimal(18,2)) as tmkn_revenuey1,
        cast(null as STRING)     as tmkn_objectiverouting,
        cast(null as timestamp)   as tmkn_contractstartdate,
        cast(null as timestamp)   as tmkn_contractenddate,
        cast(null as STRING)     as tmkn_primaryname,
        cast(null as STRING)     as tmkn_primarymobilenumber,
        cast(null as STRING)     as tmkn_primaryemail,
        cast(null as STRING)     as tmkn_blocknumber,
        CAST(NULL AS STRING)    as tmkn_commercialname,
        CAST(NULL AS decimal)   as tmkn_humancapitalactionplanmaxapproved,
        CAST(NULL AS decimal)    as tmkn_plangrant,
        CAST(NULL AS decimal)   as tmkn_profilegrantmax,
        CAST(NULL AS decimal)    as tmkn_actplangrant,
        CAST(NULL AS decimal)   as tmkn_humancapitaltotalcommitted

    from os2_source
),
mis as (

    select
        -- =========================
        -- COMMON / OS1 COLUMNS
        -- =========================
        cast(null as STRING)     as os1_source_table,

        cast(application_id as STRING) as application_id,
        application_no,

        cast(null as STRING)     as cr_license_no,
        cast(null as STRING)     as commercial_name,
        cast(null as bigint)      as program_id,
        cast(null as integer)     as application_status_id,
        cast(null as integer)     as application_type_id,
        cast(null as integer)     as assessor_recommendation_id,
        cast(null as integer)     as application_loan_status_id,
        cast(null as integer)     as customer_user_id,
        cast(null as integer)     as rm_user_id,
        cast(null as integer)     as assessor_user_id,
        cast(null as integer)     as approver_user_id,
        cast(null as STRING)     as application_type,
        cast(null as STRING)     as assessor_recommendation,
        cast(null as STRING)     as financing_approved_loan_status,
        cast(null as STRING)     as sector,
        cast(null as STRING)     as turnover,
        cast(null as STRING)     as program_name,
        cast(null as STRING)     as program_type_name,

        cast(null as STRING)     as application_status,
        cast(null as STRING)     as internal_status,

        cast(null as timestamp)   as created_on,

        approved_on,

        cast(null as timestamp)   as contract_start_date,
        end_date                  as contract_end_date,

        cast(null as timestamp)   as approve_pending_customer_confirmation_on,
        cast(null as timestamp)   as saved_on,
        cast(null as timestamp)   as spending_period_end_date,

        cast(null as boolean)     as is_financing_approved,
        cast(null as decimal(18,2)) as program_support_cap,
        cast(null as decimal(18,2)) as financing_cap,
        cast(null as decimal(18,2)) as financing_guarantee_cap,
        cast(null as decimal(18,2)) as approved_financing_amount,
        cast(null as decimal(18,2)) as approved_guarantee_amount,
        cast(null as integer)     as loan_tenor,
        cast(null as decimal(18,2)) as interest_rate,
        cast(null as timestamp)   as loan_start_date,
        cast(null as timestamp)   as loan_end_date,
        cast(null as decimal(18,2)) as total_interest_amount,
        cast(null as integer)     as grace_period,
        cast(null as decimal(18,2)) as monthly_installment,
        cast(null as decimal(18,2)) as total_requested,
        cast(null as decimal(18,2)) as approved_grant_maximum,

        approved_amount           as approved_grant,

        cast(null as decimal(18,2)) as consumed_amount,
        cast(null as decimal(18,2)) as remaining_amount,
        cast(null as integer)     as cap,

        tamkeen_share,

        cast(null as integer)     as total_number_employees,
        cast(null as STRING)     as bank_name,
        cast(null as STRING)     as rm_name,
        cast(null as STRING)     as rm_remarks,
        cast(null as integer)     as rm_score,
        cast(null as decimal(18,2)) as rm_recommended_financing,
        cast(null as decimal(18,2)) as rm_recommended_grant,
        cast(null as decimal(18,2)) as rm_assessment_support_cap,
        cast(null as STRING)     as assessor_name,
        cast(null as STRING)     as assessor_remarks,
        cast(null as integer)     as assessor_score,
        cast(null as decimal(18,2)) as assessor_recommended_financing,
        cast(null as decimal(18,2)) as assessor_recommended_grant,
        cast(null as decimal(18,2)) as assessor_assessment_support_cap,
        cast(null as STRING)     as approver_name,
        cast(null as STRING)     as approver_remarks,
        cast(null as decimal(18,2)) as approver_assessment_support_cap,

        certificate_name          as training_name_of_course_certification,

        cast(null as STRING)     as training_program_type,

        product_name              as training_program,

        employment_details        as training_overview,

        cast(total_fees as integer) as training_cost,

        cast(null as STRING)     as training_mode_of_delivery,

        tpcs_start_date           as training_estimated_start_date,
        tpcs_end_date             as training_estimated_end_date,

        cast(null as integer)     as training_total_hours,
        cast(null as boolean)     as training_include_practical_hours,
        cast(null as boolean)     as training_include_job_training,
        cast(null as STRING)     as training_provider_type,

        training_provider,

        certificate_awarding_body as training_awarding_body_name,

        cast(null as STRING)     as training_awarding_body_details,
        cast(null as STRING)     as training_country,

        individual_name           as customer_name,
        individual_mobile         as customer_mobile,
        individual_email          as customer_email,
        individual_nationality    as customer_nationality,
        individual_gender         as customer_gender,
        individual_date_of_birth  as customer_date_of_birth,
        individual_cpr            as customer_cpr_number,
        individual_cpr            as customer_cr_or_cpr,
        individual_name           as customer_application_name,

        source_system_name,

        is_deleted,

        CURRENT_DATE as report_date,
        dbt_updated_at,

        -- =========================
        -- OS2 COLUMNS
        -- =========================
        cast(null as STRING)     as reference_number,
        cast(null as STRING)     as program_type,
        cast(null as STRING)     as cr_license_cpr,
        cast(null as STRING)     as customer_enterprise_name,
        cast(null as timestamp)   as approved_on_date,
        cast(null as timestamp)   as monitoring_due_date,
        cast(null as decimal(18,2)) as total_approved_amount_tamkeen_share_old,

        submitted_date            as submitted_on,

        cast(null as STRING)     as is_hipo_application,
        cast(null as decimal(18,2)) as programcap,
        cast(null as decimal(18,2)) as applicationcap,
        cast(null as decimal(18,2)) as tkshareamt,
        cast(null as decimal(18,2)) as applicationcapunutilized,
        cast(null as decimal(18,2)) as customershareamt,
        cast(null as decimal(18,2)) as totalcostwvat,
        cast(null as timestamp)   as starton,
        cast(null as timestamp)   as endon,
        cast(null as timestamp)   as monitoringduedate,
        cast(null as timestamp)   as spendingperiodduedate,
        cast(null as timestamp)   as claimingperiodduedate,
        cast(null as integer)     as duration,
        cast(null as boolean)     as isactive,
        cast(null as STRING)     as createdby,
        cast(null as timestamp)   as createdon,
        cast(null as STRING)     as updatedby,
        cast(null as timestamp)   as updatedon,
        cast(null as timestamp)   as submittedon,
        cast(null as timestamp)   as approvedon,
        cast(null as STRING)     as amendappinstancedocgudi_ar,
        cast(null as boolean)     as haswagesupportmolemployees,
        cast(null as decimal(18,2)) as calculatedeconomicvalue,
        cast(null as decimal(18,2)) as calculatedgrantamount,

        -- =========================
        -- MIS COLUMNS
        -- =========================
        application_subtype,
        mis_source_table,
        individual_id,
        certificate_id,
        pid,
        product_id,
        individual_name,
        certificate_name,
        product_pid_name,
        product_name,
        training_provider,
        sponsorship,
        certificate_approval,
        assessment_analyst,
        tpcs_start_date,
        tpcs_end_date,
        tpcs_exam_date,
        submitted_date,
        application_date,
        end_date,
        total_fees,
        salary,
        certificate_cap,
        state,
        status_reason,
        has_no_payment,
        pass,
        study_type,
        workflow_status,
        withdraw_reason,
        violation_status,
        segment,
        employment_details,
        justification_for_certification,
        modified_by,
        modified_on,
        individual_cpr,
        individual_date_of_birth,
        individual_graduation_date,
        individual_email,
        individual_mobile,
        individual_addr_flat,
        individual_addr_building,
        individual_addr_road,
        individual_addr_block,
        individual_addr_area,
        individual_school_level,
        individual_university_level,
        individual_university_specialization,
        individual_highest_degree,
        individual_gender,
        individual_qualification,
        individual_nationality,
        certificate_external_id,
        certificate_old_category,
        certificate_broad,
        certificate_detailed,
        certificate_narrow,
        certificate_awarding_body,
        certificate_type,
        certificate_category,
        first_submit_for_analysis_date,
        last_submit_for_analysis_date,
        first_send_back_date,
        last_send_back_date,
        last_send_for_interview_date,
        last_send_to_manager_date,
        last_approved_by_manager_date,
        last_rejected_date,
        last_withdraw_date,
        company_id,
        company_name,
        cr_number,
        enterprise_application_id,
        employee_application_id,
        approved_amount,
        paid_amount,
        tmkn_area,
        tmkn_crregistrationdate,
        tws_job_title,
        tws_job,
        tmkn_maincr,
        tmkn_industry,
        tmkn_workflowstatus,
        tmkn_isapproved,
        tmkn_humancapitalgrant,
        tmkn_totalcommitted,
        tmkn_monitoringduedate,
        tmkn_revenuey1,
        tmkn_objectiverouting,
        tmkn_contractstartdate,
        tmkn_contractenddate,
        tmkn_primaryname,
        tmkn_primarymobilenumber,
        tmkn_primaryemail,
        tmkn_blocknumber,
        tmkn_commercialname,
        tmkn_humancapitalactionplanmaxapproved		,
        tmkn_plangrant,
        tmkn_profilegrantmax,
        tmkn_actplangrant,
        tmkn_humancapitaltotalcommitted

    from mis_source
)

select * from os1
union all
select * from os2
union all
select * from mis
)
SELECT
    `os1_source_table`,
    `application_id`,
    `application_no`,
    `cr_license_no`,
    `commercial_name`,
    `program_id`,
    `application_status_id`,
    `application_type_id`,
    `assessor_recommendation_id`,
    `application_loan_status_id`,
    `customer_user_id`,
    `rm_user_id`,
    `assessor_user_id`,
    `approver_user_id`,
    `application_type`,
    `assessor_recommendation`,
    `financing_approved_loan_status`,
    `sector`,
    `turnover`,
    `program_name`,
    `program_type_name`,
    `application_status`,
    `internal_status`,
    `created_on`,
    `approved_on`,
    `contract_start_date`,
    `contract_end_date`,
    `approve_pending_customer_confirmation_on`,
    `saved_on`,
    `spending_period_end_date`,
    `is_financing_approved`,
    `program_support_cap`,
    `financing_cap`,
    `financing_guarantee_cap`,
    `approved_financing_amount`,
    `approved_guarantee_amount`,
    `loan_tenor`,
    `interest_rate`,
    `loan_start_date`,
    `loan_end_date`,
    `total_interest_amount`,
    `grace_period`,
    `monthly_installment`,
    `total_requested`,
    `approved_grant_maximum`,
    `approved_grant`,
    `consumed_amount`,
    `remaining_amount`,
    `cap`,
    `tamkeen_share`,
    `total_number_employees`,
    `bank_name`,
    `rm_name`,
    `rm_remarks`,
    `rm_score`,
    `rm_recommended_financing`,
    `rm_recommended_grant`,
    `rm_assessment_support_cap`,
    `assessor_name`,
    `assessor_remarks`,
    `assessor_score`,
    `assessor_recommended_financing`,
    `assessor_recommended_grant`,
    `assessor_assessment_support_cap`,
    `approver_name`,
    `approver_remarks`,
    `approver_assessment_support_cap`,
    `training_name_of_course_certification`,
    `training_program_type`,
    `training_program`,
    `training_overview`,
    `training_cost`,
    `training_mode_of_delivery`,
    `training_estimated_start_date`,
    `training_estimated_end_date`,
    `training_total_hours`,
    `training_include_practical_hours`,
    `training_include_job_training`,
    `training_provider_type`,
    `training_provider_name`,
    `training_awarding_body_name`,
    `training_awarding_body_details`,
    `training_country`,
    `customer_name`,
    `customer_mobile`,
    `customer_email`,
    `customer_nationality`,
    `customer_gender`,
    `customer_date_of_birth`,
    `customer_cpr_number`,
    `customer_cr_or_cpr`,
    `customer_application_name`,
    `source_system_name`,
    `is_deleted`,
    `report_date`,
    `dbt_updated_at`,
    `reference_number`,
    `program_type`
FROM bronze_raw
),

silver_layer AS (
SELECT
    os1_source_table,
    application_id,
    application_no,
    cr_license_no,
    commercial_name,
    program_id,
    application_status_id,
    application_type_id,
    assessor_recommendation_id,
    application_loan_status_id,
    customer_user_id,
    rm_user_id,
    assessor_user_id,
    approver_user_id,
    application_type,
    assessor_recommendation,
    financing_approved_loan_status,
    sector,
    turnover,
    program_name,
    program_type_name,
    application_status,
    internal_status,
    created_on,
    approved_on,
    contract_start_date,
    contract_end_date,
    approve_pending_customer_confirmation_on,
    saved_on,
    spending_period_end_date,
    is_financing_approved,
    program_support_cap,
    financing_cap,
    financing_guarantee_cap,
    approved_financing_amount,
    approved_guarantee_amount,
    loan_tenor,
    interest_rate,
    loan_start_date,
    loan_end_date,
    total_interest_amount,
    grace_period,
    monthly_installment,
    total_requested,
    approved_grant_maximum,
    approved_grant,
    consumed_amount,
    remaining_amount,
    cap,
    tamkeen_share,
    total_number_employees,
    bank_name,
    rm_name,
    rm_remarks,
    rm_score,
    rm_recommended_financing,
    rm_recommended_grant,
    rm_assessment_support_cap,
    assessor_name,
    assessor_remarks,
    assessor_score,
    assessor_recommended_financing,
    assessor_recommended_grant,
    assessor_assessment_support_cap,
    approver_name,
    approver_remarks,
    approver_assessment_support_cap,
    training_name_of_course_certification,
    training_program_type,
    training_program,
    training_overview,
    training_cost,
    training_mode_of_delivery,
    training_estimated_start_date,
    training_estimated_end_date,
    training_total_hours,
    training_include_practical_hours,
    training_include_job_training,
    training_provider_type,
    training_provider_name,
    training_awarding_body_name,
    training_awarding_body_details,
    training_country,
    customer_name,
    customer_mobile,
    customer_email,
    customer_nationality,
    customer_gender,
    customer_date_of_birth,
    customer_cpr_number,
    customer_cr_or_cpr,
    customer_application_name,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    reference_number,
    program_type
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.application_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'os1_source_table'),
        (2, 'application_id'),
        (3, 'application_no'),
        (4, 'cr_license_no'),
        (5, 'commercial_name'),
        (6, 'program_id'),
        (7, 'application_status_id'),
        (8, 'application_type_id'),
        (9, 'assessor_recommendation_id'),
        (10, 'application_loan_status_id'),
        (11, 'customer_user_id'),
        (12, 'rm_user_id'),
        (13, 'assessor_user_id'),
        (14, 'approver_user_id'),
        (15, 'application_type'),
        (16, 'assessor_recommendation'),
        (17, 'financing_approved_loan_status'),
        (18, 'sector'),
        (19, 'turnover'),
        (20, 'program_name'),
        (21, 'program_type_name'),
        (22, 'application_status'),
        (23, 'internal_status'),
        (24, 'created_on'),
        (25, 'approved_on'),
        (26, 'contract_start_date'),
        (27, 'contract_end_date'),
        (28, 'approve_pending_customer_confirmation_on'),
        (29, 'saved_on'),
        (30, 'spending_period_end_date'),
        (31, 'is_financing_approved'),
        (32, 'program_support_cap'),
        (33, 'financing_cap'),
        (34, 'financing_guarantee_cap'),
        (35, 'approved_financing_amount'),
        (36, 'approved_guarantee_amount'),
        (37, 'loan_tenor'),
        (38, 'interest_rate'),
        (39, 'loan_start_date'),
        (40, 'loan_end_date'),
        (41, 'total_interest_amount'),
        (42, 'grace_period'),
        (43, 'monthly_installment'),
        (44, 'total_requested'),
        (45, 'approved_grant_maximum'),
        (46, 'approved_grant'),
        (47, 'consumed_amount'),
        (48, 'remaining_amount'),
        (49, 'cap'),
        (50, 'tamkeen_share'),
        (51, 'total_number_employees'),
        (52, 'bank_name'),
        (53, 'rm_name'),
        (54, 'rm_remarks'),
        (55, 'rm_score'),
        (56, 'rm_recommended_financing'),
        (57, 'rm_recommended_grant'),
        (58, 'rm_assessment_support_cap'),
        (59, 'assessor_name'),
        (60, 'assessor_remarks'),
        (61, 'assessor_score'),
        (62, 'assessor_recommended_financing'),
        (63, 'assessor_recommended_grant'),
        (64, 'assessor_assessment_support_cap'),
        (65, 'approver_name'),
        (66, 'approver_remarks'),
        (67, 'approver_assessment_support_cap'),
        (68, 'training_name_of_course_certification'),
        (69, 'training_program_type'),
        (70, 'training_program'),
        (71, 'training_overview'),
        (72, 'training_cost'),
        (73, 'training_mode_of_delivery'),
        (74, 'training_estimated_start_date'),
        (75, 'training_estimated_end_date'),
        (76, 'training_total_hours'),
        (77, 'training_include_practical_hours'),
        (78, 'training_include_job_training'),
        (79, 'training_provider_type'),
        (80, 'training_provider_name'),
        (81, 'training_awarding_body_name'),
        (82, 'training_awarding_body_details'),
        (83, 'training_country'),
        (84, 'customer_name'),
        (85, 'customer_mobile'),
        (86, 'customer_email'),
        (87, 'customer_nationality'),
        (88, 'customer_gender'),
        (89, 'customer_date_of_birth'),
        (90, 'customer_cpr_number'),
        (91, 'customer_cr_or_cpr'),
        (92, 'customer_application_name'),
        (93, 'source_system_name'),
        (94, 'is_deleted'),
        (95, 'report_date'),
        (96, 'dbt_updated_at'),
        (97, 'reference_number'),
        (98, 'program_type')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'os1_source_table'),
        (2, 'application_id'),
        (3, 'application_no'),
        (4, 'cr_license_no'),
        (5, 'commercial_name'),
        (6, 'program_id'),
        (7, 'application_status_id'),
        (8, 'application_type_id'),
        (9, 'assessor_recommendation_id'),
        (10, 'application_loan_status_id'),
        (11, 'customer_user_id'),
        (12, 'rm_user_id'),
        (13, 'assessor_user_id'),
        (14, 'approver_user_id'),
        (15, 'application_type'),
        (16, 'assessor_recommendation'),
        (17, 'financing_approved_loan_status'),
        (18, 'sector'),
        (19, 'turnover'),
        (20, 'program_name'),
        (21, 'program_type_name'),
        (22, 'application_status'),
        (23, 'internal_status'),
        (24, 'created_on'),
        (25, 'approved_on'),
        (26, 'contract_start_date'),
        (27, 'contract_end_date'),
        (28, 'approve_pending_customer_confirmation_on'),
        (29, 'saved_on'),
        (30, 'spending_period_end_date'),
        (31, 'is_financing_approved'),
        (32, 'program_support_cap'),
        (33, 'financing_cap'),
        (34, 'financing_guarantee_cap'),
        (35, 'approved_financing_amount'),
        (36, 'approved_guarantee_amount'),
        (37, 'loan_tenor'),
        (38, 'interest_rate'),
        (39, 'loan_start_date'),
        (40, 'loan_end_date'),
        (41, 'total_interest_amount'),
        (42, 'grace_period'),
        (43, 'monthly_installment'),
        (44, 'total_requested'),
        (45, 'approved_grant_maximum'),
        (46, 'approved_grant'),
        (47, 'consumed_amount'),
        (48, 'remaining_amount'),
        (49, 'cap'),
        (50, 'tamkeen_share'),
        (51, 'total_number_employees'),
        (52, 'bank_name'),
        (53, 'rm_name'),
        (54, 'rm_remarks'),
        (55, 'rm_score'),
        (56, 'rm_recommended_financing'),
        (57, 'rm_recommended_grant'),
        (58, 'rm_assessment_support_cap'),
        (59, 'assessor_name'),
        (60, 'assessor_remarks'),
        (61, 'assessor_score'),
        (62, 'assessor_recommended_financing'),
        (63, 'assessor_recommended_grant'),
        (64, 'assessor_assessment_support_cap'),
        (65, 'approver_name'),
        (66, 'approver_remarks'),
        (67, 'approver_assessment_support_cap'),
        (68, 'training_name_of_course_certification'),
        (69, 'training_program_type'),
        (70, 'training_program'),
        (71, 'training_overview'),
        (72, 'training_cost'),
        (73, 'training_mode_of_delivery'),
        (74, 'training_estimated_start_date'),
        (75, 'training_estimated_end_date'),
        (76, 'training_total_hours'),
        (77, 'training_include_practical_hours'),
        (78, 'training_include_job_training'),
        (79, 'training_provider_type'),
        (80, 'training_provider_name'),
        (81, 'training_awarding_body_name'),
        (82, 'training_awarding_body_details'),
        (83, 'training_country'),
        (84, 'customer_name'),
        (85, 'customer_mobile'),
        (86, 'customer_email'),
        (87, 'customer_nationality'),
        (88, 'customer_gender'),
        (89, 'customer_date_of_birth'),
        (90, 'customer_cpr_number'),
        (91, 'customer_cr_or_cpr'),
        (92, 'customer_application_name'),
        (93, 'source_system_name'),
        (94, 'is_deleted'),
        (95, 'report_date'),
        (96, 'dbt_updated_at'),
        (97, 'reference_number'),
        (98, 'program_type')
),

bronze_normalized AS (
    SELECT
        CAST(`os1_source_table` AS STRING) AS `os1_source_table`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`program_id` AS STRING) AS `program_id`,
        CAST(`application_status_id` AS STRING) AS `application_status_id`,
        CAST(`application_type_id` AS STRING) AS `application_type_id`,
        CAST(`assessor_recommendation_id` AS STRING) AS `assessor_recommendation_id`,
        CAST(`application_loan_status_id` AS STRING) AS `application_loan_status_id`,
        CAST(`customer_user_id` AS STRING) AS `customer_user_id`,
        CAST(`rm_user_id` AS STRING) AS `rm_user_id`,
        CAST(`assessor_user_id` AS STRING) AS `assessor_user_id`,
        CAST(`approver_user_id` AS STRING) AS `approver_user_id`,
        CAST(`application_type` AS STRING) AS `application_type`,
        CAST(`assessor_recommendation` AS STRING) AS `assessor_recommendation`,
        CAST(`financing_approved_loan_status` AS STRING) AS `financing_approved_loan_status`,
        CAST(`sector` AS STRING) AS `sector`,
        CAST(`turnover` AS STRING) AS `turnover`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_type_name` AS STRING) AS `program_type_name`,
        CAST(`application_status` AS STRING) AS `application_status`,
        CAST(`internal_status` AS STRING) AS `internal_status`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`approve_pending_customer_confirmation_on` AS STRING) AS `approve_pending_customer_confirmation_on`,
        CAST(`saved_on` AS STRING) AS `saved_on`,
        CAST(`spending_period_end_date` AS STRING) AS `spending_period_end_date`,
        CAST(`is_financing_approved` AS STRING) AS `is_financing_approved`,
        CAST(`program_support_cap` AS STRING) AS `program_support_cap`,
        CAST(`financing_cap` AS STRING) AS `financing_cap`,
        CAST(`financing_guarantee_cap` AS STRING) AS `financing_guarantee_cap`,
        CAST(`approved_financing_amount` AS STRING) AS `approved_financing_amount`,
        CAST(`approved_guarantee_amount` AS STRING) AS `approved_guarantee_amount`,
        CAST(`loan_tenor` AS STRING) AS `loan_tenor`,
        CAST(`interest_rate` AS STRING) AS `interest_rate`,
        CAST(`loan_start_date` AS STRING) AS `loan_start_date`,
        CAST(`loan_end_date` AS STRING) AS `loan_end_date`,
        CAST(`total_interest_amount` AS STRING) AS `total_interest_amount`,
        CAST(`grace_period` AS STRING) AS `grace_period`,
        CAST(`monthly_installment` AS STRING) AS `monthly_installment`,
        CAST(`total_requested` AS STRING) AS `total_requested`,
        CAST(`approved_grant_maximum` AS STRING) AS `approved_grant_maximum`,
        CAST(`approved_grant` AS STRING) AS `approved_grant`,
        CAST(`consumed_amount` AS STRING) AS `consumed_amount`,
        CAST(`remaining_amount` AS STRING) AS `remaining_amount`,
        CAST(`cap` AS STRING) AS `cap`,
        CAST(`tamkeen_share` AS STRING) AS `tamkeen_share`,
        CAST(`total_number_employees` AS STRING) AS `total_number_employees`,
        CAST(`bank_name` AS STRING) AS `bank_name`,
        CAST(`rm_name` AS STRING) AS `rm_name`,
        CAST(`rm_remarks` AS STRING) AS `rm_remarks`,
        CAST(`rm_score` AS STRING) AS `rm_score`,
        CAST(`rm_recommended_financing` AS STRING) AS `rm_recommended_financing`,
        CAST(`rm_recommended_grant` AS STRING) AS `rm_recommended_grant`,
        CAST(`rm_assessment_support_cap` AS STRING) AS `rm_assessment_support_cap`,
        CAST(`assessor_name` AS STRING) AS `assessor_name`,
        CAST(`assessor_remarks` AS STRING) AS `assessor_remarks`,
        CAST(`assessor_score` AS STRING) AS `assessor_score`,
        CAST(`assessor_recommended_financing` AS STRING) AS `assessor_recommended_financing`,
        CAST(`assessor_recommended_grant` AS STRING) AS `assessor_recommended_grant`,
        CAST(`assessor_assessment_support_cap` AS STRING) AS `assessor_assessment_support_cap`,
        CAST(`approver_name` AS STRING) AS `approver_name`,
        CAST(`approver_remarks` AS STRING) AS `approver_remarks`,
        CAST(`approver_assessment_support_cap` AS STRING) AS `approver_assessment_support_cap`,
        CAST(`training_name_of_course_certification` AS STRING) AS `training_name_of_course_certification`,
        CAST(`training_program_type` AS STRING) AS `training_program_type`,
        CAST(`training_program` AS STRING) AS `training_program`,
        CAST(`training_overview` AS STRING) AS `training_overview`,
        CAST(`training_cost` AS STRING) AS `training_cost`,
        CAST(`training_mode_of_delivery` AS STRING) AS `training_mode_of_delivery`,
        CAST(`training_estimated_start_date` AS STRING) AS `training_estimated_start_date`,
        CAST(`training_estimated_end_date` AS STRING) AS `training_estimated_end_date`,
        CAST(`training_total_hours` AS STRING) AS `training_total_hours`,
        CAST(`training_include_practical_hours` AS STRING) AS `training_include_practical_hours`,
        CAST(`training_include_job_training` AS STRING) AS `training_include_job_training`,
        CAST(`training_provider_type` AS STRING) AS `training_provider_type`,
        CAST(`training_provider_name` AS STRING) AS `training_provider_name`,
        CAST(`training_awarding_body_name` AS STRING) AS `training_awarding_body_name`,
        CAST(`training_awarding_body_details` AS STRING) AS `training_awarding_body_details`,
        CAST(`training_country` AS STRING) AS `training_country`,
        CAST(`customer_name` AS STRING) AS `customer_name`,
        CAST(`customer_mobile` AS STRING) AS `customer_mobile`,
        CAST(`customer_email` AS STRING) AS `customer_email`,
        CAST(`customer_nationality` AS STRING) AS `customer_nationality`,
        CAST(`customer_gender` AS STRING) AS `customer_gender`,
        CAST(`customer_date_of_birth` AS STRING) AS `customer_date_of_birth`,
        CAST(`customer_cpr_number` AS STRING) AS `customer_cpr_number`,
        CAST(`customer_cr_or_cpr` AS STRING) AS `customer_cr_or_cpr`,
        CAST(`customer_application_name` AS STRING) AS `customer_application_name`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`reference_number` AS STRING) AS `reference_number`,
        CAST(`program_type` AS STRING) AS `program_type`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`os1_source_table` AS STRING) AS `os1_source_table`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`commercial_name` AS STRING) AS `commercial_name`,
        CAST(`program_id` AS STRING) AS `program_id`,
        CAST(`application_status_id` AS STRING) AS `application_status_id`,
        CAST(`application_type_id` AS STRING) AS `application_type_id`,
        CAST(`assessor_recommendation_id` AS STRING) AS `assessor_recommendation_id`,
        CAST(`application_loan_status_id` AS STRING) AS `application_loan_status_id`,
        CAST(`customer_user_id` AS STRING) AS `customer_user_id`,
        CAST(`rm_user_id` AS STRING) AS `rm_user_id`,
        CAST(`assessor_user_id` AS STRING) AS `assessor_user_id`,
        CAST(`approver_user_id` AS STRING) AS `approver_user_id`,
        CAST(`application_type` AS STRING) AS `application_type`,
        CAST(`assessor_recommendation` AS STRING) AS `assessor_recommendation`,
        CAST(`financing_approved_loan_status` AS STRING) AS `financing_approved_loan_status`,
        CAST(`sector` AS STRING) AS `sector`,
        CAST(`turnover` AS STRING) AS `turnover`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_type_name` AS STRING) AS `program_type_name`,
        CAST(`application_status` AS STRING) AS `application_status`,
        CAST(`internal_status` AS STRING) AS `internal_status`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`approve_pending_customer_confirmation_on` AS STRING) AS `approve_pending_customer_confirmation_on`,
        CAST(`saved_on` AS STRING) AS `saved_on`,
        CAST(`spending_period_end_date` AS STRING) AS `spending_period_end_date`,
        CAST(`is_financing_approved` AS STRING) AS `is_financing_approved`,
        CAST(`program_support_cap` AS STRING) AS `program_support_cap`,
        CAST(`financing_cap` AS STRING) AS `financing_cap`,
        CAST(`financing_guarantee_cap` AS STRING) AS `financing_guarantee_cap`,
        CAST(`approved_financing_amount` AS STRING) AS `approved_financing_amount`,
        CAST(`approved_guarantee_amount` AS STRING) AS `approved_guarantee_amount`,
        CAST(`loan_tenor` AS STRING) AS `loan_tenor`,
        CAST(`interest_rate` AS STRING) AS `interest_rate`,
        CAST(`loan_start_date` AS STRING) AS `loan_start_date`,
        CAST(`loan_end_date` AS STRING) AS `loan_end_date`,
        CAST(`total_interest_amount` AS STRING) AS `total_interest_amount`,
        CAST(`grace_period` AS STRING) AS `grace_period`,
        CAST(`monthly_installment` AS STRING) AS `monthly_installment`,
        CAST(`total_requested` AS STRING) AS `total_requested`,
        CAST(`approved_grant_maximum` AS STRING) AS `approved_grant_maximum`,
        CAST(`approved_grant` AS STRING) AS `approved_grant`,
        CAST(`consumed_amount` AS STRING) AS `consumed_amount`,
        CAST(`remaining_amount` AS STRING) AS `remaining_amount`,
        CAST(`cap` AS STRING) AS `cap`,
        CAST(`tamkeen_share` AS STRING) AS `tamkeen_share`,
        CAST(`total_number_employees` AS STRING) AS `total_number_employees`,
        CAST(`bank_name` AS STRING) AS `bank_name`,
        CAST(`rm_name` AS STRING) AS `rm_name`,
        CAST(`rm_remarks` AS STRING) AS `rm_remarks`,
        CAST(`rm_score` AS STRING) AS `rm_score`,
        CAST(`rm_recommended_financing` AS STRING) AS `rm_recommended_financing`,
        CAST(`rm_recommended_grant` AS STRING) AS `rm_recommended_grant`,
        CAST(`rm_assessment_support_cap` AS STRING) AS `rm_assessment_support_cap`,
        CAST(`assessor_name` AS STRING) AS `assessor_name`,
        CAST(`assessor_remarks` AS STRING) AS `assessor_remarks`,
        CAST(`assessor_score` AS STRING) AS `assessor_score`,
        CAST(`assessor_recommended_financing` AS STRING) AS `assessor_recommended_financing`,
        CAST(`assessor_recommended_grant` AS STRING) AS `assessor_recommended_grant`,
        CAST(`assessor_assessment_support_cap` AS STRING) AS `assessor_assessment_support_cap`,
        CAST(`approver_name` AS STRING) AS `approver_name`,
        CAST(`approver_remarks` AS STRING) AS `approver_remarks`,
        CAST(`approver_assessment_support_cap` AS STRING) AS `approver_assessment_support_cap`,
        CAST(`training_name_of_course_certification` AS STRING) AS `training_name_of_course_certification`,
        CAST(`training_program_type` AS STRING) AS `training_program_type`,
        CAST(`training_program` AS STRING) AS `training_program`,
        CAST(`training_overview` AS STRING) AS `training_overview`,
        CAST(`training_cost` AS STRING) AS `training_cost`,
        CAST(`training_mode_of_delivery` AS STRING) AS `training_mode_of_delivery`,
        CAST(`training_estimated_start_date` AS STRING) AS `training_estimated_start_date`,
        CAST(`training_estimated_end_date` AS STRING) AS `training_estimated_end_date`,
        CAST(`training_total_hours` AS STRING) AS `training_total_hours`,
        CAST(`training_include_practical_hours` AS STRING) AS `training_include_practical_hours`,
        CAST(`training_include_job_training` AS STRING) AS `training_include_job_training`,
        CAST(`training_provider_type` AS STRING) AS `training_provider_type`,
        CAST(`training_provider_name` AS STRING) AS `training_provider_name`,
        CAST(`training_awarding_body_name` AS STRING) AS `training_awarding_body_name`,
        CAST(`training_awarding_body_details` AS STRING) AS `training_awarding_body_details`,
        CAST(`training_country` AS STRING) AS `training_country`,
        CAST(`customer_name` AS STRING) AS `customer_name`,
        CAST(`customer_mobile` AS STRING) AS `customer_mobile`,
        CAST(`customer_email` AS STRING) AS `customer_email`,
        CAST(`customer_nationality` AS STRING) AS `customer_nationality`,
        CAST(`customer_gender` AS STRING) AS `customer_gender`,
        CAST(`customer_date_of_birth` AS STRING) AS `customer_date_of_birth`,
        CAST(`customer_cpr_number` AS STRING) AS `customer_cpr_number`,
        CAST(`customer_cr_or_cpr` AS STRING) AS `customer_cr_or_cpr`,
        CAST(`customer_application_name` AS STRING) AS `customer_application_name`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`reference_number` AS STRING) AS `reference_number`,
        CAST(`program_type` AS STRING) AS `program_type`
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
        'application_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'application_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'application_base' AS table_name,
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
        'application_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'application_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
