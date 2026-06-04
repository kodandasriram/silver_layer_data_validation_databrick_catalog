-- Compare bronze-layer query output with silver-layer table output for customer_individual_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\Silver_layer_03-June-2026\converted db script to databricks\Databricks_union of all sources\customer_individual_base.sql
-- Silver source: converted db script to databricks\silver_layer scripts\customer_individual_base_silver_layer.sql

WITH
bronze_layer AS (
/*
Generated Databricks union layer for customer_individual_base.
Column order and typed NULL placeholders follow dbt model: customer_individual_base.sql.
Source transformations are embedded whole from the converted Databricks OS1/OS2/MIS scripts.
dbt macros expanded to Databricks TRY_CAST / string cleanup expressions.
*/

/*
 =================================================================================================

Name        : CUSTOMER_INDIVIDUAL_UNIFIED_BASE
Description : Cross-system Silver view of the Customer Individual master, combining
              the OS2 (NEO2) and MIS source-specific per-person tables into a single
              unified base.

              Built as a UNION ALL of:
                - customer_individual_base_os2  (OSUSR_ZMZ_INDIVIDUAL + OSUSR_ZMZ_CUSTOMER)
                - customer_individual_base_mis  (MIS_INDIVIDUALBASE)

              Both source models are at the same grain (one row per individual person
              in their respective source), so this UNION is a true cross-system master.
              Use source_system_name to filter or attribute rows back to their origin.

              NOTE â€” OS1 not included:
              OS1 does not expose a per-individual master table. Its equivalent
              file (previously misnamed customer_individual_employee_base_os1.sql,
              renamed to application_employee_base_os1.sql) is at application-employee
              grain â€” a different fact and not a master. If a future requirement
              surfaces a per-individual entity in OS1, this UNION can be extended.

              NOTE on duplicates:
              The same individual (same CPR) may appear in BOTH OS2 and MIS. This
              UNION does NOT deduplicate â€” downstream consumers should be aware
              that one person may show twice with different source_system_name
              values. If a deduplicated view is needed later, add a CPR-based
              MAX_BY(...) layer on top.

Source Models : customer_individual_base_os2
                customer_individual_base_mis

Target Table : CUSTOMER_INDIVIDUAL_UNIFIED_BASE
Load Type    : Full Load
Materialized : table
Format       : PARQUET
Tags         : silver, customer_individual, unified, daily

Revision History:
--------------------------------------------------------------
Version | Date       | Author     | Description
--------------------------------------------------------------
1.0     | 2026-05-18 | Pandi     | Initial version unifying OS2 + MIS customer-individual masters
================================================================================================= 
*/




-- ============================================================================
-- OS2 DATA
-- ============================================================================

WITH
    customer_individual_base_os2 AS (
/* =================================================================================================
Name        : APPLICATION_SUPPORT_INDIVIDUAL_BASE_OS2
Description : This model extracts and transforms application support, wage, and training-related
              attributes for individuals from the NEO2 (OS2) source system Bronze Layer and loads
              into the target table as part of the Silver Layer data pipeline.
Source Tables : neo2.OSUSR_2DA_APPLICATIONSUPPORT
                neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_398_APPLICATIONSTATUS
                neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_COMPANY
                neo2.OSUSR_QM6_PORTALUSER
                neo2.OSUSR_ZMZ_INDIVIDUAL
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_398_SUPPORTTYPE
                neo2.OSUSR_398_SUPPORTAREA
                neo2.OSUSR_2DA_APPLICATIONSUPPORTSTATUS
                neo2.OSUSR_NTP_APPLICATIONCUSTOMERINDIVIDUAL
                neo2.OSUSR_3QQ_APPLICANTSEGMENT
                neo2.OSUSR_3QQ_TRAININGTRACK
                neo2.OSUSR_398_COUNTRY
                neo2.OSUSR_398_ACADEMICDEGREE
                neo2.OSUSR_2DA_PROVIDERTYPE
                neo2.OSUSR_2DA_EXTERNALPROVIDER
                neo2.OSUSR_VW9_TRAINING
                neo2.OSUSR_VW9_CERTIFICATION
                neo2.OSUSR_R9T_TRAININGPROGRAM
                neo2.OSUSR_GUR_AUTHORIZEDENTITIES
                neo2.OSUSR_R9T_TRAININGPROGRAMPROVIDER
                neo2.OSUSR_3QQ_TRAININGPROGRAMTYPE1
                neo2.OSUSR_VW9_TRAININGDELIVERYTYPE
                neo2.OSUSR_VW9_TRAININGPAYMENTTYPE
                neo2.OSUSR_R9T_TRAININGDETAILAREA
                neo2.OSUSR_R9T_TRAININGKNOWLEDGEAREA
                neo2.OSUSR_2DA_EMPLOYEE
                neo2.OSUSR_2DA_JOBLEVEL
                neo2.OSUSR_VYW_WAGE
                neo2.OSUSR_3QQ_WAGETRACK
                neo2.OSUSR_NTP_AMENDMENTREQUEST
                neo2.OSUSR_VYW_WAGESUPPORTPLAN
                neo2.OSUSR_1AT_ASSESSMENT
                neo2.OSUSR_1AT_ASSESSMENTSTATUS
                neo2.OSSYS_BPM_PROCESS
                neo2.OSSYS_BPM_ACTIVITY
                neo2.OSUSR_NTP_WITHDRAWALREQUEST
                neo2.OSUSR_NTP_WITHDRAWALSTATUS
                neo2.OSUSR_2DA_SUPPORTSTRUCTURE
                neo2.OSUSR_2DA_EMPLOYEEACKNOWLEDGMENT
                neo2.OSUSR_2DA_APPLICATIONSUPPORTACTION
                neo2.OSUSR_NTP_APPLICATIONCONTACTDETAILS
                neo2.OSUSR_GUR_AUTHORIZEDENTITIES
Target Table : OSUSR_2DA_APPLICATIONSUPPORT_INDIVIDUAL
Load Type    : Full Load
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily
Revision History:
--------------------------------------------------------------
Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-05-12 |  Kaviya        | Initial version
================================================================================================= */

WITH tmkncap_cte AS (
    SELECT
        WSP.WAGEID                                                                        AS ID_APPLICATION_SUPPORT,
        SUM(CASE WHEN WSP.TKSHAREAMT IS NOT NULL THEN WSP.TKSHAREAMT ELSE 0 END)         AS TOTAL_TAMKEEN_CAP_AMOUNT,
        MIN(COALESCE(WSP.MONTHSTARTDATE, WSP.MONTHPAYMENTDATE))                          AS START_SUPPORT,
        MAX(WSP.MONTHENDDATE)                                                             AS END_SUPPORT,
        date_diff(MONTH,
            MIN(COALESCE(WSP.MONTHSTARTDATE, WSP.MONTHPAYMENTDATE)),
            MAX(date_add(WSP.MONTHENDDATE, 1))
        )                                                                                 AS SUPPORT_DURATION
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VYW_WAGESUPPORTPLAN` WSP
        INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORT` APPSUP
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
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_1AT_ASSESSMENT` ass
        INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_PROCESS` pro
            ON pro.TOP_PROCESS_ID = ass.PROCESSID
        INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY` act
            ON act.PROCESS_ID = pro.ID
        LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_1AT_ASSESSMENTSTATUS` AssessmentStatus
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
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_WITHDRAWALREQUEST` withdraw
        LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_WITHDRAWALSTATUS` withdrawstat
            ON withdrawstat.CODE = withdraw.STATUSID
        INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY` act
            ON act.PROCESS_ID = withdraw.PROCESSID
),

ss_cte AS (
    SELECT
        ASP.APPLICATIONID,
        SUM(SS.REQUESTEDAMT)                                                              AS SUPPORT_STRUCTURE_REQUESTED_AMOUNT,
        SUM(SS.TKSHAREOVR)                                                                AS SUPPORT_STRUCTURE_TAMKEEN_SHARE_OVER,
        SUM(SS.TKSHARE)                                                                   AS SUPPORT_STRUCTURE_TAMKEEN_SHARE
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_SUPPORTSTRUCTURE` SS
        JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORT` ASP
            ON ASP.ID = SS.APPLICATIONSUPPORTID
    GROUP BY ASP.APPLICATIONID
)
, cte_base as (
SELECT
    APPSUP.ID                                                                                as id,
    APPSUP.AMENDMENTREQUESTID                                                             AS amendmentrequestid,
    APP.ID                                                                                AS applicationid,
    APPSUP.INDIVIDUALID                                                                   AS individualid,
    APPSUP.ID                                                                             AS application_support_id,
    APP.GUID                                                                              AS guid,
    APP.REFERENCENUMBER                                                                   AS application_no,
    ProgVer.COMMERCIALNAME_EN                                                             AS program_name,
    ProgVer.COMMERCIALNAME_AR                                                             AS program_name_ar,
    AppWFS.LABEL                                                                          AS workflow_status,
    CASE WHEN APP.ISACTIVE THEN 'No' ELSE 'Yes' END                                      AS is_active,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE WHEN APP.CREATEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE (APP.CREATEDON + INTERVAL 3 HOURS) END
        ELSE CASE WHEN amdment.CREATEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE (amdment.CREATEDON + INTERVAL 3 HOURS) END
    END                                                                                   AS created_on,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE WHEN APP.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE (APP.SUBMITTEDON + INTERVAL 3 HOURS) END
        ELSE CASE WHEN amdment.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE (amdment.SUBMITTEDON + INTERVAL 3 HOURS) END
    END                                                                                   AS submitted_on,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL
        THEN CASE WHEN APP.APPROVEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                  ELSE (APP.APPROVEDON + INTERVAL 3 HOURS) END
        ELSE asses_amed.CLOSED
    END                                                                                   AS approved_on,
    CASE WHEN APP.CUSTOMERTYPEID = 'CMP' THEN 'Enterprise' ELSE 'Individual' END         AS customer_type,
    CASE WHEN APP.STARTON <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.STARTON AS DATE) END                                     AS start_date,
    CASE WHEN APP.ENDON <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.ENDON AS DATE) END                                       AS end_date,
    CASE WHEN WAGE.STARTDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(WAGE.STARTDATE AS DATE) END                                  AS start_date_wage,
    CASE WHEN WAGE.ENDDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(WAGE.ENDDATE AS DATE) END                                    AS end_date_wage,
    WAGE.TOTALDURATION                                                                    AS duration_months_wage,
    APP.DURATION                                                                          AS duration_months_application,
    CASE
        WHEN APP.ISHIPOOPTIONID = 1 THEN 'HiPo'
        WHEN APP.ISHIPOOPTIONID = 2 THEN 'Non-HiPo'
        ELSE NULL
    END                                                                                   AS is_hipo,
    CASE WHEN APP.MONITORINGDUEDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.MONITORINGDUEDATE AS DATE) END                           AS monitoring_due_date_application,
    CASE WHEN APP.SPENDINGPERIODDUEDATE <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(APP.SPENDINGPERIODDUEDATE AS DATE) END                       AS spending_period_end_date_application,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCUS.NAMEEN
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE UPPER(TRIM(CUS.NAMEEN)) END
    END                                                                                   AS commercial_name_en,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCUS.NAMEAR
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE UPPER(TRIM(CUS.NAMEAR)) END
    END                                                                                   AS commercial_name_ar,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCMP.CODE
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE TRIM(CMP.CODE) END
    END                                                                                   AS cr_license_no,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCMP.MAINCODE
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE TRIM(CMP.MAINCODE) END
    END                                                                                   AS cr_license_no_main,
    CASE
        WHEN ProgVer.COMMERCIALNAME_EN IN (
            'On-the-Job Training Program',
            'On-the-Job Training Program "Lawyers Track"'
        ) THEN OJTCMP.REGISTRATIONDATE
        ELSE CASE WHEN APP.CUSTOMERTYPEID = 'IND' THEN NULL ELSE CAST(CMP.REGISTRATIONDATE AS DATE) END
    END                                                                                   AS registration_date,
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
    END                                                                                   AS cr_license_type,
    UPPER(TRIM(CusApp.NAMEEN))                                                            AS individual_name_en,
	UPPER(TRIM(CusApp.NAMEAR))                                                            AS individual_name_ar,
    UPPER(TRIM(PORTUSR.NAME))                                                             AS portal_user_name,
    LOWER(TRIM(PORTUSR.EMAIL))                                                            AS email,
    CONCAT(
        COALESCE(PORTUSR.MOBILECOUNTRYPREFIX, ''),
        CASE WHEN PORTUSR.MOBILEPHONE IS NOT NULL AND PORTUSR.MOBILEPHONE <> '' THEN ' ' ELSE '' END,
        COALESCE(PORTUSR.MOBILEPHONE, '')
    )                                                                                     AS mobile_no,
    CusIndApp.CPRNUMBER                                                                   AS cpr,
    CAST(CusIndApp.DATEOFBIRTH AS DATE)                                                   AS date_of_birth,
    CASE
        WHEN CusIndApp.GENDERID = 1 THEN 'Male'
        WHEN CusIndApp.GENDERID = 2 THEN 'Female'
        ELSE NULL
    END                                                                                   AS gender,
    SuppArea.LABEL                                                                        AS support_area,
    SuppType.LABEL                                                                        AS support_type,
    WAGETRACK.LABEL                                                                       AS support_track_wage,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN LOWER(TRIM(APPCusIND.EMAILADDRESS))
         ELSE LOWER(TRIM(Emp.EMAILADDRESS))
    END                                                                                   AS customer_contact_individual_email,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
        THEN CONCAT(COALESCE(APPCusIND.MOBILECOUNTRYPREFIX, ''), ' ', COALESCE(APPCusIND.MOBILENUMBER, ''))
        ELSE CONCAT(COALESCE(Emp.MOBILECOUNTRYPREFIX, ''), ' ', COALESCE(Emp.MOBILENUMBER, ''))
    END                                                                                   AS customer_contact_individual_mobile_no,
    CASE WHEN Emp.DATEDEGREEGRADUATION <= TIMESTAMP '1900-01-01 00:00:00'
         THEN NULL ELSE CAST(Emp.DATEDEGREEGRADUATION AS DATE)
    END                                                                                   AS graduation_date,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL OR EMP.JOBCURRENTWAGE = 0
         THEN UPPER(TRIM(APPCusIND.DEGREESPECIALIZATION))
         ELSE UPPER(TRIM(Emp.DEGREESPECIALIZATION))
    END                                                                                   AS highest_educational_specialization,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN UPPER(TRIM(APPCusIND.EMPLOYERNAME))
         ELSE UPPER(TRIM(EMP.EMPLOYERNAME))
    END                                                                                   AS employer_name,
    UPPER(TRIM(Emp.JOBTITLE))                                                             AS job_title,
    CASE WHEN CAST(Emp.JOININGDATE AS DATE) = DATE '1900-01-01'
         THEN NULL ELSE CAST(Emp.JOININGDATE AS DATE)
    END                                                                                   AS joining_date,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL OR EMP.JOBCURRENTWAGE = 0
         THEN IndSeg.LABEL ELSE IndSegWage.LABEL
    END                                                                                   AS individual_segment,
    TraTrack.LABEL                                                                        AS training_track,
    Emp.CURRENTMONTHSEXPERIENCE                                                           AS months_of_experience_current,
    Emp.TOTALMONTHSEXPERIENCE                                                             AS months_of_experience_total,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN UPPER(TRIM(APPCusIND.UNIVERSITYNAME))
         ELSE UPPER(TRIM(Emp.UNIVERSITYNAME))
    END                                                                                   AS university_name,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL
         THEN Country.COUNTRYNAME ELSE CountryEmpUni.COUNTRYNAME
    END                                                                                   AS university_location,
    CASE WHEN APPCusIND.ISENTREPRENEUR THEN 'Yes' ELSE 'No' END                          AS is_entrepreneur,
    CASE WHEN Emp.JOBCURRENTWAGE > 0
         THEN Emp.JOBCURRENTWAGE ELSE APPCusIND.CURRENTWAGE
    END                                                                                   AS wage_current,
    WAGE.REQUESTEDINCREMENTAMOUNT                                                         AS requested_increment,
    WAGE.REQUESTEDSTIPEND                                                                 AS requested_stipend,
    WAGE.NEWWAGE                                                                          AS wage_new,
    CASE WHEN EMP.JOBCURRENTWAGE IS NULL OR EMP.JOBCURRENTWAGE = 0
         THEN AcaDegree.LABEL ELSE AcaDegreeWage.LABEL
    END                                                                                   AS highest_educational_degree,
    CASE WHEN APPSUP.ISACTIVE THEN 'Yes' ELSE 'No' END                                   AS is_active_individual,
    ProvType.LABEL                                                                        AS training_provider_type,
    ProvLocCR.CODE                                                                        AS training_provider_cr_license_no,
    CASE
        WHEN APPSUP.PROVIDERID IS NOT NULL THEN ProvLoc.NAMEEN
        WHEN APPSUP.EXTERNALPROVIDERID IS NOT NULL THEN ProvOverseas.NAME
        ELSE NULL
    END                                                                                   AS training_provider,
    CASE
        WHEN APPSUP.PROVIDERID > 0 THEN 'Bahrain'
        WHEN APPSUP.EXTERNALPROVIDERID > 0 THEN CountryVendor.COUNTRYNAME
        ELSE NULL
    END                                                                                   AS training_provider_location,
    Trainingprogram.NAME                                                                  AS certificate_name,
    CAST(TRA.TRAININGSTARTDATE AS DATE)                                                   AS training_start_date,
    CAST(
        CASE WHEN TRA.TRAININGENDDATE > TRA.TRAININGASSESSMENTDATE
             THEN TRA.TRAININGENDDATE ELSE TRA.TRAININGASSESSMENTDATE END
    AS DATE)                                                                              AS training_end_date,
    TRAMODE.LABEL                                                                         AS training_mode_of_delivery,
    TRA.TKSHAREAMT                                                                        AS grant_approved_training,
    TRA.CUSTOMERSHARETOTAL                                                                AS grant_approved_training_customer_share,
    TRAPAYTYPE.LABEL                                                                      AS training_payment_type,
    Trainingprogram.AWARDINGBODYNAME                                                      AS certificate_awarding_body,
    TrnPrgPrv.TRAININGPROGRAMCAP                                                          AS certificate_cap,
    TrnPrgPrv.TRAININGHOURS                                                               AS certificate_training_hours,
    Trainingprogram.TRAININGPROGAMSTATUSID                                                AS certificate_status,
    TRAAREA.LABEL                                                                         AS training_knowledge_area,
    TRAAREADET.LABEL                                                                      AS training_knowledge_area_detailed,
    JobLevelCur.LABEL                                                                     AS job_level_current,
    JobLevelNew.LABEL                                                                     AS job_level_new,
    CASE WHEN EMP.EMPLOYMENTTYPEID = 'FT' THEN 'Full-Time'
         WHEN EMPLOYMENTTYPEID = 'PT' THEN 'Part-Time'
         ELSE NULL
    END                                                                                   AS employment_type,
    CASE WHEN EMP.EMPLOYEECONTRACTTYPEID = 'PC' THEN 'Permanent'
         WHEN EMPLOYMENTTYPEID = 'TC' THEN 'Temporary'
         ELSE NULL
    END                                                                                   AS employment_contract_type,
    ''                                                                                    AS latest_activity,
    ''                                                                                    AS workflow_status_last_activity,
    WAGE.TKSHAREAMT                                                                       AS grant_approved_wage,
    tmkncap.total_tamkeen_cap_amount,
    WAGE.CUSTOMERSHAREAMT                                                                 AS total_customer_share_amount,
    WAGE.TKSHAREAMT                                                                       AS total_tamkeen_share_amount_wage,
    CASE
        WHEN APP.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
        ELSE date_diff(YEAR,
            CAST(CusIndApp.DATEOFBIRTH AS DATE),
            CAST(APP.SUBMITTEDON AS DATE)
        )
    END                                                                                   AS individual_age,
    CASE
        WHEN APP.SUBMITTEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
        ELSE date_diff(YEAR,
            CAST(CusIndApp.DATEOFBIRTH AS DATE),
            CAST(current_timestamp AS DATE)
        )
    END                                                                                   AS individual_age_live,
    AppSuppWFS.LABEL                                                                      AS workflow_status_application_support,
    asses.ASSESSMENT_STATUS_LABEL                                                         AS workflow_status_application_detailed,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL THEN asses.ASSESSMENT_STATUS_LABEL
        ELSE asses_amed2.ASSESSMENT_STATUS_LABEL
    END                                                                                   AS workflow_status_application_support_detailed,
    CASE
        WHEN AppWFS.LABEL IN ('Rejected') THEN asses.CLOSED
        ELSE NULL
    END                                                                                   AS rejected_on,
    TrainingProgramType.NAME                                                              AS certificate_type,
    CASE
        WHEN APPSUP.ACTIVESTATUSID = 'INA' THEN 'No'
        WHEN APPSUP.ACTIVESTATUSID = 'ACT' THEN 'Yes'
        ELSE NULL
    END                                                                                   AS is_active_application_support,
    CASE
        WHEN ACK.EMPLOYEESUBMISSIONDATE <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
        ELSE (ACK.EMPLOYEESUBMISSIONDATE + INTERVAL 3 HOURS)
    END                                                                                   AS confirmed_on,
    CASE
        WHEN Withdrawal.STATUS = 'Accepted'
        THEN (Withdrawal.CLOSED + INTERVAL 3 HOURS)
        ELSE NULL
    END                                                                                   AS withdrawn_on,
    AuthEnt.NAME                                                                          AS authorized_training_provider,
    Trainingprogram.NAME                                                                  AS certification,
    AppSuppAction.LABEL                                                                   AS application_support_action_id,
    TRA.ITEMCOSTTOTAL                                                                     AS cost_of_training,
    APP.DURATION                                                                          AS training_duration,
    APPCONT.PRIMARYEMAIL                                                                  AS primary_email_contact_details,
    CONCAT(
        COALESCE(APPCONT.PRIMARYMOBILECOUNTRYPREFIX, ''),
        ' ',
        COALESCE(APPCONT.PRIMARYMOBILENUMBER, '')
    )                                                                                     AS primary_mobile_number_contact_details,
    APPCONT.PRIMARYNAME                                                                   AS primary_contact_name_contact_details,
    APP.CALCULATEDGRANTAMOUNT                                                             AS calculated_grant_amount,
    CASE
        WHEN APPSUP.AMENDMENTREQUESTID IS NULL THEN
            CASE
                WHEN APP.APPROVEDON <= TIMESTAMP '1900-01-01 00:00:00' THEN NULL
                ELSE (APP.APPROVEDON + INTERVAL 3 HOURS)
            END
        ELSE
            CASE
                WHEN amdment.APPROVEDON <= TIMESTAMP '1900-01-01 00:00:00'
                     AND asses_amed.CLOSED = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
                WHEN amdment.APPROVEDON <> TIMESTAMP '1900-01-01 00:00:00'
                THEN (amdment.APPROVEDON + INTERVAL 3 HOURS)
                ELSE (asses_amed.CLOSED + INTERVAL 3 HOURS)
            END
    END                                                                                   AS approved_on_new,
    APPSUP.REFERENCENUMBER                                                                AS application_support_ref,
    ss.SUPPORT_STRUCTURE_REQUESTED_AMOUNT                                                 AS approved_other,
    APS.LABEL                                                                             AS amendment_status,
    CASE WHEN emp.EMPLOYERNAME = ''
         THEN APPCusIND.EMPLOYERNAME ELSE emp.EMPLOYERNAME
    END                                                                                   AS employer_name_employee_details,
    WAGE.STARTDATE                                                                        AS start_support,
    WAGE.ENDDATE                                                                          AS end_support,
    WAGE.TOTALDURATION                                                                    AS support_duration,
	    -- ind.mobilecountryprefix                              AS mobile_country_prefix,
    cast (null as STRING) mobile_country_prefix,
    -- ind.mobilenumber                                     AS mobile_number,
    cast (null as STRING) mobile_number,
    FALSE as is_deleted,
    'NEO2'                                                                                AS source_system_name,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)                              AS dbt_updated_at,
    APPSUP.createdon,
    APPSUP.updatedon,
    ROW_NUMBER() OVER (

    PARTITION BY APPSUP.ID

    ORDER BY APPSUP.UPDATEDON DESC, APPSUP.CREATEDON DESC

  ) AS rnk

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORT` APPSUP
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION4` APP
        ON APP.ID = APPSUP.APPLICATIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_APPLICATIONSTATUS` AppWFS
        ON AppWFS.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCUSTOMER` APPCUS
        ON APP.ID = APPCUS.APPLICATIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE` CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` CUS
        ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY` CMP
        ON CUS.ID = CMP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_QM6_PORTALUSER` PORTUSR
        ON PORTUSR.ID = APP.PORTALUSERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL` IND
        ON CUS.ID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` CusApp
        ON APPSUP.INDIVIDUALID = CusApp.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL` CusIndApp
        ON APPSUP.INDIVIDUALID = CusIndApp.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION` ProgVer
        ON ProgVer.ID = APP.PROGRAMVERSIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_SUPPORTTYPE` SuppType
        ON APPSUP.SUPPORTTYPEID = SuppType.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_SUPPORTAREA` SuppArea
        ON SuppType.SUPPORTAREAID = SuppArea.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORTSTATUS` AppSuppWFS
        ON AppSuppWFS.CODE = APPSUP.APPLICATIONSUPPORTSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCUSTOMERINDIVIDUAL` APPCusIND
        ON APPCUS.ID = APPCusIND.APPLICATIONCUSTOMERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_APPLICANTSEGMENT` IndSeg
        ON IndSeg.CODE = APPCusIND.APPLICANTSEGMENTID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_TRAININGTRACK` TraTrack
        ON TraTrack.ID = APPCusIND.TRAININGTRACKID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_COUNTRY` Country
        ON Country.ID = APPCusIND.UNIVERSITYLOCATIONS
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_ACADEMICDEGREE` AcaDegree
        ON AcaDegree.CODE = APPCusIND.ACADEMICDEGREEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_PROVIDERTYPE` ProvType
        ON ProvType.ID = APPSUP.PROVIDERTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` ProvLoc
        ON ProvLoc.ID = APPSUP.PROVIDERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY` ProvLocCR
        ON ProvLocCR.ID = ProvLoc.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EXTERNALPROVIDER` ProvOverseas
        ON ProvOverseas.ID = APPSUP.EXTERNALPROVIDERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_COUNTRY` CountryVendor
        ON CountryVendor.ID = ProvOverseas.COUNTRYID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_TRAINING` TRA
        ON TRA.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_CERTIFICATION` TRACertif
        ON TRACertif.ID = TRA.CERTIFICATIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_R9T_TRAININGPROGRAM` Trainingprogram
        ON TRACertif.TRAININGPROGRAMID = Trainingprogram.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_GUR_AUTHORIZEDENTITIES` AuthEnt
        ON AuthEnt.CUSTOMERID = APPSUP.PROVIDERID AND AuthEnt.PROFILETYPEID = 'TRP'
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_R9T_TRAININGPROGRAMPROVIDER` TrnPrgPrv
        ON TrnPrgPrv.TRAININGPROGRAMID = Trainingprogram.ID AND TrnPrgPrv.AUTHORIZEDPROVIDERID = AuthEnt.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_TRAININGPROGRAMTYPE1` TrainingProgramType
        ON TrainingProgramType.ID = Trainingprogram.TRAININGTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_TRAININGDELIVERYTYPE` TRAMODE
        ON TRAMODE.CODE = TRA.TRAININGDELIVERYTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_TRAININGPAYMENTTYPE` TRAPAYTYPE
        ON TRAPAYTYPE.CODE = TRA.TRAININGPAYMENTTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_R9T_TRAININGDETAILAREA` TRAAREADET
        ON TRAAREADET.ID = Trainingprogram.TRAININGDETAILAREAID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_R9T_TRAININGKNOWLEDGEAREA` TRAAREA
        ON TRAAREA.ID = Trainingprogram.TRAININGKNOWLEDGEAREAID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EMPLOYEE` Emp
        ON Emp.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_COUNTRY` CountryEmpUni
        ON CountryEmpUni.ID = Emp.UNIVERSITYLOCATIONS
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_ACADEMICDEGREE` AcaDegreeWage
        ON AcaDegreeWage.CODE = Emp.ACADEMICDEGREEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_APPLICANTSEGMENT` IndSegWage
        ON IndSegWage.CODE = Emp.SEGMENTTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_JOBLEVEL` JobLevelCur
        ON JobLevelCur.CODE = Emp.JOBLEVELID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VYW_WAGE` WAGE
        ON WAGE.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_JOBLEVEL` JobLevelNew
        ON JobLevelNew.CODE = WAGE.NEWJOBLEVELID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_WAGETRACK` WAGETRACK
        ON WAGETRACK.CODE = WAGE.WAGETRACKID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_AMENDMENTREQUEST4` amdment
        ON amdment.ID = APPSUP.AMENDMENTREQUESTID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER` OJTCUS
        ON Emp.EMPLOYERID = OJTCUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY` OJTCMP
        ON OJTCUS.ID = OJTCMP.ID
    LEFT JOIN assessment_cte asses
        ON asses.APPLICATIONID = APP.ID AND (asses.RN = 1 OR asses.RN IS NULL)
    LEFT JOIN assessment_cte asses_amed
        ON asses_amed.AMENDMENTREQUESTID = APPSUP.AMENDMENTREQUESTID AND asses_amed.APPROVAL = 'Yes'
    LEFT JOIN assessment_cte asses_amed2
        ON asses_amed2.AMENDMENTREQUESTID = APPSUP.AMENDMENTREQUESTID AND (asses_amed2.RN = 1 OR asses_amed2.RN IS NULL)
    LEFT JOIN withdrawal_cte Withdrawal
        ON Withdrawal.APPLICATIONID = APP.ID AND Withdrawal.RN = 1 AND Withdrawal.STATUS <> 'Draft'
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EMPLOYEEACKNOWLEDGMENT` ACK
        ON APPSUP.ID = ACK.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORTACTION` AppSuppAction
        ON AppSuppAction.CODE = APPSUP.APPLICATIONSUPPORTACTIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATIONCONTACTDETAILS` APPCONT
        ON APPCONT.APPLICATIONID = APP.ID
    LEFT JOIN ss_cte ss
        ON APP.ID = ss.APPLICATIONID
    LEFT JOIN tmkncap_cte tmkncap
        ON tmkncap.ID_APPLICATION_SUPPORT = APPSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_APPLICATIONSTATUS` APS
        ON APS.CODE = amdment.AMENDMENTSTATUSID

WHERE (
      APPSUP.INDIVIDUALID IS NOT NULL
  AND (ProgVer.PROGRAMID <> 46 OR ProgVer.PROGRAMID IS NULL)
  AND (APP.APPLICATIONSTATUSID <> 'PM' OR APP.APPLICATIONSTATUSID IS NULL)
  AND NOT (
      SuppArea.LABEL = 'Support Structure'
      AND ProgVer.COMMERCIALNAME_EN = 'Medical Fellowship Program'
  )
  OR SuppArea.LABEL IS NULL
) )
SELECT
    id,
    amendmentrequestid,
    applicationid,
    individualid,
    application_support_id,
    guid,
    application_no,
    program_name,
    program_name_ar,
    workflow_status,
    is_active,
    created_on,
    submitted_on,
    approved_on,
    customer_type,
    start_date,
    end_date,
    start_date_wage,
    end_date_wage,
    duration_months_wage,
    duration_months_application,
    is_hipo,
    monitoring_due_date_application,
    spending_period_end_date_application,
    commercial_name_en,
    commercial_name_ar,
    cr_license_no,
    cr_license_no_main,
    registration_date,
    cr_license_type,
    individual_name_en,
    individual_name_ar,
    portal_user_name,
    email,
    mobile_no,
    cpr,
    date_of_birth,
    gender,
    support_area,
    support_type,
    support_track_wage,
    customer_contact_individual_email,
    customer_contact_individual_mobile_no,
    graduation_date,
    highest_educational_specialization,
    employer_name,
    job_title,
    joining_date,
    individual_segment,
    training_track,
    months_of_experience_current,
    months_of_experience_total,
    university_name,
    university_location,
    is_entrepreneur,
    wage_current,
    requested_increment,
    requested_stipend,
    wage_new,
    highest_educational_degree,
    is_active_individual,
    training_provider_type,
    training_provider_cr_license_no,
    training_provider,
    training_provider_location,
    certificate_name,
    training_start_date,
    training_end_date,
    training_mode_of_delivery,
    grant_approved_training,
    grant_approved_training_customer_share,
    training_payment_type,
    certificate_awarding_body,
    certificate_cap,
    certificate_training_hours,
    certificate_status,
    training_knowledge_area,
    training_knowledge_area_detailed,
    job_level_current,
    job_level_new,
    employment_type,
    employment_contract_type,
    latest_activity,
    workflow_status_last_activity,
    grant_approved_wage,
    total_tamkeen_cap_amount,
    total_customer_share_amount,
    total_tamkeen_share_amount_wage,
    individual_age,
    individual_age_live,
    workflow_status_application_support,
    workflow_status_application_detailed,
    workflow_status_application_support_detailed,
    rejected_on,
    certificate_type,
    is_active_application_support,
    confirmed_on,
    withdrawn_on,
    authorized_training_provider,
    certification,
    application_support_action_id,
    cost_of_training,
    training_duration,
    primary_email_contact_details,
    primary_mobile_number_contact_details,
    primary_contact_name_contact_details,
    calculated_grant_amount,
    approved_on_new,
    application_support_ref,
    approved_other,
    amendment_status,
    employer_name_employee_details,
    start_support,
    end_support,
    support_duration,
    mobile_country_prefix,
    mobile_number,
    is_deleted,
    source_system_name,
    TRY_CAST(dbt_updated_at AS TIMESTAMP) AS dbt_updated_at,
    TRY_CAST(createdon AS TIMESTAMP) AS createdon,
    TRY_CAST(updatedon AS TIMESTAMP) AS updatedon
FROM cte_base app
WHERE rnk = 1
),
    customer_individual_base_mis AS (
/*
============================================================================
silver_customer_individual_mis.sql
============================================================================
Per-source intermediate Silver model for the Customer Individual domain â€” MIS only.

Sources (within Customer Individual domain):
  â˜… MIS_individual    â€” anchor: individual customer / applicant entity
    tmkn_pid          â€” joined: program identifier reference (FK from individual)

Reference SPs (where these tables appear):
  - RPT-058_Individual_Applications        (uses both, joined)
  - RPT-051_TWS_Training_Enrollments       (uses both, joined via training enrollment)
  - RPT-044, RPT-045, RPT-047, others      (use mis_individual via emp.tws_individual_refrences)

The Customer Individual domain is focused on the *individual person*. Cross-
domain joins to Application, Training, Wage etc. are NOT performed here â€”
those joins happen in the unified Silver layer downstream.

mis_individual is the anchor. tmkn_pid is a separate small reference table
that holds program identifiers â€” it has its own lifecycle, but in MIS it's
typically referenced FROM application or enrollment rows via a `tws_Product`
or `tmkn_PID` foreign key. Whether it should be a separate Silver model or
part of Customer Individual is a domain-modelling judgement call.

Approach taken: include both as parallel UNIONed entities in Customer
Individual, with a discriminator. tmkn_pid is small (PID + product name) so
its rows occupy minimal width. The unified Silver layer can decide whether
to keep both or split them.

Cleansing only â€” no business logic.
============================================================================
*/


-- ============================================================================
-- MIS_individual â€” the individual customer entity
-- ============================================================================
SELECT
    'MIS_individual' AS mis_source_table,

    -- Identifiers
    CAST(ind.mis_individualid AS STRING)                AS individual_id,
    ind.mis_name                                         AS individual_name,
    ind.mis_cpr                                          AS cpr,

    -- Personal details
    CASE WHEN EXTRACT(YEAR FROM ind.mis_dateofbirth) > 1900
         THEN ind.mis_dateofbirth END                    AS date_of_birth,
    CASE WHEN EXTRACT(YEAR FROM ind.tmkn_graduation_date) > 1900
         THEN ind.tmkn_graduation_date END               AS graduation_date,

    -- Contact
    ind.mis_email                                        AS email,
    ind.mis_mobile                                       AS mobile,

    -- Address
    ind.mis_addr_flat                                    AS addr_flat,
    ind.mis_addr_building                                AS addr_building,
    ind.mis_addr_road                                    AS addr_road,
    ind.mis_addr_block                              AS addr_block,
    ind.mis_addr_area                                AS addr_area,

    -- Education / qualifications (option-set decoded)
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('mis_schoollevel')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.mis_schoollevel AS STRING)

)              AS school_level,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('mis_unversitylevel')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.mis_unversitylevel AS STRING)

)           AS university_level,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('mis_universityspecialization')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.mis_universityspecialization AS STRING)

) AS university_specialization,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('tmkn_highest_degree_obtained')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.tmkn_highest_degree_obtained AS STRING)

) AS highest_degree_obtained,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('mis_qualification')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.mis_qualification AS STRING)

)            AS qualification,

    -- Demographic (option-set decoded)
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('mis_gender')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.mis_gender AS STRING)

)                   AS gender,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('MIS_INDIVIDUALBASE')

      AND LOWER(sm.attributename) = LOWER('MIS_Nationality')

      AND CAST(sm.attributevalue AS STRING) = CAST(ind.mis_nationality AS STRING)

)              AS nationality,

    -- Placeholders for tmkn_pid branch (NULL in this branch)
    CAST(NULL AS STRING)                                AS pid_product_name,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`MIS_INDIVIDUALBASE` ind


UNION ALL


-- ============================================================================
-- tmkn_pid â€” Program Identifier reference (small lookup-style entity)
-- ============================================================================
SELECT
    'tmkn_pid' AS mis_source_table,

    -- Identifiers (use tmkn_pidId mapped to individual_id slot for shape compatibility)
    CAST(pid.tmkn_pidid AS STRING)                      AS individual_id,
    CAST(NULL AS STRING)                                AS individual_name,
    CAST(NULL AS STRING)                                AS cpr,

    -- Personal details (NULL in this branch)
    CAST(NULL AS DATE)                                   AS date_of_birth,
    CAST(NULL AS DATE)                                   AS graduation_date,
    CAST(NULL AS STRING)                                AS email,
    CAST(NULL AS STRING)                                AS mobile,
    CAST(NULL AS STRING)                                AS addr_flat,
    CAST(NULL AS STRING)                                AS addr_building,
    CAST(NULL AS STRING)                                AS addr_road,
    CAST(NULL AS STRING)                                AS addr_block,
    CAST(NULL AS STRING)                                AS addr_area,
    CAST(NULL AS STRING)                                AS school_level,
    CAST(NULL AS STRING)                                AS university_level,
    CAST(NULL AS STRING)                                AS university_specialization,
    CAST(NULL AS STRING)                                AS highest_degree_obtained,
    CAST(NULL AS STRING)                                AS qualification,
    CAST(NULL AS STRING)                                AS gender,
    CAST(NULL AS STRING)                                AS nationality,

    -- PID-specific fields
    pid.tmkn_productname                                 AS pid_product_name,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_PIDBASE` pid
)
SELECT
    -- =========================================================================
    -- COMMON / TECHNICAL
    -- =========================================================================
    CAST(id AS STRING)                                            AS id,
    CAST(amendmentrequestid AS STRING)                            AS amendmentrequestid,
    CAST(applicationid AS STRING)                                 AS applicationid,
    CAST(individualid AS STRING)                                  AS individualid,
    CAST(application_support_id AS STRING)                        AS application_support_id,

    guid,
    application_no,
    program_name,
    program_name_ar,
    workflow_status,
    is_active,
    created_on,
    submitted_on,
    approved_on,
    customer_type,
    start_date,
    end_date,
    start_date_wage,
    end_date_wage,
    duration_months_wage,
    duration_months_application,
    is_hipo,
    monitoring_due_date_application,
    spending_period_end_date_application,

    commercial_name_en,
    commercial_name_ar,
    cr_license_no,
    cr_license_no_main,
    registration_date,
    cr_license_type,

    individual_name_en,
    individual_name_ar,
    portal_user_name,
    email,
    mobile_no,
    cpr,
    date_of_birth,
    gender,

    support_area,
    support_type,
    support_track_wage,

    customer_contact_individual_email,
    customer_contact_individual_mobile_no,

    graduation_date,
    highest_educational_specialization,
    employer_name,
    job_title,
    joining_date,
    individual_segment,
    training_track,
    months_of_experience_current,
    months_of_experience_total,

    university_name,
    university_location,
    is_entrepreneur,
    wage_current,
    requested_increment,
    requested_stipend,
    wage_new,

    highest_educational_degree, 
    is_active_individual,

    training_provider_type,
    training_provider_cr_license_no,
    training_provider,
    training_provider_location,

    certificate_name,
    training_start_date,
    training_end_date,
    training_mode_of_delivery,
    grant_approved_training,
    grant_approved_training_customer_share,
    training_payment_type,
    certificate_awarding_body,
    certificate_cap,
    certificate_training_hours,
    certificate_status,
    training_knowledge_area,
    training_knowledge_area_detailed,

    job_level_current,
    job_level_new,
    employment_type,
    employment_contract_type,

    latest_activity,
    workflow_status_last_activity,

    grant_approved_wage,
    total_tamkeen_cap_amount,
    total_customer_share_amount,
    total_tamkeen_share_amount_wage,

    individual_age,
    individual_age_live,

    workflow_status_application_support,
    workflow_status_application_detailed,
    workflow_status_application_support_detailed,

    rejected_on,
    certificate_type,
    is_active_application_support,
    confirmed_on,
    withdrawn_on,

    authorized_training_provider,
    certification,
    application_support_action_id,
    cost_of_training,
    training_duration,

    primary_email_contact_details,
    primary_mobile_number_contact_details,
    primary_contact_name_contact_details,

    calculated_grant_amount,
    approved_on_new,
    application_support_ref,
    approved_other,
    amendment_status,

    employer_name_employee_details,
    start_support,
    end_support,
    support_duration,

    mobile_country_prefix,
    mobile_number,

    is_deleted,
    source_system_name,
    dbt_updated_at,
    createdon,
    updatedon,

    -- =========================================================================
    -- MIS EXTRA COLUMNS
    -- =========================================================================
    CAST(NULL AS STRING)                                          AS mis_source_table,
    CAST(NULL AS STRING)                                          AS individual_name,
    CAST(NULL AS STRING)                                          AS addr_flat,
    CAST(NULL AS STRING)                                          AS addr_building,
    CAST(NULL AS STRING)                                          AS addr_road,
    CAST(NULL AS STRING)                                          AS addr_block,
    CAST(NULL AS STRING)                                          AS addr_area,
    CAST(NULL AS STRING)                                          AS school_level,
    CAST(NULL AS STRING)                                          AS university_level,
    CAST(NULL AS STRING)                                          AS university_specialization,
    CAST(NULL AS STRING)                                          AS highest_degree_obtained,
    CAST(NULL AS STRING)                                          AS qualification,
    CAST(NULL AS STRING)                                          AS nationality,
    CAST(NULL AS STRING)                                          AS pid_product_name
FROM customer_individual_base_os2

UNION ALL

-- ============================================================================
-- MIS DATA
-- ============================================================================

SELECT
    -- =========================================================================
    -- COMMON / TECHNICAL
    -- =========================================================================
    CAST(individual_id AS STRING)                                 AS id,
    CAST(NULL AS STRING)                                          AS amendmentrequestid,
    CAST(NULL AS STRING)                                          AS applicationid,
    CAST(individual_id AS STRING)                                 AS individualid,
    CAST(NULL AS STRING)                                          AS application_support_id,

    CAST(NULL AS STRING)                                          AS guid,
    CAST(NULL AS STRING)                                          AS application_no,
    pid_product_name                                               AS program_name,
    CAST(NULL AS STRING)                                          AS program_name_ar,
    CAST(NULL AS STRING)                                          AS workflow_status,
    CAST(NULL AS STRING)                                          AS is_active,
    CAST(NULL AS TIMESTAMP)                                        AS created_on,
    CAST(NULL AS TIMESTAMP)                                        AS submitted_on,
    CAST(NULL AS TIMESTAMP)                                        AS approved_on,
    'Individual'                                                   AS customer_type,
    CAST(NULL AS DATE)                                             AS start_date,
    CAST(NULL AS DATE)                                             AS end_date,
    CAST(NULL AS DATE)                                             AS start_date_wage,
    CAST(NULL AS DATE)                                             AS end_date_wage,
    CAST(NULL AS BIGINT)                                           AS duration_months_wage,
    CAST(NULL AS BIGINT)                                           AS duration_months_application,
    CAST(NULL AS STRING)                                          AS is_hipo,
    CAST(NULL AS DATE)                                             AS monitoring_due_date_application,
    CAST(NULL AS DATE)                                             AS spending_period_end_date_application,

    CAST(NULL AS STRING)                                          AS commercial_name_en,
    CAST(NULL AS STRING)                                          AS commercial_name_ar,
    CAST(NULL AS STRING)                                          AS cr_license_no,
    CAST(NULL AS STRING)                                          AS cr_license_no_main,
    CAST(NULL AS DATE)                                             AS registration_date,
    CAST(NULL AS STRING)                                          AS cr_license_type,

    individual_name                                                AS individual_name_en,
    CAST(NULL AS STRING)                                          AS individual_name_ar,
    CAST(NULL AS STRING)                                          AS portal_user_name,
    email,
    mobile                                                         AS mobile_no,
    cpr,
    date_of_birth,
    gender,

    CAST(NULL AS STRING)                                          AS support_area,
    CAST(NULL AS STRING)                                          AS support_type,
    CAST(NULL AS STRING)                                          AS support_track_wage,

    CAST(NULL AS STRING)                                          AS customer_contact_individual_email,
    CAST(NULL AS STRING)                                          AS customer_contact_individual_mobile_no,

    graduation_date,
    university_specialization                                      AS highest_educational_specialization,
    CAST(NULL AS STRING)                                          AS employer_name,
    CAST(NULL AS STRING)                                          AS job_title,
    CAST(NULL AS DATE)                                             AS joining_date,
    CAST(NULL AS STRING)                                          AS individual_segment,
    CAST(NULL AS STRING)                                          AS training_track,
    CAST(NULL AS BIGINT)                                           AS months_of_experience_current,
    CAST(NULL AS BIGINT)                                           AS months_of_experience_total,

    CAST(NULL AS STRING)                                          AS university_name,
    CAST(NULL AS STRING)                                          AS university_location,
    CAST(NULL AS STRING)                                          AS is_entrepreneur,
    CAST(NULL AS DOUBLE)                                           AS wage_current,
    CAST(NULL AS DOUBLE)                                           AS requested_increment,
    CAST(NULL AS DOUBLE)                                           AS requested_stipend,
    CAST(NULL AS DOUBLE)                                           AS wage_new,

    highest_degree_obtained                                        AS highest_educational_degree,
    CAST(NULL AS STRING)                                          AS is_active_individual,

    CAST(NULL AS STRING)                                          AS training_provider_type,
    CAST(NULL AS STRING)                                          AS training_provider_cr_license_no,
    CAST(NULL AS STRING)                                          AS training_provider,
    CAST(NULL AS STRING)                                          AS training_provider_location,

    CAST(NULL AS STRING)                                          AS certificate_name,
    CAST(NULL AS DATE)                                             AS training_start_date,
    CAST(NULL AS DATE)                                             AS training_end_date,
    CAST(NULL AS STRING)                                          AS training_mode_of_delivery,
    CAST(NULL AS DOUBLE)                                           AS grant_approved_training,
    CAST(NULL AS DOUBLE)                                           AS grant_approved_training_customer_share,
    CAST(NULL AS STRING)                                          AS training_payment_type,
    CAST(NULL AS STRING)                                          AS certificate_awarding_body,
    CAST(NULL AS DOUBLE)                                           AS certificate_cap,
    CAST(NULL AS BIGINT)                                           AS certificate_training_hours,
    CAST(NULL AS STRING)                                          AS certificate_status,
    CAST(NULL AS STRING)                                          AS training_knowledge_area,
    CAST(NULL AS STRING)                                          AS training_knowledge_area_detailed,

    CAST(NULL AS STRING)                                          AS job_level_current,
    CAST(NULL AS STRING)                                          AS job_level_new,
    CAST(NULL AS STRING)                                          AS employment_type,
    CAST(NULL AS STRING)                                          AS employment_contract_type,

    CAST(NULL AS STRING)                                          AS latest_activity,
    CAST(NULL AS STRING)                                          AS workflow_status_last_activity,

    CAST(NULL AS DOUBLE)                                           AS grant_approved_wage,
    CAST(NULL AS DOUBLE)                                           AS total_tamkeen_cap_amount,
    CAST(NULL AS DOUBLE)                                           AS total_customer_share_amount,
    CAST(NULL AS DOUBLE)                                           AS total_tamkeen_share_amount_wage,

    CAST(NULL AS BIGINT)                                           AS individual_age,
    CAST(NULL AS BIGINT)                                           AS individual_age_live,

    CAST(NULL AS STRING)                                          AS workflow_status_application_support,
    CAST(NULL AS STRING)                                          AS workflow_status_application_detailed,
    CAST(NULL AS STRING)                                          AS workflow_status_application_support_detailed,

    CAST(NULL AS TIMESTAMP)                                        AS rejected_on,
    CAST(NULL AS STRING)                                          AS certificate_type,
    CAST(NULL AS STRING)                                          AS is_active_application_support,
    CAST(NULL AS TIMESTAMP)                                        AS confirmed_on,
    CAST(NULL AS TIMESTAMP)                                        AS withdrawn_on,

    CAST(NULL AS STRING)                                          AS authorized_training_provider,
    CAST(NULL AS STRING)                                          AS certification,
    CAST(NULL AS STRING)                                          AS application_support_action_id,
    CAST(NULL AS DOUBLE)                                           AS cost_of_training,
    CAST(NULL AS BIGINT)                                           AS training_duration,

    CAST(NULL AS STRING)                                          AS primary_email_contact_details,
    CAST(NULL AS STRING)                                          AS primary_mobile_number_contact_details,
    CAST(NULL AS STRING)                                          AS primary_contact_name_contact_details,

    CAST(NULL AS DOUBLE)                                           AS calculated_grant_amount,
    CAST(NULL AS TIMESTAMP)                                        AS approved_on_new,
    CAST(NULL AS STRING)                                          AS application_support_ref,
    CAST(NULL AS DOUBLE)                                           AS approved_other,
    CAST(NULL AS STRING)                                          AS amendment_status,

    CAST(NULL AS STRING)                                          AS employer_name_employee_details,
    CAST(NULL AS DATE)                                             AS start_support,
    CAST(NULL AS DATE)                                             AS end_support,
    CAST(NULL AS BIGINT)                                           AS support_duration,

    CAST(NULL AS STRING)                                          AS mobile_country_prefix,
    CAST(NULL AS STRING)                                          AS mobile_number,

    is_deleted,
    source_system_name,
    dbt_updated_at,
    cast(NULL AS TIMESTAMP) AS createdon,
    cast(NULL AS TIMESTAMP) AS updatedon,

    -- =========================================================================
    -- MIS EXTRA COLUMNS
    -- =========================================================================
    mis_source_table,
    individual_name,
    addr_flat,
    addr_building,
    addr_road,
    addr_block,
    addr_area,
    school_level,
    university_level,
    university_specialization,
    highest_degree_obtained,
    qualification,
    nationality,
    pid_product_name
FROM customer_individual_base_mis
),

silver_layer AS (
SELECT
    `id`,
    `amendmentrequestid`,
    `applicationid`,
    `individualid`,
    `application_support_id`,
    `guid`,
    `application_no`,
    `program_name`,
    `program_name_ar`,
    `workflow_status`,
    `is_active`,
    `created_on`,
    `submitted_on`,
    `approved_on`,
    `customer_type`,
    `start_date`,
    `end_date`,
    `start_date_wage`,
    `end_date_wage`,
    `duration_months_wage`,
    `duration_months_application`,
    `is_hipo`,
    `monitoring_due_date_application`,
    `spending_period_end_date_application`,
    `commercial_name_en`,
    `commercial_name_ar`,
    `cr_license_no`,
    `cr_license_no_main`,
    `registration_date`,
    `cr_license_type`,
    `individual_name_en`,
    `individual_name_ar`,
    `portal_user_name`,
    `email`,
    `mobile_no`,
    `cpr`,
    `date_of_birth`,
    `gender`,
    `support_area`,
    `support_type`,
    `support_track_wage`,
    `customer_contact_individual_email`,
    `customer_contact_individual_mobile_no`,
    `graduation_date`,
    `highest_educational_specialization`,
    `employer_name`,
    `job_title`,
    `joining_date`,
    `individual_segment`,
    `training_track`,
    `months_of_experience_current`,
    `months_of_experience_total`,
    `university_name`,
    `university_location`,
    `is_entrepreneur`,
    `wage_current`,
    `requested_increment`,
    `requested_stipend`,
    `wage_new`,
    `highest_educational_degree`,
    `is_active_individual`,
    `training_provider_type`,
    `training_provider_cr_license_no`,
    `training_provider`,
    `training_provider_location`,
    `certificate_name`,
    `training_start_date`,
    `training_end_date`,
    `training_mode_of_delivery`,
    `grant_approved_training`,
    `grant_approved_training_customer_share`,
    `training_payment_type`,
    `certificate_awarding_body`,
    `certificate_cap`,
    `certificate_training_hours`,
    `certificate_status`,
    `training_knowledge_area`,
    `training_knowledge_area_detailed`,
    `job_level_current`,
    `job_level_new`,
    `employment_type`,
    `employment_contract_type`,
    `latest_activity`,
    `workflow_status_last_activity`,
    `grant_approved_wage`,
    `total_tamkeen_cap_amount`,
    `total_customer_share_amount`,
    `total_tamkeen_share_amount_wage`,
    `individual_age`,
    `individual_age_live`,
    `workflow_status_application_support`,
    `workflow_status_application_detailed`,
    `workflow_status_application_support_detailed`,
    `rejected_on`,
    `certificate_type`,
    `is_active_application_support`,
    `confirmed_on`,
    `withdrawn_on`,
    `authorized_training_provider`,
    `certification`,
    `application_support_action_id`,
    `cost_of_training`,
    `training_duration`,
    `primary_email_contact_details`,
    `primary_mobile_number_contact_details`,
    `primary_contact_name_contact_details`,
    `calculated_grant_amount`,
    `approved_on_new`,
    `application_support_ref`,
    `approved_other`,
    `amendment_status`,
    `employer_name_employee_details`,
    `start_support`,
    `end_support`,
    `support_duration`,
    `mobile_country_prefix`,
    `mobile_number`,
    `is_deleted`,
    `source_system_name`,
    `dbt_updated_at`,
    `createdon`,
    `updatedon`,
    `mis_source_table`,
    `individual_name`,
    `addr_flat`,
    `addr_building`,
    `addr_road`,
    `addr_block`,
    `addr_area`,
    `school_level`,
    `university_level`,
    `university_specialization`,
    `highest_degree_obtained`,
    `qualification`,
    `nationality`,
    `pid_product_name`
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`customer_individual_base`
),

bronze_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'id'),
        (2, 'amendmentrequestid'),
        (3, 'applicationid'),
        (4, 'individualid'),
        (5, 'application_support_id'),
        (6, 'guid'),
        (7, 'application_no'),
        (8, 'program_name'),
        (9, 'program_name_ar'),
        (10, 'workflow_status'),
        (11, 'is_active'),
        (12, 'created_on'),
        (13, 'submitted_on'),
        (14, 'approved_on'),
        (15, 'customer_type'),
        (16, 'start_date'),
        (17, 'end_date'),
        (18, 'start_date_wage'),
        (19, 'end_date_wage'),
        (20, 'duration_months_wage'),
        (21, 'duration_months_application'),
        (22, 'is_hipo'),
        (23, 'monitoring_due_date_application'),
        (24, 'spending_period_end_date_application'),
        (25, 'commercial_name_en'),
        (26, 'commercial_name_ar'),
        (27, 'cr_license_no'),
        (28, 'cr_license_no_main'),
        (29, 'registration_date'),
        (30, 'cr_license_type'),
        (31, 'individual_name_en'),
        (32, 'individual_name_ar'),
        (33, 'portal_user_name'),
        (34, 'email'),
        (35, 'mobile_no'),
        (36, 'cpr'),
        (37, 'date_of_birth'),
        (38, 'gender'),
        (39, 'support_area'),
        (40, 'support_type'),
        (41, 'support_track_wage'),
        (42, 'customer_contact_individual_email'),
        (43, 'customer_contact_individual_mobile_no'),
        (44, 'graduation_date'),
        (45, 'highest_educational_specialization'),
        (46, 'employer_name'),
        (47, 'job_title'),
        (48, 'joining_date'),
        (49, 'individual_segment'),
        (50, 'training_track'),
        (51, 'months_of_experience_current'),
        (52, 'months_of_experience_total'),
        (53, 'university_name'),
        (54, 'university_location'),
        (55, 'is_entrepreneur'),
        (56, 'wage_current'),
        (57, 'requested_increment'),
        (58, 'requested_stipend'),
        (59, 'wage_new'),
        (60, 'highest_educational_degree'),
        (61, 'is_active_individual'),
        (62, 'training_provider_type'),
        (63, 'training_provider_cr_license_no'),
        (64, 'training_provider'),
        (65, 'training_provider_location'),
        (66, 'certificate_name'),
        (67, 'training_start_date'),
        (68, 'training_end_date'),
        (69, 'training_mode_of_delivery'),
        (70, 'grant_approved_training'),
        (71, 'grant_approved_training_customer_share'),
        (72, 'training_payment_type'),
        (73, 'certificate_awarding_body'),
        (74, 'certificate_cap'),
        (75, 'certificate_training_hours'),
        (76, 'certificate_status'),
        (77, 'training_knowledge_area'),
        (78, 'training_knowledge_area_detailed'),
        (79, 'job_level_current'),
        (80, 'job_level_new'),
        (81, 'employment_type'),
        (82, 'employment_contract_type'),
        (83, 'latest_activity'),
        (84, 'workflow_status_last_activity'),
        (85, 'grant_approved_wage'),
        (86, 'total_tamkeen_cap_amount'),
        (87, 'total_customer_share_amount'),
        (88, 'total_tamkeen_share_amount_wage'),
        (89, 'individual_age'),
        (90, 'individual_age_live'),
        (91, 'workflow_status_application_support'),
        (92, 'workflow_status_application_detailed'),
        (93, 'workflow_status_application_support_detailed'),
        (94, 'rejected_on'),
        (95, 'certificate_type'),
        (96, 'is_active_application_support'),
        (97, 'confirmed_on'),
        (98, 'withdrawn_on'),
        (99, 'authorized_training_provider'),
        (100, 'certification'),
        (101, 'application_support_action_id'),
        (102, 'cost_of_training'),
        (103, 'training_duration'),
        (104, 'primary_email_contact_details'),
        (105, 'primary_mobile_number_contact_details'),
        (106, 'primary_contact_name_contact_details'),
        (107, 'calculated_grant_amount'),
        (108, 'approved_on_new'),
        (109, 'application_support_ref'),
        (110, 'approved_other'),
        (111, 'amendment_status'),
        (112, 'employer_name_employee_details'),
        (113, 'start_support'),
        (114, 'end_support'),
        (115, 'support_duration'),
        (116, 'mobile_country_prefix'),
        (117, 'mobile_number'),
        (118, 'is_deleted'),
        (119, 'source_system_name'),
        (120, 'dbt_updated_at'),
        (121, 'createdon'),
        (122, 'updatedon'),
        (123, 'mis_source_table'),
        (124, 'individual_name'),
        (125, 'addr_flat'),
        (126, 'addr_building'),
        (127, 'addr_road'),
        (128, 'addr_block'),
        (129, 'addr_area'),
        (130, 'school_level'),
        (131, 'university_level'),
        (132, 'university_specialization'),
        (133, 'highest_degree_obtained'),
        (134, 'qualification'),
        (135, 'nationality'),
        (136, 'pid_product_name')
    ) AS t(column_position, column_name)
),

silver_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'id'),
        (2, 'amendmentrequestid'),
        (3, 'applicationid'),
        (4, 'individualid'),
        (5, 'application_support_id'),
        (6, 'guid'),
        (7, 'application_no'),
        (8, 'program_name'),
        (9, 'program_name_ar'),
        (10, 'workflow_status'),
        (11, 'is_active'),
        (12, 'created_on'),
        (13, 'submitted_on'),
        (14, 'approved_on'),
        (15, 'customer_type'),
        (16, 'start_date'),
        (17, 'end_date'),
        (18, 'start_date_wage'),
        (19, 'end_date_wage'),
        (20, 'duration_months_wage'),
        (21, 'duration_months_application'),
        (22, 'is_hipo'),
        (23, 'monitoring_due_date_application'),
        (24, 'spending_period_end_date_application'),
        (25, 'commercial_name_en'),
        (26, 'commercial_name_ar'),
        (27, 'cr_license_no'),
        (28, 'cr_license_no_main'),
        (29, 'registration_date'),
        (30, 'cr_license_type'),
        (31, 'individual_name_en'),
        (32, 'individual_name_ar'),
        (33, 'portal_user_name'),
        (34, 'email'),
        (35, 'mobile_no'),
        (36, 'cpr'),
        (37, 'date_of_birth'),
        (38, 'gender'),
        (39, 'support_area'),
        (40, 'support_type'),
        (41, 'support_track_wage'),
        (42, 'customer_contact_individual_email'),
        (43, 'customer_contact_individual_mobile_no'),
        (44, 'graduation_date'),
        (45, 'highest_educational_specialization'),
        (46, 'employer_name'),
        (47, 'job_title'),
        (48, 'joining_date'),
        (49, 'individual_segment'),
        (50, 'training_track'),
        (51, 'months_of_experience_current'),
        (52, 'months_of_experience_total'),
        (53, 'university_name'),
        (54, 'university_location'),
        (55, 'is_entrepreneur'),
        (56, 'wage_current'),
        (57, 'requested_increment'),
        (58, 'requested_stipend'),
        (59, 'wage_new'),
        (60, 'highest_educational_degree'),
        (61, 'is_active_individual'),
        (62, 'training_provider_type'),
        (63, 'training_provider_cr_license_no'),
        (64, 'training_provider'),
        (65, 'training_provider_location'),
        (66, 'certificate_name'),
        (67, 'training_start_date'),
        (68, 'training_end_date'),
        (69, 'training_mode_of_delivery'),
        (70, 'grant_approved_training'),
        (71, 'grant_approved_training_customer_share'),
        (72, 'training_payment_type'),
        (73, 'certificate_awarding_body'),
        (74, 'certificate_cap'),
        (75, 'certificate_training_hours'),
        (76, 'certificate_status'),
        (77, 'training_knowledge_area'),
        (78, 'training_knowledge_area_detailed'),
        (79, 'job_level_current'),
        (80, 'job_level_new'),
        (81, 'employment_type'),
        (82, 'employment_contract_type'),
        (83, 'latest_activity'),
        (84, 'workflow_status_last_activity'),
        (85, 'grant_approved_wage'),
        (86, 'total_tamkeen_cap_amount'),
        (87, 'total_customer_share_amount'),
        (88, 'total_tamkeen_share_amount_wage'),
        (89, 'individual_age'),
        (90, 'individual_age_live'),
        (91, 'workflow_status_application_support'),
        (92, 'workflow_status_application_detailed'),
        (93, 'workflow_status_application_support_detailed'),
        (94, 'rejected_on'),
        (95, 'certificate_type'),
        (96, 'is_active_application_support'),
        (97, 'confirmed_on'),
        (98, 'withdrawn_on'),
        (99, 'authorized_training_provider'),
        (100, 'certification'),
        (101, 'application_support_action_id'),
        (102, 'cost_of_training'),
        (103, 'training_duration'),
        (104, 'primary_email_contact_details'),
        (105, 'primary_mobile_number_contact_details'),
        (106, 'primary_contact_name_contact_details'),
        (107, 'calculated_grant_amount'),
        (108, 'approved_on_new'),
        (109, 'application_support_ref'),
        (110, 'approved_other'),
        (111, 'amendment_status'),
        (112, 'employer_name_employee_details'),
        (113, 'start_support'),
        (114, 'end_support'),
        (115, 'support_duration'),
        (116, 'mobile_country_prefix'),
        (117, 'mobile_number'),
        (118, 'is_deleted'),
        (119, 'source_system_name'),
        (120, 'dbt_updated_at'),
        (121, 'createdon'),
        (122, 'updatedon'),
        (123, 'mis_source_table'),
        (124, 'individual_name'),
        (125, 'addr_flat'),
        (126, 'addr_building'),
        (127, 'addr_road'),
        (128, 'addr_block'),
        (129, 'addr_area'),
        (130, 'school_level'),
        (131, 'university_level'),
        (132, 'university_specialization'),
        (133, 'highest_degree_obtained'),
        (134, 'qualification'),
        (135, 'nationality'),
        (136, 'pid_product_name')
    ) AS t(column_position, column_name)
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`individualid` AS STRING) AS `individualid`,
        CAST(`application_support_id` AS STRING) AS `application_support_id`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_name_ar` AS STRING) AS `program_name_ar`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`is_active` AS STRING) AS `is_active`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`start_date` AS STRING) AS `start_date`,
        CAST(`end_date` AS STRING) AS `end_date`,
        CAST(`start_date_wage` AS STRING) AS `start_date_wage`,
        CAST(`end_date_wage` AS STRING) AS `end_date_wage`,
        CAST(`duration_months_wage` AS STRING) AS `duration_months_wage`,
        CAST(`duration_months_application` AS STRING) AS `duration_months_application`,
        CAST(`is_hipo` AS STRING) AS `is_hipo`,
        CAST(`monitoring_due_date_application` AS STRING) AS `monitoring_due_date_application`,
        CAST(`spending_period_end_date_application` AS STRING) AS `spending_period_end_date_application`,
        CAST(`commercial_name_en` AS STRING) AS `commercial_name_en`,
        CAST(`commercial_name_ar` AS STRING) AS `commercial_name_ar`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`cr_license_no_main` AS STRING) AS `cr_license_no_main`,
        CAST(`registration_date` AS STRING) AS `registration_date`,
        CAST(`cr_license_type` AS STRING) AS `cr_license_type`,
        CAST(`individual_name_en` AS STRING) AS `individual_name_en`,
        CAST(`individual_name_ar` AS STRING) AS `individual_name_ar`,
        CAST(`portal_user_name` AS STRING) AS `portal_user_name`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`mobile_no` AS STRING) AS `mobile_no`,
        CAST(`cpr` AS STRING) AS `cpr`,
        CAST(`date_of_birth` AS STRING) AS `date_of_birth`,
        CAST(`gender` AS STRING) AS `gender`,
        CAST(`support_area` AS STRING) AS `support_area`,
        CAST(`support_type` AS STRING) AS `support_type`,
        CAST(`support_track_wage` AS STRING) AS `support_track_wage`,
        CAST(`customer_contact_individual_email` AS STRING) AS `customer_contact_individual_email`,
        CAST(`customer_contact_individual_mobile_no` AS STRING) AS `customer_contact_individual_mobile_no`,
        CAST(`graduation_date` AS STRING) AS `graduation_date`,
        CAST(`highest_educational_specialization` AS STRING) AS `highest_educational_specialization`,
        CAST(`employer_name` AS STRING) AS `employer_name`,
        CAST(`job_title` AS STRING) AS `job_title`,
        CAST(`joining_date` AS STRING) AS `joining_date`,
        CAST(`individual_segment` AS STRING) AS `individual_segment`,
        CAST(`training_track` AS STRING) AS `training_track`,
        CAST(`months_of_experience_current` AS STRING) AS `months_of_experience_current`,
        CAST(`months_of_experience_total` AS STRING) AS `months_of_experience_total`,
        CAST(`university_name` AS STRING) AS `university_name`,
        CAST(`university_location` AS STRING) AS `university_location`,
        CAST(`is_entrepreneur` AS STRING) AS `is_entrepreneur`,
        CAST(`wage_current` AS STRING) AS `wage_current`,
        CAST(`requested_increment` AS STRING) AS `requested_increment`,
        CAST(`requested_stipend` AS STRING) AS `requested_stipend`,
        CAST(`wage_new` AS STRING) AS `wage_new`,
        CAST(`highest_educational_degree` AS STRING) AS `highest_educational_degree`,
        CAST(`is_active_individual` AS STRING) AS `is_active_individual`,
        CAST(`training_provider_type` AS STRING) AS `training_provider_type`,
        CAST(`training_provider_cr_license_no` AS STRING) AS `training_provider_cr_license_no`,
        CAST(`training_provider` AS STRING) AS `training_provider`,
        CAST(`training_provider_location` AS STRING) AS `training_provider_location`,
        CAST(`certificate_name` AS STRING) AS `certificate_name`,
        CAST(`training_start_date` AS STRING) AS `training_start_date`,
        CAST(`training_end_date` AS STRING) AS `training_end_date`,
        CAST(`training_mode_of_delivery` AS STRING) AS `training_mode_of_delivery`,
        CAST(`grant_approved_training` AS STRING) AS `grant_approved_training`,
        CAST(`grant_approved_training_customer_share` AS STRING) AS `grant_approved_training_customer_share`,
        CAST(`training_payment_type` AS STRING) AS `training_payment_type`,
        CAST(`certificate_awarding_body` AS STRING) AS `certificate_awarding_body`,
        CAST(`certificate_cap` AS STRING) AS `certificate_cap`,
        CAST(`certificate_training_hours` AS STRING) AS `certificate_training_hours`,
        CAST(`certificate_status` AS STRING) AS `certificate_status`,
        CAST(`training_knowledge_area` AS STRING) AS `training_knowledge_area`,
        CAST(`training_knowledge_area_detailed` AS STRING) AS `training_knowledge_area_detailed`,
        CAST(`job_level_current` AS STRING) AS `job_level_current`,
        CAST(`job_level_new` AS STRING) AS `job_level_new`,
        CAST(`employment_type` AS STRING) AS `employment_type`,
        CAST(`employment_contract_type` AS STRING) AS `employment_contract_type`,
        CAST(`latest_activity` AS STRING) AS `latest_activity`,
        CAST(`workflow_status_last_activity` AS STRING) AS `workflow_status_last_activity`,
        CAST(`grant_approved_wage` AS STRING) AS `grant_approved_wage`,
        CAST(`total_tamkeen_cap_amount` AS STRING) AS `total_tamkeen_cap_amount`,
        CAST(`total_customer_share_amount` AS STRING) AS `total_customer_share_amount`,
        CAST(`total_tamkeen_share_amount_wage` AS STRING) AS `total_tamkeen_share_amount_wage`,
        CAST(`individual_age` AS STRING) AS `individual_age`,
        CAST(`individual_age_live` AS STRING) AS `individual_age_live`,
        CAST(`workflow_status_application_support` AS STRING) AS `workflow_status_application_support`,
        CAST(`workflow_status_application_detailed` AS STRING) AS `workflow_status_application_detailed`,
        CAST(`workflow_status_application_support_detailed` AS STRING) AS `workflow_status_application_support_detailed`,
        CAST(`rejected_on` AS STRING) AS `rejected_on`,
        CAST(`certificate_type` AS STRING) AS `certificate_type`,
        CAST(`is_active_application_support` AS STRING) AS `is_active_application_support`,
        CAST(`confirmed_on` AS STRING) AS `confirmed_on`,
        CAST(`withdrawn_on` AS STRING) AS `withdrawn_on`,
        CAST(`authorized_training_provider` AS STRING) AS `authorized_training_provider`,
        CAST(`certification` AS STRING) AS `certification`,
        CAST(`application_support_action_id` AS STRING) AS `application_support_action_id`,
        CAST(`cost_of_training` AS STRING) AS `cost_of_training`,
        CAST(`training_duration` AS STRING) AS `training_duration`,
        CAST(`primary_email_contact_details` AS STRING) AS `primary_email_contact_details`,
        CAST(`primary_mobile_number_contact_details` AS STRING) AS `primary_mobile_number_contact_details`,
        CAST(`primary_contact_name_contact_details` AS STRING) AS `primary_contact_name_contact_details`,
        CAST(`calculated_grant_amount` AS STRING) AS `calculated_grant_amount`,
        CAST(`approved_on_new` AS STRING) AS `approved_on_new`,
        CAST(`application_support_ref` AS STRING) AS `application_support_ref`,
        CAST(`approved_other` AS STRING) AS `approved_other`,
        CAST(`amendment_status` AS STRING) AS `amendment_status`,
        CAST(`employer_name_employee_details` AS STRING) AS `employer_name_employee_details`,
        CAST(`start_support` AS STRING) AS `start_support`,
        CAST(`end_support` AS STRING) AS `end_support`,
        CAST(`support_duration` AS STRING) AS `support_duration`,
        CAST(`mobile_country_prefix` AS STRING) AS `mobile_country_prefix`,
        CAST(`mobile_number` AS STRING) AS `mobile_number`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`individual_name` AS STRING) AS `individual_name`,
        CAST(`addr_flat` AS STRING) AS `addr_flat`,
        CAST(`addr_building` AS STRING) AS `addr_building`,
        CAST(`addr_road` AS STRING) AS `addr_road`,
        CAST(`addr_block` AS STRING) AS `addr_block`,
        CAST(`addr_area` AS STRING) AS `addr_area`,
        CAST(`school_level` AS STRING) AS `school_level`,
        CAST(`university_level` AS STRING) AS `university_level`,
        CAST(`university_specialization` AS STRING) AS `university_specialization`,
        CAST(`highest_degree_obtained` AS STRING) AS `highest_degree_obtained`,
        CAST(`qualification` AS STRING) AS `qualification`,
        CAST(`nationality` AS STRING) AS `nationality`,
        CAST(`pid_product_name` AS STRING) AS `pid_product_name`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`individualid` AS STRING) AS `individualid`,
        CAST(`application_support_id` AS STRING) AS `application_support_id`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_name_ar` AS STRING) AS `program_name_ar`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`is_active` AS STRING) AS `is_active`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`start_date` AS STRING) AS `start_date`,
        CAST(`end_date` AS STRING) AS `end_date`,
        CAST(`start_date_wage` AS STRING) AS `start_date_wage`,
        CAST(`end_date_wage` AS STRING) AS `end_date_wage`,
        CAST(`duration_months_wage` AS STRING) AS `duration_months_wage`,
        CAST(`duration_months_application` AS STRING) AS `duration_months_application`,
        CAST(`is_hipo` AS STRING) AS `is_hipo`,
        CAST(`monitoring_due_date_application` AS STRING) AS `monitoring_due_date_application`,
        CAST(`spending_period_end_date_application` AS STRING) AS `spending_period_end_date_application`,
        CAST(`commercial_name_en` AS STRING) AS `commercial_name_en`,
        CAST(`commercial_name_ar` AS STRING) AS `commercial_name_ar`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`cr_license_no_main` AS STRING) AS `cr_license_no_main`,
        CAST(`registration_date` AS STRING) AS `registration_date`,
        CAST(`cr_license_type` AS STRING) AS `cr_license_type`,
        CAST(`individual_name_en` AS STRING) AS `individual_name_en`,
        CAST(`individual_name_ar` AS STRING) AS `individual_name_ar`,
        CAST(`portal_user_name` AS STRING) AS `portal_user_name`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`mobile_no` AS STRING) AS `mobile_no`,
        CAST(`cpr` AS STRING) AS `cpr`,
        CAST(`date_of_birth` AS STRING) AS `date_of_birth`,
        CAST(`gender` AS STRING) AS `gender`,
        CAST(`support_area` AS STRING) AS `support_area`,
        CAST(`support_type` AS STRING) AS `support_type`,
        CAST(`support_track_wage` AS STRING) AS `support_track_wage`,
        CAST(`customer_contact_individual_email` AS STRING) AS `customer_contact_individual_email`,
        CAST(`customer_contact_individual_mobile_no` AS STRING) AS `customer_contact_individual_mobile_no`,
        CAST(`graduation_date` AS STRING) AS `graduation_date`,
        CAST(`highest_educational_specialization` AS STRING) AS `highest_educational_specialization`,
        CAST(`employer_name` AS STRING) AS `employer_name`,
        CAST(`job_title` AS STRING) AS `job_title`,
        CAST(`joining_date` AS STRING) AS `joining_date`,
        CAST(`individual_segment` AS STRING) AS `individual_segment`,
        CAST(`training_track` AS STRING) AS `training_track`,
        CAST(`months_of_experience_current` AS STRING) AS `months_of_experience_current`,
        CAST(`months_of_experience_total` AS STRING) AS `months_of_experience_total`,
        CAST(`university_name` AS STRING) AS `university_name`,
        CAST(`university_location` AS STRING) AS `university_location`,
        CAST(`is_entrepreneur` AS STRING) AS `is_entrepreneur`,
        CAST(`wage_current` AS STRING) AS `wage_current`,
        CAST(`requested_increment` AS STRING) AS `requested_increment`,
        CAST(`requested_stipend` AS STRING) AS `requested_stipend`,
        CAST(`wage_new` AS STRING) AS `wage_new`,
        CAST(`highest_educational_degree` AS STRING) AS `highest_educational_degree`,
        CAST(`is_active_individual` AS STRING) AS `is_active_individual`,
        CAST(`training_provider_type` AS STRING) AS `training_provider_type`,
        CAST(`training_provider_cr_license_no` AS STRING) AS `training_provider_cr_license_no`,
        CAST(`training_provider` AS STRING) AS `training_provider`,
        CAST(`training_provider_location` AS STRING) AS `training_provider_location`,
        CAST(`certificate_name` AS STRING) AS `certificate_name`,
        CAST(`training_start_date` AS STRING) AS `training_start_date`,
        CAST(`training_end_date` AS STRING) AS `training_end_date`,
        CAST(`training_mode_of_delivery` AS STRING) AS `training_mode_of_delivery`,
        CAST(`grant_approved_training` AS STRING) AS `grant_approved_training`,
        CAST(`grant_approved_training_customer_share` AS STRING) AS `grant_approved_training_customer_share`,
        CAST(`training_payment_type` AS STRING) AS `training_payment_type`,
        CAST(`certificate_awarding_body` AS STRING) AS `certificate_awarding_body`,
        CAST(`certificate_cap` AS STRING) AS `certificate_cap`,
        CAST(`certificate_training_hours` AS STRING) AS `certificate_training_hours`,
        CAST(`certificate_status` AS STRING) AS `certificate_status`,
        CAST(`training_knowledge_area` AS STRING) AS `training_knowledge_area`,
        CAST(`training_knowledge_area_detailed` AS STRING) AS `training_knowledge_area_detailed`,
        CAST(`job_level_current` AS STRING) AS `job_level_current`,
        CAST(`job_level_new` AS STRING) AS `job_level_new`,
        CAST(`employment_type` AS STRING) AS `employment_type`,
        CAST(`employment_contract_type` AS STRING) AS `employment_contract_type`,
        CAST(`latest_activity` AS STRING) AS `latest_activity`,
        CAST(`workflow_status_last_activity` AS STRING) AS `workflow_status_last_activity`,
        CAST(`grant_approved_wage` AS STRING) AS `grant_approved_wage`,
        CAST(`total_tamkeen_cap_amount` AS STRING) AS `total_tamkeen_cap_amount`,
        CAST(`total_customer_share_amount` AS STRING) AS `total_customer_share_amount`,
        CAST(`total_tamkeen_share_amount_wage` AS STRING) AS `total_tamkeen_share_amount_wage`,
        CAST(`individual_age` AS STRING) AS `individual_age`,
        CAST(`individual_age_live` AS STRING) AS `individual_age_live`,
        CAST(`workflow_status_application_support` AS STRING) AS `workflow_status_application_support`,
        CAST(`workflow_status_application_detailed` AS STRING) AS `workflow_status_application_detailed`,
        CAST(`workflow_status_application_support_detailed` AS STRING) AS `workflow_status_application_support_detailed`,
        CAST(`rejected_on` AS STRING) AS `rejected_on`,
        CAST(`certificate_type` AS STRING) AS `certificate_type`,
        CAST(`is_active_application_support` AS STRING) AS `is_active_application_support`,
        CAST(`confirmed_on` AS STRING) AS `confirmed_on`,
        CAST(`withdrawn_on` AS STRING) AS `withdrawn_on`,
        CAST(`authorized_training_provider` AS STRING) AS `authorized_training_provider`,
        CAST(`certification` AS STRING) AS `certification`,
        CAST(`application_support_action_id` AS STRING) AS `application_support_action_id`,
        CAST(`cost_of_training` AS STRING) AS `cost_of_training`,
        CAST(`training_duration` AS STRING) AS `training_duration`,
        CAST(`primary_email_contact_details` AS STRING) AS `primary_email_contact_details`,
        CAST(`primary_mobile_number_contact_details` AS STRING) AS `primary_mobile_number_contact_details`,
        CAST(`primary_contact_name_contact_details` AS STRING) AS `primary_contact_name_contact_details`,
        CAST(`calculated_grant_amount` AS STRING) AS `calculated_grant_amount`,
        CAST(`approved_on_new` AS STRING) AS `approved_on_new`,
        CAST(`application_support_ref` AS STRING) AS `application_support_ref`,
        CAST(`approved_other` AS STRING) AS `approved_other`,
        CAST(`amendment_status` AS STRING) AS `amendment_status`,
        CAST(`employer_name_employee_details` AS STRING) AS `employer_name_employee_details`,
        CAST(`start_support` AS STRING) AS `start_support`,
        CAST(`end_support` AS STRING) AS `end_support`,
        CAST(`support_duration` AS STRING) AS `support_duration`,
        CAST(`mobile_country_prefix` AS STRING) AS `mobile_country_prefix`,
        CAST(`mobile_number` AS STRING) AS `mobile_number`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`individual_name` AS STRING) AS `individual_name`,
        CAST(`addr_flat` AS STRING) AS `addr_flat`,
        CAST(`addr_building` AS STRING) AS `addr_building`,
        CAST(`addr_road` AS STRING) AS `addr_road`,
        CAST(`addr_block` AS STRING) AS `addr_block`,
        CAST(`addr_area` AS STRING) AS `addr_area`,
        CAST(`school_level` AS STRING) AS `school_level`,
        CAST(`university_level` AS STRING) AS `university_level`,
        CAST(`university_specialization` AS STRING) AS `university_specialization`,
        CAST(`highest_degree_obtained` AS STRING) AS `highest_degree_obtained`,
        CAST(`qualification` AS STRING) AS `qualification`,
        CAST(`nationality` AS STRING) AS `nationality`,
        CAST(`pid_product_name` AS STRING) AS `pid_product_name`
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
        'customer_individual_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'customer_individual_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'customer_individual_base' AS table_name,
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
        'customer_individual_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'customer_individual_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
