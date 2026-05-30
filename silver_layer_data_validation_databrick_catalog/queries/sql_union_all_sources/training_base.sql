WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for training_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: training_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH training_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : TRAINING_SUPPORT_BASE_OS2
Description : Unified Silver model for OS2 training-related application supports.
              Grain: one row per training application support (direct or amendment-driven),
              for both Enterprise ('ENT') and Individual ('IND') profile types.

              Replaces the previously-separated TRAINING_ENTERPRISE_BASE_OS2 and
              TRAINING_INDIVIDUAL_BASE_OS2 models by combining their logic into a
              single base table differentiated by PROFILE_TYPE.

              Note: this captures training at the application-support layer (the
              program-funding contract). The MIS counterpart, TRAINING_ENROLLMENT_BASE_MIS,
              captures training at the enrollment-event layer (per-employee per-certificate).
              The two are NOT 1:1 â€” see TRAINING_UNIFIED_BASE for the cross-system view.

Source Tables : neo2.OSUSR_2DA_APPLICATIONSUPPORT
                neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_NTP_AMENDMENTREQUEST
                neo2.OSUSR_1AT_ASSESSMENT
                neo2.OSUSR_1AT_ASSESSMENTSTATUS
                neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_INDIVIDUAL
                neo2.OSUSR_VW9_TRAINING
                neo2.OSUSR_VW9_CERTIFICATION
                neo2.OSUSR_R9T_TRAININGPROGRAM
                neo2.OSUSR_3QQ_TRAININGTYPE
                neo2.OSUSR_GUR_AUTHORIZEDENTITIES
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_3QQ_PROGRAM
                neo2.OSUSR_VW9_TRAININGPAYMENTTYPE
                neo2.OSUSR_VW9_TRAININGPAYEE
                neo2.OSUSR_398_APPLICATIONSTATUS
                neo2.OSUSR_2DA_APPLICATIONSUPPORTSTATUS
                neo2.OSUSR_2DA_PROVIDERTYPE
                neo2.OSUSR_2DA_EMPLOYEEACKNOWLEDGMENT
                neo2.OSUSR_2DA_EMPLOYEEACKNOWLEDGMENTSTATUS

Target Table : TRAINING_SUPPORT_BASE_OS2
Load Type    : Full Load
Materialized : table
Format       : PARQUET
Tags         : os2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author     | Description
--------------------------------------------------------------
1.0     | 2026-05-18 | Venkat     | Merged TRAINING_ENTERPRISE_BASE_OS2 and
                                    TRAINING_INDIVIDUAL_BASE_OS2 into a single
                                    base model with PROFILE_TYPE differentiator

Notes:
- PROFILE_TYPE column added to distinguish ENT vs IND rows; downstream models
  can filter on this column to recover the previously-separated behaviour.
- Enterprise-only columns (employee_status, training_provider_type,
  cr_license_no) remain in the unified schema and will be NULL for IND rows.
- All date columns are now consistently +3h-shifted and 1900-01-01-coerced
  (the individual model previously did neither; this is an improvement).
- Two UNION ALL branches preserved per source models:
    Branch 1: direct application supports (appsup -> app)
    Branch 2: amendment supports (appsup -> amendreq -> app)
================================================================================================= 
*/
WITH final_base AS (

    -- =================================================================
    -- BRANCH 1 : direct application supports (no amendment)
    -- =================================================================
    SELECT
        CAST(current_date AS DATE)                                          AS extract_date,
        program.profiletypeid                                               AS profile_type,
        app.id                                                              AS application_id,
        amt.amendmentrequestid                                              AS amendment_id,
        app.referencenumber                                                 AS application_no,
        app_sta.label                                                       AS workflow_status,
        assessmentstatus.label                                              AS assessment_workflow_status,
        appsuppwfs.label                                                    AS support_decision,
        progver.commercialname_en                                           AS program_name,

        CASE
            WHEN app.submittedon = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.submittedon + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS submitted_on,

        CASE
            WHEN app.approvedon = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.approvedon + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS approved_on,

        CASE
            WHEN app.starton = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.starton + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS contract_start_date,

        CASE
            WHEN app.endon = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.endon + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS contract_end_date,

        -- approval_letter_accepted_on: source column commented out in both upstream
        -- models; preserved as NULL until app.approvalletteracceptedon is exposed
        CAST(NULL AS STRING)                                               AS approval_letter_accepted_on,

        UPPER(TRIM(cusapp.nameen))                                          AS customer_full_name,
        cusindapp.cprnumber                                                 AS cpr,
        appsup.activestatusid                                               AS employee_status,
        providertype.label                                                  AS training_provider_type,
        authtra.name                                                        AS training_provider_name,
        authtra.code                                                        AS cr_license_no,
        trainingprogram.name                                                AS training_name,
        trainingtype.label                                                  AS training_program_type,
        paytype.label                                                       AS training_payment_type,

        CASE
            WHEN tra.payeetypeid = 'CST' THEN 'Customer'
            WHEN tra.payeetypeid = 'TP'  THEN 'Training Provider'
            ELSE NULL
        END                                                                 AS payee,

        CASE
            WHEN tra.trainingstartdate = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(tra.trainingstartdate + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS training_start_date,

        CASE
            WHEN tra.trainingenddate = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(tra.trainingenddate + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS training_end_date,

        CASE
            WHEN tra.trainingassessmentdate = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(tra.trainingassessmentdate + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS training_assessment_date,

        trainingprogram.inputcapamount                                      AS certification_cap_amount_bhd,
        tra.itemcapamt                                                      AS tamkeen_cap_amount_bhd,
        tra.tkshareamt                                                      AS tamkeen_share_amount_bhd,
        (tra.tkshareactualpct * 100)                                        AS tamkeen_share_pct,
        (100 - (tra.tkshareactualpct * 100))                                AS customer_share_pct,
        tra.customershare                                                   AS customer_share_amount_with_vat,

        CASE
            WHEN tra.itemcapamt = 0           THEN NULL
            WHEN tra.itemamtclaimed    > 0    THEN tra.itemcapamt - tra.itemamtclaimed
            WHEN tra.itemamtavailable  > 0    THEN tra.itemcapamt - tra.itemamtavailable
            WHEN tra.itemamtinprogress > 0    THEN tra.itemcapamt - tra.itemamtinprogress
            ELSE NULL
        END                                                                 AS unutilized_amount_bhd,

        tra.itemvatamt                                                      AS total_vat_amount_bhd,

        -- effective_tamkeen_share_amount_bhd: source column appsupext.tksharetotalamt
        -- commented out in both upstream models; preserved as NULL
        CAST(NULL AS STRING)                                               AS effective_tamkeen_share_amount_bhd

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORT appsup

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION app
        ON app.id = appsup.applicationid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENT amt
        ON amt.applicationid = app.id
       AND app.isactive = TRUE

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER appcus
        ON appcus.applicationid = app.id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE cusprof
        ON cusprof.id = appcus.customerprofileid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER cus
        ON cus.id = cusprof.customerid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAINING tra
        ON tra.applicationsupportid = appsup.id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_CERTIFICATION tracertif
        ON tracertif.id = tra.certificationid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_R9T_TRAININGPROGRAM trainingprogram
        ON trainingprogram.id = tracertif.trainingprogramid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_TRAININGTYPE trainingtype
        ON trainingtype.`order` = trainingprogram.trainingtypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_INDIVIDUAL cusindapp
        ON cusindapp.id = appsup.individualid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_GUR_AUTHORIZEDENTITIES authtra
        ON authtra.id = trainingprogram.authorizedproviderid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION progver
        ON progver.id = app.programversionid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAM program
        ON program.id = progver.programid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAININGPAYMENTTYPE paytype
        ON paytype.code = tra.trainingpaymenttypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAININGPAYEE payee
        ON payee.code = tra.payeetypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS app_sta
        ON app_sta.code = app.applicationstatusid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORTSTATUS appsuppwfs
        ON appsuppwfs.code = appsup.applicationsupportstatusid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_PROVIDERTYPE providertype
        ON providertype.id = appsup.providertypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENTSTATUS assessmentstatus
        ON assessmentstatus.code = amt.assessmentstatusid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER cusapp
        ON cusapp.id = appsup.individualid

    WHERE program.profiletypeid IN ('ENT', 'IND')
      AND appsup.isactive = TRUE
      AND app.isactive    = TRUE
      AND (
            (
                appsup.activestatusid               = 'INA'
                AND appsup.applicationsupportstatusid = 'REM'
            )
            OR appsup.activestatusid = 'ACT'
          )

    UNION ALL

    -- =================================================================
    -- BRANCH 2 : amendment-driven application supports
    -- =================================================================
    SELECT
        CAST(current_date AS DATE)                                          AS extract_date,
        program.profiletypeid                                               AS profile_type,
        app.id                                                              AS application_id,
        amt.amendmentrequestid                                              AS amendment_id,
        app.referencenumber                                                 AS application_no,
        app_sta.label                                                       AS workflow_status,
        assessmentstatus.label                                              AS assessment_workflow_status,
        appsuppwfs.label                                                    AS support_decision,
        progver.commercialname_en                                           AS program_name,

        CASE
            WHEN app.submittedon = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.submittedon + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS submitted_on,

        CASE
            WHEN app.approvedon = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.approvedon + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS approved_on,

        CASE
            WHEN app.starton = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.starton + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS contract_start_date,

        CASE
            WHEN app.endon = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(app.endon + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS contract_end_date,

        CAST(NULL AS STRING)                                               AS approval_letter_accepted_on,

        UPPER(TRIM(cusapp.nameen))                                          AS customer_full_name,
        cusindapp.cprnumber                                                 AS cpr,
        appsup.activestatusid                                               AS employee_status,
        providertype.label                                                  AS training_provider_type,
        authtra.name                                                        AS training_provider_name,
        authtra.code                                                        AS cr_license_no,
        trainingprogram.name                                                AS training_name,
        trainingtype.label                                                  AS training_program_type,
        paytype.label                                                       AS training_payment_type,

        CASE
            WHEN tra.payeetypeid = 'CST' THEN 'Customer'
            WHEN tra.payeetypeid = 'TP'  THEN 'Training Provider'
            ELSE NULL
        END                                                                 AS payee,

        CASE
            WHEN tra.trainingstartdate = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(tra.trainingstartdate + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS training_start_date,

        CASE
            WHEN tra.trainingenddate = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(tra.trainingenddate + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS training_end_date,

        CASE
            WHEN tra.trainingassessmentdate = TIMESTAMP '1900-01-01 00:00:00' THEN NULL
            ELSE CAST(tra.trainingassessmentdate + INTERVAL '3' HOUR AS DATE)
        END                                                                 AS training_assessment_date,

        trainingprogram.inputcapamount                                      AS certification_cap_amount_bhd,
        tra.itemcapamt                                                      AS tamkeen_cap_amount_bhd,
        tra.tkshareamt                                                      AS tamkeen_share_amount_bhd,
        (tra.tkshareactualpct * 100)                                        AS tamkeen_share_pct,
        (100 - (tra.tkshareactualpct * 100))                                AS customer_share_pct,
        tra.customershare                                                   AS customer_share_amount_with_vat,

        CASE
            WHEN tra.itemcapamt = 0           THEN NULL
            WHEN tra.itemamtclaimed    > 0    THEN tra.itemcapamt - tra.itemamtclaimed
            WHEN tra.itemamtavailable  > 0    THEN tra.itemcapamt - tra.itemamtavailable
            WHEN tra.itemamtinprogress > 0    THEN tra.itemcapamt - tra.itemamtinprogress
            ELSE NULL
        END                                                                 AS unutilized_amount_bhd,

        tra.itemvatamt                                                      AS total_vat_amount_bhd,
        CAST(NULL AS STRING)                                               AS effective_tamkeen_share_amount_bhd

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORT appsup

    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_AMENDMENTREQUEST amendreq
        ON amendreq.id = appsup.amendmentrequestid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENT amt
        ON amt.amendmentrequestid = amendreq.id

    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION app
        ON app.id = amendreq.applicationid
       AND app.isactive = TRUE

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER appcus
        ON appcus.applicationid = app.id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE cusprof
        ON cusprof.id = appcus.customerprofileid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER cus
        ON cus.id = cusprof.customerid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAINING tra
        ON tra.applicationsupportid = appsup.id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_CERTIFICATION tracertif
        ON tracertif.id = tra.certificationid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_R9T_TRAININGPROGRAM trainingprogram
        ON trainingprogram.id = tracertif.trainingprogramid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_TRAININGTYPE trainingtype
        ON trainingtype.`order` = trainingprogram.trainingtypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_INDIVIDUAL cusindapp
        ON cusindapp.id = appsup.individualid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_GUR_AUTHORIZEDENTITIES authtra
        ON authtra.id = trainingprogram.authorizedproviderid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION progver
        ON progver.id = app.programversionid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAM program
        ON program.id = progver.programid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAININGPAYMENTTYPE paytype
        ON paytype.code = tra.trainingpaymenttypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_TRAININGPAYEE payee
        ON payee.code = tra.payeetypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS app_sta
        ON app_sta.code = app.applicationstatusid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_APPLICATIONSUPPORTSTATUS appsuppwfs
        ON appsuppwfs.code = appsup.applicationsupportstatusid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_PROVIDERTYPE providertype
        ON providertype.id = appsup.providertypeid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENTSTATUS assessmentstatus
        ON assessmentstatus.code = amt.assessmentstatusid

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER cusapp
        ON cusapp.id = appsup.individualid

    WHERE program.profiletypeid IN ('ENT', 'IND')
      AND appsup.isactive = TRUE
      AND app.isactive    = TRUE
      AND (
            (
                appsup.activestatusid               = 'INA'
                AND appsup.applicationsupportstatusid = 'REM'
            )
            OR appsup.activestatusid = 'ACT'
          )

)

SELECT
    extract_date,
    profile_type,
    application_id,
    amendment_id,
    application_no,
    workflow_status,
    assessment_workflow_status,
    support_decision,
    program_name,
    submitted_on,
    approved_on,
    contract_start_date,
    contract_end_date,
    approval_letter_accepted_on,
    customer_full_name,
    cpr,
    employee_status,
    training_provider_type,
    training_provider_name,
    cr_license_no,
    training_name,
    training_program_type,
    training_payment_type,
    payee,
    training_start_date,
    training_end_date,
    training_assessment_date,
    certification_cap_amount_bhd,
    tamkeen_cap_amount_bhd,
    tamkeen_share_amount_bhd,
    tamkeen_share_pct,
    customer_share_pct,
    customer_share_amount_with_vat,
    unutilized_amount_bhd,
    total_vat_amount_bhd,
    effective_tamkeen_share_amount_bhd,

    'NEO2'                                                                  AS source_system_name,
    FALSE                                                                   AS is_deleted,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp)                 AS dbt_updated_at

FROM final_base
),
training_base_mis_source AS (
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
/*
============================================================================
training_enrollment_base_mis.sql
============================================================================
Per-source intermediate Silver model for the Training domain Ã¢â‚¬â€ MIS only.

Grain: one row per training enrollment (per employee per certificate).

Anchor: tws_trainingenrollment
Reference SP: RPT-051_TWS_Training_Enrollments

Note: this captures training at the enrollment-event layer. The OS2
counterpart, TRAINING_SUPPORT_BASE_OS2, captures training at the
application-support layer (the program-funding contract). The two are NOT
1:1 Ã¢â‚¬â€ an OS2 application support may correspond to zero, one, or many MIS
enrollments. For a cross-system view see TRAINING_UNIFIED_BASE.

This SP builds a wide training enrollment view by joining:
  - tws_trainingenrollment (anchor)
  - tws_employeeapplication (employee app context)
  - MIS_individual (individual details)
  - tws_enterpriseapplication (enterprise app context)
  - tmkn_company (company details)
  - mis_certificate (certificate details)
  - mis_certificateexprie (certificate expiration / training duration)
  - tmkn_pid (product PID)

Plus 17 status-history milestones via tws_trainingenrollment_sh.

Cross-domain note: this model joins extensively to Customer Individual,
Customer Enterprise, and Certification domains. In the unified Silver layer
downstream, those joins may be redundant if those domains are also unioned
in. For now, we mirror what RPT-051 does Ã¢â‚¬â€ the team can decide later whether
to thin this down.

Sentinel-1900 dates handled inline.
Option-set decoding via option_set_map CTE.
============================================================================
*/


-- ============================================================================
-- Pre-aggregated training-enrollment status history
-- Replaces the @StatusHistory cursor pattern from the SP
-- ============================================================================
trn_status_history AS (
    SELECT
        sh.tws_training_enrollment_reference     AS training_enrollment_id,
        sh.tws_status_report                     AS status_report_id,
        COUNT(sh.tws_trainingenrollment_shid)    AS occurrence_count,
        MIN(sh.createdon)                        AS first_created_on,
        MAX(sh.createdon)                        AS last_created_on
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_TRAININGENROLLMENT_SHBASE sh
    WHERE sh.tws_training_enrollment_reference IS NOT NULL
      AND sh.statecode = 0
    GROUP BY
        sh.tws_training_enrollment_reference,
        sh.tws_status_report
),

final_base as (
SELECT
    'tws_trainingenrollment' AS mis_source_table,

    -- Identifiers
    CAST(trn.tws_trainingenrollmentid AS STRING)        AS training_enrollment_id,
    trn.tws_name                                         AS training_enrollment_name,
    CAST(trn.tws_paymentrequest AS STRING)              AS payment_request_id,
    trn.tws_paymentrequest                          AS payment_request_name,
    CAST(trn.tws_certificate AS STRING)                 AS certificate_id,
    trn.tws_certificate                              AS certificate_name,
    CAST(trn.tws_employee_application AS STRING)        AS employee_application_id,
    trn.tws_employee_application                    AS employee_application_name,
    CAST(emp.tws_employeeapplicationid AS STRING)       AS employee_application_anchor_id,
    CAST(entapp.tws_enterpriseapplicationid AS STRING)  AS enterprise_application_id,
    entapp.tws_name                                      AS enterprise_application_name,
    CAST(entapp.tws_maincompany AS STRING)              AS company_id,

    -- Training provider / delivery
   trn.tws_training_provider                       AS training_provider,
    CASE
        WHEN crtexp.mis_trainingprovider = '7B4F243C-92E5-E311-B53F-005056820012' THEN 'Overseas'
        WHEN crtexp.mis_trainingprovider = '1F945306-92E5-E311-B53F-005056820012' THEN 'Self Study'
        ELSE 'Local'
    END                                                  AS training_provider_type,

    -- Training timeline (sentinel handling for proposed/finish dates if needed)
    trn.tws_start_date                                   AS start_date,
    trn.tws_proposed_finish_date                         AS proposed_finish_date,
    trn.tws_finish_date                                  AS finish_date,
    trn.tws_submittedon                                  AS submitted_on,
    trn.tws_approvedon                                   AS approved_on,

    -- Financial
    trn.tws_training_cost                                AS training_cost,
    trn.tws_tamkeen_share                                AS tamkeen_share,
    trn.tws_employer_share                               AS employer_share,
    trn.tws_discount                                     AS discount,
    trn.tws_total_expense                                AS total_expense,
    trn.tws_total_travel_cap                             AS total_travel_cap,

    -- Workflow / status (decoded via option-sets)
     CASE WHEN trn.tws_payable_to IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollment') || '|' || lower('tws_payable_to') || '|' || CAST(trn.tws_payable_to AS STRING)) END AS payable_to, 
     CASE WHEN trn.tws_workflow_status IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollment') || '|' || lower('tws_workflow_status') || '|' || CAST(trn.tws_workflow_status AS STRING)) END AS workflow_status, 
     CASE WHEN trn.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollment') || '|' || lower('statuscode') || '|' || CAST(trn.statuscode AS STRING)) END AS status_reason, 
     CASE WHEN trn.tws_supportpercentage IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tws_trainingenrollment') || '|' || lower('tws_SupportPercentage') || '|' || CAST(trn.tws_supportpercentage AS STRING)) END AS support_percentage, 

    -- Other attributes
    trn.tws_certificate_approval                    AS certificate_approval,
    trn.tmkn_createdbypartner                       AS created_by_partner,
    trn.tws_checker                                 AS checker_name,
    trn.tmkn_monitoringbatch                        AS monitoring_batch,
    trn.tws_justification_for_choosing_cert              AS justification_for_choosing_cert,
    trn.tws_product                                  AS product_pid_name,

    -- Joined: individual customer context
    ind.mis_cpr                                          AS individual_cpr,
    ind.mis_name                                         AS individual_name,
    ind.mis_email                                        AS individual_email,
    ind.mis_mobile                                       AS individual_mobile,
     CASE WHEN ind.mis_gender IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('MIS_Gender') || '|' || CAST(ind.mis_gender AS STRING)) END AS individual_gender, 
     CASE WHEN ind.tmkn_highest_degree_obtained IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('tmkn_highest_degree_obtained') || '|' || CAST(ind.tmkn_highest_degree_obtained AS STRING)) END AS individual_highest_degree, 
     CASE WHEN ind.mis_universityspecialization IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('MIS_individual') || '|' || lower('mis_UniversitySpecialization') || '|' || CAST(ind.mis_universityspecialization AS STRING)) END AS individual_university_specialization, 
    -- Joined: employee application context
    emp.tws_job                                     AS job_title,

    -- Joined: enterprise application + company context
    com.tmkn_commercialnameenglish                       AS commercial_name_english,
    com.tmkn_commercialnamearabic                        AS commercial_name_arabic,
   com.tmkn_tamkeencompanycategory                 AS tamkeen_company_category,
    com.tmkn_tamkeencompanymaincategory              AS tamkeen_company_main_category,
    com.tmkn_activitysector                       AS activity_sector,

    -- Joined: certificate context
    cert.mis_broad                                  AS certificate_broad,
    cert.mis_detailed                               AS certificate_detailed,
    cert.mis_narrow                                AS certificate_narrow,
     CASE WHEN cert.mis_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_type') || '|' || CAST(cert.mis_type AS STRING)) END AS certificate_type, 
    cert.mis_cap                                         AS certificate_cap,
    cert.mis_awardingbody                           AS awarding_body,

    -- Joined: certificate expiration / training duration
    crtexp.tmkn_hours                                    AS training_duration_hours,

    -- Joined: PID
    pid.tmkn_productname                                 AS product_pid,

    -- Status history milestones
    sh_submit.last_created_on                            AS last_submission_date,
    --sh_send_checker.last_created_by                      AS created_by_send_to_checker,
    --sh_send_manager.last_created_by                      AS created_by_send_to_manager,
    --sh_approved.last_created_by                          AS created_by_mark_approved,
    --sh_disapproved.last_created_by                       AS created_by_mark_disapproved,
    CAST(NULL AS STRING) AS created_by_send_to_checker,
    CAST(NULL AS STRING) AS created_by_send_to_manager,
    CAST(NULL AS STRING) AS created_by_mark_approved,
    CAST(NULL AS STRING) AS created_by_mark_disapproved,
    sh_disapproved.last_created_on                       AS last_disapproved_date,
    --sh_withdrawn.last_created_by                         AS created_by_mark_withdrawn,
    --sh_dropout.last_created_by                           AS created_by_mark_dropout,
    CAST(NULL AS STRING) AS created_by_mark_withdrawn,
    CAST(NULL AS STRING) AS created_by_mark_dropout,
    sh_started.last_created_on                           AS last_started_date,
    sh_finished.last_created_on                          AS last_finished_date,
    sh_mark_approved_emp.last_created_on                 AS last_mark_approved_by_employee_date,
    sh_mark_disapproved_emp.last_created_on              AS last_mark_disapproved_by_employee_date,
    sh_mark_disapproved_parent.last_created_on           AS last_mark_disapproved_due_parent_date,
    sh_send_back_maker.first_created_on                  AS first_send_back_to_maker_date,
    sh_send_back_maker.last_created_on                   AS last_send_back_to_maker_date,
    sh_send_back_maker.occurrence_count                  AS total_count_send_back_to_maker,
    sh_send_back_portal.first_created_on                 AS first_send_back_to_portal_date,
    sh_send_back_portal.last_created_on                  AS last_send_back_to_portal_date,
    sh_send_back_portal.occurrence_count                 AS total_count_send_back_to_portal,
    sh_send_tsp.last_created_on                          AS last_send_to_tsp_date,
    sh_tsp_accepted.last_created_on                      AS last_tsp_accepted_date,
    sh_tsp_rejected.last_created_on                      AS last_tsp_rejected_date,

    -- Audit
    trn.createdon                                        AS created_on,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_TRAININGENROLLMENTBASE trn
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_EMPLOYEEAPPLICATIONBASE emp
       ON emp.tws_employeeapplicationid = trn.tws_employee_application
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_INDIVIDUALBASE ind
       ON ind.mis_individualid = emp.tws_individual_refrences
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TWS_ENTERPRISEAPPLICATIONBASE entapp
       ON entapp.tws_enterpriseapplicationid = emp.tws_enterprise_application
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_COMPANYBASE com
       ON com.tmkn_companyid = entapp.tws_maincompany
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_CERTIFICATEBASE cert
       ON cert.mis_certificateid = trn.tws_certificate
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_CERTIFICATEEXPRIEBASE crtexp
       ON crtexp.mis_certificateexprieid = trn.tws_certificate_approval
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_PIDBASE pid
       ON pid.tmkn_pidid = trn.tws_product

-- Status history milestones (status report IDs from RPT-051)
LEFT JOIN  trn_status_history sh_submit
       ON sh_submit.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_submit.status_report_id = 150000000  -- Submit (Send To Maker)
LEFT JOIN  trn_status_history sh_send_checker
       ON sh_send_checker.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_send_checker.status_report_id = 150000002  -- Send To Checker
LEFT JOIN  trn_status_history sh_send_manager
       ON sh_send_manager.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_send_manager.status_report_id = 150000013  -- Send to Manager
LEFT JOIN  trn_status_history sh_approved
       ON sh_approved.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_approved.status_report_id = 150000003  -- Mark As Approved
LEFT JOIN  trn_status_history sh_disapproved
       ON sh_disapproved.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_disapproved.status_report_id = 150000004  -- Mark As Disapproved
LEFT JOIN  trn_status_history sh_withdrawn
       ON sh_withdrawn.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_withdrawn.status_report_id = 150000012  -- Mark As Withdrawn
LEFT JOIN  trn_status_history sh_dropout
       ON sh_dropout.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_dropout.status_report_id = 150000015  -- Mark As Dropout
LEFT JOIN  trn_status_history sh_started
       ON sh_started.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_started.status_report_id = 150000009  -- Training Started
LEFT JOIN  trn_status_history sh_finished
       ON sh_finished.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_finished.status_report_id = 150000010  -- Training Finished
LEFT JOIN  trn_status_history sh_mark_approved_emp
       ON sh_mark_approved_emp.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_mark_approved_emp.status_report_id = 150000017  -- Approved by Employee
LEFT JOIN  trn_status_history sh_mark_disapproved_emp
       ON sh_mark_disapproved_emp.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_mark_disapproved_emp.status_report_id = 150000018  -- Disapproved by Employee
LEFT JOIN  trn_status_history sh_mark_disapproved_parent
       ON sh_mark_disapproved_parent.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_mark_disapproved_parent.status_report_id = 150000005  -- Disapproved Due Parent
LEFT JOIN  trn_status_history sh_send_back_maker
       ON sh_send_back_maker.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_send_back_maker.status_report_id = 150000014  -- Send Back To Maker
LEFT JOIN  trn_status_history sh_send_back_portal
       ON sh_send_back_portal.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_send_back_portal.status_report_id = 150000001  -- Send Back To Portal
LEFT JOIN  trn_status_history sh_send_tsp
       ON sh_send_tsp.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_send_tsp.status_report_id = 150000006  -- Send To TSP
LEFT JOIN  trn_status_history sh_tsp_accepted
       ON sh_tsp_accepted.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_tsp_accepted.status_report_id = 150000007  -- TSP Accepted
LEFT JOIN  trn_status_history sh_tsp_rejected
       ON sh_tsp_rejected.training_enrollment_id = trn.tws_trainingenrollmentid
      AND sh_tsp_rejected.status_report_id = 150000008  -- TSP Rejected
)
SELECT
    mis_source_table,
    training_enrollment_id,
    training_enrollment_name,
    payment_request_id,
    payment_request_name,
    certificate_id,
    certificate_name,
    employee_application_id,
    employee_application_name,
    employee_application_anchor_id,
    enterprise_application_id,
    enterprise_application_name,
    company_id,
    training_provider,
    training_provider_type,
    start_date,
    proposed_finish_date,
    finish_date,
    submitted_on,
    approved_on,
    training_cost,
    tamkeen_share,
    employer_share,
    discount,
    total_expense,
    total_travel_cap,
    payable_to,
    workflow_status,
    status_reason,
    support_percentage,
    certificate_approval,
    created_by_partner,
    checker_name,
    monitoring_batch,
    justification_for_choosing_cert,
    product_pid_name,
    individual_cpr,
    individual_name,
    individual_email,
    individual_mobile,
    individual_gender,
    individual_highest_degree,
    individual_university_specialization,
    job_title,
    commercial_name_english,
    commercial_name_arabic,
    tamkeen_company_category,
    tamkeen_company_main_category,
    activity_sector,
    certificate_broad,
    certificate_detailed,
    certificate_narrow,
    certificate_type,
    certificate_cap,
    awarding_body,
    training_duration_hours,
    product_pid,
    last_submission_date,
    created_by_send_to_checker,
    created_by_send_to_manager,
    created_by_mark_approved,
    created_by_mark_disapproved,
    last_disapproved_date,
    created_by_mark_withdrawn,
    created_by_mark_dropout,
    last_started_date,
    last_finished_date,
    last_mark_approved_by_employee_date,
    last_mark_disapproved_by_employee_date,
    last_mark_disapproved_due_parent_date,
    first_send_back_to_maker_date,
    last_send_back_to_maker_date,
    total_count_send_back_to_maker,
    first_send_back_to_portal_date,
    last_send_back_to_portal_date,
    total_count_send_back_to_portal,
    last_send_to_tsp_date,
    last_tsp_accepted_date,
    last_tsp_rejected_date,
    created_on,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at
from final_base
),
os2_branch AS (

    SELECT
        extract_date                                                    AS extract_date,
        source_system_name,
        CAST('application_support' AS STRING)                          AS source_grain,

        profile_type                                                    AS profile_type,

        CAST(application_id AS STRING)                                 AS training_record_id,

        CAST(application_id AS STRING)                                 AS application_id,
        amendment_id                                                    AS amendment_id,
        application_no                                                  AS application_no,

        workflow_status                                                 AS workflow_status,
        assessment_workflow_status                                      AS assessment_workflow_status,
        support_decision                                                AS support_decision,

        program_name                                                    AS program_name,

        submitted_on                                                    AS submitted_on,
        approved_on                                                     AS approved_on,
        contract_start_date                                             AS contract_start_date,
        contract_end_date                                               AS contract_end_date,
        approval_letter_accepted_on                                     AS approval_letter_accepted_on,

        customer_full_name                                              AS customer_full_name,
        cpr                                                             AS cpr,
        employee_status                                                 AS employee_status,

        training_provider_type                                          AS training_provider_type,
        training_provider_name                                          AS training_provider_name,

        cr_license_no                                                   AS cr_license_no,

        training_name                                                   AS training_name,
        training_program_type                                           AS training_program_type,
        training_payment_type                                           AS training_payment_type,

        payee                                                           AS payee,

        training_start_date                                             AS training_start_date,
        training_end_date                                               AS training_end_date,
        training_assessment_date                                        AS training_assessment_date,

        certification_cap_amount_bhd                                    AS certification_cap_amount_bhd,
        tamkeen_cap_amount_bhd                                          AS tamkeen_cap_amount_bhd,
        tamkeen_share_amount_bhd                                        AS tamkeen_share_amount_bhd,
        tamkeen_share_pct                                               AS tamkeen_share_pct,
        customer_share_pct                                              AS customer_share_pct,
        customer_share_amount_with_vat                                  AS customer_share_amount_with_vat,
        unutilized_amount_bhd                                           AS unutilized_amount_bhd,
        total_vat_amount_bhd                                            AS total_vat_amount_bhd,
        effective_tamkeen_share_amount_bhd                              AS effective_tamkeen_share_amount_bhd,

        is_deleted,
        dbt_updated_at,
        CAST(NULL AS STRING)                                           AS mis_source_table,
        CAST(NULL AS STRING)                                           AS training_enrollment_id,
        CAST(NULL AS STRING)                                           AS training_enrollment_name,
        CAST(NULL AS STRING)                                           AS payment_request_id,
        CAST(NULL AS STRING)                                           AS payment_request_name,
        CAST(NULL AS STRING)                                           AS certificate_id,
        CAST(NULL AS STRING)                                           AS certificate_name,
        CAST(NULL AS STRING)                                           AS employee_application_id,
        CAST(NULL AS STRING)                                           AS employee_application_name,
        CAST(NULL AS STRING)                                           AS employee_application_anchor_id,
        CAST(NULL AS STRING)                                           AS enterprise_application_id,
        CAST(NULL AS STRING)                                           AS enterprise_application_name,
        CAST(NULL AS STRING)                                           AS company_id,

        CAST(NULL AS DATE)                                              AS proposed_finish_date,

        CAST(NULL AS DOUBLE)                                            AS employer_share,
        CAST(NULL AS DOUBLE)                                            AS discount,
        CAST(NULL AS DOUBLE)                                            AS total_expense,
        CAST(NULL AS DOUBLE)                                            AS total_travel_cap,

        CAST(NULL AS STRING)                                           AS payable_to,
        CAST(NULL AS STRING)                                           AS status_reason,

        CAST(NULL AS STRING)                                            AS support_percentage,

        CAST(NULL AS STRING)                                           AS certificate_approval,
        CAST(NULL AS STRING)                                           AS created_by_partner,
        CAST(NULL AS STRING)                                           AS checker_name,
        CAST(NULL AS STRING)                                           AS monitoring_batch,
        CAST(NULL AS STRING)                                           AS justification_for_choosing_cert,
        CAST(NULL AS STRING)                                           AS product_pid_name,

        CAST(NULL AS STRING)                                           AS individual_email,
        CAST(NULL AS STRING)                                           AS individual_mobile,
        CAST(NULL AS STRING)                                           AS individual_gender,
        CAST(NULL AS STRING)                                           AS individual_highest_degree,
        CAST(NULL AS STRING)                                           AS individual_university_specialization,
        CAST(NULL AS STRING)                                           AS job_title,

        CAST(NULL AS STRING)                                           AS commercial_name_english,
        CAST(NULL AS STRING)                                           AS commercial_name_arabic,

        CAST(NULL AS STRING)                                           AS tamkeen_company_category,
        CAST(NULL AS STRING)                                           AS tamkeen_company_main_category,
        CAST(NULL AS STRING)                                           AS activity_sector,

        CAST(NULL AS STRING)                                           AS certificate_broad,
        CAST(NULL AS STRING)                                           AS certificate_detailed,
        CAST(NULL AS STRING)                                           AS certificate_narrow,
        CAST(NULL AS STRING)                                           AS certificate_type,

        CAST(NULL AS DOUBLE)                                            AS certificate_cap,

        CAST(NULL AS STRING)                                           AS awarding_body,

        CAST(NULL AS DOUBLE)                                            AS training_duration_hours,

        CAST(NULL AS STRING)                                           AS product_pid,

        CAST(NULL AS TIMESTAMP)                                         AS last_submission_date,
        CAST(NULL AS STRING)                                         AS created_by_send_to_checker,
        CAST(NULL AS STRING)                                         AS created_by_send_to_manager,
        CAST(NULL AS STRING)                                         AS created_by_mark_approved,
        CAST(NULL AS STRING)                                         AS created_by_mark_disapproved,
        CAST(NULL AS TIMESTAMP)                                         AS last_disapproved_date,
        CAST(NULL AS STRING)                                         AS created_by_mark_withdrawn,
        CAST(NULL AS STRING)                                         AS created_by_mark_dropout,
        CAST(NULL AS TIMESTAMP)                                         AS last_started_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_finished_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_mark_approved_by_employee_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_mark_disapproved_by_employee_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_mark_disapproved_due_parent_date,
        CAST(NULL AS TIMESTAMP)                                         AS first_send_back_to_maker_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_send_back_to_maker_date,

        CAST(NULL AS INTEGER)                                           AS total_count_send_back_to_maker,

        CAST(NULL AS TIMESTAMP)                                         AS first_send_back_to_portal_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_send_back_to_portal_date,

        CAST(NULL AS INTEGER)                                           AS total_count_send_back_to_portal,

        CAST(NULL AS TIMESTAMP)                                         AS last_send_to_tsp_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_tsp_accepted_date,
        CAST(NULL AS TIMESTAMP)                                         AS last_tsp_rejected_date,

        CAST(NULL AS TIMESTAMP)                                         AS created_on,
        CAST(NULL AS DATE)                                              AS report_date

    from training_base_os2_source

),

mis_branch AS (

    SELECT
        CAST(report_date AS DATE)                                       AS extract_date,
        source_system_name                                              AS source_system_name,
        CAST('training_enrollment' AS STRING)                          AS source_grain,

        CAST('ENT' AS STRING)                                          AS profile_type,

        CAST(training_enrollment_id AS STRING)                         AS training_record_id,

        enterprise_application_id                                       AS application_id,

        CAST(NULL AS BIGINT)                                           AS amendment_id,

        enterprise_application_name                                     AS application_no,

        workflow_status                                                 AS workflow_status,

        CAST(NULL AS STRING)                                           AS assessment_workflow_status,

        CAST(NULL AS STRING)                                           AS support_decision,

        CAST(NULL AS STRING)                                           AS program_name,

        CAST(submitted_on AS DATE)                                      AS submitted_on,
        CAST(approved_on AS DATE)                                       AS approved_on,

        CAST(NULL AS DATE)                                              AS contract_start_date,
        CAST(NULL AS DATE)                                              AS contract_end_date,
        CAST(NULL AS STRING)                                           AS approval_letter_accepted_on,

        individual_name                                                 AS customer_full_name,
        individual_cpr                                                  AS cpr,

        CAST(NULL AS STRING)                                           AS employee_status,

        training_provider_type                                          AS training_provider_type,
        training_provider                                               AS training_provider_name,

        CAST(NULL AS STRING)                                           AS cr_license_no,

        certificate_detailed                                            AS training_name,

        CAST(NULL AS STRING)                                           AS training_program_type,
        CAST(NULL AS STRING)                                           AS training_payment_type,

        payable_to                                                      AS payee,

        CAST(start_date AS DATE)                                        AS training_start_date,
        CAST(finish_date AS DATE)                                       AS training_end_date,

        CAST(NULL AS DATE)                                              AS training_assessment_date,

        CAST(certificate_cap AS DOUBLE)                                 AS certification_cap_amount_bhd,
        CAST(training_cost AS DOUBLE)                                   AS tamkeen_cap_amount_bhd,
        CAST(tamkeen_share AS DOUBLE)                                   AS tamkeen_share_amount_bhd,

        CAST(support_percentage AS DOUBLE)                              AS tamkeen_share_pct,

        CAST(NULL AS DOUBLE)                                            AS customer_share_pct,
        CAST(NULL AS DOUBLE)                                            AS customer_share_amount_with_vat,
        CAST(NULL AS DOUBLE)                                            AS unutilized_amount_bhd,
        CAST(NULL AS DOUBLE)                                            AS total_vat_amount_bhd,
        CAST(NULL AS STRING)                                           AS effective_tamkeen_share_amount_bhd,

        is_deleted,
        dbt_updated_at,
        mis_source_table,
        training_enrollment_id,
        training_enrollment_name,
        payment_request_id,
        payment_request_name,
        certificate_id,
        certificate_name,
        employee_application_id,
        employee_application_name,
        employee_application_anchor_id,
        enterprise_application_id,
        enterprise_application_name,
        company_id,
        proposed_finish_date,
        employer_share,
        discount,
        total_expense,
        total_travel_cap,
        payable_to,
        status_reason,
        support_percentage,
        certificate_approval,
        created_by_partner,
        checker_name,
        monitoring_batch,
        justification_for_choosing_cert,
        product_pid_name,
        individual_email,
        individual_mobile,
        individual_gender,
        individual_highest_degree,
        individual_university_specialization,
        job_title,
        commercial_name_english,
        commercial_name_arabic,
        tamkeen_company_category,
        tamkeen_company_main_category,
        activity_sector,
        certificate_broad,
        certificate_detailed,
        certificate_narrow,
        certificate_type,
        certificate_cap,
        awarding_body,
        training_duration_hours,
        product_pid,
        last_submission_date,
        created_by_send_to_checker,
        created_by_send_to_manager,
        created_by_mark_approved,
        created_by_mark_disapproved,
        last_disapproved_date,
        created_by_mark_withdrawn,
        created_by_mark_dropout,
        last_started_date,
        last_finished_date,
        last_mark_approved_by_employee_date,
        last_mark_disapproved_by_employee_date,
        last_mark_disapproved_due_parent_date,
        first_send_back_to_maker_date,
        last_send_back_to_maker_date,
        total_count_send_back_to_maker,
        first_send_back_to_portal_date,
        last_send_back_to_portal_date,
        total_count_send_back_to_portal,
        last_send_to_tsp_date,
        last_tsp_accepted_date,
        last_tsp_rejected_date,
        created_on,
        report_date

    from training_base_mis_source

)

SELECT *
FROM os2_branch

UNION ALL

SELECT *
FROM mis_branch
),

silver_layer AS (
SELECT
    extract_date,
    source_system_name,
    source_grain,
    profile_type,
    training_record_id,
    application_id,
    amendment_id,
    application_no,
    workflow_status,
    assessment_workflow_status,
    support_decision,
    program_name,
    submitted_on,
    approved_on,
    contract_start_date,
    contract_end_date,
    approval_letter_accepted_on,
    customer_full_name,
    cpr,
    employee_status,
    training_provider_type,
    training_provider_name,
    cr_license_no,
    training_name,
    training_program_type,
    training_payment_type,
    payee,
    training_start_date,
    training_end_date,
    training_assessment_date,
    certification_cap_amount_bhd,
    tamkeen_cap_amount_bhd,
    tamkeen_share_amount_bhd,
    tamkeen_share_pct,
    customer_share_pct,
    customer_share_amount_with_vat,
    unutilized_amount_bhd,
    total_vat_amount_bhd,
    effective_tamkeen_share_amount_bhd,
    is_deleted,
    dbt_updated_at,
    mis_source_table,
    training_enrollment_id,
    training_enrollment_name,
    payment_request_id,
    payment_request_name,
    certificate_id,
    certificate_name,
    employee_application_id,
    employee_application_name,
    employee_application_anchor_id,
    enterprise_application_id,
    enterprise_application_name,
    company_id,
    proposed_finish_date,
    employer_share,
    discount,
    total_expense,
    total_travel_cap,
    payable_to,
    status_reason,
    support_percentage,
    certificate_approval,
    created_by_partner,
    checker_name,
    monitoring_batch,
    justification_for_choosing_cert,
    product_pid_name,
    individual_email,
    individual_mobile,
    individual_gender,
    individual_highest_degree,
    individual_university_specialization,
    job_title,
    commercial_name_english,
    commercial_name_arabic,
    tamkeen_company_category,
    tamkeen_company_main_category,
    activity_sector,
    certificate_broad,
    certificate_detailed,
    certificate_narrow,
    certificate_type,
    certificate_cap,
    awarding_body,
    training_duration_hours,
    product_pid,
    last_submission_date,
    created_by_send_to_checker,
    created_by_send_to_manager,
    created_by_mark_approved,
    created_by_mark_disapproved,
    last_disapproved_date,
    created_by_mark_withdrawn,
    created_by_mark_dropout,
    last_started_date,
    last_finished_date,
    last_mark_approved_by_employee_date,
    last_mark_disapproved_by_employee_date,
    last_mark_disapproved_due_parent_date,
    first_send_back_to_maker_date,
    last_send_back_to_maker_date,
    total_count_send_back_to_maker,
    first_send_back_to_portal_date,
    last_send_back_to_portal_date,
    total_count_send_back_to_portal,
    last_send_to_tsp_date,
    last_tsp_accepted_date,
    last_tsp_rejected_date,
    created_on,
    report_date
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.training_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'source_system_name'),
        (3, 'source_grain'),
        (4, 'profile_type'),
        (5, 'training_record_id'),
        (6, 'application_id'),
        (7, 'amendment_id'),
        (8, 'application_no'),
        (9, 'workflow_status'),
        (10, 'assessment_workflow_status'),
        (11, 'support_decision'),
        (12, 'program_name'),
        (13, 'submitted_on'),
        (14, 'approved_on'),
        (15, 'contract_start_date'),
        (16, 'contract_end_date'),
        (17, 'approval_letter_accepted_on'),
        (18, 'customer_full_name'),
        (19, 'cpr'),
        (20, 'employee_status'),
        (21, 'training_provider_type'),
        (22, 'training_provider_name'),
        (23, 'cr_license_no'),
        (24, 'training_name'),
        (25, 'training_program_type'),
        (26, 'training_payment_type'),
        (27, 'payee'),
        (28, 'training_start_date'),
        (29, 'training_end_date'),
        (30, 'training_assessment_date'),
        (31, 'certification_cap_amount_bhd'),
        (32, 'tamkeen_cap_amount_bhd'),
        (33, 'tamkeen_share_amount_bhd'),
        (34, 'tamkeen_share_pct'),
        (35, 'customer_share_pct'),
        (36, 'customer_share_amount_with_vat'),
        (37, 'unutilized_amount_bhd'),
        (38, 'total_vat_amount_bhd'),
        (39, 'effective_tamkeen_share_amount_bhd'),
        (40, 'is_deleted'),
        (41, 'dbt_updated_at'),
        (42, 'mis_source_table'),
        (43, 'training_enrollment_id'),
        (44, 'training_enrollment_name'),
        (45, 'payment_request_id'),
        (46, 'payment_request_name'),
        (47, 'certificate_id'),
        (48, 'certificate_name'),
        (49, 'employee_application_id'),
        (50, 'employee_application_name'),
        (51, 'employee_application_anchor_id'),
        (52, 'enterprise_application_id'),
        (53, 'enterprise_application_name'),
        (54, 'company_id'),
        (55, 'proposed_finish_date'),
        (56, 'employer_share'),
        (57, 'discount'),
        (58, 'total_expense'),
        (59, 'total_travel_cap'),
        (60, 'payable_to'),
        (61, 'status_reason'),
        (62, 'support_percentage'),
        (63, 'certificate_approval'),
        (64, 'created_by_partner'),
        (65, 'checker_name'),
        (66, 'monitoring_batch'),
        (67, 'justification_for_choosing_cert'),
        (68, 'product_pid_name'),
        (69, 'individual_email'),
        (70, 'individual_mobile'),
        (71, 'individual_gender'),
        (72, 'individual_highest_degree'),
        (73, 'individual_university_specialization'),
        (74, 'job_title'),
        (75, 'commercial_name_english'),
        (76, 'commercial_name_arabic'),
        (77, 'tamkeen_company_category'),
        (78, 'tamkeen_company_main_category'),
        (79, 'activity_sector'),
        (80, 'certificate_broad'),
        (81, 'certificate_detailed'),
        (82, 'certificate_narrow'),
        (83, 'certificate_type'),
        (84, 'certificate_cap'),
        (85, 'awarding_body'),
        (86, 'training_duration_hours'),
        (87, 'product_pid'),
        (88, 'last_submission_date'),
        (89, 'created_by_send_to_checker'),
        (90, 'created_by_send_to_manager'),
        (91, 'created_by_mark_approved'),
        (92, 'created_by_mark_disapproved'),
        (93, 'last_disapproved_date'),
        (94, 'created_by_mark_withdrawn'),
        (95, 'created_by_mark_dropout'),
        (96, 'last_started_date'),
        (97, 'last_finished_date'),
        (98, 'last_mark_approved_by_employee_date'),
        (99, 'last_mark_disapproved_by_employee_date'),
        (100, 'last_mark_disapproved_due_parent_date'),
        (101, 'first_send_back_to_maker_date'),
        (102, 'last_send_back_to_maker_date'),
        (103, 'total_count_send_back_to_maker'),
        (104, 'first_send_back_to_portal_date'),
        (105, 'last_send_back_to_portal_date'),
        (106, 'total_count_send_back_to_portal'),
        (107, 'last_send_to_tsp_date'),
        (108, 'last_tsp_accepted_date'),
        (109, 'last_tsp_rejected_date'),
        (110, 'created_on'),
        (111, 'report_date')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'source_system_name'),
        (3, 'source_grain'),
        (4, 'profile_type'),
        (5, 'training_record_id'),
        (6, 'application_id'),
        (7, 'amendment_id'),
        (8, 'application_no'),
        (9, 'workflow_status'),
        (10, 'assessment_workflow_status'),
        (11, 'support_decision'),
        (12, 'program_name'),
        (13, 'submitted_on'),
        (14, 'approved_on'),
        (15, 'contract_start_date'),
        (16, 'contract_end_date'),
        (17, 'approval_letter_accepted_on'),
        (18, 'customer_full_name'),
        (19, 'cpr'),
        (20, 'employee_status'),
        (21, 'training_provider_type'),
        (22, 'training_provider_name'),
        (23, 'cr_license_no'),
        (24, 'training_name'),
        (25, 'training_program_type'),
        (26, 'training_payment_type'),
        (27, 'payee'),
        (28, 'training_start_date'),
        (29, 'training_end_date'),
        (30, 'training_assessment_date'),
        (31, 'certification_cap_amount_bhd'),
        (32, 'tamkeen_cap_amount_bhd'),
        (33, 'tamkeen_share_amount_bhd'),
        (34, 'tamkeen_share_pct'),
        (35, 'customer_share_pct'),
        (36, 'customer_share_amount_with_vat'),
        (37, 'unutilized_amount_bhd'),
        (38, 'total_vat_amount_bhd'),
        (39, 'effective_tamkeen_share_amount_bhd'),
        (40, 'is_deleted'),
        (41, 'dbt_updated_at'),
        (42, 'mis_source_table'),
        (43, 'training_enrollment_id'),
        (44, 'training_enrollment_name'),
        (45, 'payment_request_id'),
        (46, 'payment_request_name'),
        (47, 'certificate_id'),
        (48, 'certificate_name'),
        (49, 'employee_application_id'),
        (50, 'employee_application_name'),
        (51, 'employee_application_anchor_id'),
        (52, 'enterprise_application_id'),
        (53, 'enterprise_application_name'),
        (54, 'company_id'),
        (55, 'proposed_finish_date'),
        (56, 'employer_share'),
        (57, 'discount'),
        (58, 'total_expense'),
        (59, 'total_travel_cap'),
        (60, 'payable_to'),
        (61, 'status_reason'),
        (62, 'support_percentage'),
        (63, 'certificate_approval'),
        (64, 'created_by_partner'),
        (65, 'checker_name'),
        (66, 'monitoring_batch'),
        (67, 'justification_for_choosing_cert'),
        (68, 'product_pid_name'),
        (69, 'individual_email'),
        (70, 'individual_mobile'),
        (71, 'individual_gender'),
        (72, 'individual_highest_degree'),
        (73, 'individual_university_specialization'),
        (74, 'job_title'),
        (75, 'commercial_name_english'),
        (76, 'commercial_name_arabic'),
        (77, 'tamkeen_company_category'),
        (78, 'tamkeen_company_main_category'),
        (79, 'activity_sector'),
        (80, 'certificate_broad'),
        (81, 'certificate_detailed'),
        (82, 'certificate_narrow'),
        (83, 'certificate_type'),
        (84, 'certificate_cap'),
        (85, 'awarding_body'),
        (86, 'training_duration_hours'),
        (87, 'product_pid'),
        (88, 'last_submission_date'),
        (89, 'created_by_send_to_checker'),
        (90, 'created_by_send_to_manager'),
        (91, 'created_by_mark_approved'),
        (92, 'created_by_mark_disapproved'),
        (93, 'last_disapproved_date'),
        (94, 'created_by_mark_withdrawn'),
        (95, 'created_by_mark_dropout'),
        (96, 'last_started_date'),
        (97, 'last_finished_date'),
        (98, 'last_mark_approved_by_employee_date'),
        (99, 'last_mark_disapproved_by_employee_date'),
        (100, 'last_mark_disapproved_due_parent_date'),
        (101, 'first_send_back_to_maker_date'),
        (102, 'last_send_back_to_maker_date'),
        (103, 'total_count_send_back_to_maker'),
        (104, 'first_send_back_to_portal_date'),
        (105, 'last_send_back_to_portal_date'),
        (106, 'total_count_send_back_to_portal'),
        (107, 'last_send_to_tsp_date'),
        (108, 'last_tsp_accepted_date'),
        (109, 'last_tsp_rejected_date'),
        (110, 'created_on'),
        (111, 'report_date')
),

bronze_normalized AS (
    SELECT
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`source_grain` AS STRING) AS `source_grain`,
        CAST(`profile_type` AS STRING) AS `profile_type`,
        CAST(`training_record_id` AS STRING) AS `training_record_id`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`amendment_id` AS STRING) AS `amendment_id`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`assessment_workflow_status` AS STRING) AS `assessment_workflow_status`,
        CAST(`support_decision` AS STRING) AS `support_decision`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`approval_letter_accepted_on` AS STRING) AS `approval_letter_accepted_on`,
        CAST(`customer_full_name` AS STRING) AS `customer_full_name`,
        CAST(`cpr` AS STRING) AS `cpr`,
        CAST(`employee_status` AS STRING) AS `employee_status`,
        CAST(`training_provider_type` AS STRING) AS `training_provider_type`,
        CAST(`training_provider_name` AS STRING) AS `training_provider_name`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`training_name` AS STRING) AS `training_name`,
        CAST(`training_program_type` AS STRING) AS `training_program_type`,
        CAST(`training_payment_type` AS STRING) AS `training_payment_type`,
        CAST(`payee` AS STRING) AS `payee`,
        CAST(`training_start_date` AS STRING) AS `training_start_date`,
        CAST(`training_end_date` AS STRING) AS `training_end_date`,
        CAST(`training_assessment_date` AS STRING) AS `training_assessment_date`,
        CAST(`certification_cap_amount_bhd` AS STRING) AS `certification_cap_amount_bhd`,
        CAST(`tamkeen_cap_amount_bhd` AS STRING) AS `tamkeen_cap_amount_bhd`,
        CAST(`tamkeen_share_amount_bhd` AS STRING) AS `tamkeen_share_amount_bhd`,
        CAST(`tamkeen_share_pct` AS STRING) AS `tamkeen_share_pct`,
        CAST(`customer_share_pct` AS STRING) AS `customer_share_pct`,
        CAST(`customer_share_amount_with_vat` AS STRING) AS `customer_share_amount_with_vat`,
        CAST(`unutilized_amount_bhd` AS STRING) AS `unutilized_amount_bhd`,
        CAST(`total_vat_amount_bhd` AS STRING) AS `total_vat_amount_bhd`,
        CAST(`effective_tamkeen_share_amount_bhd` AS STRING) AS `effective_tamkeen_share_amount_bhd`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`training_enrollment_id` AS STRING) AS `training_enrollment_id`,
        CAST(`training_enrollment_name` AS STRING) AS `training_enrollment_name`,
        CAST(`payment_request_id` AS STRING) AS `payment_request_id`,
        CAST(`payment_request_name` AS STRING) AS `payment_request_name`,
        CAST(`certificate_id` AS STRING) AS `certificate_id`,
        CAST(`certificate_name` AS STRING) AS `certificate_name`,
        CAST(`employee_application_id` AS STRING) AS `employee_application_id`,
        CAST(`employee_application_name` AS STRING) AS `employee_application_name`,
        CAST(`employee_application_anchor_id` AS STRING) AS `employee_application_anchor_id`,
        CAST(`enterprise_application_id` AS STRING) AS `enterprise_application_id`,
        CAST(`enterprise_application_name` AS STRING) AS `enterprise_application_name`,
        CAST(`company_id` AS STRING) AS `company_id`,
        CAST(`proposed_finish_date` AS STRING) AS `proposed_finish_date`,
        CAST(`employer_share` AS STRING) AS `employer_share`,
        CAST(`discount` AS STRING) AS `discount`,
        CAST(`total_expense` AS STRING) AS `total_expense`,
        CAST(`total_travel_cap` AS STRING) AS `total_travel_cap`,
        CAST(`payable_to` AS STRING) AS `payable_to`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`support_percentage` AS STRING) AS `support_percentage`,
        CAST(`certificate_approval` AS STRING) AS `certificate_approval`,
        CAST(`created_by_partner` AS STRING) AS `created_by_partner`,
        CAST(`checker_name` AS STRING) AS `checker_name`,
        CAST(`monitoring_batch` AS STRING) AS `monitoring_batch`,
        CAST(`justification_for_choosing_cert` AS STRING) AS `justification_for_choosing_cert`,
        CAST(`product_pid_name` AS STRING) AS `product_pid_name`,
        CAST(`individual_email` AS STRING) AS `individual_email`,
        CAST(`individual_mobile` AS STRING) AS `individual_mobile`,
        CAST(`individual_gender` AS STRING) AS `individual_gender`,
        CAST(`individual_highest_degree` AS STRING) AS `individual_highest_degree`,
        CAST(`individual_university_specialization` AS STRING) AS `individual_university_specialization`,
        CAST(`job_title` AS STRING) AS `job_title`,
        CAST(`commercial_name_english` AS STRING) AS `commercial_name_english`,
        CAST(`commercial_name_arabic` AS STRING) AS `commercial_name_arabic`,
        CAST(`tamkeen_company_category` AS STRING) AS `tamkeen_company_category`,
        CAST(`tamkeen_company_main_category` AS STRING) AS `tamkeen_company_main_category`,
        CAST(`activity_sector` AS STRING) AS `activity_sector`,
        CAST(`certificate_broad` AS STRING) AS `certificate_broad`,
        CAST(`certificate_detailed` AS STRING) AS `certificate_detailed`,
        CAST(`certificate_narrow` AS STRING) AS `certificate_narrow`,
        CAST(`certificate_type` AS STRING) AS `certificate_type`,
        CAST(`certificate_cap` AS STRING) AS `certificate_cap`,
        CAST(`awarding_body` AS STRING) AS `awarding_body`,
        CAST(`training_duration_hours` AS STRING) AS `training_duration_hours`,
        CAST(`product_pid` AS STRING) AS `product_pid`,
        CAST(`last_submission_date` AS STRING) AS `last_submission_date`,
        CAST(`created_by_send_to_checker` AS STRING) AS `created_by_send_to_checker`,
        CAST(`created_by_send_to_manager` AS STRING) AS `created_by_send_to_manager`,
        CAST(`created_by_mark_approved` AS STRING) AS `created_by_mark_approved`,
        CAST(`created_by_mark_disapproved` AS STRING) AS `created_by_mark_disapproved`,
        CAST(`last_disapproved_date` AS STRING) AS `last_disapproved_date`,
        CAST(`created_by_mark_withdrawn` AS STRING) AS `created_by_mark_withdrawn`,
        CAST(`created_by_mark_dropout` AS STRING) AS `created_by_mark_dropout`,
        CAST(`last_started_date` AS STRING) AS `last_started_date`,
        CAST(`last_finished_date` AS STRING) AS `last_finished_date`,
        CAST(`last_mark_approved_by_employee_date` AS STRING) AS `last_mark_approved_by_employee_date`,
        CAST(`last_mark_disapproved_by_employee_date` AS STRING) AS `last_mark_disapproved_by_employee_date`,
        CAST(`last_mark_disapproved_due_parent_date` AS STRING) AS `last_mark_disapproved_due_parent_date`,
        CAST(`first_send_back_to_maker_date` AS STRING) AS `first_send_back_to_maker_date`,
        CAST(`last_send_back_to_maker_date` AS STRING) AS `last_send_back_to_maker_date`,
        CAST(`total_count_send_back_to_maker` AS STRING) AS `total_count_send_back_to_maker`,
        CAST(`first_send_back_to_portal_date` AS STRING) AS `first_send_back_to_portal_date`,
        CAST(`last_send_back_to_portal_date` AS STRING) AS `last_send_back_to_portal_date`,
        CAST(`total_count_send_back_to_portal` AS STRING) AS `total_count_send_back_to_portal`,
        CAST(`last_send_to_tsp_date` AS STRING) AS `last_send_to_tsp_date`,
        CAST(`last_tsp_accepted_date` AS STRING) AS `last_tsp_accepted_date`,
        CAST(`last_tsp_rejected_date` AS STRING) AS `last_tsp_rejected_date`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`report_date` AS STRING) AS `report_date`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`source_grain` AS STRING) AS `source_grain`,
        CAST(`profile_type` AS STRING) AS `profile_type`,
        CAST(`training_record_id` AS STRING) AS `training_record_id`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`amendment_id` AS STRING) AS `amendment_id`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`assessment_workflow_status` AS STRING) AS `assessment_workflow_status`,
        CAST(`support_decision` AS STRING) AS `support_decision`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`contract_start_date` AS STRING) AS `contract_start_date`,
        CAST(`contract_end_date` AS STRING) AS `contract_end_date`,
        CAST(`approval_letter_accepted_on` AS STRING) AS `approval_letter_accepted_on`,
        CAST(`customer_full_name` AS STRING) AS `customer_full_name`,
        CAST(`cpr` AS STRING) AS `cpr`,
        CAST(`employee_status` AS STRING) AS `employee_status`,
        CAST(`training_provider_type` AS STRING) AS `training_provider_type`,
        CAST(`training_provider_name` AS STRING) AS `training_provider_name`,
        CAST(`cr_license_no` AS STRING) AS `cr_license_no`,
        CAST(`training_name` AS STRING) AS `training_name`,
        CAST(`training_program_type` AS STRING) AS `training_program_type`,
        CAST(`training_payment_type` AS STRING) AS `training_payment_type`,
        CAST(`payee` AS STRING) AS `payee`,
        CAST(`training_start_date` AS STRING) AS `training_start_date`,
        CAST(`training_end_date` AS STRING) AS `training_end_date`,
        CAST(`training_assessment_date` AS STRING) AS `training_assessment_date`,
        CAST(`certification_cap_amount_bhd` AS STRING) AS `certification_cap_amount_bhd`,
        CAST(`tamkeen_cap_amount_bhd` AS STRING) AS `tamkeen_cap_amount_bhd`,
        CAST(`tamkeen_share_amount_bhd` AS STRING) AS `tamkeen_share_amount_bhd`,
        CAST(`tamkeen_share_pct` AS STRING) AS `tamkeen_share_pct`,
        CAST(`customer_share_pct` AS STRING) AS `customer_share_pct`,
        CAST(`customer_share_amount_with_vat` AS STRING) AS `customer_share_amount_with_vat`,
        CAST(`unutilized_amount_bhd` AS STRING) AS `unutilized_amount_bhd`,
        CAST(`total_vat_amount_bhd` AS STRING) AS `total_vat_amount_bhd`,
        CAST(`effective_tamkeen_share_amount_bhd` AS STRING) AS `effective_tamkeen_share_amount_bhd`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`training_enrollment_id` AS STRING) AS `training_enrollment_id`,
        CAST(`training_enrollment_name` AS STRING) AS `training_enrollment_name`,
        CAST(`payment_request_id` AS STRING) AS `payment_request_id`,
        CAST(`payment_request_name` AS STRING) AS `payment_request_name`,
        CAST(`certificate_id` AS STRING) AS `certificate_id`,
        CAST(`certificate_name` AS STRING) AS `certificate_name`,
        CAST(`employee_application_id` AS STRING) AS `employee_application_id`,
        CAST(`employee_application_name` AS STRING) AS `employee_application_name`,
        CAST(`employee_application_anchor_id` AS STRING) AS `employee_application_anchor_id`,
        CAST(`enterprise_application_id` AS STRING) AS `enterprise_application_id`,
        CAST(`enterprise_application_name` AS STRING) AS `enterprise_application_name`,
        CAST(`company_id` AS STRING) AS `company_id`,
        CAST(`proposed_finish_date` AS STRING) AS `proposed_finish_date`,
        CAST(`employer_share` AS STRING) AS `employer_share`,
        CAST(`discount` AS STRING) AS `discount`,
        CAST(`total_expense` AS STRING) AS `total_expense`,
        CAST(`total_travel_cap` AS STRING) AS `total_travel_cap`,
        CAST(`payable_to` AS STRING) AS `payable_to`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`support_percentage` AS STRING) AS `support_percentage`,
        CAST(`certificate_approval` AS STRING) AS `certificate_approval`,
        CAST(`created_by_partner` AS STRING) AS `created_by_partner`,
        CAST(`checker_name` AS STRING) AS `checker_name`,
        CAST(`monitoring_batch` AS STRING) AS `monitoring_batch`,
        CAST(`justification_for_choosing_cert` AS STRING) AS `justification_for_choosing_cert`,
        CAST(`product_pid_name` AS STRING) AS `product_pid_name`,
        CAST(`individual_email` AS STRING) AS `individual_email`,
        CAST(`individual_mobile` AS STRING) AS `individual_mobile`,
        CAST(`individual_gender` AS STRING) AS `individual_gender`,
        CAST(`individual_highest_degree` AS STRING) AS `individual_highest_degree`,
        CAST(`individual_university_specialization` AS STRING) AS `individual_university_specialization`,
        CAST(`job_title` AS STRING) AS `job_title`,
        CAST(`commercial_name_english` AS STRING) AS `commercial_name_english`,
        CAST(`commercial_name_arabic` AS STRING) AS `commercial_name_arabic`,
        CAST(`tamkeen_company_category` AS STRING) AS `tamkeen_company_category`,
        CAST(`tamkeen_company_main_category` AS STRING) AS `tamkeen_company_main_category`,
        CAST(`activity_sector` AS STRING) AS `activity_sector`,
        CAST(`certificate_broad` AS STRING) AS `certificate_broad`,
        CAST(`certificate_detailed` AS STRING) AS `certificate_detailed`,
        CAST(`certificate_narrow` AS STRING) AS `certificate_narrow`,
        CAST(`certificate_type` AS STRING) AS `certificate_type`,
        CAST(`certificate_cap` AS STRING) AS `certificate_cap`,
        CAST(`awarding_body` AS STRING) AS `awarding_body`,
        CAST(`training_duration_hours` AS STRING) AS `training_duration_hours`,
        CAST(`product_pid` AS STRING) AS `product_pid`,
        CAST(`last_submission_date` AS STRING) AS `last_submission_date`,
        CAST(`created_by_send_to_checker` AS STRING) AS `created_by_send_to_checker`,
        CAST(`created_by_send_to_manager` AS STRING) AS `created_by_send_to_manager`,
        CAST(`created_by_mark_approved` AS STRING) AS `created_by_mark_approved`,
        CAST(`created_by_mark_disapproved` AS STRING) AS `created_by_mark_disapproved`,
        CAST(`last_disapproved_date` AS STRING) AS `last_disapproved_date`,
        CAST(`created_by_mark_withdrawn` AS STRING) AS `created_by_mark_withdrawn`,
        CAST(`created_by_mark_dropout` AS STRING) AS `created_by_mark_dropout`,
        CAST(`last_started_date` AS STRING) AS `last_started_date`,
        CAST(`last_finished_date` AS STRING) AS `last_finished_date`,
        CAST(`last_mark_approved_by_employee_date` AS STRING) AS `last_mark_approved_by_employee_date`,
        CAST(`last_mark_disapproved_by_employee_date` AS STRING) AS `last_mark_disapproved_by_employee_date`,
        CAST(`last_mark_disapproved_due_parent_date` AS STRING) AS `last_mark_disapproved_due_parent_date`,
        CAST(`first_send_back_to_maker_date` AS STRING) AS `first_send_back_to_maker_date`,
        CAST(`last_send_back_to_maker_date` AS STRING) AS `last_send_back_to_maker_date`,
        CAST(`total_count_send_back_to_maker` AS STRING) AS `total_count_send_back_to_maker`,
        CAST(`first_send_back_to_portal_date` AS STRING) AS `first_send_back_to_portal_date`,
        CAST(`last_send_back_to_portal_date` AS STRING) AS `last_send_back_to_portal_date`,
        CAST(`total_count_send_back_to_portal` AS STRING) AS `total_count_send_back_to_portal`,
        CAST(`last_send_to_tsp_date` AS STRING) AS `last_send_to_tsp_date`,
        CAST(`last_tsp_accepted_date` AS STRING) AS `last_tsp_accepted_date`,
        CAST(`last_tsp_rejected_date` AS STRING) AS `last_tsp_rejected_date`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`report_date` AS STRING) AS `report_date`
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
        'training_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'training_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'training_base' AS table_name,
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
        'training_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'training_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
