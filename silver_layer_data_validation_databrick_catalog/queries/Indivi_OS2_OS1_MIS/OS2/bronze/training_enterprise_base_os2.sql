with base_application_support as (

    select
        CAST(CURRENT_DATE AS DATE)                       AS EXTRACT_DATE,
        app.id                                                    as application_id,
        amt.amendmentrequestid                                   as amendment_id,
        app.referencenumber                                      as application_no,
        app_sta.label                                            as workflow_status,
        assessmentstatus.label                                   as assessment_workflow_status,
        appsuppwfs.label                                         as support_decision,
        progver.commercialname_en                                as program_name,

        case when app.submittedon = DATE '1900-01-01'
            then null else cast(app.submittedon + INTERVAL '3' HOUR as date)
        end                                                      as submitted_on,

        case when app.approvedon = DATE '1900-01-01'
            then null else cast(app.approvedon+ INTERVAL '3' HOUR as date)
        end                                                      as approved_on,

        case when app.starton = DATE '1900-01-01'
            then null else cast(app.starton + INTERVAL '3' HOUR as date)
        end                                                      as contract_start_date,

        case when app.endon = DATE '1900-01-01'
            then null else cast(app.endon + INTERVAL '3' HOUR as date)
        end                                                      as contract_end_date,

        -- case when app.approvalletteracceptedon = DATE '1900-01-01'
        --     then null else cast(app.approvalletteracceptedon + INTERVAL '3' HOUR as date)
        -- end                                                      as approval_letter_accepted_on,

        upper(ltrim(rtrim(cusapp.nameen)))                       as customer_full_name,
        cusindapp.cprnumber                                      as cpr,
        appsup.activestatusid                                    as employee_status,
        providertype.label                                      as training_provider_type,
        authtra.name                                             as training_provider_name,
        authtra.code                                             as provider_cr_license_no,
        trainingprogram.name                                     as training_name,
        trainingtype.label                                       as training_program_type,
        paytype.label                                            as training_payment_type,

        case tra.payeetypeid
            when 'CST' then 'Customer'
            when 'TP'  then 'Training Provider'
        end                                                      as payee,

        case when tra.trainingstartdate = DATE '1900-01-01'
            then null else cast(tra.trainingstartdate + INTERVAL '3' HOUR as date)
        end                                                      as training_start_date,

        case when tra.trainingenddate = DATE '1900-01-01'
            then null else cast( tra.trainingenddate + INTERVAL '3' HOUR as date)
        end                                                      as training_end_date,

        case when tra.trainingassessmentdate = DATE '1900-01-01'
            then null else cast(tra.trainingassessmentdate + INTERVAL '3' HOUR as date)
        end                                                      as training_assessment_date,

        trainingprogram.inputcapamount                           as certification_cap_amount_bhd,
        tra.itemcapamt                                           as tamkeen_cap_amount_bhd,
        tra.tkshareamt                                           as tamkeen_share_amount_bhd,
        tra.tkshareactualpct * 100                                as tamkeen_share_pct,
        100 - (tra.tkshareactualpct * 100)                        as customer_share_pct,
        tra.customershare                                       as customer_share_amount_with_vat,

        case
            when tra.itemcapamt = 0 then null
            when tra.itemamtclaimed > 0 then tra.itemcapamt - tra.itemamtclaimed
            when tra.itemamtavailable > 0 then tra.itemcapamt - tra.itemamtavailable
            when tra.itemamtinprogress > 0 then tra.itemcapamt - tra.itemamtinprogress
        end                                                      as unutilized_amount_bhd,

        tra.itemvatamt                                           as total_vat_amount_bhd,

        -- case
        --     when appsupext.tksharetotalamt = 0
        --         then null else round(appsupext.tksharetotalamt, 3)
        -- end                                                      as effective_tamkeen_share_amount_bhd
        TRA.CERTIFICATIONID,
        TRA.TRAININGDELIVERYTYPEID,
        TRA.ITEMAMTAVAILABLE,
        TRA.ITEMAMTINPROGRESS,
        TRA.CUSTOMERSHARETOTAL,
        TRA.TRAININGPAYMENTTYPEID,
        TRA.TKSHAREAMTAUTO,
        TRA.PAYEETYPEID,
        TRAININGPROGRAM.AUTHORIZEDPROVIDERID,
        TRAININGPROGRAM.OVERVIEW,
        TRAININGPROGRAM.AWARDINGBODYNAME,
        TRAININGPROGRAM.TRAININGPROGAMSTATUSID,
        TRAININGPROGRAM.ESTIMATEDTRAININGCOST,
        TRAININGPROGRAM.TRAININGHOURS,
        TRAININGPROGRAM.TRAININGKNOWLEDGEAREAID,
        TRAININGPROGRAM.TRAININGDETAILAREAID,
        TRAININGPROGRAM.INPUTCAPAMOUNT,
        TRAININGPROGRAM.CERTIFICATEID,
        TRAININGPROGRAM.TRAININGPROGRAMTYPEID,
        TRAININGPROGRAM.TRAININGTYPEID,
        TRAININGPROGRAM.APPLICANTSEGMENTID,
        TRAININGPROGRAM.ISMOLRELATED,
        TRAININGPROGRAM.ISEXEMPTED,
        TRAININGPROGRAM.ISALLOWPAPER,
        TRAININGPROGRAM.TRAINING_SUPPORTTYPE,
        TRAININGPROGRAM.ISSUBMITTEDINAPP,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT
    from dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT appsup
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION app
        on app.id = appsup.applicationid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT amt
        on amt.applicationid = app.id
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS assessmentstatus
        on assessmentstatus.code = amt.assessmentstatusid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_TRAINING tra
        on tra.applicationsupportid = appsup.id
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_CERTIFICATION tracertif
        on tracertif.id = tra.certificationid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_TRAININGPROGRAM trainingprogram
        on trainingprogram.id = tracertif.trainingprogramid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_TRAININGTYPE trainingtype
        on trainingtype."order" = trainingprogram.trainingtypeid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_GUR_AUTHORIZEDENTITIES authtra
        on trainingprogram.authorizedproviderid = authtra.id
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION progver
        on progver.id = app.programversionid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM program
        on program.id = progver.programid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_TRAININGPAYMENTTYPE paytype
        on paytype.code = tra.trainingpaymenttypeid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS app_sta
        on app_sta.code = app.applicationstatusid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTSTATUS appsuppwfs
        on appsuppwfs.code = appsup.applicationsupportstatusid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_PROVIDERTYPE providertype
        on providertype.id = appsup.providertypeid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER cusapp
        on appsup.individualid = cusapp.id
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_INDIVIDUAL cusindapp
        on appsup.individualid = cusindapp.id

    where
        program.profiletypeid = 'ENT'
        and appsup.isactive = TRUE
        and app.isactive = TRUE
        and (
            (appsup.activestatusid = 'INA' and appsup.applicationsupportstatusid = 'REM')
            or appsup.activestatusid = 'ACT'
        )
),

amendment_support as (

    select *
    from base_application_support
    where amendment_id is not null
)

select * from base_application_support
union all
select * from amendment_support
