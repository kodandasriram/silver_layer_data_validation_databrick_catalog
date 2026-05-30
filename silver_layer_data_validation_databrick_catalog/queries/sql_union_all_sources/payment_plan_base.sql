WITH
bronze_layer AS (
-- Standalone Trino SQL generated from payment_plan_base.sql.
-- Final column order aligned to silver_layer_query/payment_plan_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : PAYMENT_PLAN_BASE_NTP
Description : This model extracts and transforms payment plan-related attributes
              from the NEO2 (NTP) source system Bronze Layer and loads into the
              PAYMENT_PLAN target table as part of the Silver Layer data pipeline.
              It supports incremental loading with merge strategy and implements
              soft delete handling using a post-hook.

Source Tables : neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_2DA_PAYMENTPLAN
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_3QQ_PROGRAM
                neo2.OSUSR_398_APPLICATIONSTATUS
                neo2.OSUSR_1AT_ASSESSMENT
                neo2.OSSYS_BPM_PROCESS
                neo2.OSSYS_BPM_ACTIVITY
                neo2.OSUSR_1AT_ASSESSMENTSTATUS

Target Table : PAYMENT_PLAN
Load Type    : Incremental Load (Merge + Soft Delete)
Materialized : incremental
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------
Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-12 |  Abitha       | Initial version

================================================================================================= 
*/
-- ============================================================================
-- CTE: Latest assessment activity per application
-- Replaces temp table pattern from source query
-- ============================================================================
WITH TEMP_ASSESSMENT2 AS (
    SELECT
        ACT.NAME                                                   AS activity_name,
        ASSESSMENTSTATUS.LABEL                                     AS assessment_status_label,
        ASS.APPLICATIONID                                          AS application_id,
        ASS.AMENDMENTREQUESTID                                     AS amendment_request_id,
        ROW_NUMBER() OVER (
            PARTITION BY ASS.APPLICATIONID
            ORDER BY ACT.ID DESC
        )                                                          AS rn
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENT              ASS
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_PROCESS           PRO
        ON PRO.TOP_PROCESS_ID = ASS.PROCESSID
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_BPM_ACTIVITY          ACT
        ON ACT.PROCESS_ID = PRO.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_1AT_ASSESSMENTSTATUS   ASSESSMENTSTATUS
        ON ASS.ASSESSMENTSTATUSID = ASSESSMENTSTATUS.CODE
),

-- ============================================================================
-- CTE: Core business logic
-- ============================================================================
FINAL_DATA AS (
    SELECT
        -- Audit / extract
        CAST(
            current_timestamp() + INTERVAL '6' HOUR AS DATE
        )                                                          AS extract_date,

        -- Application identifiers
        APP.id                                                     AS application_id,
        APP.referencenumber                                        AS application_no,
        APP.programversionid,                                      
        APP.decisionprogramversionid,                              
        APP.portaluserid,                                        
        APP.customertypeid,                                         
        APP.beneficiaryid,                                         
        APP.applicationstatusid,                                    
        APP.profilinginstanceguid,                                 
        APP.applicationinstanceformguid,                            
        APP.applicationinstancedocguid,                             
        APP.customerinstanceformguid,                               
        APP.findatainstanceformguid,                               
        APP.customerinstancedocguid,                                
        APP.guid,                                                   
        APP.bindinginstancedocguid,                                 
        APP.amendapprovalinstancedocguid,                           
        APP.hipoinstanceformguid,                                   
        APP.analysisinstanceformguid,                               
        APP.ishipooptionid,                                         
        APP.iseligibleoptionid,                                     
        APP.programcap,                                             
        APP.programcapid,                                           
        APP.applicationcap,                                         
        APP.tkshareamt,                                            
        APP.applicationcapunutilized,                              
        APP.customershareamt,                                       
        APP.totalcostwvat,                                          
        APP.starton,                                                
        APP.endon,                                                  
        APP.monitoringduedate,                                      
        APP.spendingperiodduedate,                                  
        APP.claimingperiodduedate,                                  
        APP.duration,                                               
        APP.isactive,                                               
        APP.createdby,                                              
        APP.createdon,                                              
        APP.updatedby,                                              
        APP.updatedon,                                             
        APP.submittedon,                                            
        APP.approvedon,                                             
        APP.bindinginstancedocguid_ar,                              
        APP.amendappinstancedocgudi_ar,                             
        APP.assessmentprocessid,                                 
        APP.grantcalcinstanceformguid,                              
        APP.evcinstanceformguid,                                    
        APP.haswagesupportmolemployees,                             
        APP.calculatedeconomicvalue,                                
        APP.calculatedgrantamount,                                  
        APP.internalinstancedocguid,                                
        -- Payment Support / Schedule
        PAYPLAN.APPLICATIONSUPPORTID                               AS application_support_id,
        PAYPLAN.ID                                                 AS payment_schedule_id,

        -- Program
        PROGVER.COMMERCIALNAME_EN                                  AS program_name,

        -- Customer
        CASE
            WHEN CUS.CUSTOMERTYPEID = 'CMP'
                THEN UPPER(TRIM(CUS.NAMEEN))
            ELSE NULL
        END                                                        AS commercial_name_en,

        -- Payment Plan
        PAYPLAN.PAYMENTNUMBER                                      AS payment_number,
        PAYPLAN.PAYMENTDATE                                        AS payment_date,
        PAYPLAN.BEGININGBALANCE                                    AS beginning_balance,
        PAYPLAN.SCHEDULEDPAYMENT                                   AS scheduled_payment_bhd,
        PAYPLAN.TOTALPAYMENT                                       AS total_payment_bhd,
        PAYPLAN.PROFIT                                             AS profit_bhd,
        PAYPLAN.PRINCIPAL                                          AS principal_bhd,
        PAYPLAN.CUMULATIVEPROFIT                                   AS cumulative_profit_bhd,
        PAYPLAN.TKPROFITSUBSIDY                                    AS profit_subsidy_by_tamkeen_bhd,
        PAYPLAN.TKPRINCIPALAMT                                     AS principal_auto_calculated_bhd,
        PAYPLAN.TKPROFITAMT                                        AS profit_auto_calculated_bhd,
        PAYPLAN.PAYMENTPLANSTATUSID                                AS workflow_status_payment_plan,

        -- Assessment
        ASSESSMENT.ACTIVITY_NAME                                   AS latest_activity_name,
        ASSESSMENT.ASSESSMENT_STATUS_LABEL                         AS assessment_status_label,
        ASSESSMENT.AMENDMENT_REQUEST_ID                            AS amendment_request_id,
        ROW_NUMBER() OVER (PARTITION BY APP.ID ORDER BY APP.UPDATEDON DESC NULLS LAST, APP.CREATEDON DESC NULLS LAST) AS RNK,

        -- Standard audit columns
        FALSE                                                      AS is_deleted,
        'NEO2'                                                     AS source_system_name,
        CURRENT_DATE                                               AS report_date,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)   AS dbt_updated_at

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION             APP
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_PAYMENTPLAN       PAYPLAN
        ON PAYPLAN.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAMVERSION     PROGVER
        ON PROGVER.ID = APP.PROGRAMVERSIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE    CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER           CUS
        ON CUS.ID = CUSPROF.CUSTOMERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_PROGRAM            P
        ON P.ID = PROGVER.PROGRAMID
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_APPLICATIONSTATUS APP_STA
        ON APP_STA.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN TEMP_ASSESSMENT2                                     ASSESSMENT
        ON ASSESSMENT.APPLICATION_ID = APP.ID
       AND ASSESSMENT.rn = 1

    WHERE CUSPROF.PROFILETYPEID = 'ENT'
)

SELECT
    application_id,
    extract_date,
    application_no,
    programversionid,
    decisionprogramversionid,
    portaluserid,
    customertypeid,
    beneficiaryid,
    applicationstatusid,
    profilinginstanceguid,
    applicationinstanceformguid,
    applicationinstancedocguid,
    customerinstanceformguid,
    findatainstanceformguid,
    customerinstancedocguid,
    guid,
    bindinginstancedocguid,
    amendapprovalinstancedocguid,
    hipoinstanceformguid,
    analysisinstanceformguid,
    ishipooptionid,
    iseligibleoptionid,
    programcap,
    programcapid,
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
    bindinginstancedocguid_ar,
    amendappinstancedocgudi_ar,
    assessmentprocessid,
    grantcalcinstanceformguid,
    evcinstanceformguid,
    haswagesupportmolemployees,
    calculatedeconomicvalue,
    calculatedgrantamount,
    internalinstancedocguid,
    application_support_id,
    payment_schedule_id,
    program_name,
    commercial_name_en,
    payment_number,
    payment_date,
    beginning_balance,
    scheduled_payment_bhd,
    total_payment_bhd,
    profit_bhd,
    principal_bhd,
    cumulative_profit_bhd,
    profit_subsidy_by_tamkeen_bhd,
    principal_auto_calculated_bhd,
    profit_auto_calculated_bhd,
    workflow_status_payment_plan,
    latest_activity_name,
    assessment_status_label,
    amendment_request_id,
    is_deleted,
    source_system_name,
    report_date,
    dbt_updated_at
FROM FINAL_DATA where rnk=1
),

silver_layer AS (
SELECT
    application_id,
    extract_date,
    application_no,
    programversionid,
    decisionprogramversionid,
    portaluserid,
    customertypeid,
    beneficiaryid,
    applicationstatusid,
    profilinginstanceguid,
    applicationinstanceformguid,
    applicationinstancedocguid,
    customerinstanceformguid,
    findatainstanceformguid,
    customerinstancedocguid,
    guid,
    bindinginstancedocguid,
    amendapprovalinstancedocguid,
    hipoinstanceformguid,
    analysisinstanceformguid,
    ishipooptionid,
    iseligibleoptionid,
    programcap,
    programcapid,
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
    bindinginstancedocguid_ar,
    amendappinstancedocgudi_ar,
    assessmentprocessid,
    grantcalcinstanceformguid,
    evcinstanceformguid,
    haswagesupportmolemployees,
    calculatedeconomicvalue,
    calculatedgrantamount,
    internalinstancedocguid,
    application_support_id,
    payment_schedule_id,
    program_name,
    commercial_name_en,
    payment_number,
    payment_date,
    beginning_balance,
    scheduled_payment_bhd,
    total_payment_bhd,
    profit_bhd,
    principal_bhd,
    cumulative_profit_bhd,
    profit_subsidy_by_tamkeen_bhd,
    principal_auto_calculated_bhd,
    profit_auto_calculated_bhd,
    workflow_status_payment_plan,
    latest_activity_name,
    assessment_status_label,
    amendment_request_id,
    is_deleted,
    source_system_name,
    report_date,
    dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.payment_plan_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'application_id'),
        (2, 'extract_date'),
        (3, 'application_no'),
        (4, 'programversionid'),
        (5, 'decisionprogramversionid'),
        (6, 'portaluserid'),
        (7, 'customertypeid'),
        (8, 'beneficiaryid'),
        (9, 'applicationstatusid'),
        (10, 'profilinginstanceguid'),
        (11, 'applicationinstanceformguid'),
        (12, 'applicationinstancedocguid'),
        (13, 'customerinstanceformguid'),
        (14, 'findatainstanceformguid'),
        (15, 'customerinstancedocguid'),
        (16, 'guid'),
        (17, 'bindinginstancedocguid'),
        (18, 'amendapprovalinstancedocguid'),
        (19, 'hipoinstanceformguid'),
        (20, 'analysisinstanceformguid'),
        (21, 'ishipooptionid'),
        (22, 'iseligibleoptionid'),
        (23, 'programcap'),
        (24, 'programcapid'),
        (25, 'applicationcap'),
        (26, 'tkshareamt'),
        (27, 'applicationcapunutilized'),
        (28, 'customershareamt'),
        (29, 'totalcostwvat'),
        (30, 'starton'),
        (31, 'endon'),
        (32, 'monitoringduedate'),
        (33, 'spendingperiodduedate'),
        (34, 'claimingperiodduedate'),
        (35, 'duration'),
        (36, 'isactive'),
        (37, 'createdby'),
        (38, 'createdon'),
        (39, 'updatedby'),
        (40, 'updatedon'),
        (41, 'submittedon'),
        (42, 'approvedon'),
        (43, 'bindinginstancedocguid_ar'),
        (44, 'amendappinstancedocgudi_ar'),
        (45, 'assessmentprocessid'),
        (46, 'grantcalcinstanceformguid'),
        (47, 'evcinstanceformguid'),
        (48, 'haswagesupportmolemployees'),
        (49, 'calculatedeconomicvalue'),
        (50, 'calculatedgrantamount'),
        (51, 'internalinstancedocguid'),
        (52, 'application_support_id'),
        (53, 'payment_schedule_id'),
        (54, 'program_name'),
        (55, 'commercial_name_en'),
        (56, 'payment_number'),
        (57, 'payment_date'),
        (58, 'beginning_balance'),
        (59, 'scheduled_payment_bhd'),
        (60, 'total_payment_bhd'),
        (61, 'profit_bhd'),
        (62, 'principal_bhd'),
        (63, 'cumulative_profit_bhd'),
        (64, 'profit_subsidy_by_tamkeen_bhd'),
        (65, 'principal_auto_calculated_bhd'),
        (66, 'profit_auto_calculated_bhd'),
        (67, 'workflow_status_payment_plan'),
        (68, 'latest_activity_name'),
        (69, 'assessment_status_label'),
        (70, 'amendment_request_id'),
        (71, 'is_deleted'),
        (72, 'source_system_name'),
        (73, 'report_date'),
        (74, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'application_id'),
        (2, 'extract_date'),
        (3, 'application_no'),
        (4, 'programversionid'),
        (5, 'decisionprogramversionid'),
        (6, 'portaluserid'),
        (7, 'customertypeid'),
        (8, 'beneficiaryid'),
        (9, 'applicationstatusid'),
        (10, 'profilinginstanceguid'),
        (11, 'applicationinstanceformguid'),
        (12, 'applicationinstancedocguid'),
        (13, 'customerinstanceformguid'),
        (14, 'findatainstanceformguid'),
        (15, 'customerinstancedocguid'),
        (16, 'guid'),
        (17, 'bindinginstancedocguid'),
        (18, 'amendapprovalinstancedocguid'),
        (19, 'hipoinstanceformguid'),
        (20, 'analysisinstanceformguid'),
        (21, 'ishipooptionid'),
        (22, 'iseligibleoptionid'),
        (23, 'programcap'),
        (24, 'programcapid'),
        (25, 'applicationcap'),
        (26, 'tkshareamt'),
        (27, 'applicationcapunutilized'),
        (28, 'customershareamt'),
        (29, 'totalcostwvat'),
        (30, 'starton'),
        (31, 'endon'),
        (32, 'monitoringduedate'),
        (33, 'spendingperiodduedate'),
        (34, 'claimingperiodduedate'),
        (35, 'duration'),
        (36, 'isactive'),
        (37, 'createdby'),
        (38, 'createdon'),
        (39, 'updatedby'),
        (40, 'updatedon'),
        (41, 'submittedon'),
        (42, 'approvedon'),
        (43, 'bindinginstancedocguid_ar'),
        (44, 'amendappinstancedocgudi_ar'),
        (45, 'assessmentprocessid'),
        (46, 'grantcalcinstanceformguid'),
        (47, 'evcinstanceformguid'),
        (48, 'haswagesupportmolemployees'),
        (49, 'calculatedeconomicvalue'),
        (50, 'calculatedgrantamount'),
        (51, 'internalinstancedocguid'),
        (52, 'application_support_id'),
        (53, 'payment_schedule_id'),
        (54, 'program_name'),
        (55, 'commercial_name_en'),
        (56, 'payment_number'),
        (57, 'payment_date'),
        (58, 'beginning_balance'),
        (59, 'scheduled_payment_bhd'),
        (60, 'total_payment_bhd'),
        (61, 'profit_bhd'),
        (62, 'principal_bhd'),
        (63, 'cumulative_profit_bhd'),
        (64, 'profit_subsidy_by_tamkeen_bhd'),
        (65, 'principal_auto_calculated_bhd'),
        (66, 'profit_auto_calculated_bhd'),
        (67, 'workflow_status_payment_plan'),
        (68, 'latest_activity_name'),
        (69, 'assessment_status_label'),
        (70, 'amendment_request_id'),
        (71, 'is_deleted'),
        (72, 'source_system_name'),
        (73, 'report_date'),
        (74, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`programversionid` AS STRING) AS `programversionid`,
        CAST(`decisionprogramversionid` AS STRING) AS `decisionprogramversionid`,
        CAST(`portaluserid` AS STRING) AS `portaluserid`,
        CAST(`customertypeid` AS STRING) AS `customertypeid`,
        CAST(`beneficiaryid` AS STRING) AS `beneficiaryid`,
        CAST(`applicationstatusid` AS STRING) AS `applicationstatusid`,
        CAST(`profilinginstanceguid` AS STRING) AS `profilinginstanceguid`,
        CAST(`applicationinstanceformguid` AS STRING) AS `applicationinstanceformguid`,
        CAST(`applicationinstancedocguid` AS STRING) AS `applicationinstancedocguid`,
        CAST(`customerinstanceformguid` AS STRING) AS `customerinstanceformguid`,
        CAST(`findatainstanceformguid` AS STRING) AS `findatainstanceformguid`,
        CAST(`customerinstancedocguid` AS STRING) AS `customerinstancedocguid`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`bindinginstancedocguid` AS STRING) AS `bindinginstancedocguid`,
        CAST(`amendapprovalinstancedocguid` AS STRING) AS `amendapprovalinstancedocguid`,
        CAST(`hipoinstanceformguid` AS STRING) AS `hipoinstanceformguid`,
        CAST(`analysisinstanceformguid` AS STRING) AS `analysisinstanceformguid`,
        CAST(`ishipooptionid` AS STRING) AS `ishipooptionid`,
        CAST(`iseligibleoptionid` AS STRING) AS `iseligibleoptionid`,
        CAST(`programcap` AS STRING) AS `programcap`,
        CAST(`programcapid` AS STRING) AS `programcapid`,
        CAST(`applicationcap` AS STRING) AS `applicationcap`,
        CAST(`tkshareamt` AS STRING) AS `tkshareamt`,
        CAST(`applicationcapunutilized` AS STRING) AS `applicationcapunutilized`,
        CAST(`customershareamt` AS STRING) AS `customershareamt`,
        CAST(`totalcostwvat` AS STRING) AS `totalcostwvat`,
        CAST(`starton` AS STRING) AS `starton`,
        CAST(`endon` AS STRING) AS `endon`,
        CAST(`monitoringduedate` AS STRING) AS `monitoringduedate`,
        CAST(`spendingperiodduedate` AS STRING) AS `spendingperiodduedate`,
        CAST(`claimingperiodduedate` AS STRING) AS `claimingperiodduedate`,
        CAST(`duration` AS STRING) AS `duration`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`createdby` AS STRING) AS `createdby`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`submittedon` AS STRING) AS `submittedon`,
        CAST(`approvedon` AS STRING) AS `approvedon`,
        CAST(`bindinginstancedocguid_ar` AS STRING) AS `bindinginstancedocguid_ar`,
        CAST(`amendappinstancedocgudi_ar` AS STRING) AS `amendappinstancedocgudi_ar`,
        CAST(`assessmentprocessid` AS STRING) AS `assessmentprocessid`,
        CAST(`grantcalcinstanceformguid` AS STRING) AS `grantcalcinstanceformguid`,
        CAST(`evcinstanceformguid` AS STRING) AS `evcinstanceformguid`,
        CAST(`haswagesupportmolemployees` AS STRING) AS `haswagesupportmolemployees`,
        CAST(`calculatedeconomicvalue` AS STRING) AS `calculatedeconomicvalue`,
        CAST(`calculatedgrantamount` AS STRING) AS `calculatedgrantamount`,
        CAST(`internalinstancedocguid` AS STRING) AS `internalinstancedocguid`,
        CAST(`application_support_id` AS STRING) AS `application_support_id`,
        CAST(`payment_schedule_id` AS STRING) AS `payment_schedule_id`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`commercial_name_en` AS STRING) AS `commercial_name_en`,
        CAST(`payment_number` AS STRING) AS `payment_number`,
        CAST(`payment_date` AS STRING) AS `payment_date`,
        CAST(`beginning_balance` AS STRING) AS `beginning_balance`,
        CAST(`scheduled_payment_bhd` AS STRING) AS `scheduled_payment_bhd`,
        CAST(`total_payment_bhd` AS STRING) AS `total_payment_bhd`,
        CAST(`profit_bhd` AS STRING) AS `profit_bhd`,
        CAST(`principal_bhd` AS STRING) AS `principal_bhd`,
        CAST(`cumulative_profit_bhd` AS STRING) AS `cumulative_profit_bhd`,
        CAST(`profit_subsidy_by_tamkeen_bhd` AS STRING) AS `profit_subsidy_by_tamkeen_bhd`,
        CAST(`principal_auto_calculated_bhd` AS STRING) AS `principal_auto_calculated_bhd`,
        CAST(`profit_auto_calculated_bhd` AS STRING) AS `profit_auto_calculated_bhd`,
        CAST(`workflow_status_payment_plan` AS STRING) AS `workflow_status_payment_plan`,
        CAST(`latest_activity_name` AS STRING) AS `latest_activity_name`,
        CAST(`assessment_status_label` AS STRING) AS `assessment_status_label`,
        CAST(`amendment_request_id` AS STRING) AS `amendment_request_id`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`programversionid` AS STRING) AS `programversionid`,
        CAST(`decisionprogramversionid` AS STRING) AS `decisionprogramversionid`,
        CAST(`portaluserid` AS STRING) AS `portaluserid`,
        CAST(`customertypeid` AS STRING) AS `customertypeid`,
        CAST(`beneficiaryid` AS STRING) AS `beneficiaryid`,
        CAST(`applicationstatusid` AS STRING) AS `applicationstatusid`,
        CAST(`profilinginstanceguid` AS STRING) AS `profilinginstanceguid`,
        CAST(`applicationinstanceformguid` AS STRING) AS `applicationinstanceformguid`,
        CAST(`applicationinstancedocguid` AS STRING) AS `applicationinstancedocguid`,
        CAST(`customerinstanceformguid` AS STRING) AS `customerinstanceformguid`,
        CAST(`findatainstanceformguid` AS STRING) AS `findatainstanceformguid`,
        CAST(`customerinstancedocguid` AS STRING) AS `customerinstancedocguid`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`bindinginstancedocguid` AS STRING) AS `bindinginstancedocguid`,
        CAST(`amendapprovalinstancedocguid` AS STRING) AS `amendapprovalinstancedocguid`,
        CAST(`hipoinstanceformguid` AS STRING) AS `hipoinstanceformguid`,
        CAST(`analysisinstanceformguid` AS STRING) AS `analysisinstanceformguid`,
        CAST(`ishipooptionid` AS STRING) AS `ishipooptionid`,
        CAST(`iseligibleoptionid` AS STRING) AS `iseligibleoptionid`,
        CAST(`programcap` AS STRING) AS `programcap`,
        CAST(`programcapid` AS STRING) AS `programcapid`,
        CAST(`applicationcap` AS STRING) AS `applicationcap`,
        CAST(`tkshareamt` AS STRING) AS `tkshareamt`,
        CAST(`applicationcapunutilized` AS STRING) AS `applicationcapunutilized`,
        CAST(`customershareamt` AS STRING) AS `customershareamt`,
        CAST(`totalcostwvat` AS STRING) AS `totalcostwvat`,
        CAST(`starton` AS STRING) AS `starton`,
        CAST(`endon` AS STRING) AS `endon`,
        CAST(`monitoringduedate` AS STRING) AS `monitoringduedate`,
        CAST(`spendingperiodduedate` AS STRING) AS `spendingperiodduedate`,
        CAST(`claimingperiodduedate` AS STRING) AS `claimingperiodduedate`,
        CAST(`duration` AS STRING) AS `duration`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`createdby` AS STRING) AS `createdby`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`submittedon` AS STRING) AS `submittedon`,
        CAST(`approvedon` AS STRING) AS `approvedon`,
        CAST(`bindinginstancedocguid_ar` AS STRING) AS `bindinginstancedocguid_ar`,
        CAST(`amendappinstancedocgudi_ar` AS STRING) AS `amendappinstancedocgudi_ar`,
        CAST(`assessmentprocessid` AS STRING) AS `assessmentprocessid`,
        CAST(`grantcalcinstanceformguid` AS STRING) AS `grantcalcinstanceformguid`,
        CAST(`evcinstanceformguid` AS STRING) AS `evcinstanceformguid`,
        CAST(`haswagesupportmolemployees` AS STRING) AS `haswagesupportmolemployees`,
        CAST(`calculatedeconomicvalue` AS STRING) AS `calculatedeconomicvalue`,
        CAST(`calculatedgrantamount` AS STRING) AS `calculatedgrantamount`,
        CAST(`internalinstancedocguid` AS STRING) AS `internalinstancedocguid`,
        CAST(`application_support_id` AS STRING) AS `application_support_id`,
        CAST(`payment_schedule_id` AS STRING) AS `payment_schedule_id`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`commercial_name_en` AS STRING) AS `commercial_name_en`,
        CAST(`payment_number` AS STRING) AS `payment_number`,
        CAST(`payment_date` AS STRING) AS `payment_date`,
        CAST(`beginning_balance` AS STRING) AS `beginning_balance`,
        CAST(`scheduled_payment_bhd` AS STRING) AS `scheduled_payment_bhd`,
        CAST(`total_payment_bhd` AS STRING) AS `total_payment_bhd`,
        CAST(`profit_bhd` AS STRING) AS `profit_bhd`,
        CAST(`principal_bhd` AS STRING) AS `principal_bhd`,
        CAST(`cumulative_profit_bhd` AS STRING) AS `cumulative_profit_bhd`,
        CAST(`profit_subsidy_by_tamkeen_bhd` AS STRING) AS `profit_subsidy_by_tamkeen_bhd`,
        CAST(`principal_auto_calculated_bhd` AS STRING) AS `principal_auto_calculated_bhd`,
        CAST(`profit_auto_calculated_bhd` AS STRING) AS `profit_auto_calculated_bhd`,
        CAST(`workflow_status_payment_plan` AS STRING) AS `workflow_status_payment_plan`,
        CAST(`latest_activity_name` AS STRING) AS `latest_activity_name`,
        CAST(`assessment_status_label` AS STRING) AS `assessment_status_label`,
        CAST(`amendment_request_id` AS STRING) AS `amendment_request_id`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
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
        'payment_plan_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_plan_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_plan_base' AS table_name,
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
        'payment_plan_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_plan_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
