-- Compare bronze-layer query output with silver-layer table output for payment_assessment_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\payment_assessment_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\payment_assessment_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Databricks SQL generated from payment_assessment_base.sql.
-- Final column order aligned to silver_layer_query/payment_assessment_base_silver_layer.sql.
-- Standalone Databricks SQL converted from dbt model.
/*
 =================================================================================================

Name        : PAYMENT_REQUEST_BASE_NTP
Description : This model extracts and transforms payment request-related attributes
              from the NEO2 (NTP) source system Bronze Layer and loads into the
              PAYMENT_REQUEST target table as part of the Silver Layer data pipeline.
              It supports incremental loading with merge strategy and implements
              soft delete handling using a post-hook.

Source Tables : neo2.OSUSR_WZ3_PAYMENTREQUEST
                neo2.OSUSR_wz3_PaymentAssessment
                neo2.OSUSR_wz3_PaymentAssessmentStatus
                neo2.ossys_BPM_Activity
                neo2.ossys_BPM_Process
                neo2.ossys_BPM_Activity_Definition
                neo2.OSSYS_USER
                neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_398_APPLICATIONSTATUS
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_3QQ_PROGRAM
                neo2.OSUSR_3qq_InforDimensionMapping3
                neo2.OSUSR_WZ3_PAYMENTREQUESTSTATUS
                neo2.OSUSR_398_PAYMENTREQUESTTYPES
                neo2.OSUSR_398_PAYEETYPE
                neo2.OSUSR_TLV_IBAN
                neo2.OSUSR_TLV_IBANSTATUS
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_INDIVIDUAL
                neo2.OSUSR_ZMZ_COMPANY
                neo2.OSUSR_2DA_APPLICATIONSUPPORT
                neo2.OSUSR_2DA_APPLICATIONSUPPORTSTATUS
                neo2.OSUSR_2DA_EMPLOYEE

Target Table : PAYMENT_REQUEST
Load Type    : Incremental Load (Merge + Soft Delete)
Materialized : incremental
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-12 |     Abitha    | Initial version

================================================================================================= 
*/
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
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTASSESSMENT`          PayAss
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTASSESSMENTSTATUS` PayAssStat
           ON PayAss.ASSESSMENTSTATUSID = PayAssStat.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY`              act
           ON act.PROCESS_ID = PayAss.PROCESSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      U
           ON act.USER_ID = U.ID
    WHERE act.USER_ID IS NOT NULL
),

SEND_TO_INFO AS (
    SELECT
        PayAss.PAYMENTREQUESTID                                        AS PAYMENTREQUESTID,
        MAX(act.CLOSED)                                                AS MAX_EXECUTE_PAYMENT_DATE,
        COUNT(*)                                                       AS NUMBER_OF_EXECUTE_PAYMENT_TRIES
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTASSESSMENT`          PayAss
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_PROCESS`               pro
           ON pro.TOP_PROCESS_ID = PayAss.PROCESSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY`              act
           ON act.PROCESS_ID = pro.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY_DEFINITION`   actdef
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
            ELSE PayReq.CREATEDON + INTERVAL 3 HOURS
        END                                                            AS CREATED_ON_PAYMENT_REQUEST_GENERATED,
        CASE
            WHEN PayReq.SUBMITTEDON = TIMESTAMP '1900-01-01 00:00:00'
                THEN NULL
            ELSE PayReq.SUBMITTEDON + INTERVAL 3 HOURS
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
            ELSE APP.APPROVEDON + INTERVAL 3 HOURS
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
            ELSE SEND_TO_INFO.MAX_EXECUTE_PAYMENT_DATE + INTERVAL 3 HOURS
        END                                                            AS MAX_EXECUTE_PAYMENT_DATE,
        SEND_TO_INFO.NUMBER_OF_EXECUTE_PAYMENT_TRIES                   AS NUMBER_OF_EXECUTE_PAYMENT_TRIES,
        IBAN.ACCOUNTNAME                                               AS ACCOUNT_NAME,
        FALSE                                                          AS IS_DELETED,
        'NEO2'                                                         AS SOURCE_SYSTEM_NAME,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)        AS DBT_UPDATED_AT,
        PayReq.createdon,
        PayReq.updatedon,
        ROW_NUMBER() OVER (PARTITION BY PayReq.ID ORDER BY PayReq.UPDATEDON DESC NULLS LAST, PayReq.CREATEDON DESC NULLS LAST) AS rnk

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTREQUEST`             PayReq
    LEFT JOIN PAYASS
           ON PAYASS.PAYMENTREQUESTID = PayReq.ID
          AND PAYASS.RNK = 1
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION`           APP
           ON PayReq.APPLICATIONID = APP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_APPLICATIONSTATUS`     APST
           ON APP.APPLICATIONSTATUSID = APST.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION`        PV
           ON APP.PROGRAMVERSIONID = PV.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAM`               P
           ON P.ID = PV.PROGRAMID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_INFORDIMENSIONMAPPING3` DIM
           ON P.GUID = DIM.PROGRAMGUID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTREQUESTSTATUS`  PayReq_Stat
           ON PayReq.PAYMENTSTATUSID = PayReq_Stat.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_PAYMENTREQUESTTYPES`   PayReq_Type
           ON PayReq.PAYMENTREQUESTTYPEID = PayReq_Type.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_PAYEETYPE`             PayeeType
           ON PayeeType.CODE = PayReq.PAYEETYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_IBAN`                  IBAN
           ON PayReq.IBANID2 = IBAN.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_IBANSTATUS`            IBST
           ON IBAN.IBANSTATUSID = IBST.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE`       CusProf
           ON IBAN.CUSTOMERPROFILEID = CusProf.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER`              Cus
           ON CusProf.CUSTOMERID = Cus.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL`            IND
           ON CusProf.CUSTOMERID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY`               CMP
           ON CusProf.CUSTOMERID = CMP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORT`    APPSUP
           ON APPSUP.APPLICATIONID = PayReq.APPLICATIONID
          AND APPSUP.ACTIVESTATUSID = 'ACT'
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORTSTATUS` AppSuppWFS
           ON AppSuppWFS.CODE = APPSUP.APPLICATIONSUPPORTSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EMPLOYEE`              Emp
           ON APPSUP.ID = Emp.APPLICATIONSUPPORTID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER`              Vendor_CUS
           ON Vendor_CUS.ID = Emp.EMPLOYERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY`               Vendor_CMP
           ON Vendor_CUS.ID = Vendor_CMP.ID
    LEFT JOIN SEND_TO_INFO
           ON SEND_TO_INFO.PAYMENTREQUESTID = PayReq.ID
)

SELECT
    id,
    paymentstatusid,
    paymentrequesttypeid,
    ibanid,
    ibanid2,
    applicationid,
    supportareaid,
    portaluserid,
    payeetypeid,
    processreference,
    totalcostvalue,
    customersharevalue,
    updatedon,
    createdon,
    updatedby,
    guid,
    customerprofileid,
    submittedon,
    isbeingprocessed,
    reconciliationdate,
    paymentidentifier,
    cashrequirementnumber,
    lastsubmissiondate,
    sendtoinforon,
    processedon,
    eligibilitystatusid,
    aitoolresult,
    frombank,
    issampling,
    extract_date,
    application_reference,
    payment_request_reference,
    payment_request_status,
    total_amount_tamkeen_share,
    fawateer_reference,
    fawateer_flag,
    created_on_payment_request_generated,
    submitted_on_payment_request_submitted,
    payment_type,
    iban,
    iban_status,
    payee_type,
    payee,
    payee_cpr_cr_license,
    approved_on_application,
    workflow_status_application,
    workflow_status_employee,
    assigned_to,
    vendor_cr_license,
    vendor_name_training_provider_name,
    program_name,
    dimension4,
    updated_by,
    max_execute_payment_date,
    number_of_execute_payment_tries,
    account_name,
    is_deleted,
    UPPER(NULLIF(TRIM(CAST(SOURCE_SYSTEM_NAME AS STRING)), ''))                     AS source_system_name,
    TRY_CAST(NULLIF(CAST(DBT_UPDATED_AT AS STRING), '') AS TIMESTAMP)                        AS dbt_updated_at
FROM CTE_PAYMENT_REQUEST per
where rnk = 1
),

silver_layer AS (
SELECT
    id,
    paymentstatusid,
    paymentrequesttypeid,
    ibanid,
    ibanid2,
    applicationid,
    supportareaid,
    portaluserid,
    payeetypeid,
    processreference,
    totalcostvalue,
    customersharevalue,
    updatedon,
    createdon,
    updatedby,
    guid,
    customerprofileid,
    submittedon,
    isbeingprocessed,
    reconciliationdate,
    paymentidentifier,
    cashrequirementnumber,
    lastsubmissiondate,
    sendtoinforon,
    processedon,
    eligibilitystatusid,
    aitoolresult,
    frombank,
    issampling,
    extract_date,
    application_reference,
    payment_request_reference,
    payment_request_status,
    total_amount_tamkeen_share,
    fawateer_reference,
    fawateer_flag,
    created_on_payment_request_generated,
    submitted_on_payment_request_submitted,
    payment_type,
    iban,
    iban_status,
    payee_type,
    payee,
    payee_cpr_cr_license,
    approved_on_application,
    workflow_status_application,
    workflow_status_employee,
    assigned_to,
    vendor_cr_license,
    vendor_name_training_provider_name,
    program_name,
    dimension4,
    updated_by,
    max_execute_payment_date,
    number_of_execute_payment_tries,
    account_name,
    is_deleted,
    source_system_name,
    dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`payment_assessment_base`
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'paymentstatusid'),
        (3, 'paymentrequesttypeid'),
        (4, 'ibanid'),
        (5, 'ibanid2'),
        (6, 'applicationid'),
        (7, 'supportareaid'),
        (8, 'portaluserid'),
        (9, 'payeetypeid'),
        (10, 'processreference'),
        (11, 'totalcostvalue'),
        (12, 'customersharevalue'),
        (13, 'updatedon'),
        (14, 'createdon'),
        (15, 'updatedby'),
        (16, 'guid'),
        (17, 'customerprofileid'),
        (18, 'submittedon'),
        (19, 'isbeingprocessed'),
        (20, 'reconciliationdate'),
        (21, 'paymentidentifier'),
        (22, 'cashrequirementnumber'),
        (23, 'lastsubmissiondate'),
        (24, 'sendtoinforon'),
        (25, 'processedon'),
        (26, 'eligibilitystatusid'),
        (27, 'aitoolresult'),
        (28, 'frombank'),
        (29, 'issampling'),
        (30, 'extract_date'),
        (31, 'application_reference'),
        (32, 'payment_request_reference'),
        (33, 'payment_request_status'),
        (34, 'total_amount_tamkeen_share'),
        (35, 'fawateer_reference'),
        (36, 'fawateer_flag'),
        (37, 'created_on_payment_request_generated'),
        (38, 'submitted_on_payment_request_submitted'),
        (39, 'payment_type'),
        (40, 'iban'),
        (41, 'iban_status'),
        (42, 'payee_type'),
        (43, 'payee'),
        (44, 'payee_cpr_cr_license'),
        (45, 'approved_on_application'),
        (46, 'workflow_status_application'),
        (47, 'workflow_status_employee'),
        (48, 'assigned_to'),
        (49, 'vendor_cr_license'),
        (50, 'vendor_name_training_provider_name'),
        (51, 'program_name'),
        (52, 'dimension4'),
        (53, 'updated_by'),
        (54, 'max_execute_payment_date'),
        (55, 'number_of_execute_payment_tries'),
        (56, 'account_name'),
        (57, 'is_deleted'),
        (58, 'source_system_name'),
        (59, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'paymentstatusid'),
        (3, 'paymentrequesttypeid'),
        (4, 'ibanid'),
        (5, 'ibanid2'),
        (6, 'applicationid'),
        (7, 'supportareaid'),
        (8, 'portaluserid'),
        (9, 'payeetypeid'),
        (10, 'processreference'),
        (11, 'totalcostvalue'),
        (12, 'customersharevalue'),
        (13, 'updatedon'),
        (14, 'createdon'),
        (15, 'updatedby'),
        (16, 'guid'),
        (17, 'customerprofileid'),
        (18, 'submittedon'),
        (19, 'isbeingprocessed'),
        (20, 'reconciliationdate'),
        (21, 'paymentidentifier'),
        (22, 'cashrequirementnumber'),
        (23, 'lastsubmissiondate'),
        (24, 'sendtoinforon'),
        (25, 'processedon'),
        (26, 'eligibilitystatusid'),
        (27, 'aitoolresult'),
        (28, 'frombank'),
        (29, 'issampling'),
        (30, 'extract_date'),
        (31, 'application_reference'),
        (32, 'payment_request_reference'),
        (33, 'payment_request_status'),
        (34, 'total_amount_tamkeen_share'),
        (35, 'fawateer_reference'),
        (36, 'fawateer_flag'),
        (37, 'created_on_payment_request_generated'),
        (38, 'submitted_on_payment_request_submitted'),
        (39, 'payment_type'),
        (40, 'iban'),
        (41, 'iban_status'),
        (42, 'payee_type'),
        (43, 'payee'),
        (44, 'payee_cpr_cr_license'),
        (45, 'approved_on_application'),
        (46, 'workflow_status_application'),
        (47, 'workflow_status_employee'),
        (48, 'assigned_to'),
        (49, 'vendor_cr_license'),
        (50, 'vendor_name_training_provider_name'),
        (51, 'program_name'),
        (52, 'dimension4'),
        (53, 'updated_by'),
        (54, 'max_execute_payment_date'),
        (55, 'number_of_execute_payment_tries'),
        (56, 'account_name'),
        (57, 'is_deleted'),
        (58, 'source_system_name'),
        (59, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`paymentstatusid` AS STRING) AS `paymentstatusid`,
        CAST(`paymentrequesttypeid` AS STRING) AS `paymentrequesttypeid`,
        CAST(`ibanid` AS STRING) AS `ibanid`,
        CAST(`ibanid2` AS STRING) AS `ibanid2`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`supportareaid` AS STRING) AS `supportareaid`,
        CAST(`portaluserid` AS STRING) AS `portaluserid`,
        CAST(`payeetypeid` AS STRING) AS `payeetypeid`,
        CAST(`processreference` AS STRING) AS `processreference`,
        CAST(`totalcostvalue` AS STRING) AS `totalcostvalue`,
        CAST(`customersharevalue` AS STRING) AS `customersharevalue`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`customerprofileid` AS STRING) AS `customerprofileid`,
        CAST(`submittedon` AS STRING) AS `submittedon`,
        CAST(`isbeingprocessed` AS STRING) AS `isbeingprocessed`,
        CAST(`reconciliationdate` AS STRING) AS `reconciliationdate`,
        CAST(`paymentidentifier` AS STRING) AS `paymentidentifier`,
        CAST(`cashrequirementnumber` AS STRING) AS `cashrequirementnumber`,
        CAST(`lastsubmissiondate` AS STRING) AS `lastsubmissiondate`,
        CAST(`sendtoinforon` AS STRING) AS `sendtoinforon`,
        CAST(`processedon` AS STRING) AS `processedon`,
        CAST(`eligibilitystatusid` AS STRING) AS `eligibilitystatusid`,
        CAST(`aitoolresult` AS STRING) AS `aitoolresult`,
        CAST(`frombank` AS STRING) AS `frombank`,
        CAST(`issampling` AS STRING) AS `issampling`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`application_reference` AS STRING) AS `application_reference`,
        CAST(`payment_request_reference` AS STRING) AS `payment_request_reference`,
        CAST(`payment_request_status` AS STRING) AS `payment_request_status`,
        CAST(`total_amount_tamkeen_share` AS STRING) AS `total_amount_tamkeen_share`,
        CAST(`fawateer_reference` AS STRING) AS `fawateer_reference`,
        CAST(`fawateer_flag` AS STRING) AS `fawateer_flag`,
        CAST(`created_on_payment_request_generated` AS STRING) AS `created_on_payment_request_generated`,
        CAST(`submitted_on_payment_request_submitted` AS STRING) AS `submitted_on_payment_request_submitted`,
        CAST(`payment_type` AS STRING) AS `payment_type`,
        CAST(`iban` AS STRING) AS `iban`,
        CAST(`iban_status` AS STRING) AS `iban_status`,
        CAST(`payee_type` AS STRING) AS `payee_type`,
        CAST(`payee` AS STRING) AS `payee`,
        CAST(`payee_cpr_cr_license` AS STRING) AS `payee_cpr_cr_license`,
        CAST(`approved_on_application` AS STRING) AS `approved_on_application`,
        CAST(`workflow_status_application` AS STRING) AS `workflow_status_application`,
        CAST(`workflow_status_employee` AS STRING) AS `workflow_status_employee`,
        CAST(`assigned_to` AS STRING) AS `assigned_to`,
        CAST(`vendor_cr_license` AS STRING) AS `vendor_cr_license`,
        CAST(`vendor_name_training_provider_name` AS STRING) AS `vendor_name_training_provider_name`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`dimension4` AS STRING) AS `dimension4`,
        CAST(`updated_by` AS STRING) AS `updated_by`,
        CAST(`max_execute_payment_date` AS STRING) AS `max_execute_payment_date`,
        CAST(`number_of_execute_payment_tries` AS STRING) AS `number_of_execute_payment_tries`,
        CAST(`account_name` AS STRING) AS `account_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`paymentstatusid` AS STRING) AS `paymentstatusid`,
        CAST(`paymentrequesttypeid` AS STRING) AS `paymentrequesttypeid`,
        CAST(`ibanid` AS STRING) AS `ibanid`,
        CAST(`ibanid2` AS STRING) AS `ibanid2`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`supportareaid` AS STRING) AS `supportareaid`,
        CAST(`portaluserid` AS STRING) AS `portaluserid`,
        CAST(`payeetypeid` AS STRING) AS `payeetypeid`,
        CAST(`processreference` AS STRING) AS `processreference`,
        CAST(`totalcostvalue` AS STRING) AS `totalcostvalue`,
        CAST(`customersharevalue` AS STRING) AS `customersharevalue`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`guid` AS STRING) AS `guid`,
        CAST(`customerprofileid` AS STRING) AS `customerprofileid`,
        CAST(`submittedon` AS STRING) AS `submittedon`,
        CAST(`isbeingprocessed` AS STRING) AS `isbeingprocessed`,
        CAST(`reconciliationdate` AS STRING) AS `reconciliationdate`,
        CAST(`paymentidentifier` AS STRING) AS `paymentidentifier`,
        CAST(`cashrequirementnumber` AS STRING) AS `cashrequirementnumber`,
        CAST(`lastsubmissiondate` AS STRING) AS `lastsubmissiondate`,
        CAST(`sendtoinforon` AS STRING) AS `sendtoinforon`,
        CAST(`processedon` AS STRING) AS `processedon`,
        CAST(`eligibilitystatusid` AS STRING) AS `eligibilitystatusid`,
        CAST(`aitoolresult` AS STRING) AS `aitoolresult`,
        CAST(`frombank` AS STRING) AS `frombank`,
        CAST(`issampling` AS STRING) AS `issampling`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`application_reference` AS STRING) AS `application_reference`,
        CAST(`payment_request_reference` AS STRING) AS `payment_request_reference`,
        CAST(`payment_request_status` AS STRING) AS `payment_request_status`,
        CAST(`total_amount_tamkeen_share` AS STRING) AS `total_amount_tamkeen_share`,
        CAST(`fawateer_reference` AS STRING) AS `fawateer_reference`,
        CAST(`fawateer_flag` AS STRING) AS `fawateer_flag`,
        CAST(`created_on_payment_request_generated` AS STRING) AS `created_on_payment_request_generated`,
        CAST(`submitted_on_payment_request_submitted` AS STRING) AS `submitted_on_payment_request_submitted`,
        CAST(`payment_type` AS STRING) AS `payment_type`,
        CAST(`iban` AS STRING) AS `iban`,
        CAST(`iban_status` AS STRING) AS `iban_status`,
        CAST(`payee_type` AS STRING) AS `payee_type`,
        CAST(`payee` AS STRING) AS `payee`,
        CAST(`payee_cpr_cr_license` AS STRING) AS `payee_cpr_cr_license`,
        CAST(`approved_on_application` AS STRING) AS `approved_on_application`,
        CAST(`workflow_status_application` AS STRING) AS `workflow_status_application`,
        CAST(`workflow_status_employee` AS STRING) AS `workflow_status_employee`,
        CAST(`assigned_to` AS STRING) AS `assigned_to`,
        CAST(`vendor_cr_license` AS STRING) AS `vendor_cr_license`,
        CAST(`vendor_name_training_provider_name` AS STRING) AS `vendor_name_training_provider_name`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`dimension4` AS STRING) AS `dimension4`,
        CAST(`updated_by` AS STRING) AS `updated_by`,
        CAST(`max_execute_payment_date` AS STRING) AS `max_execute_payment_date`,
        CAST(`number_of_execute_payment_tries` AS STRING) AS `number_of_execute_payment_tries`,
        CAST(`account_name` AS STRING) AS `account_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
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
        'payment_assessment_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_assessment_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_assessment_base' AS table_name,
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
        'payment_assessment_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_assessment_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
