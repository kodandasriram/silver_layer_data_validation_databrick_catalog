WITH TEMP_ASSESSMENT2 AS (
    SELECT
        ACT.NAME                                                   AS ACTIVITY_NAME,
        ASSESSMENTSTATUS.LABEL                                     AS ASSESSMENT_STATUS_LABEL,
        ASS.APPLICATIONID                                          AS APPLICATION_ID,
        ASS.AMENDMENTREQUESTID                                     AS AMENDMENT_REQUEST_ID,
        ROW_NUMBER() OVER (
            PARTITION BY ASS.APPLICATIONID
            ORDER BY ACT.ID DESC
        )                                                          AS RN
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT              ASS
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS           PRO
        ON PRO.TOP_PROCESS_ID = ASS.PROCESSID
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY          ACT
        ON ACT.PROCESS_ID = PRO.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS   ASSESSMENTSTATUS
        ON ASS.ASSESSMENTSTATUSID = ASSESSMENTSTATUS.CODE
),


-- ============================================================================
-- CTE: Core business logic
-- ============================================================================
FINAL_DATA AS (
    SELECT
        -- Audit / Extract
        CAST(
            DATE_ADD('hour', 6, CURRENT_TIMESTAMP) AS DATE
        )                                                          AS EXTRACT_DATE,

        -- Application identifiers
        APP.ID                                                     AS APPLICATION_ID,
        APP.REFERENCENUMBER                                        AS APPLICATION_NO,
        APP.PROGRAMVERSIONID,                                      
        APP.DECISIONPROGRAMVERSIONID,                              
        APP.PORTALUSERID,                                        
        APP.CUSTOMERTYPEID,                                         
        APP.BENEFICIARYID,                                         
        APP.APPLICATIONSTATUSID,                                    
        APP.PROFILINGINSTANCEGUID,                                 
        APP.APPLICATIONINSTANCEFORMGUID,                            
        APP.APPLICATIONINSTANCEDOCGUID,                             
        APP.CUSTOMERINSTANCEFORMGUID,                               
        APP.FINDATAINSTANCEFORMGUID,                               
        APP.CUSTOMERINSTANCEDOCGUID,                                
        APP.GUID,                                                   
        APP.BINDINGINSTANCEDOCGUID,                                 
        APP.AMENDAPPROVALINSTANCEDOCGUID,                           
        APP.HIPOINSTANCEFORMGUID,                                   
        APP.ANALYSISINSTANCEFORMGUID,                               
        APP.ISHIPOOPTIONID,                                         
        APP.ISELIGIBLEOPTIONID,                                     
        APP.PROGRAMCAP,                                             
        APP.PROGRAMCAPID,                                           
        APP.APPLICATIONCAP,                                         
        APP.TKSHAREAMT,                                            
        APP.APPLICATIONCAPUNUTILIZED,                              
        APP.CUSTOMERSHAREAMT,                                       
        APP.TOTALCOSTWVAT,                                          
        APP.STARTON,                                                
        APP.ENDON,                                                  
        APP.MONITORINGDUEDATE,                                      
        APP.SPENDINGPERIODDUEDATE,                                  
        APP.CLAIMINGPERIODDUEDATE,                                  
        APP.DURATION,                                               
        APP.ISACTIVE,                                               
        APP.CREATEDBY,                                              
        APP.CREATEDON,                                              
        APP.UPDATEDBY,                                              
        APP.UPDATEDON,                                             
        APP.SUBMITTEDON,                                            
        APP.APPROVEDON,                                             
        APP.BINDINGINSTANCEDOCGUID_AR,                              
        APP.AMENDAPPINSTANCEDOCGUDI_AR,                             
        APP.ASSESSMENTPROCESSID,                                 
        APP.GRANTCALCINSTANCEFORMGUID,                              
        APP.EVCINSTANCEFORMGUID,                                    
        APP.HASWAGESUPPORTMOLEMPLOYEES,                             
        APP.CALCULATEDECONOMICVALUE,                                
        APP.CALCULATEDGRANTAMOUNT,                                  
        APP.INTERNALINSTANCEDOCGUID,                                
        -- Payment Support / Schedule
        PAYPLAN.APPLICATIONSUPPORTID                               AS APPLICATION_SUPPORT_ID,
        PAYPLAN.ID                                                 AS PAYMENT_SCHEDULE_ID,

        -- Program
        PROGVER.COMMERCIALNAME_EN                                  AS PROGRAM_NAME,

        -- Customer
        CASE
            WHEN CUS.CUSTOMERTYPEID = 'CMP'
                THEN UPPER(TRIM(CUS.NAMEEN))
            ELSE NULL
        END                                                        AS COMMERCIAL_NAME_EN,

        -- Payment Plan
        PAYPLAN.PAYMENTNUMBER                                      AS PAYMENT_NUMBER,
        PAYPLAN.PAYMENTDATE                                        AS PAYMENT_DATE,
        PAYPLAN.BEGININGBALANCE                                    AS BEGINNING_BALANCE,
        PAYPLAN.SCHEDULEDPAYMENT                                   AS SCHEDULED_PAYMENT_BHD,
        PAYPLAN.TOTALPAYMENT                                       AS TOTAL_PAYMENT_BHD,
        PAYPLAN.PROFIT                                             AS PROFIT_BHD,
        PAYPLAN.PRINCIPAL                                          AS PRINCIPAL_BHD,
        PAYPLAN.CUMULATIVEPROFIT                                   AS CUMULATIVE_PROFIT_BHD,
        PAYPLAN.TKPROFITSUBSIDY                                    AS PROFIT_SUBSIDY_BY_TAMKEEN_BHD,
        PAYPLAN.TKPRINCIPALAMT                                     AS PRINCIPAL_AUTO_CALCULATED_BHD,
        PAYPLAN.TKPROFITAMT                                        AS PROFIT_AUTO_CALCULATED_BHD,
        PAYPLAN.PAYMENTPLANSTATUSID                                AS WORKFLOW_STATUS_PAYMENT_PLAN,

        -- Assessment
        ASSESSMENT.ACTIVITY_NAME                                   AS LATEST_ACTIVITY_NAME,
        ASSESSMENT.ASSESSMENT_STATUS_LABEL                         AS ASSESSMENT_STATUS_LABEL,
        ASSESSMENT.AMENDMENT_REQUEST_ID                            AS AMENDMENT_REQUEST_ID,

        -- Standard audit columns
        FALSE                                                      AS IS_DELETED,
        'Neo2'                                                     AS SOURCE_SYSTEM_NAME,
        CURRENT_DATE                                               AS REPORT_DATE,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)   AS DBT_UPDATED_AT

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION             APP
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_PAYMENTPLAN       PAYPLAN
        ON PAYPLAN.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION     PROGVER
        ON PROGVER.ID = APP.PROGRAMVERSIONID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
        ON APPCUS.APPLICATIONID = APP.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE    CUSPROF
        ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER           CUS
        ON CUS.ID = CUSPROF.CUSTOMERID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM            P
        ON P.ID = PROGVER.PROGRAMID
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS APP_STA
        ON APP_STA.CODE = APP.APPLICATIONSTATUSID
    LEFT JOIN TEMP_ASSESSMENT2                                     ASSESSMENT
        ON ASSESSMENT.APPLICATION_ID = APP.ID
       AND ASSESSMENT.RN = 1

    WHERE CUSPROF.PROFILETYPEID = 'ENT'
)


SELECT
    EXTRACT_DATE,
    APPLICATION_ID,
    APPLICATION_NO,
    PROGRAMVERSIONID,
    DECISIONPROGRAMVERSIONID,
    PORTALUSERID,
    CUSTOMERTYPEID,
    BENEFICIARYID,
    APPLICATIONSTATUSID,
    PROFILINGINSTANCEGUID,
    APPLICATIONINSTANCEFORMGUID,
    APPLICATIONINSTANCEDOCGUID,
    CUSTOMERINSTANCEFORMGUID,
    FINDATAINSTANCEFORMGUID,
    CUSTOMERINSTANCEDOCGUID,
    GUID,
    BINDINGINSTANCEDOCGUID,
    AMENDAPPROVALINSTANCEDOCGUID,
    HIPOINSTANCEFORMGUID,
    ANALYSISINSTANCEFORMGUID,
    ISHIPOOPTIONID,
    ISELIGIBLEOPTIONID,
    PROGRAMCAP,
    PROGRAMCAPID,
    APPLICATIONCAP,
    TKSHAREAMT,
    APPLICATIONCAPUNUTILIZED,
    CUSTOMERSHAREAMT,
    TOTALCOSTWVAT,
    STARTON,
    ENDON,
    MONITORINGDUEDATE,
    SPENDINGPERIODDUEDATE,
    CLAIMINGPERIODDUEDATE,
    DURATION,
    ISACTIVE,
    CREATEDBY,
    CREATEDON,
    UPDATEDBY,
    UPDATEDON,
    SUBMITTEDON,
    APPROVEDON,
    BINDINGINSTANCEDOCGUID_AR,
    AMENDAPPINSTANCEDOCGUDI_AR,
    ASSESSMENTPROCESSID,
    GRANTCALCINSTANCEFORMGUID,
    EVCINSTANCEFORMGUID,
    HASWAGESUPPORTMOLEMPLOYEES,
    CALCULATEDECONOMICVALUE,
    CALCULATEDGRANTAMOUNT,
    INTERNALINSTANCEDOCGUID,
    APPLICATION_SUPPORT_ID,
    PAYMENT_SCHEDULE_ID,
    PROGRAM_NAME,
    COMMERCIAL_NAME_EN,
    PAYMENT_NUMBER,
    PAYMENT_DATE,
    BEGINNING_BALANCE,
    SCHEDULED_PAYMENT_BHD,
    TOTAL_PAYMENT_BHD,
    PROFIT_BHD,
    PRINCIPAL_BHD,
    CUMULATIVE_PROFIT_BHD,
    PROFIT_SUBSIDY_BY_TAMKEEN_BHD,
    PRINCIPAL_AUTO_CALCULATED_BHD,
    PROFIT_AUTO_CALCULATED_BHD,
    WORKFLOW_STATUS_PAYMENT_PLAN,
    LATEST_ACTIVITY_NAME,
    ASSESSMENT_STATUS_LABEL,
    AMENDMENT_REQUEST_ID,
    IS_DELETED,
    SOURCE_SYSTEM_NAME,
    REPORT_DATE,
    DBT_UPDATED_AT
FROM FINAL_DATA
