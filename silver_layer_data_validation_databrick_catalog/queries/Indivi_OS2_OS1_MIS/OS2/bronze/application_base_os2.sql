SELECT 
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
        app.calculatedgrantamount                                   AS calculatedgrantamount--,    
        --CURRENT_TIMESTAMP                                           AS dbt_updated_on

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION ProgVer
    ON APP.PROGRAMVERSIONID = ProgVer.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM Program
    ON ProgVer.PROGRAMID = Program.ID

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

WHERE APST.LABEL <> 'Draft'
