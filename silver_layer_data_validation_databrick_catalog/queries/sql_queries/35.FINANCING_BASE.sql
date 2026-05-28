--Note make sure table OSUSR_MM5_YESNOOPTION change dynamicaly

WITH bronze_layer AS (
    SELECT
        fina.id,
        financingamountrequested,
        tenor,
        graceperiod,
        factype.label as facilitytype,  	--facilitytypeid, --OSUSR_h95_FacilityType(ID)
        yesno.label as revolvingloan, 	--revolvingloan, --OSUSR_398_YESNOOPTION (ID)
        availabilityperiod,
        distype.label as disbursementtype, --disbursementtypeid, --OSUSR_2DA_DISBURSEMENTTYPE(ID)
        ptype.label as financingproducttype, --financingproducttypeid, --OSUSR_2da_FinancingProductType(ID)
        financingproducttypeother,
        pay.label as paymentfrequency, --paymentfrequencyid, --OSUSR_H95_PAYMENTFREQUENCY(CODE)
        bankapprovaldate,
        facil.label as facilitieswithbank, 			--OSUSR_MM5_YESNOOPTION(ID)
        bank.label as workingcapitalfacilitieswith, 	--OSUSR_MM5_YESNOOPTION(ID)
        internalriskrating,
        securitycoverage,
        cashconversioncycle,
        debtservicecoverageratio,
        machineryandequipment,
        technology,
        marketingandbranding,
        workingcapital,
        fixturesandfittings,
        facilitybreakupotheramount,
        facilitybreakupothervalue,
        commentworkcapital,
        commentrevolvingloan,
        fixedassetsamtrequested,
        fixedassestsamtremaining,
        workingcapitalamtrequested,
        workingcapitalamtremaining,
        commentworkingcapital,
        financingamtrequested,
        financingamtremaining,
        workingcapitalcapid, --OSUSR_2DA_FINANCINGCAP(ID)
        workingcapitalremaningcap,
        financingamtcapid,--OSUSR_2DA_FINANCINGCAP(ID)
        financingamtremaningcap,
        isanyfieldupdatedforamendmen,
        fina.isactive,
        createdby,
        createdon,
        updatedby,
        updatedon
    FROM 	dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".osusr_2da_financing as fina 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".osusr_h95_facilitytype as factype 
        ON factype.ID = fina.facilitytypeid 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_YESNOOPTION as yesno 
        ON yesno.id = fina.revolvingloan 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_DISBURSEMENTTYPE as distype
        ON distype.id = fina.disbursementtypeid  
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2da_FinancingProductType as ptype 
        ON ptype.id = fina.financingproducttypeid 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_PAYMENTFREQUENCY as pay
        ON pay.code = fina.paymentfrequencyid
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_YESNOOPTION4 as facil
        ON facil.ID = fina.facilitieswithbank
        left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_YESNOOPTION4 as bank
        ON bank.ID = fina.workingcapitalfacilitieswith
    
),

silver_layer AS (
   SELECT 
        id,
        financingamountrequested,
        tenor,
        graceperiod,
        facilitytype,
        revolvingloan,
        availabilityperiod,
        disbursementtype,
        financingproducttype,
        financingproducttypeother,
        paymentfrequency,
        bankapprovaldate,
        facilitieswithbank,
        workingcapitalfacilitieswith,
        internalriskrating,
        securitycoverage,
        cashconversioncycle,
        debtservicecoverageratio,
        machineryandequipment,
        technology,
        marketingandbranding,
        workingcapital,
        fixturesandfittings,
        facilitybreakupotheramount,
        facilitybreakupothervalue,
        commentworkcapital,
        commentrevolvingloan,
        fixedassetsamtrequested,
        fixedassestsamtremaining,
        workingcapitalamtrequested,
        workingcapitalamtremaining,
        commentworkingcapital,
        financingamtrequested,
        financingamtremaining,
        workingcapitalcapid,
        workingcapitalremaningcap,
        financingamtcapid,
        financingamtremaningcap,
        isanyfieldupdatedforamendmen,
        isactive,
        createdby,
        createdon,
        updatedby,
        updatedon
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".FINANCING_BASE
)

-- =========================================
-- =========================================
-- COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 🔥 PRIMARY KEY NULL VALIDATION
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- DUPLICATE CHECK
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

-- ✅ COLUMN MISMATCH (IMPROVED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.financingamountrequested AS VARCHAR), '') <> COALESCE(CAST(s.financingamountrequested AS VARCHAR), '')
     OR COALESCE(CAST(b.tenor AS VARCHAR), '') <> COALESCE(CAST(s.tenor AS VARCHAR), '')
     OR COALESCE(CAST(b.graceperiod AS VARCHAR), '') <> COALESCE(CAST(s.graceperiod AS VARCHAR), '')
     OR COALESCE(CAST(b.facilitytype AS VARCHAR), '') <> COALESCE(CAST(s.facilitytype AS VARCHAR), '')
     OR COALESCE(CAST(b.revolvingloan AS VARCHAR), '') <> COALESCE(CAST(s.revolvingloan AS VARCHAR), '')
     OR COALESCE(CAST(b.availabilityperiod AS VARCHAR), '') <> COALESCE(CAST(s.availabilityperiod AS VARCHAR), '')
     OR COALESCE(CAST(b.disbursementtype AS VARCHAR), '') <> COALESCE(CAST(s.disbursementtype AS VARCHAR), '')
     OR COALESCE(CAST(b.financingproducttype AS VARCHAR), '') <> COALESCE(CAST(s.financingproducttype AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentfrequency AS VARCHAR), '') <> COALESCE(CAST(s.paymentfrequency AS VARCHAR), '')
     OR COALESCE(CAST(b.bankapprovaldate AS VARCHAR), '') <> COALESCE(CAST(s.bankapprovaldate AS VARCHAR), '')
     OR COALESCE(CAST(b.facilitieswithbank AS VARCHAR), '') <> COALESCE(CAST(s.facilitieswithbank AS VARCHAR), '')
     OR COALESCE(CAST(b.workingcapitalfacilitieswith AS VARCHAR), '') <> COALESCE(CAST(s.workingcapitalfacilitieswith AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
) t