WITH bronze_layer AS (
    SELECT
        applicationsupportid,
        machineryandequipment,
        technology,
        marketingandbranding,
        fixturesandfittings,
        facilitybreakupotheramount,
        facilitybreakupothervalue,
        createdby,        
        updatedby,
        updatedon,
        createdon
        --,bronze_created_on
        --,bronze_updated_on 
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".osusr_2da_financingbreakup 
),

silver_layer AS (
   SELECT 
        applicationsupportid,
        machineryandequipment,
        technology,
        marketingandbranding,
        fixturesandfittings,
        facilitybreakupotheramount,
        facilitybreakupothervalue,
        createdby,
        updatedby,
        updatedon,
        createdon
        --,is_deleted
        --,source_system_name
        --,dbt_updated_at        
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".FINANCING_BREAKUP_BASE
)

-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- Duplicate Bronze
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT applicationsupportid 
    FROM bronze_layer 
    GROUP BY applicationsupportid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- Duplicate Silver
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT applicationsupportid 
    FROM silver_layer 
    GROUP BY applicationsupportid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- ✅ COLUMN MISMATCH (FIXED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.applicationsupportid = s.applicationsupportid
    WHERE
        COALESCE(CAST(b.machineryandequipment AS VARCHAR), '') <> COALESCE(CAST(s.machineryandequipment AS VARCHAR), '')
     OR COALESCE(CAST(b.technology AS VARCHAR), '') <> COALESCE(CAST(s.technology AS VARCHAR), '')
     OR COALESCE(CAST(b.marketingandbranding AS VARCHAR), '') <> COALESCE(CAST(s.marketingandbranding AS VARCHAR), '')
     OR COALESCE(CAST(b.fixturesandfittings AS VARCHAR), '') <> COALESCE(CAST(s.fixturesandfittings AS VARCHAR), '')
     OR COALESCE(CAST(b.facilitybreakupotheramount AS VARCHAR), '') <> COALESCE(CAST(s.facilitybreakupotheramount AS VARCHAR), '')
     OR COALESCE(CAST(b.facilitybreakupothervalue AS VARCHAR), '') <> COALESCE(CAST(s.facilitybreakupothervalue AS VARCHAR), '')
     OR COALESCE(CAST(b.createdby AS VARCHAR), '') <> COALESCE(CAST(s.createdby AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedby AS VARCHAR), '') <> COALESCE(CAST(s.updatedby AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        applicationsupportid,
        CAST(machineryandequipment AS VARCHAR),
        CAST(technology AS VARCHAR),
        CAST(marketingandbranding AS VARCHAR),
        CAST(fixturesandfittings AS VARCHAR),
        CAST(facilitybreakupotheramount AS VARCHAR),
        CAST(facilitybreakupothervalue AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        applicationsupportid,
        CAST(machineryandequipment AS VARCHAR),
        CAST(technology AS VARCHAR),
        CAST(marketingandbranding AS VARCHAR),
        CAST(fixturesandfittings AS VARCHAR),
        CAST(facilitybreakupotheramount AS VARCHAR),
        CAST(facilitybreakupothervalue AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        applicationsupportid,
        CAST(machineryandequipment AS VARCHAR),
        CAST(technology AS VARCHAR),
        CAST(marketingandbranding AS VARCHAR),
        CAST(fixturesandfittings AS VARCHAR),
        CAST(facilitybreakupotheramount AS VARCHAR),
        CAST(facilitybreakupothervalue AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        applicationsupportid,
        CAST(machineryandequipment AS VARCHAR),
        CAST(technology AS VARCHAR),
        CAST(marketingandbranding AS VARCHAR),
        CAST(fixturesandfittings AS VARCHAR),
        CAST(facilitybreakupotheramount AS VARCHAR),
        CAST(facilitybreakupothervalue AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer
) t;