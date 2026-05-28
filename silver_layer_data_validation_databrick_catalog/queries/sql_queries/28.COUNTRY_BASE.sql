WITH bronze_layer AS (
    SELECT
        id,
        countryname,
        alpha2code,
        alpha3code,
        phoneindicator,
        regionid,
        subregionid,
        isindependent,
        isgccmember
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY
),

silver_layer AS (
   SELECT 
        id,
        countryname,
        alpha2code,
        alpha3code,
        phoneindicator,
        regionid,
        subregionid,
        isindependent,
        isgccmember
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".COUNTRY_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT
    'COUNT_VALIDATION' AS validation_type,
    COUNT(*) AS bronze_count,
    (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer

UNION ALL

-- Duplicate Bronze
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

-- Duplicate Silver
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

-- ✅ Column Mismatch Count (CORRECTED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.countryname AS VARCHAR), '') <> COALESCE(CAST(s.countryname AS VARCHAR), '')
     OR COALESCE(CAST(b.alpha2code AS VARCHAR), '') <> COALESCE(CAST(s.alpha2code AS VARCHAR), '')
     OR COALESCE(CAST(b.alpha3code AS VARCHAR), '') <> COALESCE(CAST(s.alpha3code AS VARCHAR), '')
     OR COALESCE(CAST(b.phoneindicator AS VARCHAR), '') <> COALESCE(CAST(s.phoneindicator AS VARCHAR), '')
     OR COALESCE(CAST(b.regionid AS VARCHAR), '') <> COALESCE(CAST(s.regionid AS VARCHAR), '')
     OR COALESCE(CAST(b.subregionid AS VARCHAR), '') <> COALESCE(CAST(s.subregionid AS VARCHAR), '')
     OR COALESCE(CAST(b.isindependent AS VARCHAR), '') <> COALESCE(CAST(s.isindependent AS VARCHAR), '')
     OR COALESCE(CAST(b.isgccmember AS VARCHAR), '') <> COALESCE(CAST(s.isgccmember AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

-- Silver not in Bronze
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;