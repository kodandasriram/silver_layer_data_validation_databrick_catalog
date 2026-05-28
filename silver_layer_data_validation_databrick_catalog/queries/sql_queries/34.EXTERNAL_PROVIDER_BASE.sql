--Note table OSUSR_MM5_CURRENCY name changes dynamically
WITH bronze_layer AS (
    SELECT
        ext.id,
        ext.name,
        country.countryname AS countryname,
        name.nameen as currencyname_en,
        name.namear as currencyname_ar,
        ext.exchangerate,
        ext.mobilecountryprefix,
        ext.contactnumber,
        ext.taxnumber,
        ext.email,
        ext.onlinepresence,
        ext.isactive,
        ext.createdby,
        ext.createdon,
        ext.updatedby,
        ext.updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EXTERNALPROVIDER ext
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY country 
        ON country.id = ext.countryid
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_CURRENCY4 name
        ON name.isocode = ext.currencyid
),

silver_layer AS (
   SELECT 
        id,
        name,
        countryname,
        currencyname_en,
        currencyname_ar,
        exchangerate,
        mobilecountryprefix,
        contactnumber,
        taxnumber,
        email,
        onlinepresence,
        isactive,
        createdby,
        createdon,
        updatedby,
        updatedon
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".EXTERNAL_PROVIDER_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

-- ✅ COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- ✅ PRIMARY KEY NULL VALIDATION
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- DUPLICATE Bronze
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1) t

UNION ALL

-- DUPLICATE Silver
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1) t

UNION ALL

-- ✅ COLUMN MISMATCH (UPDATED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.name AS VARCHAR), '') <> COALESCE(CAST(s.name AS VARCHAR), '')
     OR COALESCE(CAST(b.countryname AS VARCHAR), '') <> COALESCE(CAST(s.countryname AS VARCHAR), '')
     OR COALESCE(CAST(b.currencyname_en AS VARCHAR), '') <> COALESCE(CAST(s.currencyname_en AS VARCHAR), '')
     OR COALESCE(CAST(b.currencyname_ar AS VARCHAR), '') <> COALESCE(CAST(s.currencyname_ar AS VARCHAR), '')
     OR COALESCE(CAST(b.exchangerate AS VARCHAR), '') <> COALESCE(CAST(s.exchangerate AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilecountryprefix AS VARCHAR), '') <> COALESCE(CAST(s.mobilecountryprefix AS VARCHAR), '')
     OR COALESCE(CAST(b.contactnumber AS VARCHAR), '') <> COALESCE(CAST(s.contactnumber AS VARCHAR), '')
     OR COALESCE(CAST(b.taxnumber AS VARCHAR), '') <> COALESCE(CAST(s.taxnumber AS VARCHAR), '')
     OR COALESCE(CAST(b.email AS VARCHAR), '') <> COALESCE(CAST(s.email AS VARCHAR), '')
     OR COALESCE(CAST(b.onlinepresence AS VARCHAR), '') <> COALESCE(CAST(s.onlinepresence AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
     OR COALESCE(CAST(b.createdby AS VARCHAR), '') <> COALESCE(CAST(s.createdby AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedby AS VARCHAR), '') <> COALESCE(CAST(s.updatedby AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER (UPDATED)
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        name,
        countryname,
        currencyname_en,
        currencyname_ar,
        exchangerate,
        mobilecountryprefix,
        contactnumber,
        taxnumber,
        email,
        onlinepresence,
        isactive,
        createdby,
        CAST(createdon AS VARCHAR),
        updatedby,
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        name,
        countryname,
        currencyname_en,
        currencyname_ar,
        exchangerate,
        mobilecountryprefix,
        contactnumber,
        taxnumber,
        email,
        onlinepresence,
        isactive,
        createdby,
        CAST(createdon AS VARCHAR),
        updatedby,
        CAST(updatedon AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE (UPDATED)
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        name,
        countryname,
        currencyname_en,
        currencyname_ar,
        exchangerate,
        mobilecountryprefix,
        contactnumber,
        taxnumber,
        email,
        onlinepresence,
        isactive,
        createdby,
        CAST(createdon AS VARCHAR),
        updatedby,
        CAST(updatedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        name,
        countryname,
        currencyname_en,
        currencyname_ar,
        exchangerate,
        mobilecountryprefix,
        contactnumber,
        taxnumber,
        email,
        onlinepresence,
        isactive,
        createdby,
        CAST(createdon AS VARCHAR),
        updatedby,
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer
) t;