WITH bronze_layer AS (
    SELECT
        id,
        applicationid,
        customerprofileid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        customerprofileid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_CUSTOMER_BASE
)

-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- Duplicate Bronze
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id 
    FROM bronze_layer 
    GROUP BY id 
    HAVING COUNT(*) > 1
)

UNION ALL

-- Duplicate Silver
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id 
    FROM silver_layer 
    GROUP BY id 
    HAVING COUNT(*) > 1
)

UNION ALL

-- ✅ COLUMN MISMATCH (FIXED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.customerprofileid AS VARCHAR), '') <> COALESCE(CAST(s.customerprofileid AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(customerprofileid AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(customerprofileid AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(customerprofileid AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(customerprofileid AS VARCHAR)
    FROM bronze_layer
) t;