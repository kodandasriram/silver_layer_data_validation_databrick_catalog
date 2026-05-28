WITH source_data AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY code
               ORDER BY bronze_updated_on DESC, bronze_created_on DESC
           ) AS rn
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_PAYMENTFREQUENCY
),

bronze_layer AS (
    SELECT
        code,
        label,
        CAST("order" AS BIGINT) AS "order",
        isactive,
        CAST(noofmonths AS BIGINT) AS noofmonths,
        'NEO2' AS source_system_name,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS dbt_updated_at
    FROM source_data
    WHERE rn = 1
),

silver_layer AS (
    SELECT
        code,
        label,
        "order",
        isactive,
        noofmonths,
        source_system_name,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".PAYMENT_FREQUENCY_BASE
)

-- =========================
-- VALIDATION
-- =========================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT code FROM bronze_layer GROUP BY code HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT code FROM silver_layer GROUP BY code HAVING COUNT(*) > 1)

UNION ALL

SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE code IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE code IS NULL

UNION ALL

SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT b.code
    FROM bronze_layer b
    JOIN silver_layer s ON b.code = s.code
    WHERE
        COALESCE(b.label, '') <> COALESCE(s.label, '')
     OR COALESCE(CAST(b."order" AS VARCHAR), '') <> COALESCE(CAST(s."order" AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
     OR COALESCE(CAST(b.noofmonths AS VARCHAR), '') <> COALESCE(CAST(s.noofmonths AS VARCHAR), '')
) t

UNION ALL

-- ⚠️ EXCLUDE dbt_updated_at (important)
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT
        code, label, "order", isactive, noofmonths, source_system_name
    FROM bronze_layer

    EXCEPT

    SELECT
        code, label, "order", isactive, noofmonths, source_system_name
    FROM silver_layer
) t

UNION ALL

SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT
        code, label, "order", isactive, noofmonths, source_system_name
    FROM silver_layer

    EXCEPT

    SELECT
        code, label, "order", isactive, noofmonths, source_system_name
    FROM bronze_layer
) t;