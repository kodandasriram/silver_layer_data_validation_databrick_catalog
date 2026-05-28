WITH bronze_layer AS (
    SELECT
        paymentsupportid,
        trainingprogramid,
        CAST(paymentitemstatusid AS VARCHAR) AS paymentitemstatus,
        CAST(amountpaidwithvat AS DECIMAL(38,8)) AS amountpaidwithvat,
        CAST(exchangerate AS DECIMAL(38,8)) AS exchangerate,
        CAST(vatpercentage AS DECIMAL(38,8)) AS vatpercentage,
        CAST(date AS TIMESTAMP(6)) AS training_date,
        'BRONZE' AS source_system_name,
        CAST(NULL AS TIMESTAMP(6)) AS dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTTRAINING
),

silver_layer AS (
    SELECT
        paymentsupportid,
        trainingprogramid,
        paymentitemstatus,
        amountpaidwithvat,
        exchangerate,
        vatpercentage,
        training_date,
        source_system_name,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".payment_training_base
)

-- =========================================
-- VALIDATION
-- =========================================

-- 1. COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATE BRONZE
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT paymentsupportid 
    FROM bronze_layer 
    GROUP BY paymentsupportid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT paymentsupportid 
    FROM silver_layer 
    GROUP BY paymentsupportid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- 4. PK NULL CHECK
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE paymentsupportid IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE paymentsupportid IS NULL

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(paymentitemstatus AS VARCHAR),
        CAST(amountpaidwithvat AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(vatpercentage AS VARCHAR),
        CAST(training_date AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(paymentitemstatus AS VARCHAR),
        CAST(amountpaidwithvat AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(vatpercentage AS VARCHAR),
        CAST(training_date AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(paymentitemstatus AS VARCHAR),
        CAST(amountpaidwithvat AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(vatpercentage AS VARCHAR),
        CAST(training_date AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(paymentsupportid AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(paymentitemstatus AS VARCHAR),
        CAST(amountpaidwithvat AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(vatpercentage AS VARCHAR),
        CAST(training_date AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- 7. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.paymentsupportid = s.paymentsupportid
    WHERE
        COALESCE(CAST(b.trainingprogramid AS VARCHAR), '') <> COALESCE(CAST(s.trainingprogramid AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentitemstatus AS VARCHAR), '') <> COALESCE(CAST(s.paymentitemstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.amountpaidwithvat AS VARCHAR), '') <> COALESCE(CAST(s.amountpaidwithvat AS VARCHAR), '')
     OR COALESCE(CAST(b.exchangerate AS VARCHAR), '') <> COALESCE(CAST(s.exchangerate AS VARCHAR), '')
     OR COALESCE(CAST(b.vatpercentage AS VARCHAR), '') <> COALESCE(CAST(s.vatpercentage AS VARCHAR), '')
     OR COALESCE(CAST(b.training_date AS VARCHAR), '') <> COALESCE(CAST(s.training_date AS VARCHAR), '')
) t;