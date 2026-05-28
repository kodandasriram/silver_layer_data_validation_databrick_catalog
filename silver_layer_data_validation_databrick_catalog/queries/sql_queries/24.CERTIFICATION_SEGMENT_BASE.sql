WITH bronze_layer AS (
    SELECT
        r9t.id,
        r9t.trainingprogramid,
        qq.label AS applicantsegment   -- converted to label
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_R9T_CERTIFICATIONSEGMENT3 r9t
    JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_APPLICANTSEGMENT qq
        ON qq.code = r9t.applicantsegmentid
),

silver_layer AS (
    SELECT 
        id,
        trainingprogramid,
        applicantsegment
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".CERTIFICATION_SEGMENT_BASE
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
SELECT
    'DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- Duplicate Silver
SELECT
    'DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- ✅ Column Mismatch Count (FIXED)
SELECT
    'COLUMN_MISMATCH_COUNT',
    COUNT(*),
    NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
        ON b.id = s.id

    WHERE
        COALESCE(CAST(b.trainingprogramid AS VARCHAR), '') 
            <> COALESCE(CAST(s.trainingprogramid AS VARCHAR), '')
     OR COALESCE(CAST(b.applicantsegment AS VARCHAR), '') 
            <> COALESCE(CAST(s.applicantsegment AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT 
        id,
        trainingprogramid,
        applicantsegment
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        trainingprogramid,
        applicantsegment
    FROM silver_layer
) t

UNION ALL

-- Silver not in Bronze
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT 
        id,
        trainingprogramid,
        applicantsegment
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        trainingprogramid,
        applicantsegment
    FROM bronze_layer
) t;