WITH bronze_layer AS (
    SELECT
        code,
        label AS assessment_status,
        isterminalstatus,
        customerlabel,
        externalcode,
        assessmentstatusparentcode,
        stagename    
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS
),

silver_layer AS (
    SELECT 
        code,
        assessment_status,
        isterminalstatus,
        customerlabel,
        externalcode,
        assessmentstatusparentcode,
        stagename
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".ASSESSMENT_STATUS_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

-- COUNT VALIDATION
SELECT
    'COUNT_VALIDATION' AS validation_type,
    COUNT(*) AS bronze_count,
    (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer

UNION ALL

-- 🔥 PRIMARY KEY NULL VALIDATION
SELECT
    'NULL_PK_BRONZE',
    COUNT(*),
    NULL
FROM bronze_layer
WHERE code IS NULL

UNION ALL

SELECT
    'NULL_PK_SILVER',
    COUNT(*),
    NULL
FROM silver_layer
WHERE code IS NULL

UNION ALL

-- Duplicate Bronze
SELECT
    'DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT code
    FROM bronze_layer
    GROUP BY code
    HAVING COUNT(*) > 1
)

UNION ALL

-- Duplicate Silver
SELECT
    'DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT code
    FROM silver_layer
    GROUP BY code
    HAVING COUNT(*) > 1
)

UNION ALL

-- Column Mismatch Count
SELECT
    'COLUMN_MISMATCH_COUNT',
    COUNT(*),
    NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
        ON b.code = s.code
    WHERE
        COALESCE(b.assessment_status,'') <> COALESCE(s.assessment_status,'')
     OR COALESCE(CAST(b.isterminalstatus AS VARCHAR),'') <> COALESCE(CAST(s.isterminalstatus AS VARCHAR),'')
     OR COALESCE(b.customerlabel,'') <> COALESCE(s.customerlabel,'')
     OR COALESCE(b.externalcode,'') <> COALESCE(s.externalcode,'')
     OR COALESCE(b.assessmentstatusparentcode,'') <> COALESCE(s.assessmentstatusparentcode,'')
     OR COALESCE(b.stagename,'') <> COALESCE(s.stagename,'')
)

UNION ALL

-- Bronze not in Silver
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
)

UNION ALL

-- Silver not in Bronze
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
);