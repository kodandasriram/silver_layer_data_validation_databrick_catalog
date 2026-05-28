WITH bronze_layer AS (
    SELECT
        app.ID,
        app.APPLICATIONID,
        app.CPRNUMBER,
        trim(supp.name) AS supportareaeligibility,
        app.DECODEDEXPRESSION,
        app.ISSUCCESSFULEVALUATION,
        app.EVALUATIONOUTPUTMESSAGE,
        app.EVALUATIONRESULT,
        app.AMENDMENTREQUESTID,
        app.ISACTIVE
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTELIGIBILITYCRITERIA app
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SUPPORTAREAELIGIBILITYCRITERIA supp
        ON supp.id = app.supportareaeligibilityid
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        cprnumber,
        supportareaeligibility as supportareaeligibility,
        decodedexpression,
        issuccessfulevaluation,
        evaluationoutputmessage,
        evaluationresult,
        amendmentrequestid,
        isactive
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATIONSUPPORTELIGIBILITYCRITERIA_BASE
)

-- =========================================
-- 1. COUNT VALIDATION
-- =========================================
SELECT
    'COUNT_VALIDATION' AS validation_type,
    COUNT(*) AS bronze_count,
    (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer

UNION ALL

-- =========================================
-- 🔥 1.1 PRIMARY KEY NULL VALIDATION
-- =========================================
SELECT
    'NULL_PK_BRONZE',
    COUNT(*),
    NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT
    'NULL_PK_SILVER',
    COUNT(*),
    NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

-- =========================================
-- 2. DUPLICATE CHECK (based on PK)
-- =========================================
SELECT
    'DUPLICATE_CHECK_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'DUPLICATE_CHECK_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
)

UNION ALL

-- =========================================
-- 3. COLUMN-LEVEL VALIDATION (Mismatch count)
-- =========================================
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
    COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
    OR COALESCE(CAST(b.cprnumber AS VARCHAR), '') <> COALESCE(CAST(s.cprnumber AS VARCHAR), '')
    OR COALESCE(CAST(b.supportareaeligibility AS VARCHAR), '') <> COALESCE(CAST(s.supportareaeligibility AS VARCHAR), '')
    OR COALESCE(CAST(b.decodedexpression AS VARCHAR), '') <> COALESCE(CAST(s.decodedexpression AS VARCHAR), '')
    OR COALESCE(CAST(b.issuccessfulevaluation AS VARCHAR), '') <> COALESCE(CAST(s.issuccessfulevaluation AS VARCHAR), '')
    OR COALESCE(CAST(b.evaluationoutputmessage AS VARCHAR), '') <> COALESCE(CAST(s.evaluationoutputmessage AS VARCHAR), '')
    OR COALESCE(CAST(b.evaluationresult AS VARCHAR), '') <> COALESCE(CAST(s.evaluationresult AS VARCHAR), '')
    OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR), '')
    OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
)

UNION ALL

-- =========================================
-- 4. DATA MISSING VALIDATION
-- =========================================
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

SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
);