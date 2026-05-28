WITH bronze_layer AS (
    SELECT
        applicationid,
        employercr,
        employername,
        jobcentercontracttypeid AS jobcentercontracttype,
        joiningdate,
        wage,
        createdon,
        updatedon,
        FALSE AS is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_JOBCENTERINDIVIDUALEMPLOYER
),

silver_layer AS (
    SELECT
        applicationid,
        employercr,
        employername,
        jobcentercontracttype,
        joiningdate,
        wage,
        createdon,
        updatedon,
        is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".JOBCENTERINDIVIDUALEMPLOYER_BASE
)

-- =========================
-- VALIDATION
-- =========================

-- 1. COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT applicationid 
    FROM bronze_layer 
    GROUP BY applicationid 
    HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT applicationid 
    FROM silver_layer 
    GROUP BY applicationid 
    HAVING COUNT(*) > 1
) t

UNION ALL

-- 3. PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE applicationid IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE applicationid IS NULL

UNION ALL

-- 4. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(applicationid AS VARCHAR),
        CAST(employercr AS VARCHAR),
        CAST(jobcentercontracttype AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(applicationid AS VARCHAR),
        CAST(employercr AS VARCHAR),
        CAST(jobcentercontracttype AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- 5. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(applicationid AS VARCHAR),
        CAST(employercr AS VARCHAR),
        CAST(jobcentercontracttype AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(applicationid AS VARCHAR),
        CAST(employercr AS VARCHAR),
        CAST(jobcentercontracttype AS VARCHAR)
    FROM bronze_layer
) t

UNION ALL

-- 6. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.applicationid = s.applicationid
    WHERE
        COALESCE(CAST(b.employercr AS VARCHAR),'') <> COALESCE(CAST(s.employercr AS VARCHAR),'')
     OR COALESCE(CAST(b.employername AS VARCHAR),'') <> COALESCE(CAST(s.employername AS VARCHAR),'')
     OR COALESCE(CAST(b.jobcentercontracttype AS VARCHAR),'') <> COALESCE(CAST(s.jobcentercontracttype AS VARCHAR),'')
     OR COALESCE(CAST(b.joiningdate AS VARCHAR),'') <> COALESCE(CAST(s.joiningdate AS VARCHAR),'')
     OR COALESCE(CAST(b.wage AS VARCHAR),'') <> COALESCE(CAST(s.wage AS VARCHAR),'')
) t;