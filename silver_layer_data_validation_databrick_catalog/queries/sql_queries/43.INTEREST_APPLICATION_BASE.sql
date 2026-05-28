WITH source_data AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatedon DESC, createdon DESC) AS rn
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_INTERESTAPPLICANTSIODETAILS4
),

bronze_layer AS (
    SELECT
        a.id,
        a.qualification,
        a.employercr,
        a.currentjob,
        a.currentsalary,
        a.isemployed,
        a.employername,
        a.joindate,
        b.programid,
        b.portaluserid,
        FALSE AS is_deleted,
        a.updatedon,
        a.createdon
    FROM source_data a
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_INTERESTAPPLICATION5 b
        ON CAST(a.id AS VARCHAR) = CAST(b.id AS VARCHAR)
    WHERE a.rn = 1
),

silver_layer AS (
    SELECT
        id,
        qualification,
        employercr,
        currentjob,
        currentsalary,
        isemployed,
        employername,
        joindate,
        programid,
        portaluserid,
        is_deleted,
        updatedon,
        createdon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".INTEREST_APPLICATION_BASE
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
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

-- 3. PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- 4. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(qualification AS VARCHAR),
        CAST(programid AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(qualification AS VARCHAR),
        CAST(programid AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- 5. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(qualification AS VARCHAR),
        CAST(programid AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(qualification AS VARCHAR),
        CAST(programid AS VARCHAR)
    FROM bronze_layer
) t

UNION ALL

-- 6. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.qualification AS VARCHAR),'') <> COALESCE(CAST(s.qualification AS VARCHAR),'')
     OR COALESCE(CAST(b.employercr AS VARCHAR),'') <> COALESCE(CAST(s.employercr AS VARCHAR),'')
     OR COALESCE(CAST(b.currentjob AS VARCHAR),'') <> COALESCE(CAST(s.currentjob AS VARCHAR),'')
     OR COALESCE(CAST(b.currentsalary AS VARCHAR),'') <> COALESCE(CAST(s.currentsalary AS VARCHAR),'')
     OR COALESCE(CAST(b.isemployed AS VARCHAR),'') <> COALESCE(CAST(s.isemployed AS VARCHAR),'')
     OR COALESCE(CAST(b.programid AS VARCHAR),'') <> COALESCE(CAST(s.programid AS VARCHAR),'')
     OR COALESCE(CAST(b.portaluserid AS VARCHAR),'') <> COALESCE(CAST(s.portaluserid AS VARCHAR),'')
) t;