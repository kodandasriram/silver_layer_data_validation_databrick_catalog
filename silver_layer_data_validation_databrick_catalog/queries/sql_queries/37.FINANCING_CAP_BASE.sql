WITH bronze_layer AS (
    SELECT
        a.id,
        c1.label AS supporttype,
        a.description,
        a.cap,
        a.startdate,
        a.enddate,
        a.createdby,
        a.updatedby,
        c2.label AS sectorisic,
        a.facilitytypeid,
        a.period,
        a.isactive,
        a.guaranteecap,
        a.createdon,
        a.updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_FINANCINGCAP a
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_SUPPORTTYPE4 c1
        ON c1.code = a.supporttypeid
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISICACTIVITY3 c2
        ON c2.code = a.sectorisicid
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_FACILITYTYPEC3
        ON C3.ID =a.FACILITYTYPEID
),

silver_layer AS (
    SELECT
        id,
        supporttype,
        description,
        cap,
        startdate,
        enddate,
        createdby,
        updatedby,
        sectorisic,
        facilitytypeid,
        period,
        isactive,
        guaranteecap,
        createdon,
        updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".FINANCING_CAP_BASE
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
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 4. PK NULL CHECK
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(supporttype AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(startdate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(sectorisic AS VARCHAR),
        CAST(facilitytypeid AS VARCHAR),
        CAST(period AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(guaranteecap AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(supporttype AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(startdate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(sectorisic AS VARCHAR),
        CAST(facilitytypeid AS VARCHAR),
        CAST(period AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(guaranteecap AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(supporttype AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(startdate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(sectorisic AS VARCHAR),
        CAST(facilitytypeid AS VARCHAR),
        CAST(period AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(guaranteecap AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(supporttype AS VARCHAR),
        CAST(description AS VARCHAR),
        CAST(cap AS VARCHAR),
        CAST(startdate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(createdby AS VARCHAR),
        CAST(updatedby AS VARCHAR),
        CAST(sectorisic AS VARCHAR),
        CAST(facilitytypeid AS VARCHAR),
        CAST(period AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(guaranteecap AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- 7. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.supporttype AS VARCHAR), '') <> COALESCE(CAST(s.supporttype AS VARCHAR), '')
     OR COALESCE(CAST(b.description AS VARCHAR), '') <> COALESCE(CAST(s.description AS VARCHAR), '')
     OR COALESCE(CAST(b.cap AS VARCHAR), '') <> COALESCE(CAST(s.cap AS VARCHAR), '')
     OR COALESCE(CAST(b.startdate AS VARCHAR), '') <> COALESCE(CAST(s.startdate AS VARCHAR), '')
     OR COALESCE(CAST(b.enddate AS VARCHAR), '') <> COALESCE(CAST(s.enddate AS VARCHAR), '')
     OR COALESCE(CAST(b.createdby AS VARCHAR), '') <> COALESCE(CAST(s.createdby AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedby AS VARCHAR), '') <> COALESCE(CAST(s.updatedby AS VARCHAR), '')
     OR COALESCE(CAST(b.sectorisic AS VARCHAR), '') <> COALESCE(CAST(s.sectorisic AS VARCHAR), '')
     OR COALESCE(CAST(b.facilitytypeid AS VARCHAR), '') <> COALESCE(CAST(s.facilitytypeid AS VARCHAR), '')
     OR COALESCE(CAST(b.period AS VARCHAR), '') <> COALESCE(CAST(s.period AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
     OR COALESCE(CAST(b.guaranteecap AS VARCHAR), '') <> COALESCE(CAST(s.guaranteecap AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
) t;