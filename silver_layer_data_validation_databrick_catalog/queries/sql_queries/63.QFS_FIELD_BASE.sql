WITH bronze_layer AS (
    SELECT
        id,
        isactive,
        code,
        datatypeid,
        name,
        description,
        regex,
        defaulttext,
        minimumtextlength,
        maximumtextlength,
        defaultdate,
        minimumdate,
        maximumdate,
        groupseparator,
        decimalseparator,
        decimalplaces,
        defaultnumber,
        minimumnumber,
        maximumnumber,
        expression
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updatedat DESC, createdat DESC
               ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QFS_FIELD
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        isactive,
        code,
        datatypeid,
        name,
        description,
        regex,
        defaulttext,
        minimumtextlength,
        maximumtextlength,
        defaultdate,
        minimumdate,
        maximumdate,
        groupseparator,
        decimalseparator,
        decimalplaces,
        defaultnumber,
        minimumnumber,
        maximumnumber,
        expression
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".QFS_FIELD_BASE
)

-- =========================
-- VALIDATION
-- =========================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT b.id
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
     OR COALESCE(CAST(b.code AS VARCHAR), '') <> COALESCE(CAST(s.code AS VARCHAR), '')
     OR COALESCE(CAST(b.name AS VARCHAR), '') <> COALESCE(CAST(s.name AS VARCHAR), '')
     OR COALESCE(CAST(b.description AS VARCHAR), '') <> COALESCE(CAST(s.description AS VARCHAR), '')
     OR COALESCE(CAST(b.datatypeid AS VARCHAR), '') <> COALESCE(CAST(s.datatypeid AS VARCHAR), '')
     OR COALESCE(CAST(b.decimalplaces AS VARCHAR), '') <> COALESCE(CAST(s.decimalplaces AS VARCHAR), '')
) t

UNION ALL

SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer
    EXCEPT
    SELECT *
    FROM silver_layer
) t

UNION ALL

SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT *
    FROM silver_layer
    EXCEPT
    SELECT *
    FROM bronze_layer
) t;