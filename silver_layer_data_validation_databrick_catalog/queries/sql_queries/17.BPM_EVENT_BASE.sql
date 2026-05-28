WITH bronze_layer AS (
    SELECT
        id,
        espace_id,
        tenant_id,
        activity_id,
        process_def_id,
        entity_record_id,
        enqueue_time,
        dequeue_time,
        error_count,
        next_run 
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ossys_BPM_Event
),

silver_layer AS (
    SELECT 
        id,
        espace_id,
        tenant_id,
        activity_id,
        process_def_id,
        entity_record_id,
        enqueue_time,
        dequeue_time,
        error_count,
        next_run     
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".BPM_EVENT_BASE
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
)

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
        ON b.id = s.id

    WHERE
        COALESCE(CAST(b.espace_id AS VARCHAR), '') <> COALESCE(CAST(s.espace_id AS VARCHAR), '')
     OR COALESCE(CAST(b.tenant_id AS VARCHAR), '') <> COALESCE(CAST(s.tenant_id AS VARCHAR), '')
     OR COALESCE(CAST(b.activity_id AS VARCHAR), '') <> COALESCE(CAST(s.activity_id AS VARCHAR), '')
     OR COALESCE(CAST(b.process_def_id AS VARCHAR), '') <> COALESCE(CAST(s.process_def_id AS VARCHAR), '')
     OR COALESCE(CAST(b.entity_record_id AS VARCHAR), '') <> COALESCE(CAST(s.entity_record_id AS VARCHAR), '')
     OR COALESCE(CAST(b.enqueue_time AS VARCHAR), '') <> COALESCE(CAST(s.enqueue_time AS VARCHAR), '')
     OR COALESCE(CAST(b.dequeue_time AS VARCHAR), '') <> COALESCE(CAST(s.dequeue_time AS VARCHAR), '')
     OR COALESCE(CAST(b.error_count AS VARCHAR), '') <> COALESCE(CAST(s.error_count AS VARCHAR), '')
     OR COALESCE(CAST(b.next_run AS VARCHAR), '') <> COALESCE(CAST(s.next_run AS VARCHAR), '')
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