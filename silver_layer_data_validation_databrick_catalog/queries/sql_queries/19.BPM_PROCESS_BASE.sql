WITH bronze_layer AS (
    SELECT
        bpmproc.tenant_id,
        bpmproc.id,
        bpmproc.label as process_label,
        bpmproc.created,
        bpmproc.created_by,
        bpmproc.process_def_id,
        bpmproc.parent_process_id,
        bpmproc.parent_activity_id,
        bpmproc.top_process_id,
        bpmproc.status_id,
        bpmproc.last_modified,
        bpmproc.last_modified_by,
        bpmproc.suspended_date,
        bpmproc.suspended_by
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ossys_BPM_Process bpmproc
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ossys_BPM_Process_Definition bpmprocdef
        ON bpmprocdef.id = bpmproc.process_def_id
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ossys_BPM_Process_Status bpmprocstatus
        ON bpmprocstatus.id = bpmproc.status_id
),

silver_layer AS (
    SELECT 
        tenant_id,
        id,
        process_label,
        created,
        created_by,
        process_def_id,
        parent_process_id,
        parent_activity_id,
        top_process_id,
        status,   -- mapped from status_id
        last_modified,
        last_modified_by,
        suspended_date,
        suspended_by
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".BPM_PROCESS_BASE
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

-- Column Mismatch Count (FIXED)
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
        COALESCE(CAST(b.tenant_id AS VARCHAR), '') <> COALESCE(CAST(s.tenant_id AS VARCHAR), '')
     OR COALESCE(CAST(b.process_label AS VARCHAR), '') <> COALESCE(CAST(s.process_label AS VARCHAR), '')
     OR COALESCE(CAST(b.created AS VARCHAR), '') <> COALESCE(CAST(s.created AS VARCHAR), '')
     OR COALESCE(CAST(b.created_by AS VARCHAR), '') <> COALESCE(CAST(s.created_by AS VARCHAR), '')
     OR COALESCE(CAST(b.process_def_id AS VARCHAR), '') <> COALESCE(CAST(s.process_def_id AS VARCHAR), '')
     OR COALESCE(CAST(b.parent_process_id AS VARCHAR), '') <> COALESCE(CAST(s.parent_process_id AS VARCHAR), '')
     OR COALESCE(CAST(b.parent_activity_id AS VARCHAR), '') <> COALESCE(CAST(s.parent_activity_id AS VARCHAR), '')
     OR COALESCE(CAST(b.top_process_id AS VARCHAR), '') <> COALESCE(CAST(s.top_process_id AS VARCHAR), '')
     OR COALESCE(CAST(b.status_id AS VARCHAR), '') <> COALESCE(CAST(s.status AS VARCHAR), '')  -- important mapping
     OR COALESCE(CAST(b.last_modified AS VARCHAR), '') <> COALESCE(CAST(s.last_modified AS VARCHAR), '')
     OR COALESCE(CAST(b.last_modified_by AS VARCHAR), '') <> COALESCE(CAST(s.last_modified_by AS VARCHAR), '')
     OR COALESCE(CAST(b.suspended_date AS VARCHAR), '') <> COALESCE(CAST(s.suspended_date AS VARCHAR), '')
     OR COALESCE(CAST(b.suspended_by AS VARCHAR), '') <> COALESCE(CAST(s.suspended_by AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT 
        tenant_id, id, process_label, created, created_by, process_def_id,
        parent_process_id, parent_activity_id, top_process_id,
        status, last_modified, last_modified_by, suspended_date, suspended_by
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
        tenant_id, id, process_label, created, created_by, process_def_id,
        parent_process_id, parent_activity_id, top_process_id,
        status, last_modified, last_modified_by, suspended_date, suspended_by
    FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;