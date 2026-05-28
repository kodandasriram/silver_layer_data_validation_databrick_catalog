WITH bronze_layer AS (
    SELECT
        t.id,
        t.paymentrequestid,
        t.inspectionrole,
        t.monitoringrole,
        t.monitoring2role,
        t.auditrole,
        t.correctionrole,
        t.approvalrole,
        t.processid,
        CAST(q.id AS VARCHAR) AS team,
        s.label AS assessmentstatus,
        s.isterminalstatus
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_wz3_PaymentAssessment t
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_wz3_PaymentAssessmentStatus s
        ON t.assessmentstatusid = s.code
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM q
        ON t.team = q.id
),

silver_layer AS (
    SELECT
        id,
        paymentrequestid,
        inspectionrole,
        monitoringrole,
        monitoring2role,
        auditrole,
        correctionrole,
        approvalrole,
        processid,
        CAST(team AS VARCHAR) AS team,
        assessmentstatus,
        isterminalstatus
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".PAYMENT_ASSESSMENT_BASE
)

-- =========================================
-- VALIDATION
-- =========================================

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
        COALESCE(CAST(b.paymentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.paymentrequestid AS VARCHAR), '')
     OR COALESCE(CAST(b.team AS VARCHAR), '') <> COALESCE(CAST(s.team AS VARCHAR), '')
     OR COALESCE(CAST(b.assessmentstatus AS VARCHAR), '') <> COALESCE(CAST(s.assessmentstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.isterminalstatus AS VARCHAR), '') <> COALESCE(CAST(s.isterminalstatus AS VARCHAR), '')
) t

UNION ALL

-- BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(paymentrequestid AS VARCHAR),
        CAST(inspectionrole AS VARCHAR),
        CAST(monitoringrole AS VARCHAR),
        CAST(monitoring2role AS VARCHAR),
        CAST(auditrole AS VARCHAR),
        CAST(correctionrole AS VARCHAR),
        CAST(approvalrole AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(assessmentstatus AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(paymentrequestid AS VARCHAR),
        CAST(inspectionrole AS VARCHAR),
        CAST(monitoringrole AS VARCHAR),
        CAST(monitoring2role AS VARCHAR),
        CAST(auditrole AS VARCHAR),
        CAST(correctionrole AS VARCHAR),
        CAST(approvalrole AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(assessmentstatus AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(paymentrequestid AS VARCHAR),
        CAST(inspectionrole AS VARCHAR),
        CAST(monitoringrole AS VARCHAR),
        CAST(monitoring2role AS VARCHAR),
        CAST(auditrole AS VARCHAR),
        CAST(correctionrole AS VARCHAR),
        CAST(approvalrole AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(assessmentstatus AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(paymentrequestid AS VARCHAR),
        CAST(inspectionrole AS VARCHAR),
        CAST(monitoringrole AS VARCHAR),
        CAST(monitoring2role AS VARCHAR),
        CAST(auditrole AS VARCHAR),
        CAST(correctionrole AS VARCHAR),
        CAST(approvalrole AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(team AS VARCHAR),
        CAST(assessmentstatus AS VARCHAR),
        CAST(isterminalstatus AS VARCHAR)
    FROM bronze_layer
) t;