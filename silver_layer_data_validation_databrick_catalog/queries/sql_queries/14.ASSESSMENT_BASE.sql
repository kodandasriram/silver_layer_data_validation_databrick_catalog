WITH bronze_layer AS (
    select
    	ass.id,
		ass.applicationid,
		ass.amendmentrequestid,
		ass.assessmentrole1,
		ass.assessmentrole2,
		ass.reviewrole,
		ass.approverole,
		ass.processid,
		team1.name AS assessmentteam1_name,
		team2.name AS assessmentteam2_name,
		team3.name AS reviewteam1_name,
		team4.name AS approveteam1_name,
		assstatus.label AS assessmentstatusid,
		team5.name AS assessmentteammol_name,
		ass.reviewrole1,
		ass.reviewrole2,
		ass.reviewteam2,
		ass.monitoringrole1,
		ass.monitoringrole2,
		ass.monitoringteam1,
		ass.monitoringteam2
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ass

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team1
        ON team1.id = ass.assessmentteam1

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team2
        ON team2.id = ass.assessmentteam2

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team3
        ON team3.id = ass.reviewteam1

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team4
        ON team4.id = ass.approveteam1

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team5
        ON team5.id = ass.assessmentteammol 

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS assstatus
        ON assstatus.code = ass.assessmentstatusid
),

silver_layer AS (
    SELECT 
        id,
        applicationid,
        amendmentrequestid,
        assessmentrole1,
        assessmentrole2,
        reviewrole,
        approverole,
        processid,
        assessmentteam1_name,
        assessmentteam2_name,
        reviewteam1_name,
        approveteam1_name,
        assessmentstatusid,
        assessmentteammol_name,
        reviewrole1,
        reviewrole2,
        reviewteam2,
        monitoringrole1,
        monitoringrole2,
        monitoringteam1,
        monitoringteam2
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".ASSESSMENT_BASE
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
-- 🔥 PRIMARY KEY NULL VALIDATION
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
-- 2. DUPLICATE CHECK
-- =========================================
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

-- =========================================
-- 3. COLUMN MISMATCH COUNT
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
        OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR), '')
        OR COALESCE(CAST(b.assessmentrole1 AS VARCHAR), '') <> COALESCE(CAST(s.assessmentrole1 AS VARCHAR), '')
        OR COALESCE(CAST(b.assessmentrole2 AS VARCHAR), '') <> COALESCE(CAST(s.assessmentrole2 AS VARCHAR), '')
        OR COALESCE(CAST(b.reviewrole AS VARCHAR), '') <> COALESCE(CAST(s.reviewrole AS VARCHAR), '')
        OR COALESCE(CAST(b.approverole AS VARCHAR), '') <> COALESCE(CAST(s.approverole AS VARCHAR), '')
        OR COALESCE(CAST(b.processid AS VARCHAR), '') <> COALESCE(CAST(s.processid AS VARCHAR), '')
        OR COALESCE(CAST(b.assessmentteam1_name AS VARCHAR), '') <> COALESCE(CAST(s.assessmentteam1_name AS VARCHAR), '')
        OR COALESCE(CAST(b.assessmentteam2_name AS VARCHAR), '') <> COALESCE(CAST(s.assessmentteam2_name AS VARCHAR), '')
        OR COALESCE(CAST(b.reviewteam1_name AS VARCHAR), '') <> COALESCE(CAST(s.reviewteam1_name AS VARCHAR), '')
        OR COALESCE(CAST(b.approveteam1_name AS VARCHAR), '') <> COALESCE(CAST(s.approveteam1_name AS VARCHAR), '')
        OR COALESCE(CAST(b.assessmentstatusid AS VARCHAR), '') <> COALESCE(CAST(s.assessmentstatusid AS VARCHAR), '')
        OR COALESCE(CAST(b.assessmentteammol_name AS VARCHAR), '') <> COALESCE(CAST(s.assessmentteammol_name AS VARCHAR), '')
        OR COALESCE(CAST(b.reviewrole1 AS VARCHAR), '') <> COALESCE(CAST(s.reviewrole1 AS VARCHAR), '')
        OR COALESCE(CAST(b.reviewrole2 AS VARCHAR), '') <> COALESCE(CAST(s.reviewrole2 AS VARCHAR), '')
        OR COALESCE(CAST(b.reviewteam2 AS VARCHAR), '') <> COALESCE(CAST(s.reviewteam2 AS VARCHAR), '')
        OR COALESCE(CAST(b.monitoringrole1 AS VARCHAR), '') <> COALESCE(CAST(s.monitoringrole1 AS VARCHAR), '')
        OR COALESCE(CAST(b.monitoringrole2 AS VARCHAR), '') <> COALESCE(CAST(s.monitoringrole2 AS VARCHAR), '')
        OR COALESCE(CAST(b.monitoringteam1 AS VARCHAR), '') <> COALESCE(CAST(s.monitoringteam1 AS VARCHAR), '')
        OR COALESCE(CAST(b.monitoringteam2 AS VARCHAR), '') <> COALESCE(CAST(s.monitoringteam2 AS VARCHAR), '')
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