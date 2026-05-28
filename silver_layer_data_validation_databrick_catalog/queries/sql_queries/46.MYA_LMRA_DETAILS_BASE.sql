WITH bronze_layer AS (
	    SELECT
	        id,
	        applicationid,
	        paymentrequestid,
	        eligibilitycriteriarequestty,
	        code,
	        totalbahrainidisableworkers,
	        issubjecttobahrainization,
	        bahrainizationtargetpct,
	        bahrainizationcurrentpct,
	        bahrainizationratediffpct,
	        noofinvestors,
	        hwtoworks,
	        activeworkers,
	        parallelexpats,
	        inprogressrequests,
	        totalnoofnonbahraniworks,
	        sitevisitmonitoringid,
	        noofnonbahrainiparallel,
	        amendmentrequestid,
	        createdon,
	        updatedon,
	        FALSE AS is_deleted
	    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_LMRA_DETAILS
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        code,
        totalbahrainidisableworkers,
        issubjecttobahrainization,
        bahrainizationtargetpct,
        bahrainizationcurrentpct,
        bahrainizationratediffpct,
        noofinvestors,
        hwtoworks,
        activeworkers,
        parallelexpats,
        inprogressrequests,
        totalnoofnonbahraniworks,
        sitevisitmonitoringid,
        noofnonbahrainiparallel,
        amendmentrequestid,
        createdon,
        updatedon,
        is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".MYA_LMRA_DETAILS_BASE
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
    SELECT id 
    FROM bronze_layer 
    GROUP BY id 
    HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id 
    FROM silver_layer 
    GROUP BY id 
    HAVING COUNT(*) > 1
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
        CAST(applicationid AS VARCHAR),
        CAST(code AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(code AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- 5. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(code AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(code AS VARCHAR)
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
        COALESCE(CAST(b.applicationid AS VARCHAR),'') <> COALESCE(CAST(s.applicationid AS VARCHAR),'')
     OR COALESCE(CAST(b.paymentrequestid AS VARCHAR),'') <> COALESCE(CAST(s.paymentrequestid AS VARCHAR),'')
     OR COALESCE(CAST(b.code AS VARCHAR),'') <> COALESCE(CAST(s.code AS VARCHAR),'')
     OR COALESCE(CAST(b.totalbahrainidisableworkers AS VARCHAR),'') <> COALESCE(CAST(s.totalbahrainidisableworkers AS VARCHAR),'')
     OR COALESCE(CAST(b.issubjecttobahrainization AS VARCHAR),'') <> COALESCE(CAST(s.issubjecttobahrainization AS VARCHAR),'')
     OR COALESCE(CAST(b.bahrainizationtargetpct AS VARCHAR),'') <> COALESCE(CAST(s.bahrainizationtargetpct AS VARCHAR),'')
     OR COALESCE(CAST(b.bahrainizationcurrentpct AS VARCHAR),'') <> COALESCE(CAST(s.bahrainizationcurrentpct AS VARCHAR),'')
     OR COALESCE(CAST(b.bahrainizationratediffpct AS VARCHAR),'') <> COALESCE(CAST(s.bahrainizationratediffpct AS VARCHAR),'')
     OR COALESCE(CAST(b.noofinvestors AS VARCHAR),'') <> COALESCE(CAST(s.noofinvestors AS VARCHAR),'')
     OR COALESCE(CAST(b.hwtoworks AS VARCHAR),'') <> COALESCE(CAST(s.hwtoworks AS VARCHAR),'')
     OR COALESCE(CAST(b.activeworkers AS VARCHAR),'') <> COALESCE(CAST(s.activeworkers AS VARCHAR),'')
     OR COALESCE(CAST(b.parallelexpats AS VARCHAR),'') <> COALESCE(CAST(s.parallelexpats AS VARCHAR),'')
     OR COALESCE(CAST(b.inprogressrequests AS VARCHAR),'') <> COALESCE(CAST(s.inprogressrequests AS VARCHAR),'')
     OR COALESCE(CAST(b.totalnoofnonbahraniworks AS VARCHAR),'') <> COALESCE(CAST(s.totalnoofnonbahraniworks AS VARCHAR),'')
     OR COALESCE(CAST(b.sitevisitmonitoringid AS VARCHAR),'') <> COALESCE(CAST(s.sitevisitmonitoringid AS VARCHAR),'')
     OR COALESCE(CAST(b.noofnonbahrainiparallel AS VARCHAR),'') <> COALESCE(CAST(s.noofnonbahrainiparallel AS VARCHAR),'')
     OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR),'') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR),'')
) t;