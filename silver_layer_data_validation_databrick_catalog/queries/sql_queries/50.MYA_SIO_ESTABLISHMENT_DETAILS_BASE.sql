WITH bronze_layer AS (
    SELECT
        id,
        applicationid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        code,
        totalbahrainiworkers,
        totalbahrainisalaries,
        totalexpatriatesalaries,
        createdon,
        updatedon,
        totalbahrainisalaries600,
        totalexpatriatessalaries600,
        sitevisitmonitoringid,
        amendmentrequestid
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updatedon DESC, createdon DESC
               ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_SIO_ESTABLISHMENTDETAILS
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        code,
        totalbahrainiworkers,
        totalbahrainisalaries,
        totalexpatriatesalaries,
        createdon,
        updatedon,
        totalbahrainisalaries600,
        totalexpatriatessalaries600,
        sitevisitmonitoringid,
        amendmentrequestid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".MYA_SIO_ESTABLISHMENT_DETAILS_BASE
)

-- =========================
-- VALIDATION START
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
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.paymentrequestid AS VARCHAR), '')
     OR COALESCE(CAST(b.code AS VARCHAR), '') <> COALESCE(CAST(s.code AS VARCHAR), '')
     OR COALESCE(CAST(b.totalbahrainiworkers AS VARCHAR), '') <> COALESCE(CAST(s.totalbahrainiworkers AS VARCHAR), '')
     OR COALESCE(CAST(b.totalbahrainisalaries AS VARCHAR), '') <> COALESCE(CAST(s.totalbahrainisalaries AS VARCHAR), '')
     OR COALESCE(CAST(b.totalexpatriatesalaries AS VARCHAR), '') <> COALESCE(CAST(s.totalexpatriatesalaries AS VARCHAR), '')
     OR COALESCE(CAST(b.sitevisitmonitoringid AS VARCHAR), '') <> COALESCE(CAST(s.sitevisitmonitoringid AS VARCHAR), '')
     OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR), '')
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