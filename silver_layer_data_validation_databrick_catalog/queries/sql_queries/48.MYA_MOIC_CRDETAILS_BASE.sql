WITH bronze_layer AS (
    SELECT
        id,
        applicationid,
        companycategoryid,
        enterprisegenderid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        enterpriseage,
        crnumber,
        status,
        companytype,
        companytypecode,
        nationality,
        nationalitycode,
        registrationdate,
        expirationdate,
        issuedcapital,
        localinvestment,
        gccinvestment,
        foreigninvestment,
        addressflat,
        addressbuilding,
        addressroad,
        addressblock,
        addresstown,
        isvirtual,
        comnercialnameen,
        comnercialnamear,
        psmonitoringid,
        CAST(amendmentrequestid AS VARCHAR) AS amendmentrequestid,
        --source_system_name,
        updatedon,
        createdon
        --dbt_updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updatedon DESC, createdon DESC
               ) AS rnk
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CRDETAILS
    ) t
    WHERE rnk = 1
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        companycategoryid,
        enterprisegenderid,
        paymentrequestid,
        eligibilitycriteriarequestty,
        enterpriseage,
        crnumber,
        status,
        companytype,
        companytypecode,
        nationality,
        nationalitycode,
        registrationdate,
        expirationdate,
        issuedcapital,
        localinvestment,
        gccinvestment,
        foreigninvestment,
        addressflat,
        addressbuilding,
        addressroad,
        addressblock,
        addresstown,
        isvirtual,
        comnercialnameen,
        comnercialnamear,
        psmonitoringid,
        CAST(amendmentrequestid AS VARCHAR) AS amendmentrequestid,
        --source_system_name,
        updatedon,
        createdon
        --dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".MYA_MOIC_CRDETAILS_BASE
)

-- =========================
-- VALIDATION OUTPUT
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
FROM bronze_layer
WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

UNION ALL

SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT b.id
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.companycategoryid AS VARCHAR), '') <> COALESCE(CAST(s.companycategoryid AS VARCHAR), '')
     OR COALESCE(CAST(b.enterprisegenderid AS VARCHAR), '') <> COALESCE(CAST(s.enterprisegenderid AS VARCHAR), '')
     OR COALESCE(CAST(b.crnumber AS VARCHAR), '') <> COALESCE(CAST(s.crnumber AS VARCHAR), '')
     OR COALESCE(CAST(b.status AS VARCHAR), '') <> COALESCE(CAST(s.status AS VARCHAR), '')
     OR COALESCE(CAST(b.enterpriseage AS VARCHAR), '') <> COALESCE(CAST(s.enterpriseage AS VARCHAR), '')
     OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR), '')
) t

UNION ALL

SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;