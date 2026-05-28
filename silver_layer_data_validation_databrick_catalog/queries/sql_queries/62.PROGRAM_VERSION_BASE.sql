WITH bronze_layer AS (
    SELECT
        id,
        programid,
        programversionstatusid,
        CAST(whitelistid AS VARCHAR) AS whitelist,
        commercialname_en,
        commercialname_ar,
        description_en,
        description_ar,
        deliveryconfirmationperiod,
        isvendorblacklistvalidation,
        iswhitelisted,
        allowdelinquentuser,
        isemployeeacknowledgement,
       createdon,
        updatedon,
        ispublic,
        publishedon,
        publishedby,
        isinternalwithdrawalallowed,
        casemanagementid,
        publishnotes,
        isupgradeable
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION
),

silver_layer AS (
    SELECT
        id,
        programid,
        programversionstatus,
        CAST(whitelist AS VARCHAR) AS whitelist,
        commercialname_en,
        commercialname_ar,
        description_en,
        description_ar,
        deliveryconfirmationperiod,
        isvendorblacklistvalidation,
        iswhitelisted,
        allowdelinquentuser,
        isemployeeacknowledgement,
        createdon,
        updatedon,
        ispublic,
        publishedon,
        publishedby,
        isinternalwithdrawalallowed,
        casemanagementid,
        publishnotes,
        isupgradeable
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".PROGRAM_VERSION_BASE
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

-- BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(programversionstatusid AS VARCHAR),
        CAST(whitelist AS VARCHAR),
        CAST(commercialname_en AS VARCHAR),
        CAST(commercialname_ar AS VARCHAR),
        CAST(description_en AS VARCHAR),
        CAST(description_ar AS VARCHAR),
        CAST(deliveryconfirmationperiod AS VARCHAR),
        CAST(isvendorblacklistvalidation AS VARCHAR),
        CAST(iswhitelisted AS VARCHAR),
        CAST(allowdelinquentuser AS VARCHAR),
        CAST(isemployeeacknowledgement AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(ispublic AS VARCHAR),
        CAST(publishedon AS VARCHAR),
        CAST(publishedby AS VARCHAR),
        CAST(isinternalwithdrawalallowed AS VARCHAR),
        CAST(casemanagementid AS VARCHAR),
        CAST(publishnotes AS VARCHAR),
        CAST(isupgradeable AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(programversionstatus AS VARCHAR),
        CAST(whitelist AS VARCHAR),
        CAST(commercialname_en AS VARCHAR),
        CAST(commercialname_ar AS VARCHAR),
        CAST(description_en AS VARCHAR),
        CAST(description_ar AS VARCHAR),
        CAST(deliveryconfirmationperiod AS VARCHAR),
        CAST(isvendorblacklistvalidation AS VARCHAR),
        CAST(iswhitelisted AS VARCHAR),
        CAST(allowdelinquentuser AS VARCHAR),
        CAST(isemployeeacknowledgement AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(ispublic AS VARCHAR),
        CAST(publishedon AS VARCHAR),
        CAST(publishedby AS VARCHAR),
        CAST(isinternalwithdrawalallowed AS VARCHAR),
        CAST(casemanagementid AS VARCHAR),
        CAST(publishnotes AS VARCHAR),
        CAST(isupgradeable AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(programversionstatus AS VARCHAR),
        CAST(whitelist AS VARCHAR),
        CAST(commercialname_en AS VARCHAR),
        CAST(commercialname_ar AS VARCHAR),
        CAST(description_en AS VARCHAR),
        CAST(description_ar AS VARCHAR),
        CAST(deliveryconfirmationperiod AS VARCHAR),
        CAST(isvendorblacklistvalidation AS VARCHAR),
        CAST(iswhitelisted AS VARCHAR),
        CAST(allowdelinquentuser AS VARCHAR),
        CAST(isemployeeacknowledgement AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(ispublic AS VARCHAR),
        CAST(publishedon AS VARCHAR),
        CAST(publishedby AS VARCHAR),
        CAST(isinternalwithdrawalallowed AS VARCHAR),
        CAST(casemanagementid AS VARCHAR),
        CAST(publishnotes AS VARCHAR),
        CAST(isupgradeable AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(programid AS VARCHAR),
        CAST(programversionstatusid AS VARCHAR),
        CAST(whitelist AS VARCHAR),
        CAST(commercialname_en AS VARCHAR),
        CAST(commercialname_ar AS VARCHAR),
        CAST(description_en AS VARCHAR),
        CAST(description_ar AS VARCHAR),
        CAST(deliveryconfirmationperiod AS VARCHAR),
        CAST(isvendorblacklistvalidation AS VARCHAR),
        CAST(iswhitelisted AS VARCHAR),
        CAST(allowdelinquentuser AS VARCHAR),
        CAST(isemployeeacknowledgement AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(ispublic AS VARCHAR),
        CAST(publishedon AS VARCHAR),
        CAST(publishedby AS VARCHAR),
        CAST(isinternalwithdrawalallowed AS VARCHAR),
        CAST(casemanagementid AS VARCHAR),
        CAST(publishnotes AS VARCHAR),
        CAST(isupgradeable AS VARCHAR)
    FROM bronze_layer
) t;