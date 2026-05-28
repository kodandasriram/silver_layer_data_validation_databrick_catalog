WITH bronze_layer AS (
    SELECT
        id,
        programstatusid,
        programgroupid,
        programversionid,
        customertypeid,
        profiletypeid,
        reference,
        initials,
        name,
        --description,
        isspecialprogram,
        cancelreason,
        activedate,
        enddate,
        cmsprogram_en,
        cmsprogram_ar,
        isshowinterestenabled,
        programminorversionid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAM
),

silver_layer AS (
    SELECT
        id,
        programstatus,
        programgroup,
        programversionid,
        customertype,
        profiletype,
        reference,
        initials,
        name,
       --- description,
        isspecialprogram,
        cancelreason,
        activedate,
        enddate,
        cmsprogram_en,
        cmsprogram_ar,
        isshowinterestenabled,
        programminorversionid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".PROGRAM_BASE
)

-- =========================
-- VALIDATION BLOCK
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
        CAST(programstatusid AS VARCHAR),
        CAST(programgroupid AS VARCHAR),
        CAST(programversionid AS VARCHAR),
        CAST(customertypeid AS VARCHAR),
        CAST(profiletypeid AS VARCHAR),
        CAST(reference AS VARCHAR),
        CAST(initials AS VARCHAR),
        CAST(name AS VARCHAR),
       --- CAST(description AS VARCHAR),
        CAST(isspecialprogram AS VARCHAR),
        CAST(cancelreason AS VARCHAR),
        CAST(activedate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(cmsprogram_en AS VARCHAR),
        CAST(cmsprogram_ar AS VARCHAR),
        CAST(isshowinterestenabled AS VARCHAR),
        CAST(programminorversionid AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(programstatus AS VARCHAR),
        CAST(programgroup AS VARCHAR),
        CAST(programversionid AS VARCHAR),
        CAST(customertype AS VARCHAR),
        CAST(profiletype AS VARCHAR),
        CAST(reference AS VARCHAR),
        CAST(initials AS VARCHAR),
        CAST(name AS VARCHAR),
        ---CAST(description AS VARCHAR),
        CAST(isspecialprogram AS VARCHAR),
        CAST(cancelreason AS VARCHAR),
        CAST(activedate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(cmsprogram_en AS VARCHAR),
        CAST(cmsprogram_ar AS VARCHAR),
        CAST(isshowinterestenabled AS VARCHAR),
        CAST(programminorversionid AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(programstatus AS VARCHAR),
        CAST(programgroup AS VARCHAR),
        CAST(programversionid AS VARCHAR),
        CAST(customertype AS VARCHAR),
        CAST(profiletype AS VARCHAR),
        CAST(reference AS VARCHAR),
        CAST(initials AS VARCHAR),
        CAST(name AS VARCHAR),
        --CAST(description AS VARCHAR),
        CAST(isspecialprogram AS VARCHAR),
        CAST(cancelreason AS VARCHAR),
        CAST(activedate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(cmsprogram_en AS VARCHAR),
        CAST(cmsprogram_ar AS VARCHAR),
        CAST(isshowinterestenabled AS VARCHAR),
        CAST(programminorversionid AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(programstatusid AS VARCHAR),
        CAST(programgroupid AS VARCHAR),
        CAST(programversionid AS VARCHAR),
        CAST(customertypeid AS VARCHAR),
        CAST(profiletypeid AS VARCHAR),
        CAST(reference AS VARCHAR),
        CAST(initials AS VARCHAR),
        CAST(name AS VARCHAR),
        --CAST(description AS VARCHAR),
        CAST(isspecialprogram AS VARCHAR),
        CAST(cancelreason AS VARCHAR),
        CAST(activedate AS VARCHAR),
        CAST(enddate AS VARCHAR),
        CAST(cmsprogram_en AS VARCHAR),
        CAST(cmsprogram_ar AS VARCHAR),
        CAST(isshowinterestenabled AS VARCHAR),
        CAST(programminorversionid AS VARCHAR)
    FROM bronze_layer
) t;