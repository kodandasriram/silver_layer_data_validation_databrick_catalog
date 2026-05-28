WITH bronze_layer AS (
    SELECT
        elg.id,
        elg.programversionid,
        elg.eligibilitycriteriatypeid,
        req.label AS eligibilityrequesttype,
        cutype.label AS customertype,
        elg.name,
        elg.description,
        elg.expression,
        type.label AS eligibilitypreventiontype,
        elg.errormessage_en,
        elg.errormessage_ar,
        elg.riskscoringid,
        elg.stakeholdertypeid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_ELIGIBILITYCRITERIA elg
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_ELIGIBILITYCRITERIAREQUESTTYPE req 
        ON req.code = elg.eligibilityrequesttypeid 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_ELIGIBILITYCRITERIAPREVENTIONTYPE type
        ON type.id = elg.eligibilitypreventiontypeid
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERTYPE cutype
        ON cutype.code = elg.customertypeid 
),

silver_layer AS (
   SELECT 
        id,
        programversionid,
        eligibilitycriteriatypeid,
        eligibilityrequesttype,
        customertype,
        name,
        description,
        expression,
        eligibilitypreventiontype,
        errormessage_en,
        errormessage_ar,
        riskscoring,
        stakeholdertypeid
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".ELIGIBILITY_CRITERIA_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1) t

UNION ALL

-- ✅ COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.programversionid AS VARCHAR), '') <> COALESCE(CAST(s.programversionid AS VARCHAR), '')
     OR COALESCE(CAST(b.eligibilitycriteriatypeid AS VARCHAR), '') <> COALESCE(CAST(s.eligibilitycriteriatypeid AS VARCHAR), '')
     OR COALESCE(CAST(b.eligibilityrequesttype AS VARCHAR), '') <> COALESCE(CAST(s.eligibilityrequesttype AS VARCHAR), '')
     OR COALESCE(CAST(b.customertype AS VARCHAR), '') <> COALESCE(CAST(s.customertype AS VARCHAR), '')
     OR COALESCE(CAST(b.name AS VARCHAR), '') <> COALESCE(CAST(s.name AS VARCHAR), '')
     OR COALESCE(CAST(b.description AS VARCHAR), '') <> COALESCE(CAST(s.description AS VARCHAR), '')
     OR COALESCE(CAST(b.expression AS VARCHAR), '') <> COALESCE(CAST(s.expression AS VARCHAR), '')
     OR COALESCE(CAST(b.eligibilitypreventiontype AS VARCHAR), '') <> COALESCE(CAST(s.eligibilitypreventiontype AS VARCHAR), '')
     OR COALESCE(CAST(b.errormessage_en AS VARCHAR), '') <> COALESCE(CAST(s.errormessage_en AS VARCHAR), '')
     OR COALESCE(CAST(b.errormessage_ar AS VARCHAR), '') <> COALESCE(CAST(s.errormessage_ar AS VARCHAR), '')
     OR COALESCE(CAST(b.riskscoringid AS VARCHAR), '') <> COALESCE(CAST(s.riskscoring AS VARCHAR), '')
     OR COALESCE(CAST(b.stakeholdertypeid AS VARCHAR), '') <> COALESCE(CAST(s.stakeholdertypeid AS VARCHAR), '')
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