WITH bronze_layer AS (
    SELECT
        CAST(p.applicationsupportid AS BIGINT) AS id,
        COALESCE(TRIM(CAST(s.LABEL AS VARCHAR)), '') AS profitratetype,
        COALESCE(CAST(p.profitrate AS VARCHAR), '') AS profitrate,
        COALESCE(CAST(p.totalprofit AS VARCHAR), '') AS totalprofit,
        COALESCE(CAST(p.totalprofitrecalculated AS VARCHAR), '') AS totalprofitrecalculated,
        COALESCE(TRIM(CAST(q.LABEL AS VARCHAR)), '') AS referenceratename,
        COALESCE(TRIM(CAST(p.referenceratename_other AS VARCHAR)), '') AS referenceratename_other,
        COALESCE(CAST(p.referenceratename_baserate AS VARCHAR), '') AS referenceratename_baserate,
        COALESCE(CAST(p.referenceratemonth AS VARCHAR), '') AS referenceratemonth,
        COALESCE(CAST(p.referenceratepercentage AS VARCHAR), '') AS referenceratepercentage,
        COALESCE(CAST(p.margin AS VARCHAR), '') AS margin,
        COALESCE(CAST(p.floatingrate AS VARCHAR), '') AS floatingrate,
        COALESCE(CAST(p.profitsettingperiod AS VARCHAR), '') AS profitsettingperiod,
        COALESCE(CAST(p.floorcap AS VARCHAR), '') AS floorcap,
        COALESCE(CAST(p.ceilingcap AS VARCHAR), '') AS ceilingcap,
        COALESCE(TRIM(CAST(p.createdby AS VARCHAR)), '') AS createdby,
        COALESCE(CAST(p.createdon AS VARCHAR), '') AS createdon,
        COALESCE(TRIM(CAST(p.updatedby AS VARCHAR)), '') AS updatedby,
        COALESCE(CAST(p.updatedon AS VARCHAR), '') AS updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".osusr_2da_profit p
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_H95_PROFITRATETYPE s
        ON p.profitratetypeid = s.id
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_REFERENCERATENAME q
        ON p.referenceratenameid = q.id
),

silver_layer AS (
    SELECT
        CAST(applicationsupportid AS BIGINT) AS id,
        COALESCE(TRIM(CAST(profitratetype AS VARCHAR)), '') AS profitratetype,
        COALESCE(CAST(profitrate AS VARCHAR), '') AS profitrate,
        COALESCE(CAST(totalprofit AS VARCHAR), '') AS totalprofit,
        COALESCE(CAST(totalprofitrecalculated AS VARCHAR), '') AS totalprofitrecalculated,
        COALESCE(TRIM(CAST(referenceratename AS VARCHAR)), '') AS referenceratename,
        COALESCE(TRIM(CAST(referenceratename_other AS VARCHAR)), '') AS referenceratename_other,
        COALESCE(CAST(referenceratename_baserate AS VARCHAR), '') AS referenceratename_baserate,
        COALESCE(CAST(referenceratemonth AS VARCHAR), '') AS referenceratemonth,
        COALESCE(CAST(referenceratepercentage AS VARCHAR), '') AS referenceratepercentage,
        COALESCE(CAST(margin AS VARCHAR), '') AS margin,
        COALESCE(CAST(floatingrate AS VARCHAR), '') AS floatingrate,
        COALESCE(CAST(profitsettingperiod AS VARCHAR), '') AS profitsettingperiod,
        COALESCE(CAST(floorcap AS VARCHAR), '') AS floorcap,
        COALESCE(CAST(ceilingcap AS VARCHAR), '') AS ceilingcap,
        COALESCE(TRIM(CAST(createdby AS VARCHAR)), '') AS createdby,
        COALESCE(CAST(createdon AS VARCHAR), '') AS createdon,
        COALESCE(TRIM(CAST(updatedby AS VARCHAR)), '') AS updatedby,
        COALESCE(CAST(updatedon AS VARCHAR), '') AS updatedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".PROFIT_BASE
),

-- =========================
-- BASIC VALIDATIONS
-- =========================
count_validation AS (
    SELECT 'COUNT_VALIDATION' AS check_name,
           (SELECT COUNT(*) FROM bronze_layer) AS value1,
           (SELECT COUNT(*) FROM silver_layer) AS value2
),

duplicate_bronze AS (
    SELECT 'DUPLICATE_BRONZE' AS check_name, COUNT(*) AS value1
    FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)
),

duplicate_silver AS (
    SELECT 'DUPLICATE_SILVER' AS check_name, COUNT(*) AS value1
    FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)
),

null_bronze AS (
    SELECT 'PK_NULL_BRONZE' AS check_name, COUNT(*) AS value1
    FROM bronze_layer WHERE id IS NULL
),

null_silver AS (
    SELECT 'PK_NULL_SILVER' AS check_name, COUNT(*) AS value1
    FROM silver_layer WHERE id IS NULL
),

-- =========================
-- ROW LEVEL COMPARISON
-- =========================
comparison AS (
    SELECT
        b.id,
        CASE WHEN b.profitratetype = s.profitratetype THEN 1 ELSE 0 END AS profitratetype_match,
        CASE WHEN b.profitrate = s.profitrate THEN 1 ELSE 0 END AS profitrate_match,
        CASE WHEN b.totalprofit = s.totalprofit THEN 1 ELSE 0 END AS totalprofit_match,
        CASE WHEN b.totalprofitrecalculated = s.totalprofitrecalculated THEN 1 ELSE 0 END AS totalprofitrecalculated_match,
        CASE WHEN b.referenceratename = s.referenceratename THEN 1 ELSE 0 END AS referenceratename_match,
        CASE WHEN b.referenceratename_other = s.referenceratename_other THEN 1 ELSE 0 END AS referenceratename_other_match,
        CASE WHEN b.referenceratename_baserate = s.referenceratename_baserate THEN 1 ELSE 0 END AS referenceratename_baserate_match,
        CASE WHEN b.referenceratemonth = s.referenceratemonth THEN 1 ELSE 0 END AS referenceratemonth_match,
        CASE WHEN b.referenceratepercentage = s.referenceratepercentage THEN 1 ELSE 0 END AS referenceratepercentage_match,
        CASE WHEN b.margin = s.margin THEN 1 ELSE 0 END AS margin_match,
        CASE WHEN b.floatingrate = s.floatingrate THEN 1 ELSE 0 END AS floatingrate_match,
        CASE WHEN b.profitsettingperiod = s.profitsettingperiod THEN 1 ELSE 0 END AS profitsettingperiod_match,
        CASE WHEN b.floorcap = s.floorcap THEN 1 ELSE 0 END AS floorcap_match,
        CASE WHEN b.ceilingcap = s.ceilingcap THEN 1 ELSE 0 END AS ceilingcap_match,
        CASE WHEN b.createdby = s.createdby THEN 1 ELSE 0 END AS createdby_match,
        CASE WHEN b.createdon = s.createdon THEN 1 ELSE 0 END AS createdon_match,
        CASE WHEN b.updatedby = s.updatedby THEN 1 ELSE 0 END AS updatedby_match,
        CASE WHEN b.updatedon = s.updatedon THEN 1 ELSE 0 END AS updatedon_match
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
),

mismatch_count AS (
    SELECT 'MISMATCH_COUNT' AS check_name, COUNT(*) AS value1
    FROM comparison
    WHERE
        profitratetype_match = 0 OR
        profitrate_match = 0 OR
        totalprofit_match = 0 OR
        totalprofitrecalculated_match = 0 OR
        referenceratename_match = 0 OR
        referenceratename_other_match = 0 OR
        referenceratename_baserate_match = 0 OR
        referenceratemonth_match = 0 OR
        referenceratepercentage_match = 0 OR
        margin_match = 0 OR
        floatingrate_match = 0 OR
        profitsettingperiod_match = 0 OR
        floorcap_match = 0 OR
        ceilingcap_match = 0 OR
        createdby_match = 0 OR
        createdon_match = 0 OR
        updatedby_match = 0 OR
        updatedon_match = 0
),

-- =========================
-- ORPHAN CHECKS
-- =========================
orphan_checks AS (
    SELECT 'IN_BRONZE_NOT_SILVER' AS check_name, COUNT(*) AS value1
    FROM bronze_layer b
    WHERE NOT EXISTS (SELECT 1 FROM silver_layer s WHERE s.id = b.id)

    UNION ALL

    SELECT 'IN_SILVER_NOT_BRONZE' AS check_name, COUNT(*) AS value1
    FROM silver_layer s
    WHERE NOT EXISTS (SELECT 1 FROM bronze_layer b WHERE b.id = s.id)
)

-- =========================
-- FINAL OUTPUT
-- =========================
SELECT check_name, value1, value2 FROM count_validation
UNION ALL
SELECT check_name, value1, NULL FROM duplicate_bronze
UNION ALL
SELECT check_name, value1, NULL FROM duplicate_silver
UNION ALL
SELECT check_name, value1, NULL FROM null_bronze
UNION ALL
SELECT check_name, value1, NULL FROM null_silver
UNION ALL
SELECT check_name, value1, NULL FROM mismatch_count
UNION ALL
SELECT check_name, value1, NULL FROM orphan_checks
ORDER BY check_name;