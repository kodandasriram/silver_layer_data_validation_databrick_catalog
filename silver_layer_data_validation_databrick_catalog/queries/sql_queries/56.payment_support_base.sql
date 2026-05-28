WITH bronze_layer AS (
    SELECT
        id,
        paymentresquestid,
        applicationsupportid,
        CAST(supporttypeid AS VARCHAR) AS supporttype,
        CAST(paymentsupportstatusid AS VARCHAR) AS paymentsupportstatus,
        documentinstanceguid,
        CAST(NULL AS VARCHAR) AS iconclass,
        CAST(NULL AS VARCHAR) AS colorcode,
        isdocumentscomplete,
        CAST(itemcostcurrencyid AS VARCHAR) AS itemcostcurrency,
        CAST(customercostfx AS DECIMAL(38,8)) AS customercostfx,
        CAST(itemcostnovatamt_fc AS DECIMAL(38,8)) AS itemcostnovatamt_fc,
        CAST(itemlinediscountamt AS DECIMAL(38,8)) AS itemlinediscountamt,
        CAST(itemcostdiscountpct AS DECIMAL(38,8)) AS itemcostdiscountpct,
        CAST(itemcostvatpct AS DECIMAL(38,8)) AS itemcostvatpct,
        itemqtdtoclaim,
        CAST(itemquotediscountamt AS DECIMAL(38,8)) AS itemquotediscountamt,
        CAST(itemcostdiscountamt AS DECIMAL(38,8)) AS itemcostdiscountamt,
        CAST(itemvatamtun_fc AS DECIMAL(38,8)) AS itemvatamtun_fc,
        CAST(itemcostun AS DECIMAL(38,8)) AS itemcostun,
        CAST(itemcosttotal AS DECIMAL(38,8)) AS itemcosttotal,
        CAST(itemcostun_fc AS DECIMAL(38,8)) AS itemcostun_fc,
        CAST(itemcosttotal_fc AS DECIMAL(38,8)) AS itemcosttotal_fc,
        CAST(supportedamt AS DECIMAL(38,8)) AS supportedamt,
        CAST(tkshareunamtauto AS DECIMAL(38,8)) AS tkshareunamtauto,
        CAST(tkshareun AS DECIMAL(38,8)) AS tkshareun,
        CAST(tksharepct AS DECIMAL(38,8)) AS tksharepct,
        CAST(tksharepctnovat AS DECIMAL(38,8)) AS tksharepctnovat,
        CAST(tksharetotal AS DECIMAL(38,8)) AS tksharetotal,
        CAST(customershareun AS DECIMAL(38,8)) AS customershareun,
        CAST(customersharetotal AS DECIMAL(38,8)) AS customersharetotal,
        itemqtddelivered,
        CAST(paymentdeliverystatusid AS VARCHAR) AS paymentdeliverystatus,
        remarks,
        internaldocumentinstanceguid,
        CAST(NULL AS BOOLEAN) AS is_deleted,
        'BRONZE' AS source_system_name,
        updatedon,
        createdon,
        createdby,
        CAST(NULL AS VARCHAR) AS dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTSUPPORT
),

silver_layer AS (
    SELECT
        id,
        paymentresquestid,
        applicationsupportid,
        supporttype,
        paymentsupportstatus,
        documentinstanceguid,
        iconclass,
        colorcode,
        isdocumentscomplete,
        itemcostcurrency,
        customercostfx,
        itemcostnovatamt_fc,
        itemlinediscountamt,
        itemcostdiscountpct,
        itemcostvatpct,
        itemqtdtoclaim,
        itemquotediscountamt,
        itemcostdiscountamt,
        itemvatamtun_fc,
        itemcostun,
        itemcosttotal,
        itemcostun_fc,
        itemcosttotal_fc,
        supportedamt,
        tkshareunamtauto,
        tkshareun,
        tksharepct,
        tksharepctnovat,
        tksharetotal,
        customershareun,
        customersharetotal,
        itemqtddelivered,
        paymentdeliverystatus,
        remarks,
        internaldocumentinstanceguid,
        is_deleted,
        source_system_name,
        updatedon,
        createdon,
        createdby,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".payment_support_base
)

-- ============================
-- VALIDATION
-- ============================

-- 1. COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATE BRONZE
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 4. PK NULL CHECK
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
)

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
)

UNION ALL

-- 7. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.supporttype AS VARCHAR), '') <> COALESCE(CAST(s.supporttype AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentsupportstatus AS VARCHAR), '') <> COALESCE(CAST(s.paymentsupportstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.customercostfx AS VARCHAR), '') <> COALESCE(CAST(s.customercostfx AS VARCHAR), '')
     OR COALESCE(CAST(b.itemcosttotal AS VARCHAR), '') <> COALESCE(CAST(s.itemcosttotal AS VARCHAR), '')
     OR COALESCE(CAST(b.supportedamt AS VARCHAR), '') <> COALESCE(CAST(s.supportedamt AS VARCHAR), '')
) t;