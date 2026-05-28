WITH bronze_layer AS (
    SELECT
        applicationsupportid,
        itemname,
        itemmodelcode,
        itembrandingname,
        itempct,
        itemcostcurrencyid,
        customercostfx,
        itemlinediscountamt,
        itemcostvatpct,
        itemqtdrequest,
        itemqtd,
        itemqtdavailable,
        itemqtdinprogress,
        itemqtdclaimed,
        itemqtycancelled,
        itemqtddelivered,
        itemquotediscountamt,
        itemcostdiscountamt,
        itemvatamtun_fc,
        itemvatamttotal_fc,
        itemvatamtun,
        itemvatamttotal,
        itemcostun,
        itemcostnovatamt,
        itemcosttotal,
        itemcostun_fc,
        itemcostnovatamt_fc,
        itemcosttotal_fc,
        supportedamt,
        supportedamt_fc,
        tkshareunamtauto,
        tkshareunovr,
        tkshareun,
        tksharepct,
        tksharepctnovat,
        tksharetotal,
        customershareun,
        customersharetotal,
        createdby,
        createdon,
        updatedby,
        updatedon,
        allowpurchaseitembeforesubmi,
        requireinspection,
        itemconfigcap,
        tkshareunauto,
        tkshareautopct,
        tkshareactualpct,
        tksharetotalauto,
        tksharetotalovr,
        itemlinediscountamt_fc,
        itemquotediscountamt,
        allowofflinepayment,
        FALSE AS is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_IEX_PRODUCT
),

silver_layer AS (
    SELECT
        applicationsupportid,
        itemname,
        itemmodelcode,
        itembrandingname,
        itempct,
        itemcostcurrencyid,
        customercostfx,
        itemlinediscountamt,
        itemcostvatpct,
        itemqtdrequest,
        itemqtd,
        itemqtdavailable,
        itemqtdinprogress,
        itemqtdclaimed,
        itemqtycancelled,
        itemqtddelivered,
        itemquotediscountamt,
        itemcostdiscountamt,
        itemvatamtun_fc,
        itemvatamttotal_fc,
        itemvatamtun,
        itemvatamttotal,
        itemcostun,
        itemcostnovatamt,
        itemcosttotal,
        itemcostun_fc,
        itemcostnovatamt_fc,
        itemcosttotal_fc,
        supportedamt,
        supportedamt_fc,
        tkshareunamtauto,
        tkshareunovr,
        tkshareun,
        tksharepct,
        tksharepctnovat,
        tksharetotal,
        customershareun,
        customersharetotal,
        createdby,
        createdon,
        updatedby,
        updatedon,
        allowpurchaseitembeforesubmi,
        requireinspection,
        itemconfigcap,
        tkshareunauto,
        tkshareautopct,
        tkshareactualpct,
        tksharetotalauto,
        tksharetotalovr,
        itemlinediscountamt_fc,
        itemquotediscountamt,
        allowofflinepayment,
        is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".IEX_PRODUCT_BASE
)

-- =========================================
-- VALIDATION
-- =========================================

-- COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT applicationsupportid FROM bronze_layer GROUP BY applicationsupportid HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT applicationsupportid FROM silver_layer GROUP BY applicationsupportid HAVING COUNT(*) > 1)

UNION ALL

-- PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL FROM bronze_layer WHERE applicationsupportid IS NULL
UNION ALL
SELECT 'PK_NULL_SILVER', COUNT(*), NULL FROM silver_layer WHERE applicationsupportid IS NULL

UNION ALL

-- BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT CAST(applicationsupportid AS VARCHAR), CAST(itemname AS VARCHAR), CAST(itemcosttotal AS VARCHAR)
    FROM bronze_layer
    EXCEPT
    SELECT CAST(applicationsupportid AS VARCHAR), CAST(itemname AS VARCHAR), CAST(itemcosttotal AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT CAST(applicationsupportid AS VARCHAR), CAST(itemname AS VARCHAR), CAST(itemcosttotal AS VARCHAR)
    FROM silver_layer
    EXCEPT
    SELECT CAST(applicationsupportid AS VARCHAR), CAST(itemname AS VARCHAR), CAST(itemcosttotal AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- COLUMN MISMATCH (lightweight)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.applicationsupportid = s.applicationsupportid
    WHERE
        COALESCE(CAST(b.itemname AS VARCHAR),'') <> COALESCE(CAST(s.itemname AS VARCHAR),'')
     OR COALESCE(CAST(b.itemcosttotal AS VARCHAR),'') <> COALESCE(CAST(s.itemcosttotal AS VARCHAR),'')
     OR COALESCE(CAST(b.tksharetotal AS VARCHAR),'') <> COALESCE(CAST(s.tksharetotal AS VARCHAR),'')
) t;