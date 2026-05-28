WITH bronze_layer AS (
    SELECT
        applicationsupportid,
        itempct,
        servicetypeid,
        paymentfrequencyid,
        servicename,
        servicedescription,
        productmake,
        model,
        subscriptionstartdate,
        subscriptionenddate,
        subscriptionqtdrequest,
        subscriptionqtd,
        subscriptionnumberpayments,
        itemcostcurrencyid,
        customercostfx,
        itemlinediscountamt,
        itemcostvatpct,
        itemqtd,
        itemqtdrequest,
        itemqtdavailable,
        itemqtdinprogress,
        itemqtdclaimed,
        itemqtddelivered,
        itemqtycancelled,
        itemquotediscountamt,
        itemcostdiscountamt,
        itemvatamt_fc,
        itemvatamt,
        itemvatamttotal_fc,
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
        itemconfigcap,
        allowofflinepayment,
        FALSE AS is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_IEX_SERVICE
),

silver_layer AS (
    SELECT
        applicationsupportid,
        itempct,
        servicetypeid,
        paymentfrequencyid,
        servicename,
        servicedescription,
        productmake,
        model,
        subscriptionstartdate,
        subscriptionenddate,
        subscriptionqtdrequest,
        subscriptionqtd,
        subscriptionnumberpayments,
        itemcostcurrencyid,
        customercostfx,
        itemlinediscountamt,
        itemcostvatpct,
        itemqtd,
        itemqtdrequest,
        itemqtdavailable,
        itemqtdinprogress,
        itemqtdclaimed,
        itemqtddelivered,
        itemqtycancelled,
        itemquotediscountamt,
        itemcostdiscountamt,
        itemvatamt_fc,
        itemvatamt,
        itemvatamttotal_fc,
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
        itemconfigcap,
        allowofflinepayment,
        is_deleted
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".IEX_SERVICE_BASE
)

-- =========================
-- VALIDATION
-- =========================

-- COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT applicationsupportid 
    FROM bronze_layer 
    GROUP BY applicationsupportid 
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT applicationsupportid 
    FROM silver_layer 
    GROUP BY applicationsupportid 
    HAVING COUNT(*) > 1
)

UNION ALL

-- PK NULL
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL 
FROM bronze_layer WHERE applicationsupportid IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL 
FROM silver_layer WHERE applicationsupportid IS NULL

UNION ALL

-- BRONZE NOT IN SILVER (light)
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(applicationsupportid AS VARCHAR),
        CAST(servicename AS VARCHAR),
        CAST(itemcosttotal AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(applicationsupportid AS VARCHAR),
        CAST(servicename AS VARCHAR),
        CAST(itemcosttotal AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(applicationsupportid AS VARCHAR),
        CAST(servicename AS VARCHAR),
        CAST(itemcosttotal AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(applicationsupportid AS VARCHAR),
        CAST(servicename AS VARCHAR),
        CAST(itemcosttotal AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- COLUMN MISMATCH (light)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.applicationsupportid = s.applicationsupportid
    WHERE
        COALESCE(CAST(b.servicename AS VARCHAR),'') <> COALESCE(CAST(s.servicename AS VARCHAR),'')
     OR COALESCE(CAST(b.itemcosttotal AS VARCHAR),'') <> COALESCE(CAST(s.itemcosttotal AS VARCHAR),'')
     OR COALESCE(CAST(b.tksharetotal AS VARCHAR),'') <> COALESCE(CAST(s.tksharetotal AS VARCHAR),'')
) t;