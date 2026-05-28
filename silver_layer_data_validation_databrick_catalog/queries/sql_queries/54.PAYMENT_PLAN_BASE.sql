WITH source_data AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id
               ORDER BY bronze_updated_on DESC, bronze_created_on DESC
           ) AS rn
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2da_PaymentPlan
),

bronze_layer AS (
    SELECT
        id,
        applicationid,
        amendmentrequestid,
        applicationsupportid,
        CAST(paymentnumber AS BIGINT) AS paymentnumber,
        paymentdate,
        CAST(beginingbalance AS DECIMAL(38,0)) AS beginingbalance,
        CAST(scheduledpayment AS DECIMAL(38,0)) AS scheduledpayment,
        CAST(totalpayment AS DECIMAL(38,0)) AS totalpayment,
        CAST(principal AS DECIMAL(38,0)) AS principal,
        CAST(profit AS DECIMAL(38,0)) AS profit,
        CAST(cumulativeprofit AS DECIMAL(38,0)) AS cumulativeprofit,
        CAST(tkprincipalamt AS DECIMAL(38,0)) AS tkprincipalamt,
        CAST(tkprofitamt AS DECIMAL(38,0)) AS tkprofitamt,
        CAST(tkprofitsubsidy AS DECIMAL(38,0)) AS tkprofitsubsidy,
        isactive,
        isdraft,
        paymentplanstatusid AS paymentplanstatus,
        disbursmentrequestid,
        activestatusid AS activestatus,
        paymentplanitemreference,
        FALSE AS is_deleted,
        'NEO2' AS source_system_name,
        updatedon,
        createdon,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS dbt_updated_at
    FROM source_data
    WHERE rn = 1
),

silver_layer AS (
    SELECT
        id,
        applicationid,
        amendmentrequestid,
        applicationsupportid,
        paymentnumber,
        paymentdate,
        beginingbalance,
        scheduledpayment,
        totalpayment,
        principal,
        profit,
        cumulativeprofit,
        tkprincipalamt,
        tkprofitamt,
        tkprofitsubsidy,
        isactive,
        isdraft,
        paymentplanstatus,
        disbursmentrequestid,
        activestatus,
        paymentplanitemreference,
        is_deleted,
        source_system_name,
        updatedon,
        createdon,
        dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".PAYMENT_PLAN_BASE
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

SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT b.id
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentnumber AS VARCHAR), '') <> COALESCE(CAST(s.paymentnumber AS VARCHAR), '')
     OR COALESCE(CAST(b.beginingbalance AS VARCHAR), '') <> COALESCE(CAST(s.beginingbalance AS VARCHAR), '')
     OR COALESCE(CAST(b.totalpayment AS VARCHAR), '') <> COALESCE(CAST(s.totalpayment AS VARCHAR), '')
     OR COALESCE(CAST(b.paymentplanstatus AS VARCHAR), '') <> COALESCE(CAST(s.paymentplanstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.activestatus AS VARCHAR), '') <> COALESCE(CAST(s.activestatus AS VARCHAR), '')
) t

UNION ALL

-- ⚠️ EXCLUDE dbt_updated_at
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT
        id, applicationid, amendmentrequestid, applicationsupportid,
        paymentnumber, paymentdate, beginingbalance, scheduledpayment,
        totalpayment, principal, profit, cumulativeprofit,
        tkprincipalamt, tkprofitamt, tkprofitsubsidy,
        isactive, isdraft, paymentplanstatus,
        disbursmentrequestid, activestatus, paymentplanitemreference,
        is_deleted, source_system_name, updatedon, createdon
    FROM bronze_layer

    EXCEPT

    SELECT
        id, applicationid, amendmentrequestid, applicationsupportid,
        paymentnumber, paymentdate, beginingbalance, scheduledpayment,
        totalpayment, principal, profit, cumulativeprofit,
        tkprincipalamt, tkprofitamt, tkprofitsubsidy,
        isactive, isdraft, paymentplanstatus,
        disbursmentrequestid, activestatus, paymentplanitemreference,
        is_deleted, source_system_name, updatedon, createdon
    FROM silver_layer
) t

UNION ALL

SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT
        id, applicationid, amendmentrequestid, applicationsupportid,
        paymentnumber, paymentdate, beginingbalance, scheduledpayment,
        totalpayment, principal, profit, cumulativeprofit,
        tkprincipalamt, tkprofitamt, tkprofitsubsidy,
        isactive, isdraft, paymentplanstatus,
        disbursmentrequestid, activestatus, paymentplanitemreference,
        is_deleted, source_system_name, updatedon, createdon
    FROM silver_layer

    EXCEPT

    SELECT
        id, applicationid, amendmentrequestid, applicationsupportid,
        paymentnumber, paymentdate, beginingbalance, scheduledpayment,
        totalpayment, principal, profit, cumulativeprofit,
        tkprincipalamt, tkprofitamt, tkprofitsubsidy,
        isactive, isdraft, paymentplanstatus,
        disbursmentrequestid, activestatus, paymentplanitemreference,
        is_deleted, source_system_name, updatedon, createdon
    FROM bronze_layer
) t;