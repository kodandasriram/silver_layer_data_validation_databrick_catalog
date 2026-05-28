-- Compare bronze-layer query output with silver-layer table output for payment_support_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\payment_support_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\payment_support_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Trino SQL generated from payment_support_base.sql.
-- Final column order aligned to silver_layer_query/payment_support_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : PAYMENT_SUPPORT_BASE_OS2
   Description   : This incremental model extracts and transforms payment 
                   support data from the NEO2 (OS2) source system Bronze 
                   Layer and loads it into the PAYMENT_SUPPORT_BASE_OS2 
                   target table as part of the Silver Layer data pipeline.

                   It captures payment request linkage, support type, payment 
                   status, financial metrics (costs, discounts, VAT, shares), 
                   delivery status, and related transactional attributes.

                   The model enriches data by joining reference tables such 
                   as support type, payment support status, currency, and 
                   payment delivery status.

                   It implements an incremental load strategy using MERGE 
                   based on the unique key (ID), processing only new and 
                   updated records using CREATEDON and UPDATEDON timestamps.

                   A post-hook ensures soft deletion handling by marking 
                   records as IS_DELETED = TRUE when they no longer exist 
                   in the source table.

   Source Tables : neo2.OSUSR_WZ3_PAYMENTSUPPORT
                   neo2.OSUSR_398_SUPPORTTYPE
                   neo2.OSUSR_WZ3_PAYMENTSUPPORTSTATUS
                   neo2.OSUSR_MM5_CURRENCY4
                   neo2.OSUSR_WZ3_PAYMENTDELIVERYSTATUS
                --    neo2.OSUSR_WZ3_PAYMENTREQUEST        
                --    neo2.OSUSR_2DA_APPLICATIONSUPPORT    

   Target Table  : PAYMENT_SUPPORT_BASE_OS2

   Load Type     : Incremental (Merge)
   Materialized  : Incremental
   Format        : PARQUET
   Tags          : neo2, daily

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author       | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-03-24   | <Author>     | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/
WITH CTE_PAYMENTSUPPORT AS (
    SELECT
        A.id,
        A.PAYMENTRESQUESTID, 
        A.applicationsupportid, 
        ST.LABEL as supporttype,
        PSS.LABEL as paymentsupportstatus,
		A.documentinstanceguid,--newly added
        PSS.iconclass,
        PSS.colorcode,
        A.isdocumentscomplete,
		a.itemcostcurrencyid,
        C.NAMEEN as itemcostcurrency,
        A.customercostfx,
        A.itemcostnovatamt_fc,
        A.itemlinediscountamt,
        A.itemcostdiscountpct,
        A.itemcostvatpct,
        A.itemqtdtoclaim,
        A.itemquotediscountamt,
        A.itemcostdiscountamt,
        A.itemvatamtun_fc,
        A.itemcostun,
        A.itemcosttotal,
        A.itemcostun_fc,
        A.itemcosttotal_fc,
        A.supportedamt,
        A.tkshareunamtauto,
        A.tkshareun,
        A.tksharepct,
        A.tksharepctnovat,
        A.tksharetotal,
        A.customershareun,
        A.customersharetotal,
        A.itemqtddelivered,
        PDS.LABEL AS paymentdeliverystatus,
        A.remarks,
		A.internaldocumentinstanceguid,-- newly added
        FALSE as is_deleted,
        'NEO2' AS source_system_name,
        a.updatedon ,
        a.CREATEDON createdon,
		A.createdby,-- newly added
		A.updatedby,-- newly added
        AST.amendmentrequestid,
        cast(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS dbt_updated_at,
        ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY A.updatedon DESC NULLS LAST, A.createdon DESC NULLS LAST) AS rnk
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTSUPPORT A
     left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTREQUEST PR
     on PR.ID = A.PAYMENTRESQUESTID
     left join  dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT  AST
     on AST.ID = A.APPLICATIONSUPPORTID
    left join  dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_SUPPORTTYPE ST
    on ST.CODE = A.SUPPORTTYPEID
    left join  dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTSUPPORTSTATUS PSS
    on PSS.CODE = A.PAYMENTSUPPORTSTATUSID
    left join  dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_CURRENCY4 C
    on C.ISOCODE  = A.ITEMCOSTCURRENCYID
    left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_WZ3_PAYMENTDELIVERYSTATUS PDS 
    ON PDS.CODE = A.PAYMENTDELIVERYSTATUSID 
)

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
    UPPER(NULLIF(TRIM(CAST(source_system_name AS VARCHAR)), '')) source_system_name,
    TRY_CAST(NULLIF(CAST(updatedon AS VARCHAR), '') AS TIMESTAMP) updatedon,
    TRY_CAST(NULLIF(CAST(createdon AS VARCHAR), '') AS TIMESTAMP)  createdon,
    createdby,
    updatedby,
    dbt_updated_at
FROM CTE_PAYMENTSUPPORT PS
WHERE rnk=1
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
    updatedby,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".payment_support_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'paymentresquestid'),
        (3, 'applicationsupportid'),
        (4, 'supporttype'),
        (5, 'paymentsupportstatus'),
        (6, 'documentinstanceguid'),
        (7, 'iconclass'),
        (8, 'colorcode'),
        (9, 'isdocumentscomplete'),
        (10, 'itemcostcurrency'),
        (11, 'customercostfx'),
        (12, 'itemcostnovatamt_fc'),
        (13, 'itemlinediscountamt'),
        (14, 'itemcostdiscountpct'),
        (15, 'itemcostvatpct'),
        (16, 'itemqtdtoclaim'),
        (17, 'itemquotediscountamt'),
        (18, 'itemcostdiscountamt'),
        (19, 'itemvatamtun_fc'),
        (20, 'itemcostun'),
        (21, 'itemcosttotal'),
        (22, 'itemcostun_fc'),
        (23, 'itemcosttotal_fc'),
        (24, 'supportedamt'),
        (25, 'tkshareunamtauto'),
        (26, 'tkshareun'),
        (27, 'tksharepct'),
        (28, 'tksharepctnovat'),
        (29, 'tksharetotal'),
        (30, 'customershareun'),
        (31, 'customersharetotal'),
        (32, 'itemqtddelivered'),
        (33, 'paymentdeliverystatus'),
        (34, 'remarks'),
        (35, 'internaldocumentinstanceguid'),
        (36, 'is_deleted'),
        (37, 'source_system_name'),
        (38, 'updatedon'),
        (39, 'createdon'),
        (40, 'createdby'),
        (41, 'updatedby'),
        (42, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'paymentresquestid'),
        (3, 'applicationsupportid'),
        (4, 'supporttype'),
        (5, 'paymentsupportstatus'),
        (6, 'documentinstanceguid'),
        (7, 'iconclass'),
        (8, 'colorcode'),
        (9, 'isdocumentscomplete'),
        (10, 'itemcostcurrency'),
        (11, 'customercostfx'),
        (12, 'itemcostnovatamt_fc'),
        (13, 'itemlinediscountamt'),
        (14, 'itemcostdiscountpct'),
        (15, 'itemcostvatpct'),
        (16, 'itemqtdtoclaim'),
        (17, 'itemquotediscountamt'),
        (18, 'itemcostdiscountamt'),
        (19, 'itemvatamtun_fc'),
        (20, 'itemcostun'),
        (21, 'itemcosttotal'),
        (22, 'itemcostun_fc'),
        (23, 'itemcosttotal_fc'),
        (24, 'supportedamt'),
        (25, 'tkshareunamtauto'),
        (26, 'tkshareun'),
        (27, 'tksharepct'),
        (28, 'tksharepctnovat'),
        (29, 'tksharetotal'),
        (30, 'customershareun'),
        (31, 'customersharetotal'),
        (32, 'itemqtddelivered'),
        (33, 'paymentdeliverystatus'),
        (34, 'remarks'),
        (35, 'internaldocumentinstanceguid'),
        (36, 'is_deleted'),
        (37, 'source_system_name'),
        (38, 'updatedon'),
        (39, 'createdon'),
        (40, 'createdby'),
        (41, 'updatedby'),
        (42, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("id" AS VARCHAR) AS "id",
        CAST("paymentresquestid" AS VARCHAR) AS "paymentresquestid",
        CAST("applicationsupportid" AS VARCHAR) AS "applicationsupportid",
        CAST("supporttype" AS VARCHAR) AS "supporttype",
        CAST("paymentsupportstatus" AS VARCHAR) AS "paymentsupportstatus",
        CAST("documentinstanceguid" AS VARCHAR) AS "documentinstanceguid",
        CAST("iconclass" AS VARCHAR) AS "iconclass",
        CAST("colorcode" AS VARCHAR) AS "colorcode",
        CAST("isdocumentscomplete" AS VARCHAR) AS "isdocumentscomplete",
        CAST("itemcostcurrency" AS VARCHAR) AS "itemcostcurrency",
        CAST("customercostfx" AS VARCHAR) AS "customercostfx",
        CAST("itemcostnovatamt_fc" AS VARCHAR) AS "itemcostnovatamt_fc",
        CAST("itemlinediscountamt" AS VARCHAR) AS "itemlinediscountamt",
        CAST("itemcostdiscountpct" AS VARCHAR) AS "itemcostdiscountpct",
        CAST("itemcostvatpct" AS VARCHAR) AS "itemcostvatpct",
        CAST("itemqtdtoclaim" AS VARCHAR) AS "itemqtdtoclaim",
        CAST("itemquotediscountamt" AS VARCHAR) AS "itemquotediscountamt",
        CAST("itemcostdiscountamt" AS VARCHAR) AS "itemcostdiscountamt",
        CAST("itemvatamtun_fc" AS VARCHAR) AS "itemvatamtun_fc",
        CAST("itemcostun" AS VARCHAR) AS "itemcostun",
        CAST("itemcosttotal" AS VARCHAR) AS "itemcosttotal",
        CAST("itemcostun_fc" AS VARCHAR) AS "itemcostun_fc",
        CAST("itemcosttotal_fc" AS VARCHAR) AS "itemcosttotal_fc",
        CAST("supportedamt" AS VARCHAR) AS "supportedamt",
        CAST("tkshareunamtauto" AS VARCHAR) AS "tkshareunamtauto",
        CAST("tkshareun" AS VARCHAR) AS "tkshareun",
        CAST("tksharepct" AS VARCHAR) AS "tksharepct",
        CAST("tksharepctnovat" AS VARCHAR) AS "tksharepctnovat",
        CAST("tksharetotal" AS VARCHAR) AS "tksharetotal",
        CAST("customershareun" AS VARCHAR) AS "customershareun",
        CAST("customersharetotal" AS VARCHAR) AS "customersharetotal",
        CAST("itemqtddelivered" AS VARCHAR) AS "itemqtddelivered",
        CAST("paymentdeliverystatus" AS VARCHAR) AS "paymentdeliverystatus",
        CAST("remarks" AS VARCHAR) AS "remarks",
        CAST("internaldocumentinstanceguid" AS VARCHAR) AS "internaldocumentinstanceguid",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("updatedon" AS VARCHAR) AS "updatedon",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("createdby" AS VARCHAR) AS "createdby",
        CAST("updatedby" AS VARCHAR) AS "updatedby",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("id" AS VARCHAR) AS "id",
        CAST("paymentresquestid" AS VARCHAR) AS "paymentresquestid",
        CAST("applicationsupportid" AS VARCHAR) AS "applicationsupportid",
        CAST("supporttype" AS VARCHAR) AS "supporttype",
        CAST("paymentsupportstatus" AS VARCHAR) AS "paymentsupportstatus",
        CAST("documentinstanceguid" AS VARCHAR) AS "documentinstanceguid",
        CAST("iconclass" AS VARCHAR) AS "iconclass",
        CAST("colorcode" AS VARCHAR) AS "colorcode",
        CAST("isdocumentscomplete" AS VARCHAR) AS "isdocumentscomplete",
        CAST("itemcostcurrency" AS VARCHAR) AS "itemcostcurrency",
        CAST("customercostfx" AS VARCHAR) AS "customercostfx",
        CAST("itemcostnovatamt_fc" AS VARCHAR) AS "itemcostnovatamt_fc",
        CAST("itemlinediscountamt" AS VARCHAR) AS "itemlinediscountamt",
        CAST("itemcostdiscountpct" AS VARCHAR) AS "itemcostdiscountpct",
        CAST("itemcostvatpct" AS VARCHAR) AS "itemcostvatpct",
        CAST("itemqtdtoclaim" AS VARCHAR) AS "itemqtdtoclaim",
        CAST("itemquotediscountamt" AS VARCHAR) AS "itemquotediscountamt",
        CAST("itemcostdiscountamt" AS VARCHAR) AS "itemcostdiscountamt",
        CAST("itemvatamtun_fc" AS VARCHAR) AS "itemvatamtun_fc",
        CAST("itemcostun" AS VARCHAR) AS "itemcostun",
        CAST("itemcosttotal" AS VARCHAR) AS "itemcosttotal",
        CAST("itemcostun_fc" AS VARCHAR) AS "itemcostun_fc",
        CAST("itemcosttotal_fc" AS VARCHAR) AS "itemcosttotal_fc",
        CAST("supportedamt" AS VARCHAR) AS "supportedamt",
        CAST("tkshareunamtauto" AS VARCHAR) AS "tkshareunamtauto",
        CAST("tkshareun" AS VARCHAR) AS "tkshareun",
        CAST("tksharepct" AS VARCHAR) AS "tksharepct",
        CAST("tksharepctnovat" AS VARCHAR) AS "tksharepctnovat",
        CAST("tksharetotal" AS VARCHAR) AS "tksharetotal",
        CAST("customershareun" AS VARCHAR) AS "customershareun",
        CAST("customersharetotal" AS VARCHAR) AS "customersharetotal",
        CAST("itemqtddelivered" AS VARCHAR) AS "itemqtddelivered",
        CAST("paymentdeliverystatus" AS VARCHAR) AS "paymentdeliverystatus",
        CAST("remarks" AS VARCHAR) AS "remarks",
        CAST("internaldocumentinstanceguid" AS VARCHAR) AS "internaldocumentinstanceguid",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("updatedon" AS VARCHAR) AS "updatedon",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("createdby" AS VARCHAR) AS "createdby",
        CAST("updatedby" AS VARCHAR) AS "updatedby",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM silver_layer
),

bronze_minus_silver AS (
    SELECT * FROM bronze_normalized
    EXCEPT ALL
    SELECT * FROM silver_normalized
),

silver_minus_bronze AS (
    SELECT * FROM silver_normalized
    EXCEPT ALL
    SELECT * FROM bronze_normalized
),

validation_results AS (
    SELECT
        'payment_support_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_support_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_support_base' AS table_name,
        'column_names_match' AS validation_point,
        CAST((
            SELECT COUNT(*)
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL OR s.column_name IS NULL
        ) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN NOT EXISTS (
            SELECT 1
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL OR s.column_name IS NULL
        ) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_support_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_support_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
