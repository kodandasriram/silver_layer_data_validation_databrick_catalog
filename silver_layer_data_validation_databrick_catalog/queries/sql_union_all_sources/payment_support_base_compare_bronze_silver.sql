-- Compare bronze-layer query output with silver-layer table output for payment_support_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\payment_support_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\payment_support_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Databricks SQL generated from payment_support_base.sql.
-- Final column order aligned to silver_layer_query/payment_support_base_silver_layer.sql.
-- Standalone Databricks SQL converted from dbt model.
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
        cast(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at,
        ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY A.updatedon DESC NULLS LAST, A.createdon DESC NULLS LAST) AS rnk
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTSUPPORT` A
     left join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTREQUEST` PR
     on PR.ID = A.PAYMENTRESQUESTID
    left join  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_SUPPORTTYPE` ST
    on ST.CODE = A.SUPPORTTYPEID
    left join  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTSUPPORTSTATUS` PSS
    on PSS.CODE = A.PAYMENTSUPPORTSTATUSID
    left join  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MM5_CURRENCY4` C
    on C.ISOCODE  = A.ITEMCOSTCURRENCYID
    left join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTDELIVERYSTATUS` PDS 
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
    UPPER(NULLIF(TRIM(CAST(source_system_name AS STRING)), '')) source_system_name,
    TRY_CAST(NULLIF(CAST(updatedon AS STRING), '') AS TIMESTAMP) updatedon,
    TRY_CAST(NULLIF(CAST(createdon AS STRING), '') AS TIMESTAMP)  createdon,
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
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`payment_support_base`
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
        CAST(`id` AS STRING) AS `id`,
        CAST(`paymentresquestid` AS STRING) AS `paymentresquestid`,
        CAST(`applicationsupportid` AS STRING) AS `applicationsupportid`,
        CAST(`supporttype` AS STRING) AS `supporttype`,
        CAST(`paymentsupportstatus` AS STRING) AS `paymentsupportstatus`,
        CAST(`documentinstanceguid` AS STRING) AS `documentinstanceguid`,
        CAST(`iconclass` AS STRING) AS `iconclass`,
        CAST(`colorcode` AS STRING) AS `colorcode`,
        CAST(`isdocumentscomplete` AS STRING) AS `isdocumentscomplete`,
        CAST(`itemcostcurrency` AS STRING) AS `itemcostcurrency`,
        CAST(`customercostfx` AS STRING) AS `customercostfx`,
        CAST(`itemcostnovatamt_fc` AS STRING) AS `itemcostnovatamt_fc`,
        CAST(`itemlinediscountamt` AS STRING) AS `itemlinediscountamt`,
        CAST(`itemcostdiscountpct` AS STRING) AS `itemcostdiscountpct`,
        CAST(`itemcostvatpct` AS STRING) AS `itemcostvatpct`,
        CAST(`itemqtdtoclaim` AS STRING) AS `itemqtdtoclaim`,
        CAST(`itemquotediscountamt` AS STRING) AS `itemquotediscountamt`,
        CAST(`itemcostdiscountamt` AS STRING) AS `itemcostdiscountamt`,
        CAST(`itemvatamtun_fc` AS STRING) AS `itemvatamtun_fc`,
        CAST(`itemcostun` AS STRING) AS `itemcostun`,
        CAST(`itemcosttotal` AS STRING) AS `itemcosttotal`,
        CAST(`itemcostun_fc` AS STRING) AS `itemcostun_fc`,
        CAST(`itemcosttotal_fc` AS STRING) AS `itemcosttotal_fc`,
        CAST(`supportedamt` AS STRING) AS `supportedamt`,
        CAST(`tkshareunamtauto` AS STRING) AS `tkshareunamtauto`,
        CAST(`tkshareun` AS STRING) AS `tkshareun`,
        CAST(`tksharepct` AS STRING) AS `tksharepct`,
        CAST(`tksharepctnovat` AS STRING) AS `tksharepctnovat`,
        CAST(`tksharetotal` AS STRING) AS `tksharetotal`,
        CAST(`customershareun` AS STRING) AS `customershareun`,
        CAST(`customersharetotal` AS STRING) AS `customersharetotal`,
        CAST(`itemqtddelivered` AS STRING) AS `itemqtddelivered`,
        CAST(`paymentdeliverystatus` AS STRING) AS `paymentdeliverystatus`,
        CAST(`remarks` AS STRING) AS `remarks`,
        CAST(`internaldocumentinstanceguid` AS STRING) AS `internaldocumentinstanceguid`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`createdby` AS STRING) AS `createdby`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`paymentresquestid` AS STRING) AS `paymentresquestid`,
        CAST(`applicationsupportid` AS STRING) AS `applicationsupportid`,
        CAST(`supporttype` AS STRING) AS `supporttype`,
        CAST(`paymentsupportstatus` AS STRING) AS `paymentsupportstatus`,
        CAST(`documentinstanceguid` AS STRING) AS `documentinstanceguid`,
        CAST(`iconclass` AS STRING) AS `iconclass`,
        CAST(`colorcode` AS STRING) AS `colorcode`,
        CAST(`isdocumentscomplete` AS STRING) AS `isdocumentscomplete`,
        CAST(`itemcostcurrency` AS STRING) AS `itemcostcurrency`,
        CAST(`customercostfx` AS STRING) AS `customercostfx`,
        CAST(`itemcostnovatamt_fc` AS STRING) AS `itemcostnovatamt_fc`,
        CAST(`itemlinediscountamt` AS STRING) AS `itemlinediscountamt`,
        CAST(`itemcostdiscountpct` AS STRING) AS `itemcostdiscountpct`,
        CAST(`itemcostvatpct` AS STRING) AS `itemcostvatpct`,
        CAST(`itemqtdtoclaim` AS STRING) AS `itemqtdtoclaim`,
        CAST(`itemquotediscountamt` AS STRING) AS `itemquotediscountamt`,
        CAST(`itemcostdiscountamt` AS STRING) AS `itemcostdiscountamt`,
        CAST(`itemvatamtun_fc` AS STRING) AS `itemvatamtun_fc`,
        CAST(`itemcostun` AS STRING) AS `itemcostun`,
        CAST(`itemcosttotal` AS STRING) AS `itemcosttotal`,
        CAST(`itemcostun_fc` AS STRING) AS `itemcostun_fc`,
        CAST(`itemcosttotal_fc` AS STRING) AS `itemcosttotal_fc`,
        CAST(`supportedamt` AS STRING) AS `supportedamt`,
        CAST(`tkshareunamtauto` AS STRING) AS `tkshareunamtauto`,
        CAST(`tkshareun` AS STRING) AS `tkshareun`,
        CAST(`tksharepct` AS STRING) AS `tksharepct`,
        CAST(`tksharepctnovat` AS STRING) AS `tksharepctnovat`,
        CAST(`tksharetotal` AS STRING) AS `tksharetotal`,
        CAST(`customershareun` AS STRING) AS `customershareun`,
        CAST(`customersharetotal` AS STRING) AS `customersharetotal`,
        CAST(`itemqtddelivered` AS STRING) AS `itemqtddelivered`,
        CAST(`paymentdeliverystatus` AS STRING) AS `paymentdeliverystatus`,
        CAST(`remarks` AS STRING) AS `remarks`,
        CAST(`internaldocumentinstanceguid` AS STRING) AS `internaldocumentinstanceguid`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`createdby` AS STRING) AS `createdby`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
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
            FROM (
                SELECT column_position, column_name
                FROM (
                    SELECT column_position, column_name FROM bronze_columns
                    EXCEPT
                    SELECT column_position, column_name FROM silver_columns
                ) bronze_only
                UNION ALL
                SELECT column_position, column_name
                FROM (
                    SELECT column_position, column_name FROM silver_columns
                    EXCEPT
                    SELECT column_position, column_name FROM bronze_columns
                ) silver_only
            ) column_differences
        ) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN NOT EXISTS (
            SELECT 1
            FROM (
                SELECT column_position, column_name
                FROM (
                    SELECT column_position, column_name FROM bronze_columns
                    EXCEPT
                    SELECT column_position, column_name FROM silver_columns
                ) bronze_only
                UNION ALL
                SELECT column_position, column_name
                FROM (
                    SELECT column_position, column_name FROM silver_columns
                    EXCEPT
                    SELECT column_position, column_name FROM bronze_columns
                ) silver_only
            ) column_differences
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
