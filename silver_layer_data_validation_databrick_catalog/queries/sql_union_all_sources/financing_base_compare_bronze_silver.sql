-- Compare bronze-layer query output with silver-layer table output for financing_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\financing_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\financing_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Databricks SQL generated from financing_base.sql.
-- Final column order aligned to silver_layer_query/financing_base_silver_layer.sql.
-- Standalone Databricks SQL converted from dbt model.
/*
 =================================================================================================

Name        : FINANCING_BASE_OS2
Description : This model extracts and transforms program-related attributes
              from the NEO2 (OS2) source system Bronze Layer and loads into the
              OSUSR_2DA_FINANCING target table as part of the Silver Layer
              data pipeline.

Source Tables : neo2.OSUSR_2DA_FINANCING
                neo2.OSUSR_H95_FACILITYTYPE
                neo2.OSUSR_398_YESNOOPTION
                neo2.OSUSR_2DA_DISBURSEMENTTYPE
                neo2.OSUSR_2DA_FINANCINGPRODUCTTYPE
                neo2.OSUSR_H95_PAYMENTFREQUENCY
                neo2.OSUSR_MM5_YESNOOPTION4

Target Table : OSUSR_2DA_FINANCING
Load Type    : Incremental Load
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author   | Description
--------------------------------------------------------------
1.0     | 2026-03-24 | Daya | Initial version

================================================================================================= 
*/
with source_cte as (
SELECT
    F.id,
    financingamountrequested,
    tenor,
    graceperiod,
    FT.LABEL AS facilitytype,
    YN1.LABEL AS revolvingloan,
    availabilityperiod,
    DT.LABEL AS disbursementtype,
    FPT.LABEL AS financingproducttype,
    financingproducttypeother,
    PF.LABEL AS paymentfrequency,
    try_cast(BANKAPPROVALDATE AS timestamp) as bankapprovaldate,
    YN2.LABEL AS facilitieswithbank,
    YN3.LABEL AS workingcapitalfacilitieswith,
    internalriskrating,
    securitycoverage,
    cashconversioncycle,
    debtservicecoverageratio,
    machineryandequipment,
    technology,
    marketingandbranding,
    workingcapital,
    fixturesandfittings,
    facilitybreakupotheramount,
    facilitybreakupothervalue,
    commentworkcapital,
    commentrevolvingloan,
    fixedassetsamtrequested,
    fixedassestsamtremaining,
    workingcapitalamtrequested,
    workingcapitalamtremaining,
    commentworkingcapital,
    financingamtrequested,
    financingamtremaining,
    workingcapitalcapid,
    workingcapitalremaningcap,
    financingamtcapid,
    financingamtremaningcap,
    isanyfieldupdatedforamendmen,
    FALSE as is_deleted,
    F.isactive,
    F.createdby,
    F.createdon,
    F.updatedby,
    F.updatedon,
    'NEO2' AS source_system_name,
    cast(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at,
    ROW_NUMBER() OVER (PARTITION BY F.id ORDER BY F.updatedon DESC NULLS LAST, F.createdon DESC NULLS LAST) AS rnk
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_FINANCING` f
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_H95_FACILITYTYPE` ft 
        ON f.FACILITYTYPEID = ft.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_YESNOOPTION` yn1
        ON f.revolvingloan = yn1.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_DISBURSEMENTTYPE` dt
        ON f.DISBURSEMENTTYPEID = dt.ID   
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_FINANCINGPRODUCTTYPE` fpt
        ON f.FINANCINGPRODUCTTYPEID = fpt.ID 
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_H95_PAYMENTFREQUENCY` pf
        ON f.PAYMENTFREQUENCYID = pf.CODE   
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MM5_YESNOOPTION4` yn2
        ON f.facilitieswithbank = yn2.ID 
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MM5_YESNOOPTION4` yn3
        ON f.workingcapitalfacilitieswith = yn3.ID            
)
 SELECT
    id,
    financingamountrequested,
    tenor,
    graceperiod,
    facilitytype,
    revolvingloan,
    availabilityperiod,
    disbursementtype,
    financingproducttype,
    financingproducttypeother,
    paymentfrequency,
    TRY_CAST(NULLIF(CAST(bankapprovaldate AS STRING), '') AS TIMESTAMP) as bankapprovaldate,
    facilitieswithbank,
    workingcapitalfacilitieswith,
    internalriskrating,
    securitycoverage,
    cashconversioncycle,
    debtservicecoverageratio,
    machineryandequipment,
    technology,
    marketingandbranding,
    workingcapital,
    fixturesandfittings,
    facilitybreakupotheramount,
    facilitybreakupothervalue,
    commentworkcapital,
    commentrevolvingloan,
    fixedassetsamtrequested,
    fixedassestsamtremaining,
    workingcapitalamtrequested,
    workingcapitalamtremaining,
    commentworkingcapital,
    financingamtrequested,
    financingamtremaining,
    workingcapitalcapid,
    workingcapitalremaningcap,
    financingamtcapid,
    financingamtremaningcap,
    isanyfieldupdatedforamendmen,
    is_deleted,
    isactive,
    createdby,
    updatedby,
    TRY_CAST(NULLIF(CAST(createdon AS STRING), '') AS TIMESTAMP) as createdon,
    TRY_CAST(NULLIF(CAST(updatedon AS STRING), '') AS TIMESTAMP) as updatedon,
    dbt_updated_at,
    UPPER(NULLIF(TRIM(CAST(source_system_name AS STRING)), '')) as source_system_name
from source_cte 
WHERE rnk=1
),

silver_layer AS (
SELECT
    id,
    financingamountrequested,
    tenor,
    graceperiod,
    facilitytype,
    revolvingloan,
    availabilityperiod,
    disbursementtype,
    financingproducttype,
    financingproducttypeother,
    paymentfrequency,
    bankapprovaldate,
    facilitieswithbank,
    workingcapitalfacilitieswith,
    internalriskrating,
    securitycoverage,
    cashconversioncycle,
    debtservicecoverageratio,
    machineryandequipment,
    technology,
    marketingandbranding,
    workingcapital,
    fixturesandfittings,
    facilitybreakupotheramount,
    facilitybreakupothervalue,
    commentworkcapital,
    commentrevolvingloan,
    fixedassetsamtrequested,
    fixedassestsamtremaining,
    workingcapitalamtrequested,
    workingcapitalamtremaining,
    commentworkingcapital,
    financingamtrequested,
    financingamtremaining,
    workingcapitalcapid,
    workingcapitalremaningcap,
    financingamtcapid,
    financingamtremaningcap,
    isanyfieldupdatedforamendmen,
    is_deleted,
    isactive,
    createdby,
    updatedby,
    createdon,
    updatedon,
    dbt_updated_at,
    source_system_name
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`financing_base`
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'financingamountrequested'),
        (3, 'tenor'),
        (4, 'graceperiod'),
        (5, 'facilitytype'),
        (6, 'revolvingloan'),
        (7, 'availabilityperiod'),
        (8, 'disbursementtype'),
        (9, 'financingproducttype'),
        (10, 'financingproducttypeother'),
        (11, 'paymentfrequency'),
        (12, 'bankapprovaldate'),
        (13, 'facilitieswithbank'),
        (14, 'workingcapitalfacilitieswith'),
        (15, 'internalriskrating'),
        (16, 'securitycoverage'),
        (17, 'cashconversioncycle'),
        (18, 'debtservicecoverageratio'),
        (19, 'machineryandequipment'),
        (20, 'technology'),
        (21, 'marketingandbranding'),
        (22, 'workingcapital'),
        (23, 'fixturesandfittings'),
        (24, 'facilitybreakupotheramount'),
        (25, 'facilitybreakupothervalue'),
        (26, 'commentworkcapital'),
        (27, 'commentrevolvingloan'),
        (28, 'fixedassetsamtrequested'),
        (29, 'fixedassestsamtremaining'),
        (30, 'workingcapitalamtrequested'),
        (31, 'workingcapitalamtremaining'),
        (32, 'commentworkingcapital'),
        (33, 'financingamtrequested'),
        (34, 'financingamtremaining'),
        (35, 'workingcapitalcapid'),
        (36, 'workingcapitalremaningcap'),
        (37, 'financingamtcapid'),
        (38, 'financingamtremaningcap'),
        (39, 'isanyfieldupdatedforamendmen'),
        (40, 'is_deleted'),
        (41, 'isactive'),
        (42, 'createdby'),
        (43, 'updatedby'),
        (44, 'createdon'),
        (45, 'updatedon'),
        (46, 'dbt_updated_at'),
        (47, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'id'),
        (2, 'financingamountrequested'),
        (3, 'tenor'),
        (4, 'graceperiod'),
        (5, 'facilitytype'),
        (6, 'revolvingloan'),
        (7, 'availabilityperiod'),
        (8, 'disbursementtype'),
        (9, 'financingproducttype'),
        (10, 'financingproducttypeother'),
        (11, 'paymentfrequency'),
        (12, 'bankapprovaldate'),
        (13, 'facilitieswithbank'),
        (14, 'workingcapitalfacilitieswith'),
        (15, 'internalriskrating'),
        (16, 'securitycoverage'),
        (17, 'cashconversioncycle'),
        (18, 'debtservicecoverageratio'),
        (19, 'machineryandequipment'),
        (20, 'technology'),
        (21, 'marketingandbranding'),
        (22, 'workingcapital'),
        (23, 'fixturesandfittings'),
        (24, 'facilitybreakupotheramount'),
        (25, 'facilitybreakupothervalue'),
        (26, 'commentworkcapital'),
        (27, 'commentrevolvingloan'),
        (28, 'fixedassetsamtrequested'),
        (29, 'fixedassestsamtremaining'),
        (30, 'workingcapitalamtrequested'),
        (31, 'workingcapitalamtremaining'),
        (32, 'commentworkingcapital'),
        (33, 'financingamtrequested'),
        (34, 'financingamtremaining'),
        (35, 'workingcapitalcapid'),
        (36, 'workingcapitalremaningcap'),
        (37, 'financingamtcapid'),
        (38, 'financingamtremaningcap'),
        (39, 'isanyfieldupdatedforamendmen'),
        (40, 'is_deleted'),
        (41, 'isactive'),
        (42, 'createdby'),
        (43, 'updatedby'),
        (44, 'createdon'),
        (45, 'updatedon'),
        (46, 'dbt_updated_at'),
        (47, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`financingamountrequested` AS STRING) AS `financingamountrequested`,
        CAST(`tenor` AS STRING) AS `tenor`,
        CAST(`graceperiod` AS STRING) AS `graceperiod`,
        CAST(`facilitytype` AS STRING) AS `facilitytype`,
        CAST(`revolvingloan` AS STRING) AS `revolvingloan`,
        CAST(`availabilityperiod` AS STRING) AS `availabilityperiod`,
        CAST(`disbursementtype` AS STRING) AS `disbursementtype`,
        CAST(`financingproducttype` AS STRING) AS `financingproducttype`,
        CAST(`financingproducttypeother` AS STRING) AS `financingproducttypeother`,
        CAST(`paymentfrequency` AS STRING) AS `paymentfrequency`,
        CAST(`bankapprovaldate` AS STRING) AS `bankapprovaldate`,
        CAST(`facilitieswithbank` AS STRING) AS `facilitieswithbank`,
        CAST(`workingcapitalfacilitieswith` AS STRING) AS `workingcapitalfacilitieswith`,
        CAST(`internalriskrating` AS STRING) AS `internalriskrating`,
        CAST(`securitycoverage` AS STRING) AS `securitycoverage`,
        CAST(`cashconversioncycle` AS STRING) AS `cashconversioncycle`,
        CAST(`debtservicecoverageratio` AS STRING) AS `debtservicecoverageratio`,
        CAST(`machineryandequipment` AS STRING) AS `machineryandequipment`,
        CAST(`technology` AS STRING) AS `technology`,
        CAST(`marketingandbranding` AS STRING) AS `marketingandbranding`,
        CAST(`workingcapital` AS STRING) AS `workingcapital`,
        CAST(`fixturesandfittings` AS STRING) AS `fixturesandfittings`,
        CAST(`facilitybreakupotheramount` AS STRING) AS `facilitybreakupotheramount`,
        CAST(`facilitybreakupothervalue` AS STRING) AS `facilitybreakupothervalue`,
        CAST(`commentworkcapital` AS STRING) AS `commentworkcapital`,
        CAST(`commentrevolvingloan` AS STRING) AS `commentrevolvingloan`,
        CAST(`fixedassetsamtrequested` AS STRING) AS `fixedassetsamtrequested`,
        CAST(`fixedassestsamtremaining` AS STRING) AS `fixedassestsamtremaining`,
        CAST(`workingcapitalamtrequested` AS STRING) AS `workingcapitalamtrequested`,
        CAST(`workingcapitalamtremaining` AS STRING) AS `workingcapitalamtremaining`,
        CAST(`commentworkingcapital` AS STRING) AS `commentworkingcapital`,
        CAST(`financingamtrequested` AS STRING) AS `financingamtrequested`,
        CAST(`financingamtremaining` AS STRING) AS `financingamtremaining`,
        CAST(`workingcapitalcapid` AS STRING) AS `workingcapitalcapid`,
        CAST(`workingcapitalremaningcap` AS STRING) AS `workingcapitalremaningcap`,
        CAST(`financingamtcapid` AS STRING) AS `financingamtcapid`,
        CAST(`financingamtremaningcap` AS STRING) AS `financingamtremaningcap`,
        CAST(`isanyfieldupdatedforamendmen` AS STRING) AS `isanyfieldupdatedforamendmen`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`createdby` AS STRING) AS `createdby`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`financingamountrequested` AS STRING) AS `financingamountrequested`,
        CAST(`tenor` AS STRING) AS `tenor`,
        CAST(`graceperiod` AS STRING) AS `graceperiod`,
        CAST(`facilitytype` AS STRING) AS `facilitytype`,
        CAST(`revolvingloan` AS STRING) AS `revolvingloan`,
        CAST(`availabilityperiod` AS STRING) AS `availabilityperiod`,
        CAST(`disbursementtype` AS STRING) AS `disbursementtype`,
        CAST(`financingproducttype` AS STRING) AS `financingproducttype`,
        CAST(`financingproducttypeother` AS STRING) AS `financingproducttypeother`,
        CAST(`paymentfrequency` AS STRING) AS `paymentfrequency`,
        CAST(`bankapprovaldate` AS STRING) AS `bankapprovaldate`,
        CAST(`facilitieswithbank` AS STRING) AS `facilitieswithbank`,
        CAST(`workingcapitalfacilitieswith` AS STRING) AS `workingcapitalfacilitieswith`,
        CAST(`internalriskrating` AS STRING) AS `internalriskrating`,
        CAST(`securitycoverage` AS STRING) AS `securitycoverage`,
        CAST(`cashconversioncycle` AS STRING) AS `cashconversioncycle`,
        CAST(`debtservicecoverageratio` AS STRING) AS `debtservicecoverageratio`,
        CAST(`machineryandequipment` AS STRING) AS `machineryandequipment`,
        CAST(`technology` AS STRING) AS `technology`,
        CAST(`marketingandbranding` AS STRING) AS `marketingandbranding`,
        CAST(`workingcapital` AS STRING) AS `workingcapital`,
        CAST(`fixturesandfittings` AS STRING) AS `fixturesandfittings`,
        CAST(`facilitybreakupotheramount` AS STRING) AS `facilitybreakupotheramount`,
        CAST(`facilitybreakupothervalue` AS STRING) AS `facilitybreakupothervalue`,
        CAST(`commentworkcapital` AS STRING) AS `commentworkcapital`,
        CAST(`commentrevolvingloan` AS STRING) AS `commentrevolvingloan`,
        CAST(`fixedassetsamtrequested` AS STRING) AS `fixedassetsamtrequested`,
        CAST(`fixedassestsamtremaining` AS STRING) AS `fixedassestsamtremaining`,
        CAST(`workingcapitalamtrequested` AS STRING) AS `workingcapitalamtrequested`,
        CAST(`workingcapitalamtremaining` AS STRING) AS `workingcapitalamtremaining`,
        CAST(`commentworkingcapital` AS STRING) AS `commentworkingcapital`,
        CAST(`financingamtrequested` AS STRING) AS `financingamtrequested`,
        CAST(`financingamtremaining` AS STRING) AS `financingamtremaining`,
        CAST(`workingcapitalcapid` AS STRING) AS `workingcapitalcapid`,
        CAST(`workingcapitalremaningcap` AS STRING) AS `workingcapitalremaningcap`,
        CAST(`financingamtcapid` AS STRING) AS `financingamtcapid`,
        CAST(`financingamtremaningcap` AS STRING) AS `financingamtremaningcap`,
        CAST(`isanyfieldupdatedforamendmen` AS STRING) AS `isanyfieldupdatedforamendmen`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`isactive` AS STRING) AS `isactive`,
        CAST(`createdby` AS STRING) AS `createdby`,
        CAST(`updatedby` AS STRING) AS `updatedby`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`
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
        'financing_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'financing_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'financing_base' AS table_name,
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
        'financing_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'financing_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
