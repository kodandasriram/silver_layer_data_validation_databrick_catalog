-- Compare bronze-layer query output with silver-layer table output for product_services_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\union of os2_os1_mis\product_services_base_os2_os1_mis_union_bronze_layer.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\product_services_base_silver_layer.sql
-- NOTE: Generated with only the 120 common columns for data mismatch counts because source column names/order differ.


WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for product_services_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: product_services_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH product_and_services_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : PRODUCT_AND_SERVICES_BASE_OS2
Description : This model extracts and transforms product and service-related
              attributes from the OS2 source system Bronze Layer and loads them
              into the PRODUCT_AND_SERVICES target table as part of the Silver
              Layer data pipeline.

              The model combines both product and service records using UNION ALL,
              enriches application support data with customer, amendment, and
              assessment details, and standardizes financial, quantity, and
              support-related attributes for downstream reporting and analytics.

Source Tables : neo2.OSUSR_1AT_ASSESSMENT
                neo2.OSSYS_BPM_PROCESS
                neo2.OSSYS_BPM_ACTIVITY
                neo2.OSUSR_1AT_ASSESSMENTSTATUS
                neo2.OSUSR_IEX_SERVICE
                neo2.OSUSR_IEX_PRODUCT
                neo2.OSUSR_2DA_APPLICATIONSUPPORT
                neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_NTP_AMENDMENTREQUEST
                neo2.OSUSR_NTP_APPLICATIONCUSTOMER
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_COMPANY

Target Table : PRODUCT_AND_SERVICES_BASE_OS2
Load Type    : Full Load
Materialized : table
Format       : PARQUET
Tags         : os2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author     | Description
--------------------------------------------------------------
1.0     | 2026-05-12 | Venkatesh  | Initial version

================================================================================================= 
*/
WITH TEMP_ASSESSMENT AS (

    SELECT
        ACT.NAME                                   AS ACTIVITY_NAME,
        ASSESSMENTSTATUS.LABEL                     AS LABEL,
        ASS.APPLICATIONID,
        ASS.AMENDMENTREQUESTID,
        ACT.CLOSED,
        ROW_NUMBER() OVER (
            PARTITION BY ASS.APPLICATIONID, ASS.AMENDMENTREQUESTID
            ORDER BY ACT.ID DESC
        )                                          AS RN,
        CASE
            WHEN ACT.NAME LIKE 'Approve%'
                 AND ASSESSMENTSTATUS.LABEL = 'Confirmed'
            THEN 'Yes'
            ELSE 'No'
        END                                        AS APPROVAL
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ASS
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_PROCESS PRO
        ON PRO.TOP_PROCESS_ID = ASS.PROCESSID
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_BPM_ACTIVITY ACT
        ON ACT.PROCESS_ID = PRO.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS ASSESSMENTSTATUS
        ON ASS.ASSESSMENTSTATUSID = ASSESSMENTSTATUS.CODE
    WHERE ACT.NAME LIKE 'Approve%'
      AND ASSESSMENTSTATUS.LABEL = 'Confirmed'

),

TEMP_PRO_SRV AS (

    SELECT
        APPLICATIONSUPPORTID,
        ITEMPCT,
        SERVICENAME                           AS ITEMNAME,
        CAST(NULL AS VARCHAR)                                  AS ITEMMODELCODE,
        CAST(NULL AS VARCHAR)                                  AS ITEMBRANDINGNAME,
        SERVICEDESCRIPTION,
        PRODUCTMAKE,
        MODEL,
        SUBSCRIPTIONSTARTDATE,
        SUBSCRIPTIONENDDATE,
        SUBSCRIPTIONQTDREQUEST,
        SUBSCRIPTIONQTD,
        SUBSCRIPTIONNUMBERPAYMENTS,
        ITEMCOSTCURRENCYID,
        CUSTOMERCOSTFX,
        ITEMLINEDISCOUNTAMT,
        ITEMCOSTVATPCT,
        ITEMQTD,
        ITEMQTDREQUEST,
        ITEMQTDAVAILABLE,
        ITEMQTDINPROGRESS,
        ITEMQTDCLAIMED,
        ITEMQTDDELIVERED,
        ITEMQTYCANCELLED,
        ITEMQUOTEDISCOUNTAMT,
        ITEMCOSTDISCOUNTAMT,
        ITEMCONFIGCAP,
        ITEMVATAMT_FC,
        ITEMVATAMT,
        ITEMVATAMTTOTAL_FC,
        ITEMVATAMTTOTAL,
        ITEMCOSTUN,
        ITEMCOSTNOVATAMT,
        ITEMCOSTTOTAL,
        ITEMCOSTUN_FC,
        ITEMCOSTNOVATAMT_FC,
        ITEMCOSTTOTAL_FC,
        SUPPORTEDAMT,
        SUPPORTEDAMT_FC,
        TKSHAREUNAUTO,
        TKSHAREUNOVR,
        TKSHAREUN,
        TKSHAREAUTOPCT,
        TKSHAREACTUALPCT,
        TKSHARETOTALAUTO,
        TKSHARETOTALOVR,
        TKSHARETOTAL,
        CUSTOMERSHAREUN,
        CUSTOMERSHARETOTAL,
        CREATEDBY,
        CREATEDON,
        UPDATEDBY,
        UPDATEDON,
        ITEMLINEDISCOUNTAMT_FC,
        ITEMQUOTEDISCOUNTAMT_FC,
        ALLOWOFFLINEPAYMENT,
        'SERVICE'                             AS PS_TYPE
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_IEX_SERVICE

    UNION ALL

    SELECT
        APPLICATIONSUPPORTID,
        ITEMPCT,
        ITEMNAME,
        ITEMMODELCODE,
        ITEMBRANDINGNAME,
        CAST(NULL AS VARCHAR)                                  AS SERVICEDESCRIPTION,
        CAST(NULL AS VARCHAR)                                  AS PRODUCTMAKE,
        CAST(NULL AS VARCHAR)                                  AS MODEL,
        CAST(NULL AS TIMESTAMP)                                  AS SUBSCRIPTIONSTARTDATE,
        CAST(NULL AS TIMESTAMP)                                  AS SUBSCRIPTIONENDDATE,
        ITEMQTDREQUEST,
        ITEMQTD,
        CAST(NULL AS INTEGER)                                  AS SUBSCRIPTIONNUMBERPAYMENTS,
        ITEMCOSTCURRENCYID,
        CUSTOMERCOSTFX,
        ITEMLINEDISCOUNTAMT,
        ITEMCOSTVATPCT,
        ITEMQTD,
        ITEMQTDREQUEST,
        ITEMQTDAVAILABLE,
        ITEMQTDINPROGRESS,
        ITEMQTDCLAIMED,
        ITEMQTDDELIVERED,
        ITEMQTYCANCELLED,
        ITEMQUOTEDISCOUNTAMT,
        ITEMCOSTDISCOUNTAMT,
        ITEMCONFIGCAP,
        ITEMVATAMTUN_FC                        AS ITEMVATAMT_FC,
        ITEMVATAMTUN                           AS ITEMVATAMT,
        ITEMVATAMTTOTAL_FC,
        ITEMVATAMTTOTAL,
        ITEMCOSTUN,
        ITEMCOSTNOVATAMT,
        ITEMCOSTTOTAL,
        ITEMCOSTUN_FC,
        ITEMCOSTNOVATAMT_FC,
        ITEMCOSTTOTAL_FC,
        SUPPORTEDAMT,
        SUPPORTEDAMT_FC,
        TKSHAREUNAUTO,
        TKSHAREUNOVR,
        TKSHAREUN,
        TKSHAREAUTOPCT,
        TKSHAREACTUALPCT,
        TKSHARETOTALAUTO,
        TKSHARETOTALOVR,
        TKSHARETOTAL,
        CUSTOMERSHAREUN,
        CUSTOMERSHARETOTAL,
        CREATEDBY,
        CREATEDON,
        UPDATEDBY,
        UPDATEDON,
        ITEMLINEDISCOUNTAMT_FC,
        ITEMQUOTEDISCOUNTAMT_FC,
        ALLOWOFFLINEPAYMENT,
        'PRODUCT'                             AS PS_TYPE
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_IEX_PRODUCT

),

FINAL_DATA AS (

    SELECT
    current_timestamp + INTERVAL '3' HOUR AS extract_date,

    appsup.applicationid AS id_application,

    app.referencenumber AS application_no,

    appsup.amendmentrequestid AS id_amendment,

    CASE
        WHEN appsup.createdby IN ('NEOT1 Migration User', 'NEOT1 Migration User_2')
            THEN 'Outsystem 2.0 (Migrated)'
        ELSE 'Outsystem 2.0'
    END AS source_system,

    appsup.id AS id_application_support,

    app.submittedon + INTERVAL '3' HOUR AS submitted_on_application,

    amdment.submittedon + INTERVAL '3' HOUR AS submitted_on_amendment,

    CASE
        WHEN app_sta.label = 'Active'
            THEN app.approvedon + INTERVAL '3' HOUR
        ELSE NULL
    END AS approved_on_application,

    CASE
        WHEN appsup.amendmentrequestid IS NULL THEN
            CASE
                WHEN app.approvedon = TIMESTAMP '1900-01-01 00:00:00'
                    THEN NULL
                ELSE app.approvedon + INTERVAL '3' HOUR
            END
        ELSE
            CASE
                WHEN amdment.approvedon = TIMESTAMP '1900-01-01 00:00:00'
                     AND asses_amed.closed = TIMESTAMP '1900-01-01 00:00:00'
                    THEN NULL
                WHEN asses_amed.closed <> TIMESTAMP '1900-01-01 00:00:00'
                    THEN asses_amed.closed + INTERVAL '3' HOUR
                ELSE amdment.approvedon + INTERVAL '3' HOUR
            END
    END AS approved_on_new,

    cus.nameen AS commercial_name,

    cmp.code AS cr_license_no,

    suptype.label AS scheme,

    appsupstat.label AS workflow_status_application_support,

    price.label AS workflow_status_price_check,

    provloccr.code AS vendor_cr_license_no,

    CASE
        WHEN appsup.providerid IS NOT NULL
            THEN provloc.nameen
        WHEN appsup.externalproviderid IS NOT NULL
            THEN provoverseas.name
        ELSE NULL
    END AS vendor_name,

    CASE
        WHEN appsup.providerid > 0
            THEN 'Bahrain'
        WHEN appsup.externalproviderid > 0
            THEN countryvendor.countryname
        ELSE NULL
    END AS vendor_location,

    CASE
        WHEN appsup.providerid > 0
            THEN 'Local'
        WHEN appsup.externalproviderid > 0
            THEN 'Overseas'
        ELSE NULL
    END AS vendor_country,

    prt.label AS payment_type_application,

    appsup.applicationsupportactionid AS process_type,

    isactive.label AS is_active,

    appsup.createdon + INTERVAL '3' HOUR AS created_on_application_support,

    ps.createdon + INTERVAL '3' HOUR AS created_on_item,

    ps.updatedon + INTERVAL '3' HOUR AS updated_on_item,

    ps.servicedescription AS service_description,

    ps.itemname AS item_name,

    ps.ps_type,

    appsup.activestatusid AS active_status_item,

    CASE
        WHEN ps.itemmodelcode IS NOT NULL
            THEN ps.itemmodelcode
        ELSE ps.model
    END AS item_code,

    CASE
        WHEN ps.itembrandingname IS NOT NULL
            THEN ps.itembrandingname
        ELSE ps.productmake
    END AS item_brand,

    ps.itemcostcurrencyid AS currency,

    ps.customercostfx AS currency_fx_rate,

    ps.itemlinediscountamt AS discount_per_quantity,

    ps.itemcostvatpct AS vat_pct,

    ps.itemqtd AS quantity,

    ps.itemqtdrequest AS quantity_requested,

    ps.itemqtdavailable AS quantity_available_to_claim,

    ps.itemqtdinprogress AS quantity_payment_in_process,

    ps.itemqtdclaimed AS quantity_paid,

    ps.itemqtddelivered AS quantity_delivered,

    ps.itemqtycancelled AS quantity_cancelled,

    ps.itemvatamt AS vat_per_quantity,

    ps.itemvatamttotal AS vat_total,

    ps.itemcostun AS item_cost_per_quantity_with_vat,

    ps.itemcostnovatamt AS item_cost_per_quantity_without_vat,

    ps.itemcosttotal AS item_cost_total_with_vat,

    ps.supportedamt AS item_cost_total_without_vat,

    ps.tkshareun AS tamkeen_share_per_quantity,

    ps.tkshareactualpct AS tamkeen_share_pct_total,

    ps.tksharetotal AS tamkeen_share_total,

    ps.customershareun AS customer_share_per_quantity,

    ps.customersharetotal AS customer_share_total,

    allowofflinepayment.label AS allow_offline_payment,

    ps.itemlinediscountamt * ps.itemqtd AS discount_total,

    CASE
        WHEN prt.label = 'Fawateer'
            THEN 'Yes'
        ELSE 'No'
    END AS is_fawateer,
    FALSE as is_deleted,
    'NEO2' AS source_system_name,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS dbt_updated_at

FROM temp_pro_srv ps

INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT appsup
    ON ps.applicationsupportid = appsup.id

INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION app
    ON app.id = appsup.applicationid

INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS app_sta
    ON app_sta.code = app.applicationstatusid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_PRICECHECKSTATUS price
    ON price.code = appsup.pricecheckstatusid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORTSTATUS appsupstat
    ON appsup.applicationsupportstatusid = appsupstat.code

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_YESNOOPTION isactive
    ON appsup.isactive = isactive.isactive

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_PROVIDERTYPE provtype
    ON provtype.id = appsup.providertypeid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER provloc
    ON provloc.id = appsup.providerid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY provloccr
    ON provloccr.id = provloc.id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EXTERNALPROVIDER provoverseas
    ON provoverseas.id = appsup.externalproviderid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY countryvendor
    ON countryvendor.id = provoverseas.countryid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER appcus
    ON app.id = appcus.applicationid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE cusprof
    ON cusprof.id = appcus.customerprofileid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER cus
    ON cusprof.customerid = cus.id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY cmp
    ON cus.id = cmp.id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_PAYMENTREQUESTTYPES prt
    ON appsup.typeofpaymentid = prt.code

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_YESNOOPTION allowofflinepayment
    ON ps.allowofflinepayment = allowofflinepayment.id

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_SUPPORTTYPE suptype
    ON suptype.code = appsup.supporttypeid

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_AMENDMENTREQUEST amdment
    ON amdment.id = appsup.amendmentrequestid

LEFT JOIN  TEMP_ASSESSMENT asses_amed
    ON asses_amed.amendmentrequestid = appsup.amendmentrequestid
   AND (
        asses_amed.rn = 1
        OR asses_amed.rn IS NULL
       )
WHERE appsup.isactive = TRUE

ORDER BY item_name

)

SELECT *
FROM FINAL_DATA
),
product_services_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR) AS option_key,
        max(sm.value) AS option_value
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".STRINGMAP sm
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR)
),
option_set_map AS (
    SELECT map_agg(option_key, option_value) AS option_values
    FROM option_set_values
),
/*
============================================================================
silver_product_services_mis.sql
============================================================================
Per-source intermediate Silver model for the Product & Services domain â€” MIS only.

The Product & Services domain captures Enterprise Support (ES) item-related
entities: items being purchased, requests for those items, special conditions
attached to items, and vendors supplying them.

Sources (Product & Services domain entities):
  â˜… TMKN_ESITEMSBASE                    â†’ ES Items (items purchased per ES application)
  â˜… TMKN_ITEMREQUESTBASE                â†’ ES Item Requests (request for items)
  â˜… TMKN_ESITEMSBASESPECIALCONDITIONBASE    â†’ Special conditions attached to ES items
    tmkn_vendor                     â†’ Vendor lookup (joined to esitems for vendor info)
    tmkn_tapproduct                 â†’ TAP product lookup (joined to esitems for product info)
    tmkn_TMKN_ESITEMSBASEspecialcondition_tmkn_esite â†’ M2M bridge: special condition â†” items

Reference SPs:
  - RPT-032_ES_Items                       (anchor: TMKN_ESITEMSBASE with all joins)
  - RPT-033_ES_Item_Request                (anchor: TMKN_ITEMREQUESTBASE)
  - RPT-089_ES_Items_Special_Condition     (anchor: TMKN_ESITEMSBASESPECIALCONDITIONBASE)

Structure decision:
  - Three parallel anchor entities (esitems, itemrequest, special_condition)
    are UNIONed because they're separate entities with different lifecycles.
  - Within each branch, related lookups (vendor, tapproduct, item-name) are
    JOINed because they're truly hierarchical (an ES item HAS a vendor, HAS
    a product reference, etc.).
  - Cross-domain references (tmkn_application, tmkn_company) are NOT joined
    here â€” preserved as FK columns for downstream re-joining.

The product_services_subtype column identifies which sub-type each row is:
  - ES_ITEM
  - ITEM_REQUEST
  - SPECIAL_CONDITION
============================================================================
*/


-- ============================================================================
-- SUB-TYPE 1: ES Items (the actual items purchased)
-- Anchor: TMKN_ESITEMSBASE with JOINs to itemrequest, vendor, tapproduct
-- ============================================================================
final_base as (
SELECT DISTINCT
    'ES_ITEM' AS product_services_subtype,
    'TMKN_ESITEMSBASE' AS mis_source_table,

    -- Identifiers
    CAST(item.tmkn_esitemsid AS VARCHAR)                 AS entity_id,
    item.tmkn_id                                         AS entity_external_id,
    item.tmkn_item_name                                  AS item_name,

    -- Foreign keys (preserved for cross-domain re-joining)
    CAST(item.tmkn_itemreq AS VARCHAR)                   AS item_request_id,
    CAST(item.tmkn_paymentrequest AS VARCHAR)            AS payment_request_id,
    CAST(item.tmkn_sitevisit AS VARCHAR)                 AS site_visit_id,
    CAST(item.tmkn_vendor AS VARCHAR)                    AS vendor_id,
    CAST(item.tmkn_item AS VARCHAR)                      AS tap_product_id,

    -- Display names (denormalised at source)
    item.tmkn_esapplication                          AS es_application_name,
    item.tmkn_paymentrequest                         AS payment_request_name,
    item.tmkn_sitevisit                              AS site_visit_name,
    item.tmkn_vendor                                 AS vendor_display_name,
    item.tmkn_product                                AS product_name,
    item.tmkn_itemreq                                AS item_request_name,
    item.tmkn_currency                               AS currency,
    item.tmkn_scheme                                 AS scheme,
    item.tmkn_benefitpayment                         AS benefit_payment_name,

    -- Item attributes
    item.tmkn_quantity                                   AS quantity,
    item.tmkn_existingquantity                           AS existing_quantity,
    item.tmkn_itemcost                                   AS item_cost,
    item.tmkn_totalcost                                  AS total_cost,
    item.tmkn_totalvat                                   AS total_vat,
    item.tmkn_vatpercentage                              AS vat_percentage,
    item.tmkn_tamkeenshare                               AS tamkeen_share,
    item.tmkn_bcshare                                    AS bc_share,
    item.tmkn_discount                                   AS discount,
    item.tmkn_fxrate                                     AS fx_rate,

    -- Vendor details (from joined tmkn_vendor)
    vnd.tmkn_vendorname                                  AS vendor_canonical_name,
    item.tmkn_vendor_name                                AS vendor_name_text,
    item.tmkn_vendor_id                                  AS vendor_id_or_cr,
    CASE WHEN vnd.tmkn_country IS NOT NULL
         THEN COALESCE(vnd.tmkn_country, '')
         ELSE COALESCE(item.tmkn_vendorcountry, '') END AS vendor_country,

    -- Product details (from joined tmkn_tapproduct)
    product.tmkn_category                            AS main_category,
    product.tmkn_category2                           AS sub_category,
    product.tmkn_highlevelcategory                   AS high_level_category,
    product.tmkn_productcode                             AS product_code,
    product.tmkn_productmake                             AS product_make,

    -- Item classification
    COALESCE(item.tmkn_itemmas, '')                  AS mas_category,
    COALESCE(item.tmkn_category,0)                     AS quality_category,
    item.tmkn_cloud_product_subcategory              AS cloud_product_sub_category,

    -- Status / workflow (decoded)
    CASE WHEN item.tmkn_amendmentstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_amendmentstatus') || '|' || CAST(item.tmkn_amendmentstatus AS VARCHAR)) END     AS amendment_status,
    CASE WHEN item.tmkn_bcsharepaymentmethod IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_bcsharepaymentmethod') || '|' || CAST(item.tmkn_bcsharepaymentmethod AS VARCHAR)) END AS bc_share_payment_method,
    CASE WHEN item.tmkn_condition IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_condition') || '|' || CAST(item.tmkn_condition AS VARCHAR)) END           AS condition,
    CASE WHEN item.tmkn_discount_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_discount_type') || '|' || CAST(item.tmkn_discount_type AS VARCHAR)) END       AS discount_type,
    CASE WHEN item.tmkn_functionality IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_functionality') || '|' || CAST(item.tmkn_functionality AS VARCHAR)) END       AS functionality,
    CASE WHEN item.tmkn_itemstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_itemstatus') || '|' || CAST(item.tmkn_itemstatus AS VARCHAR)) END          AS item_status,
    CASE WHEN item.tmkn_monitoringfinished IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_monitoringfinished') || '|' || CAST(item.tmkn_monitoringfinished AS VARCHAR)) END  AS monitoring_finished,
    CASE WHEN item.tmkn_payableto IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_payableto') || '|' || CAST(item.tmkn_payableto AS VARCHAR)) END           AS payable_to,
    CASE WHEN item.tmkn_paymentshare IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_paymentshare') || '|' || CAST(item.tmkn_paymentshare AS VARCHAR)) END        AS payment_share,
    CASE WHEN item.tmkn_servicetype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_servicetype') || '|' || CAST(item.tmkn_servicetype AS VARCHAR)) END         AS service_type,
    CASE WHEN item.tmkn_sitevisitstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_sitevisitstatus') || '|' || CAST(item.tmkn_sitevisitstatus AS VARCHAR)) END     AS site_visit_status,
    CASE WHEN item.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('statecode') || '|' || CAST(item.statecode AS VARCHAR)) END                AS state,
    CASE WHEN item.tmkn_uploadcompleted IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_uploadcompleted') || '|' || CAST(item.tmkn_uploadcompleted AS VARCHAR)) END     AS upload_completed,
    CASE WHEN item.tmkn_vatapplicable IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSBASE') || '|' || lower('tmkn_vatapplicable') || '|' || CAST(item.tmkn_vatapplicable AS VARCHAR)) END       AS vat_applicable,

    -- Special-condition specific (NULL for this branch)
    CAST(NULL AS VARCHAR)                                AS special_condition_remarks,
    CAST(NULL AS VARCHAR)                                AS special_condition_portal_note,
    CAST(NULL AS VARCHAR)                                AS special_condition_workflow_status,

    -- Item-request specific (NULL for this branch)
    CAST(NULL AS VARCHAR)                                AS is_initial_request,
    CAST(NULL AS TIMESTAMP)                              AS submitted_on,
    CAST(NULL AS VARCHAR)                                AS workflow_status,

    -- Audit
    CAST(NULL AS TIMESTAMP)                              AS created_on,
    CAST(NULL AS TIMESTAMP)                              AS modified_on,
    CAST(NULL AS VARCHAR)                                AS created_by,
    CAST(NULL AS VARCHAR)                                AS modified_by,
    CAST(NULL AS VARCHAR)                                AS owner_name,

    -- Standard trailing
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_ESITEMSBASE item
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_VENDORBASE vnd
       ON vnd.tmkn_vendorid = item.tmkn_vendor
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_TAPPRODUCTBASE product
       ON product.tmkn_tapproductid = item.tmkn_item


UNION ALL


-- ============================================================================
-- SUB-TYPE 2: Item Request (request for items, parent of esitems)
-- Anchor: tmkn_itemrequest
-- ============================================================================
SELECT DISTINCT
    'ITEM_REQUEST' AS product_services_subtype,
    'TMKN_ITEMREQUESTBASE' AS mis_source_table,

    -- Identifiers
    CAST(req.tmkn_itemrequestid AS VARCHAR)              AS entity_id,
    req.tmkn_id                                          AS entity_external_id,
    CAST(NULL AS VARCHAR)                                AS item_name,

    -- Foreign keys
    CAST(req.tmkn_itemrequestid AS VARCHAR)              AS item_request_id,
    CAST(NULL AS VARCHAR)                                AS payment_request_id,
    CAST(NULL AS VARCHAR)                                AS site_visit_id,
    CAST(NULL AS VARCHAR)                                AS vendor_id,
    CAST(NULL AS VARCHAR)                                AS tap_product_id,

    -- Display names
    req.tmkn_esapplication                           AS es_application_name,
    CAST(NULL AS VARCHAR)                                AS payment_request_name,
    CAST(NULL AS VARCHAR)                                AS site_visit_name,
    CAST(NULL AS VARCHAR)                                AS vendor_display_name,
    CAST(NULL AS VARCHAR)                                AS product_name,
    CAST(NULL AS VARCHAR)                                AS item_request_name,
    CAST(NULL AS VARCHAR)                                AS currency,
    CAST(NULL AS VARCHAR)                                AS scheme,
    CAST(NULL AS VARCHAR)                                AS benefit_payment_name,

    -- Item attributes (NULL for this branch)
    CAST(NULL AS DECIMAL(18, 2))                         AS quantity,
    CAST(NULL AS DECIMAL(18, 2))                         AS existing_quantity,
    CAST(NULL AS DECIMAL(18, 2))                         AS item_cost,
    CAST(NULL AS DECIMAL(18, 2))                         AS total_cost,
    CAST(NULL AS DECIMAL(18, 2))                         AS total_vat,
    CAST(NULL AS DECIMAL(18, 2))                         AS vat_percentage,
    CAST(NULL AS DECIMAL(18, 2))                         AS tamkeen_share,
    CAST(NULL AS DECIMAL(18, 2))                         AS bc_share,
    CAST(NULL AS DECIMAL(18, 2))                         AS discount,
    CAST(NULL AS DECIMAL(18, 2))                         AS fx_rate,

    -- Vendor (NULL)
    CAST(NULL AS VARCHAR)                                AS vendor_canonical_name,
    CAST(NULL AS VARCHAR)                                AS vendor_name_text,
    CAST(NULL AS VARCHAR)                                AS vendor_id_or_cr,
    CAST(NULL AS VARCHAR)                                AS vendor_country,

    -- Product (NULL)
    CAST(NULL AS VARCHAR)                                AS main_category,
    CAST(NULL AS VARCHAR)                                AS sub_category,
    CAST(NULL AS VARCHAR)                                AS high_level_category,
    CAST(NULL AS VARCHAR)                                AS product_code,
    CAST(NULL AS VARCHAR)                                AS product_make,

    -- Classification (NULL)
    CAST(NULL AS VARCHAR)                                AS mas_category,
    CAST(NULL AS INTEGER)                                AS quality_category,
    CAST(NULL AS VARCHAR)                                AS cloud_product_sub_category,

    -- ES Item-specific status fields (NULL for this branch)
    CAST(NULL AS VARCHAR)                                AS amendment_status,
    CAST(NULL AS VARCHAR)                                AS bc_share_payment_method,
    CAST(NULL AS VARCHAR)                                AS condition,
    CAST(NULL AS VARCHAR)                                AS discount_type,
    CAST(NULL AS VARCHAR)                                AS functionality,
    CAST(NULL AS VARCHAR)                                AS item_status,
    CAST(NULL AS VARCHAR)                                AS monitoring_finished,
    CAST(NULL AS VARCHAR)                                AS payable_to,
    CAST(NULL AS VARCHAR)                                AS payment_share,
    CAST(NULL AS VARCHAR)                                AS service_type,
    CAST(NULL AS VARCHAR)                                AS site_visit_status,
    CASE WHEN req.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ITEMREQUESTBASE') || '|' || lower('statecode') || '|' || CAST(req.statecode AS VARCHAR)) END            AS state,
    CAST(NULL AS VARCHAR)                                AS upload_completed,
    CAST(NULL AS VARCHAR)                                AS vat_applicable,

    -- Special-condition (NULL)
    CAST(NULL AS VARCHAR)                                AS special_condition_remarks,
    CAST(NULL AS VARCHAR)                                AS special_condition_portal_note,
    CAST(NULL AS VARCHAR)                                AS special_condition_workflow_status,

    -- Item-request specific
    CASE WHEN req.tmkn_isinitialrequest IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ITEMREQUESTBASE') || '|' || lower('tmkn_isinitialrequest') || '|' || CAST(req.tmkn_isinitialrequest AS VARCHAR)) END AS is_initial_request,
    req.tmkn_submittedon                                 AS submitted_on,
    CASE WHEN req.tmkn_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ITEMREQUESTBASE') || '|' || lower('tmkn_workflowstatus') || '|' || CAST(req.tmkn_workflowstatus AS VARCHAR)) END   AS workflow_status,

    -- Audit
    CAST(NULL AS TIMESTAMP)                              AS created_on,
    req.modifiedon                                       AS modified_on,
    CAST(NULL AS VARCHAR)                                AS created_by,
    CAST(NULL AS VARCHAR)                                AS modified_by,
    CAST(NULL AS VARCHAR)                                AS owner_name,

    -- Standard trailing
    'MIS', FALSE, CURRENT_DATE, CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_ITEMREQUESTBASE req


UNION ALL


-- ============================================================================
-- SUB-TYPE 3: Special Condition (special conditions attached to ES items)
-- Anchor: TMKN_ESITEMSBASEspecialcondition with M2M JOIN to esitems via bridge
-- ============================================================================
SELECT DISTINCT
    'SPECIAL_CONDITION' AS product_services_subtype,
    'TMKN_ESITEMSSPECIALCONDITIONBASE' AS mis_source_table,

    -- Identifiers
    CAST(escon.tmkn_esitemsspecialconditionid AS VARCHAR) AS entity_id,
    CAST(NULL AS VARCHAR)                                AS entity_external_id,
    itm.tmkn_item_name                                   AS item_name,

    -- Foreign keys
    CAST(NULL AS VARCHAR)                                AS item_request_id,
    CAST(NULL AS VARCHAR)                                AS payment_request_id,
    CAST(NULL AS VARCHAR)                                AS site_visit_id,
    CAST(NULL AS VARCHAR)                                AS vendor_id,
    CAST(NULL AS VARCHAR)                                AS tap_product_id,

    -- Display names
    escon.tmkn_esapplication                         AS es_application_name,
    CAST(NULL AS VARCHAR)                                AS payment_request_name,
    CAST(NULL AS VARCHAR)                                AS site_visit_name,
    CAST(NULL AS VARCHAR)                                AS vendor_display_name,
    CAST(NULL AS VARCHAR)                                AS product_name,
    CAST(NULL AS VARCHAR)                                AS item_request_name,
    CAST(NULL AS VARCHAR)                                AS currency,
    CAST(NULL AS VARCHAR)                                AS scheme,
    CAST(NULL AS VARCHAR)                                AS benefit_payment_name,

    -- Item attributes (NULL for this branch)
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),

    CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),

    CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),

    CAST(NULL AS VARCHAR), CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR),

    -- ES Item-specific (NULL)
    CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
    CASE WHEN escon.tmkn_condition IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSSPECIALCONDITIONBASE') || '|' || lower('tmkn_condition') || '|' || CAST(escon.tmkn_condition AS VARCHAR)) END AS condition,
    CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
    CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
    CASE WHEN escon.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSSPECIALCONDITIONBASE') || '|' || lower('statuscode') || '|' || CAST(escon.statuscode AS VARCHAR)) END AS state,
    CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),

    -- Special-condition specific
    escon.tmkn_remarks                                   AS special_condition_remarks,
    escon.mis_portalnote                                 AS special_condition_portal_note,
    CASE WHEN escon.tmkn_workflowstatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('TMKN_ESITEMSSPECIALCONDITIONBASE') || '|' || lower('tmkn_workflowstatus') || '|' || CAST(escon.tmkn_workflowstatus AS VARCHAR)) END AS special_condition_workflow_status,

    -- Item-request specific (NULL)
    CAST(NULL AS VARCHAR), CAST(NULL AS TIMESTAMP), CAST(NULL AS VARCHAR),

    -- Audit
    escon.createdon                                      AS created_on,
    escon.modifiedon                                     AS modified_on,
    escon.createdby                                  AS created_by,
    escon.modifiedby                                 AS modified_by,
    escon.ownerid                                    AS owner_name,

    -- Standard trailing
    'MIS', FALSE, CURRENT_DATE, CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_ESITEMSSPECIALCONDITIONBASE escon
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_TMKN_ESITEMSSPECIALCONDITION_TMKN_ESITEBASE itmcon
       ON itmcon.tmkn_esitemsspecialconditionid = escon.tmkn_esitemsspecialconditionid
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_ESITEMSBASE itm
       ON itm.tmkn_esitemsid = itmcon.tmkn_esitemsid
)
SELECT
    product_services_subtype,
    mis_source_table,
    entity_id,
    entity_external_id,
    item_name,
    item_request_id,
    payment_request_id,
    site_visit_id,
    vendor_id,
    tap_product_id,
    es_application_name,
    payment_request_name,
    site_visit_name,
    vendor_display_name,
    product_name,
    item_request_name,
    currency,
    scheme,
    benefit_payment_name,
    quantity,
    existing_quantity,
    item_cost,
    total_cost,
    total_vat,
    vat_percentage,
    tamkeen_share,
    bc_share,
    discount,
    fx_rate,
    vendor_canonical_name,
    vendor_name_text,
    vendor_id_or_cr,
    vendor_country,
    main_category,
    sub_category,
    high_level_category,
    product_code,
    product_make,
    mas_category,
    quality_category,
    cloud_product_sub_category,
    amendment_status,
    bc_share_payment_method,
    "condition",
    discount_type,
    functionality,
    item_status,
    monitoring_finished,
    payable_to,
    payment_share,
    service_type,
    site_visit_status,
    state,
    upload_completed,
    vat_applicable,
    special_condition_remarks,
    special_condition_portal_note,
    special_condition_workflow_status,
    is_initial_request,
    submitted_on,
    workflow_status,
    created_on,
    modified_on,
    created_by,
    modified_by,
    owner_name,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at
from final_base
)
select
    --common
    currency,
    scheme,
    quantity,
    vendor_country,
    --os2
    extract_date,
    id_application,
    application_no,
    id_amendment,
    source_system,
    id_application_support,
    submitted_on_application,
    submitted_on_amendment,
    approved_on_application,
    approved_on_new,
    commercial_name,
    cr_license_no,
    workflow_status_application_support,
    workflow_status_price_check,
    vendor_cr_license_no,
    vendor_name,
    vendor_location,
    payment_type_application,
    process_type,
    is_active,
    created_on_application_support,
    service_description,
    item_name,
    ps_type,
    active_status_item,
    item_code,
    item_brand,
    currency_fx_rate,
    discount_per_quantity,
    vat_pct,
    quantity_requested,
    quantity_available_to_claim,
    quantity_payment_in_process,
    quantity_paid,
    quantity_delivered,
    quantity_cancelled,
    vat_per_quantity,
    vat_total,
    item_cost_per_quantity_with_vat,
    item_cost_per_quantity_without_vat,
    item_cost_total_with_vat,
    item_cost_total_without_vat,
    tamkeen_share_per_quantity,
    tamkeen_share_pct_total,
    tamkeen_share_total,
    customer_share_per_quantity,
    customer_share_total,
    allow_offline_payment,
    discount_total,
    is_fawateer,
    --mis
    cast(null as varchar) as product_services_subtype,
    cast(null as varchar) as mis_source_table,
    cast(null as varchar) as entity_id,
    cast(null as varchar) as entity_external_id,
    cast(null as varchar) as item_name_2,
    cast(null as varchar) as item_request_id,
    cast(null as varchar) as payment_request_id,
    cast(null as varchar) as site_visit_id,
    cast(null as varchar) as vendor_id,
    cast(null as varchar) as tap_product_id,
    cast(null as varchar) as es_application_name,
    cast(null as varchar) as payment_request_name,
    cast(null as varchar) as site_visit_name,
    cast(null as varchar) as vendor_display_name,
    cast(null as varchar) as product_name,
    cast(null as varchar) as item_request_name,
    cast(null as varchar) as benefit_payment_name,
    cast(null as decimal(18,2)) as existing_quantity,
    cast(null as decimal(18,2)) as item_cost,
    cast(null as decimal(18,2)) as total_cost,
    cast(null as decimal(18,2)) as total_vat,
    cast(null as decimal(18,2)) as vat_percentage,
    cast(null as decimal(18,2)) as tamkeen_share,
    cast(null as decimal(18,2)) as bc_share,
    cast(null as decimal(18,2)) as discount,
    cast(null as decimal(18,2)) as fx_rate,
    cast(null as varchar) as vendor_canonical_name,
    cast(null as varchar) as vendor_name_text,
    cast(null as varchar) as vendor_id_or_cr,
    cast(null as varchar) as main_category,
    cast(null as varchar) as sub_category,
    cast(null as varchar) as high_level_category,
    cast(null as varchar) as product_code,
    cast(null as varchar) as product_make,
    cast(null as varchar) as mas_category,
    cast(null as integer) as quality_category,
    cast(null as varchar) as cloud_product_sub_category,
    cast(null as varchar) as amendment_status,
    cast(null as varchar) as bc_share_payment_method,
    cast(null as varchar) as condition,
    cast(null as varchar) as discount_type,
    cast(null as varchar) as functionality,
    cast(null as varchar) as item_status,
    cast(null as varchar) as monitoring_finished,
    cast(null as varchar) as payable_to,
    cast(null as varchar) as payment_share,
    cast(null as varchar) as service_type,
    cast(null as varchar) as site_visit_status,
    cast(null as varchar) as state,
    cast(null as varchar) as upload_completed,
    cast(null as varchar) as vat_applicable,
    cast(null as varchar) as special_condition_remarks,
    cast(null as varchar) as special_condition_portal_note,
    cast(null as varchar) as special_condition_workflow_status,
    cast(null as varchar) as is_initial_request,
    cast(null as timestamp) as submitted_on,
    cast(null as varchar) as workflow_status,
       created_on_item as created_on,
       updated_on_item as updated_on,
    cast(null as timestamp) as modified_on,
    cast(null as varchar) as created_by,
    cast(null as varchar) as modified_by,
    cast(null as varchar) as owner_name,
    source_system_name,
    is_deleted,
    current_date as report_date,
    dbt_updated_at

from product_and_services_base_os2_source

union all

select
   --common
    currency,
    scheme,
    quantity,
    vendor_country,
    --os2
    cast(null as timestamp with time zone) as extract_date,
    cast(null as bigint) as id_application,
    cast(null as varchar) as application_no,
    cast(null as bigint) as id_amendment,
    cast(null as varchar) as source_system,
    cast(null as bigint) as id_application_support,
    cast(null as timestamp) as submitted_on_application,
    cast(null as timestamp) as submitted_on_amendment,
    cast(null as timestamp) as approved_on_application,
    cast(null as timestamp) as approved_on_new,
    cast(null as varchar) as commercial_name,
    cast(null as varchar) as cr_license_no,
    cast(null as varchar) as workflow_status_application_support,
    cast(null as varchar) as workflow_status_price_check,
    cast(null as varchar) as vendor_cr_license_no,
    cast(null as varchar) as vendor_name,
    cast(null as varchar) as vendor_location,
    cast(null as varchar) as payment_type_application,
    cast(null as varchar) as process_type,
    cast(null as varchar) as is_active,
    cast(null as timestamp) as created_on_application_support,
    cast(null as varchar) as service_description,
    cast(null as varchar) as item_name,
    cast(null as varchar) as ps_type,
    cast(null as varchar) as active_status_item,
    cast(null as varchar) as item_code,
    cast(null as varchar) as item_brand,
    cast(null as decimal) as currency_fx_rate,
    cast(null as decimal) as discount_per_quantity,
    cast(null as decimal) as vat_pct,
    cast(null as integer) as quantity_requested,
    cast(null as decimal) as quantity_available_to_claim,
    cast(null as decimal) as quantity_payment_in_process,
    cast(null as decimal) as quantity_paid,
    cast(null as decimal) as quantity_delivered,
    cast(null as integer) as quantity_cancelled,
    cast(null as decimal) as vat_per_quantity,
    cast(null as decimal) as vat_total,
    cast(null as decimal) as item_cost_per_quantity_with_vat,
    cast(null as decimal) as item_cost_per_quantity_without_vat,
    cast(null as decimal) as item_cost_total_with_vat,
    cast(null as decimal) as item_cost_total_without_vat,
    cast(null as decimal) as tamkeen_share_per_quantity,
    cast(null as decimal) as tamkeen_share_pct_total,
    cast(null as decimal) as tamkeen_share_total,
    cast(null as decimal) as customer_share_per_quantity,
    cast(null as decimal) as customer_share_total,
    cast(null as varchar) as allow_offline_payment,
    cast(null as decimal) as discount_total,
    cast(null as varchar) as is_fawateer,
    --mis
    product_services_subtype,
    mis_source_table,
    entity_id,
    entity_external_id,
    item_name,
    item_request_id,
    payment_request_id,
    site_visit_id,
    vendor_id,
    tap_product_id,
    es_application_name,
    payment_request_name,
    site_visit_name,
    vendor_display_name,
    product_name,
    item_request_name,
    benefit_payment_name,
    existing_quantity,
    item_cost,
    total_cost,
    total_vat,
    vat_percentage,
    tamkeen_share,
    bc_share,
    discount,
    fx_rate,
    vendor_canonical_name,
    vendor_name_text,
    vendor_id_or_cr,
    main_category,
    sub_category,
    high_level_category,
    product_code,
    product_make,
    mas_category,
    quality_category,
    cloud_product_sub_category,
    amendment_status,
    bc_share_payment_method,
    condition,
    discount_type,
    functionality,
    item_status,
    monitoring_finished,
    payable_to,
    payment_share,
    service_type,
    site_visit_status,
    state,
    upload_completed,
    vat_applicable,
    special_condition_remarks,
    special_condition_portal_note,
    special_condition_workflow_status,
    is_initial_request,
    submitted_on,
    workflow_status,
    created_on,
     cast(null as timestamp) as updated_on,
    modified_on,
    created_by,
    modified_by,
    owner_name,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at
from product_services_base_mis_source
),

silver_layer AS (
SELECT
    currency,
    scheme,
    quantity,
    vendor_country,
    extract_date,
    id_application,
    application_no,
    id_amendment,
    source_system,
    id_application_support,
    submitted_on_application,
    submitted_on_amendment,
    approved_on_application,
    approved_on_new,
    commercial_name,
    cr_license_no,
    workflow_status_application_support,
    workflow_status_price_check,
    vendor_cr_license_no,
    vendor_name,
    vendor_location,
    payment_type_application,
    process_type,
    is_active,
    created_on_application_support,
    service_description,
    item_name,
    ps_type,
    active_status_item,
    item_code,
    item_brand,
    currency_fx_rate,
    discount_per_quantity,
    vat_pct,
    quantity_requested,
    quantity_available_to_claim,
    quantity_payment_in_process,
    quantity_paid,
    quantity_delivered,
    quantity_cancelled,
    vat_per_quantity,
    vat_total,
    item_cost_per_quantity_with_vat,
    item_cost_per_quantity_without_vat,
    item_cost_total_with_vat,
    item_cost_total_without_vat,
    tamkeen_share_per_quantity,
    tamkeen_share_pct_total,
    tamkeen_share_total,
    customer_share_per_quantity,
    customer_share_total,
    allow_offline_payment,
    discount_total,
    is_fawateer,
    product_services_subtype,
    mis_source_table,
    entity_id,
    entity_external_id,
    item_name_2,
    item_request_id,
    payment_request_id,
    site_visit_id,
    vendor_id,
    tap_product_id,
    es_application_name,
    payment_request_name,
    site_visit_name,
    vendor_display_name,
    product_name,
    item_request_name,
    benefit_payment_name,
    existing_quantity,
    item_cost,
    total_cost,
    total_vat,
    vat_percentage,
    tamkeen_share,
    bc_share,
    discount,
    fx_rate,
    vendor_canonical_name,
    vendor_name_text,
    vendor_id_or_cr,
    main_category,
    sub_category,
    high_level_category,
    product_code,
    product_make,
    mas_category,
    quality_category,
    cloud_product_sub_category,
    amendment_status,
    bc_share_payment_method,
    condition,
    discount_type,
    functionality,
    item_status,
    monitoring_finished,
    payable_to,
    payment_share,
    service_type,
    site_visit_status,
    state,
    upload_completed,
    vat_applicable,
    special_condition_remarks,
    special_condition_portal_note,
    special_condition_workflow_status,
    is_initial_request,
    submitted_on,
    workflow_status,
    created_on,
    updated_on,
    modified_on,
    created_by,
    modified_by,
    owner_name,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".product_services_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'currency'),
        (2, 'scheme'),
        (3, 'quantity'),
        (4, 'vendor_country'),
        (5, 'extract_date'),
        (6, 'id_application'),
        (7, 'application_no'),
        (8, 'id_amendment'),
        (9, 'source_system'),
        (10, 'id_application_support'),
        (11, 'submitted_on_application'),
        (12, 'submitted_on_amendment'),
        (13, 'approved_on_application'),
        (14, 'approved_on_new'),
        (15, 'commercial_name'),
        (16, 'cr_license_no'),
        (17, 'workflow_status_application_support'),
        (18, 'workflow_status_price_check'),
        (19, 'vendor_cr_license_no'),
        (20, 'vendor_name'),
        (21, 'vendor_location'),
        (22, 'payment_type_application'),
        (23, 'process_type'),
        (24, 'is_active'),
        (25, 'created_on_application_support'),
        (26, 'service_description'),
        (27, 'item_name'),
        (28, 'ps_type'),
        (29, 'active_status_item'),
        (30, 'item_code'),
        (31, 'item_brand'),
        (32, 'currency_fx_rate'),
        (33, 'discount_per_quantity'),
        (34, 'vat_pct'),
        (35, 'quantity_requested'),
        (36, 'quantity_available_to_claim'),
        (37, 'quantity_payment_in_process'),
        (38, 'quantity_paid'),
        (39, 'quantity_delivered'),
        (40, 'quantity_cancelled'),
        (41, 'vat_per_quantity'),
        (42, 'vat_total'),
        (43, 'item_cost_per_quantity_with_vat'),
        (44, 'item_cost_per_quantity_without_vat'),
        (45, 'item_cost_total_with_vat'),
        (46, 'item_cost_total_without_vat'),
        (47, 'tamkeen_share_per_quantity'),
        (48, 'tamkeen_share_pct_total'),
        (49, 'tamkeen_share_total'),
        (50, 'customer_share_per_quantity'),
        (51, 'customer_share_total'),
        (52, 'allow_offline_payment'),
        (53, 'discount_total'),
        (54, 'is_fawateer'),
        (55, 'product_services_subtype'),
        (56, 'mis_source_table'),
        (57, 'entity_id'),
        (58, 'entity_external_id'),
        (59, 'item_name'),
        (60, 'item_request_id'),
        (61, 'payment_request_id'),
        (62, 'site_visit_id'),
        (63, 'vendor_id'),
        (64, 'tap_product_id'),
        (65, 'es_application_name'),
        (66, 'payment_request_name'),
        (67, 'site_visit_name'),
        (68, 'vendor_display_name'),
        (69, 'product_name'),
        (70, 'item_request_name'),
        (71, 'benefit_payment_name'),
        (72, 'existing_quantity'),
        (73, 'item_cost'),
        (74, 'total_cost'),
        (75, 'total_vat'),
        (76, 'vat_percentage'),
        (77, 'tamkeen_share'),
        (78, 'bc_share'),
        (79, 'discount'),
        (80, 'fx_rate'),
        (81, 'vendor_canonical_name'),
        (82, 'vendor_name_text'),
        (83, 'vendor_id_or_cr'),
        (84, 'main_category'),
        (85, 'sub_category'),
        (86, 'high_level_category'),
        (87, 'product_code'),
        (88, 'product_make'),
        (89, 'mas_category'),
        (90, 'quality_category'),
        (91, 'cloud_product_sub_category'),
        (92, 'amendment_status'),
        (93, 'bc_share_payment_method'),
        (94, 'condition'),
        (95, 'discount_type'),
        (96, 'functionality'),
        (97, 'item_status'),
        (98, 'monitoring_finished'),
        (99, 'payable_to'),
        (100, 'payment_share'),
        (101, 'service_type'),
        (102, 'site_visit_status'),
        (103, 'state'),
        (104, 'upload_completed'),
        (105, 'vat_applicable'),
        (106, 'special_condition_remarks'),
        (107, 'special_condition_portal_note'),
        (108, 'special_condition_workflow_status'),
        (109, 'is_initial_request'),
        (110, 'submitted_on'),
        (111, 'workflow_status'),
        (112, 'created_on'),
        (113, 'updated_on'),
        (114, 'modified_on'),
        (115, 'created_by'),
        (116, 'modified_by'),
        (117, 'owner_name'),
        (118, 'source_system_name'),
        (119, 'is_deleted'),
        (120, 'report_date'),
        (121, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'currency'),
        (2, 'scheme'),
        (3, 'quantity'),
        (4, 'vendor_country'),
        (5, 'extract_date'),
        (6, 'id_application'),
        (7, 'application_no'),
        (8, 'id_amendment'),
        (9, 'source_system'),
        (10, 'id_application_support'),
        (11, 'submitted_on_application'),
        (12, 'submitted_on_amendment'),
        (13, 'approved_on_application'),
        (14, 'approved_on_new'),
        (15, 'commercial_name'),
        (16, 'cr_license_no'),
        (17, 'workflow_status_application_support'),
        (18, 'workflow_status_price_check'),
        (19, 'vendor_cr_license_no'),
        (20, 'vendor_name'),
        (21, 'vendor_location'),
        (22, 'payment_type_application'),
        (23, 'process_type'),
        (24, 'is_active'),
        (25, 'created_on_application_support'),
        (26, 'service_description'),
        (27, 'item_name'),
        (28, 'ps_type'),
        (29, 'active_status_item'),
        (30, 'item_code'),
        (31, 'item_brand'),
        (32, 'currency_fx_rate'),
        (33, 'discount_per_quantity'),
        (34, 'vat_pct'),
        (35, 'quantity_requested'),
        (36, 'quantity_available_to_claim'),
        (37, 'quantity_payment_in_process'),
        (38, 'quantity_paid'),
        (39, 'quantity_delivered'),
        (40, 'quantity_cancelled'),
        (41, 'vat_per_quantity'),
        (42, 'vat_total'),
        (43, 'item_cost_per_quantity_with_vat'),
        (44, 'item_cost_per_quantity_without_vat'),
        (45, 'item_cost_total_with_vat'),
        (46, 'item_cost_total_without_vat'),
        (47, 'tamkeen_share_per_quantity'),
        (48, 'tamkeen_share_pct_total'),
        (49, 'tamkeen_share_total'),
        (50, 'customer_share_per_quantity'),
        (51, 'customer_share_total'),
        (52, 'allow_offline_payment'),
        (53, 'discount_total'),
        (54, 'is_fawateer'),
        (55, 'product_services_subtype'),
        (56, 'mis_source_table'),
        (57, 'entity_id'),
        (58, 'entity_external_id'),
        (59, 'item_name_2'),
        (60, 'item_request_id'),
        (61, 'payment_request_id'),
        (62, 'site_visit_id'),
        (63, 'vendor_id'),
        (64, 'tap_product_id'),
        (65, 'es_application_name'),
        (66, 'payment_request_name'),
        (67, 'site_visit_name'),
        (68, 'vendor_display_name'),
        (69, 'product_name'),
        (70, 'item_request_name'),
        (71, 'benefit_payment_name'),
        (72, 'existing_quantity'),
        (73, 'item_cost'),
        (74, 'total_cost'),
        (75, 'total_vat'),
        (76, 'vat_percentage'),
        (77, 'tamkeen_share'),
        (78, 'bc_share'),
        (79, 'discount'),
        (80, 'fx_rate'),
        (81, 'vendor_canonical_name'),
        (82, 'vendor_name_text'),
        (83, 'vendor_id_or_cr'),
        (84, 'main_category'),
        (85, 'sub_category'),
        (86, 'high_level_category'),
        (87, 'product_code'),
        (88, 'product_make'),
        (89, 'mas_category'),
        (90, 'quality_category'),
        (91, 'cloud_product_sub_category'),
        (92, 'amendment_status'),
        (93, 'bc_share_payment_method'),
        (94, 'condition'),
        (95, 'discount_type'),
        (96, 'functionality'),
        (97, 'item_status'),
        (98, 'monitoring_finished'),
        (99, 'payable_to'),
        (100, 'payment_share'),
        (101, 'service_type'),
        (102, 'site_visit_status'),
        (103, 'state'),
        (104, 'upload_completed'),
        (105, 'vat_applicable'),
        (106, 'special_condition_remarks'),
        (107, 'special_condition_portal_note'),
        (108, 'special_condition_workflow_status'),
        (109, 'is_initial_request'),
        (110, 'submitted_on'),
        (111, 'workflow_status'),
        (112, 'created_on'),
        (113, 'updated_on'),
        (114, 'modified_on'),
        (115, 'created_by'),
        (116, 'modified_by'),
        (117, 'owner_name'),
        (118, 'source_system_name'),
        (119, 'is_deleted'),
        (120, 'report_date'),
        (121, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("currency" AS VARCHAR) AS "currency",
        CAST("scheme" AS VARCHAR) AS "scheme",
        CAST("quantity" AS VARCHAR) AS "quantity",
        CAST("vendor_country" AS VARCHAR) AS "vendor_country",
        CAST("extract_date" AS VARCHAR) AS "extract_date",
        CAST("id_application" AS VARCHAR) AS "id_application",
        CAST("application_no" AS VARCHAR) AS "application_no",
        CAST("id_amendment" AS VARCHAR) AS "id_amendment",
        CAST("source_system" AS VARCHAR) AS "source_system",
        CAST("id_application_support" AS VARCHAR) AS "id_application_support",
        CAST("submitted_on_application" AS VARCHAR) AS "submitted_on_application",
        CAST("submitted_on_amendment" AS VARCHAR) AS "submitted_on_amendment",
        CAST("approved_on_application" AS VARCHAR) AS "approved_on_application",
        CAST("approved_on_new" AS VARCHAR) AS "approved_on_new",
        CAST("commercial_name" AS VARCHAR) AS "commercial_name",
        CAST("cr_license_no" AS VARCHAR) AS "cr_license_no",
        CAST("workflow_status_application_support" AS VARCHAR) AS "workflow_status_application_support",
        CAST("workflow_status_price_check" AS VARCHAR) AS "workflow_status_price_check",
        CAST("vendor_cr_license_no" AS VARCHAR) AS "vendor_cr_license_no",
        CAST("vendor_name" AS VARCHAR) AS "vendor_name",
        CAST("vendor_location" AS VARCHAR) AS "vendor_location",
        CAST("payment_type_application" AS VARCHAR) AS "payment_type_application",
        CAST("process_type" AS VARCHAR) AS "process_type",
        CAST("is_active" AS VARCHAR) AS "is_active",
        CAST("created_on_application_support" AS VARCHAR) AS "created_on_application_support",
        CAST("service_description" AS VARCHAR) AS "service_description",
        CAST("item_name" AS VARCHAR) AS "item_name",
        CAST("ps_type" AS VARCHAR) AS "ps_type",
        CAST("active_status_item" AS VARCHAR) AS "active_status_item",
        CAST("item_code" AS VARCHAR) AS "item_code",
        CAST("item_brand" AS VARCHAR) AS "item_brand",
        CAST("currency_fx_rate" AS VARCHAR) AS "currency_fx_rate",
        CAST("discount_per_quantity" AS VARCHAR) AS "discount_per_quantity",
        CAST("vat_pct" AS VARCHAR) AS "vat_pct",
        CAST("quantity_requested" AS VARCHAR) AS "quantity_requested",
        CAST("quantity_available_to_claim" AS VARCHAR) AS "quantity_available_to_claim",
        CAST("quantity_payment_in_process" AS VARCHAR) AS "quantity_payment_in_process",
        CAST("quantity_paid" AS VARCHAR) AS "quantity_paid",
        CAST("quantity_delivered" AS VARCHAR) AS "quantity_delivered",
        CAST("quantity_cancelled" AS VARCHAR) AS "quantity_cancelled",
        CAST("vat_per_quantity" AS VARCHAR) AS "vat_per_quantity",
        CAST("vat_total" AS VARCHAR) AS "vat_total",
        CAST("item_cost_per_quantity_with_vat" AS VARCHAR) AS "item_cost_per_quantity_with_vat",
        CAST("item_cost_per_quantity_without_vat" AS VARCHAR) AS "item_cost_per_quantity_without_vat",
        CAST("item_cost_total_with_vat" AS VARCHAR) AS "item_cost_total_with_vat",
        CAST("item_cost_total_without_vat" AS VARCHAR) AS "item_cost_total_without_vat",
        CAST("tamkeen_share_per_quantity" AS VARCHAR) AS "tamkeen_share_per_quantity",
        CAST("tamkeen_share_pct_total" AS VARCHAR) AS "tamkeen_share_pct_total",
        CAST("tamkeen_share_total" AS VARCHAR) AS "tamkeen_share_total",
        CAST("customer_share_per_quantity" AS VARCHAR) AS "customer_share_per_quantity",
        CAST("customer_share_total" AS VARCHAR) AS "customer_share_total",
        CAST("allow_offline_payment" AS VARCHAR) AS "allow_offline_payment",
        CAST("discount_total" AS VARCHAR) AS "discount_total",
        CAST("is_fawateer" AS VARCHAR) AS "is_fawateer",
        CAST("product_services_subtype" AS VARCHAR) AS "product_services_subtype",
        CAST("mis_source_table" AS VARCHAR) AS "mis_source_table",
        CAST("entity_id" AS VARCHAR) AS "entity_id",
        CAST("entity_external_id" AS VARCHAR) AS "entity_external_id",
        CAST("item_request_id" AS VARCHAR) AS "item_request_id",
        CAST("payment_request_id" AS VARCHAR) AS "payment_request_id",
        CAST("site_visit_id" AS VARCHAR) AS "site_visit_id",
        CAST("vendor_id" AS VARCHAR) AS "vendor_id",
        CAST("tap_product_id" AS VARCHAR) AS "tap_product_id",
        CAST("es_application_name" AS VARCHAR) AS "es_application_name",
        CAST("payment_request_name" AS VARCHAR) AS "payment_request_name",
        CAST("site_visit_name" AS VARCHAR) AS "site_visit_name",
        CAST("vendor_display_name" AS VARCHAR) AS "vendor_display_name",
        CAST("product_name" AS VARCHAR) AS "product_name",
        CAST("item_request_name" AS VARCHAR) AS "item_request_name",
        CAST("benefit_payment_name" AS VARCHAR) AS "benefit_payment_name",
        CAST("existing_quantity" AS VARCHAR) AS "existing_quantity",
        CAST("item_cost" AS VARCHAR) AS "item_cost",
        CAST("total_cost" AS VARCHAR) AS "total_cost",
        CAST("total_vat" AS VARCHAR) AS "total_vat",
        CAST("vat_percentage" AS VARCHAR) AS "vat_percentage",
        CAST("tamkeen_share" AS VARCHAR) AS "tamkeen_share",
        CAST("bc_share" AS VARCHAR) AS "bc_share",
        CAST("discount" AS VARCHAR) AS "discount",
        CAST("fx_rate" AS VARCHAR) AS "fx_rate",
        CAST("vendor_canonical_name" AS VARCHAR) AS "vendor_canonical_name",
        CAST("vendor_name_text" AS VARCHAR) AS "vendor_name_text",
        CAST("vendor_id_or_cr" AS VARCHAR) AS "vendor_id_or_cr",
        CAST("main_category" AS VARCHAR) AS "main_category",
        CAST("sub_category" AS VARCHAR) AS "sub_category",
        CAST("high_level_category" AS VARCHAR) AS "high_level_category",
        CAST("product_code" AS VARCHAR) AS "product_code",
        CAST("product_make" AS VARCHAR) AS "product_make",
        CAST("mas_category" AS VARCHAR) AS "mas_category",
        CAST("quality_category" AS VARCHAR) AS "quality_category",
        CAST("cloud_product_sub_category" AS VARCHAR) AS "cloud_product_sub_category",
        CAST("amendment_status" AS VARCHAR) AS "amendment_status",
        CAST("bc_share_payment_method" AS VARCHAR) AS "bc_share_payment_method",
        CAST("condition" AS VARCHAR) AS "condition",
        CAST("discount_type" AS VARCHAR) AS "discount_type",
        CAST("functionality" AS VARCHAR) AS "functionality",
        CAST("item_status" AS VARCHAR) AS "item_status",
        CAST("monitoring_finished" AS VARCHAR) AS "monitoring_finished",
        CAST("payable_to" AS VARCHAR) AS "payable_to",
        CAST("payment_share" AS VARCHAR) AS "payment_share",
        CAST("service_type" AS VARCHAR) AS "service_type",
        CAST("site_visit_status" AS VARCHAR) AS "site_visit_status",
        CAST("state" AS VARCHAR) AS "state",
        CAST("upload_completed" AS VARCHAR) AS "upload_completed",
        CAST("vat_applicable" AS VARCHAR) AS "vat_applicable",
        CAST("special_condition_remarks" AS VARCHAR) AS "special_condition_remarks",
        CAST("special_condition_portal_note" AS VARCHAR) AS "special_condition_portal_note",
        CAST("special_condition_workflow_status" AS VARCHAR) AS "special_condition_workflow_status",
        CAST("is_initial_request" AS VARCHAR) AS "is_initial_request",
        CAST("submitted_on" AS VARCHAR) AS "submitted_on",
        CAST("workflow_status" AS VARCHAR) AS "workflow_status",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("updated_on" AS VARCHAR) AS "updated_on",
        CAST("modified_on" AS VARCHAR) AS "modified_on",
        CAST("created_by" AS VARCHAR) AS "created_by",
        CAST("modified_by" AS VARCHAR) AS "modified_by",
        CAST("owner_name" AS VARCHAR) AS "owner_name",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("report_date" AS VARCHAR) AS "report_date",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("currency" AS VARCHAR) AS "currency",
        CAST("scheme" AS VARCHAR) AS "scheme",
        CAST("quantity" AS VARCHAR) AS "quantity",
        CAST("vendor_country" AS VARCHAR) AS "vendor_country",
        CAST("extract_date" AS VARCHAR) AS "extract_date",
        CAST("id_application" AS VARCHAR) AS "id_application",
        CAST("application_no" AS VARCHAR) AS "application_no",
        CAST("id_amendment" AS VARCHAR) AS "id_amendment",
        CAST("source_system" AS VARCHAR) AS "source_system",
        CAST("id_application_support" AS VARCHAR) AS "id_application_support",
        CAST("submitted_on_application" AS VARCHAR) AS "submitted_on_application",
        CAST("submitted_on_amendment" AS VARCHAR) AS "submitted_on_amendment",
        CAST("approved_on_application" AS VARCHAR) AS "approved_on_application",
        CAST("approved_on_new" AS VARCHAR) AS "approved_on_new",
        CAST("commercial_name" AS VARCHAR) AS "commercial_name",
        CAST("cr_license_no" AS VARCHAR) AS "cr_license_no",
        CAST("workflow_status_application_support" AS VARCHAR) AS "workflow_status_application_support",
        CAST("workflow_status_price_check" AS VARCHAR) AS "workflow_status_price_check",
        CAST("vendor_cr_license_no" AS VARCHAR) AS "vendor_cr_license_no",
        CAST("vendor_name" AS VARCHAR) AS "vendor_name",
        CAST("vendor_location" AS VARCHAR) AS "vendor_location",
        CAST("payment_type_application" AS VARCHAR) AS "payment_type_application",
        CAST("process_type" AS VARCHAR) AS "process_type",
        CAST("is_active" AS VARCHAR) AS "is_active",
        CAST("created_on_application_support" AS VARCHAR) AS "created_on_application_support",
        CAST("service_description" AS VARCHAR) AS "service_description",
        CAST("item_name" AS VARCHAR) AS "item_name",
        CAST("ps_type" AS VARCHAR) AS "ps_type",
        CAST("active_status_item" AS VARCHAR) AS "active_status_item",
        CAST("item_code" AS VARCHAR) AS "item_code",
        CAST("item_brand" AS VARCHAR) AS "item_brand",
        CAST("currency_fx_rate" AS VARCHAR) AS "currency_fx_rate",
        CAST("discount_per_quantity" AS VARCHAR) AS "discount_per_quantity",
        CAST("vat_pct" AS VARCHAR) AS "vat_pct",
        CAST("quantity_requested" AS VARCHAR) AS "quantity_requested",
        CAST("quantity_available_to_claim" AS VARCHAR) AS "quantity_available_to_claim",
        CAST("quantity_payment_in_process" AS VARCHAR) AS "quantity_payment_in_process",
        CAST("quantity_paid" AS VARCHAR) AS "quantity_paid",
        CAST("quantity_delivered" AS VARCHAR) AS "quantity_delivered",
        CAST("quantity_cancelled" AS VARCHAR) AS "quantity_cancelled",
        CAST("vat_per_quantity" AS VARCHAR) AS "vat_per_quantity",
        CAST("vat_total" AS VARCHAR) AS "vat_total",
        CAST("item_cost_per_quantity_with_vat" AS VARCHAR) AS "item_cost_per_quantity_with_vat",
        CAST("item_cost_per_quantity_without_vat" AS VARCHAR) AS "item_cost_per_quantity_without_vat",
        CAST("item_cost_total_with_vat" AS VARCHAR) AS "item_cost_total_with_vat",
        CAST("item_cost_total_without_vat" AS VARCHAR) AS "item_cost_total_without_vat",
        CAST("tamkeen_share_per_quantity" AS VARCHAR) AS "tamkeen_share_per_quantity",
        CAST("tamkeen_share_pct_total" AS VARCHAR) AS "tamkeen_share_pct_total",
        CAST("tamkeen_share_total" AS VARCHAR) AS "tamkeen_share_total",
        CAST("customer_share_per_quantity" AS VARCHAR) AS "customer_share_per_quantity",
        CAST("customer_share_total" AS VARCHAR) AS "customer_share_total",
        CAST("allow_offline_payment" AS VARCHAR) AS "allow_offline_payment",
        CAST("discount_total" AS VARCHAR) AS "discount_total",
        CAST("is_fawateer" AS VARCHAR) AS "is_fawateer",
        CAST("product_services_subtype" AS VARCHAR) AS "product_services_subtype",
        CAST("mis_source_table" AS VARCHAR) AS "mis_source_table",
        CAST("entity_id" AS VARCHAR) AS "entity_id",
        CAST("entity_external_id" AS VARCHAR) AS "entity_external_id",
        CAST("item_request_id" AS VARCHAR) AS "item_request_id",
        CAST("payment_request_id" AS VARCHAR) AS "payment_request_id",
        CAST("site_visit_id" AS VARCHAR) AS "site_visit_id",
        CAST("vendor_id" AS VARCHAR) AS "vendor_id",
        CAST("tap_product_id" AS VARCHAR) AS "tap_product_id",
        CAST("es_application_name" AS VARCHAR) AS "es_application_name",
        CAST("payment_request_name" AS VARCHAR) AS "payment_request_name",
        CAST("site_visit_name" AS VARCHAR) AS "site_visit_name",
        CAST("vendor_display_name" AS VARCHAR) AS "vendor_display_name",
        CAST("product_name" AS VARCHAR) AS "product_name",
        CAST("item_request_name" AS VARCHAR) AS "item_request_name",
        CAST("benefit_payment_name" AS VARCHAR) AS "benefit_payment_name",
        CAST("existing_quantity" AS VARCHAR) AS "existing_quantity",
        CAST("item_cost" AS VARCHAR) AS "item_cost",
        CAST("total_cost" AS VARCHAR) AS "total_cost",
        CAST("total_vat" AS VARCHAR) AS "total_vat",
        CAST("vat_percentage" AS VARCHAR) AS "vat_percentage",
        CAST("tamkeen_share" AS VARCHAR) AS "tamkeen_share",
        CAST("bc_share" AS VARCHAR) AS "bc_share",
        CAST("discount" AS VARCHAR) AS "discount",
        CAST("fx_rate" AS VARCHAR) AS "fx_rate",
        CAST("vendor_canonical_name" AS VARCHAR) AS "vendor_canonical_name",
        CAST("vendor_name_text" AS VARCHAR) AS "vendor_name_text",
        CAST("vendor_id_or_cr" AS VARCHAR) AS "vendor_id_or_cr",
        CAST("main_category" AS VARCHAR) AS "main_category",
        CAST("sub_category" AS VARCHAR) AS "sub_category",
        CAST("high_level_category" AS VARCHAR) AS "high_level_category",
        CAST("product_code" AS VARCHAR) AS "product_code",
        CAST("product_make" AS VARCHAR) AS "product_make",
        CAST("mas_category" AS VARCHAR) AS "mas_category",
        CAST("quality_category" AS VARCHAR) AS "quality_category",
        CAST("cloud_product_sub_category" AS VARCHAR) AS "cloud_product_sub_category",
        CAST("amendment_status" AS VARCHAR) AS "amendment_status",
        CAST("bc_share_payment_method" AS VARCHAR) AS "bc_share_payment_method",
        CAST("condition" AS VARCHAR) AS "condition",
        CAST("discount_type" AS VARCHAR) AS "discount_type",
        CAST("functionality" AS VARCHAR) AS "functionality",
        CAST("item_status" AS VARCHAR) AS "item_status",
        CAST("monitoring_finished" AS VARCHAR) AS "monitoring_finished",
        CAST("payable_to" AS VARCHAR) AS "payable_to",
        CAST("payment_share" AS VARCHAR) AS "payment_share",
        CAST("service_type" AS VARCHAR) AS "service_type",
        CAST("site_visit_status" AS VARCHAR) AS "site_visit_status",
        CAST("state" AS VARCHAR) AS "state",
        CAST("upload_completed" AS VARCHAR) AS "upload_completed",
        CAST("vat_applicable" AS VARCHAR) AS "vat_applicable",
        CAST("special_condition_remarks" AS VARCHAR) AS "special_condition_remarks",
        CAST("special_condition_portal_note" AS VARCHAR) AS "special_condition_portal_note",
        CAST("special_condition_workflow_status" AS VARCHAR) AS "special_condition_workflow_status",
        CAST("is_initial_request" AS VARCHAR) AS "is_initial_request",
        CAST("submitted_on" AS VARCHAR) AS "submitted_on",
        CAST("workflow_status" AS VARCHAR) AS "workflow_status",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("updated_on" AS VARCHAR) AS "updated_on",
        CAST("modified_on" AS VARCHAR) AS "modified_on",
        CAST("created_by" AS VARCHAR) AS "created_by",
        CAST("modified_by" AS VARCHAR) AS "modified_by",
        CAST("owner_name" AS VARCHAR) AS "owner_name",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("report_date" AS VARCHAR) AS "report_date",
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
        'product_services_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'product_services_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'product_services_base' AS table_name,
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
        'product_services_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'product_services_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
