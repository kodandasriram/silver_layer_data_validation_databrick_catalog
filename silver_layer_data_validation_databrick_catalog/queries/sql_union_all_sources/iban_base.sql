WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for iban_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: iban_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH iban_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING) AS option_key,
        max(sm.value) AS option_value
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.STRINGMAP sm
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING)
),
option_set_map AS (
    SELECT map_from_entries(collect_list(named_struct('key', option_key, 'value', option_value))) AS option_values
    FROM option_set_values
)
/*
============================================================================
silver_iban_mis.sql
============================================================================
Per-source intermediate Silver model for the IBAN domain Ã¢â‚¬â€ MIS only.

Source: tmkn_iban (single table, IBAN bank account reference data)

Reference SPs:
  - RPT-029_Company                   (joins iban via tmkn_iban_x = company)
  - RPT-030_Business_Development      (similar pattern)
  - BCApplications                    (similar pattern)

The IBAN domain is a single-table reference domain. tmkn_iban holds bank
account records that are linked from companies and applications. There is
no internal entity hierarchy to join Ã¢â‚¬â€ the table sits flat.

In the source SPs, tmkn_iban is JOINed FROM other domain tables (Company,
BD Application) via the iban FK. Here we expose it standalone Ã¢â‚¬â€ downstream
domain tables that need iban details will JOIN to this Silver table at
Gold/AGG time.

Cleansing only Ã¢â‚¬â€ no business logic.
============================================================================
*/

SELECT
    'tmkn_iban' AS mis_source_table,

    -- Identifiers
    CAST(iban.tmkn_ibanid AS STRING)                    AS iban_id,
    iban.tmkn_name                                       AS iban_name,

    -- IBAN details
    --iban.tmkn_ibannumber                                 AS iban_number,
    --iban.tmkn_bankname                                   AS bank_name,
    --iban.tmkn_accountholder                              AS account_holder,
    --iban.tmkn_branchname                                 AS branch_name,
    CAST(NULL AS STRING) AS iban_number,
    CAST(NULL AS STRING) AS bank_name,
    CAST(NULL AS STRING) AS account_holder,
    CAST(NULL AS STRING) AS branch_name,
    CAST(NULL AS STRING) AS owner_name,
    -- Standard option-set decodes
    CASE WHEN iban.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_iban') || '|' || lower('statuscode') || '|' || CAST(iban.statuscode AS STRING)) END  AS status_reason,
    CASE WHEN iban.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('tmkn_iban') || '|' || lower('statecode') || '|' || CAST(iban.statecode AS STRING)) END   AS state,

    -- Owner / audit
    --iban.owneridname                                     AS owner_name,
    iban.createdby                                  AS created_by,
    iban.modifiedby                                  AS modified_by,
    iban.createdon                                       AS created_on,
    iban.modifiedon                                      AS modified_on,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.TMKN_IBANBASE iban
),
iban_base_os1_source AS (
/*
============================================================================
silver_iban_os1.sql
============================================================================
Per-source intermediate Silver model for the IBAN domain Ã¢â‚¬â€ OS1 only.

Source: OSUSR_PX1_IBAN (anchor)
Reference SP: RPT-186_neoTamkeen_IBAN

Lookups joined inline:
  - OSUSR_PX1_IBANTYPE        Ã¢â€ â€™ IBAN type label (e.g., 'Personal', 'Company')
  - OSUSR_PX1_BANK            Ã¢â€ â€™ bank name
  - OSUSR_PX1_IBANSTATUS      Ã¢â€ â€™ workflow status label
  - ossys_user (Ãƒâ€”2)           Ã¢â€ â€™ created-by / updated-by user names
  - OSUSR_MKZ_USEREXTENSION   Ã¢â€ â€™ CPR number for the creating user

OS1 lookup tables don't follow the CRM option-set pattern Ã¢â‚¬â€ they are standard
reference tables joined directly. No decode_optionset macro needed for OS1.

Sentinel handling: OS1 uses '01-01-1900' as the sentinel default date Ã¢â‚¬â€ null
those out for the date columns to avoid downstream confusion.

Cleansing only Ã¢â‚¬â€ no business logic. Cross-domain references (e.g., the
USERID FK to ossys_user) are preserved as columns, but the user details are
denormalised inline because RPT-186 already does that.
============================================================================
*/

SELECT
    'OSUSR_PX1_IBAN' AS os1_source_table,

    -- Identifiers
    ibn.ID                                                                AS iban_id,
    ibn.IBANNUMBER                                                        AS iban_number,
    ibn.ACCOUNTNAME                                                       AS account_name,

    -- Lookups (decoded labels)
    --ibntyp.LABEL                                                          AS iban_type,
    --bnk.BANKNAME                                                          AS bank_name,
    --ibnSts.LABEL                                                          AS workflow_status,
    CAST(NULL AS STRING) AS iban_type,
    CAST(NULL AS STRING) AS bank_name,
    CAST(NULL AS STRING) AS workflow_status,

    -- Foreign keys preserved for downstream re-joining
    ibn.IBANTYPEID                                                        AS iban_type_id,
    ibn.BANKID                                                            AS bank_id,
    ibn.IBANSTATUSID                                                      AS iban_status_id,
    ibn.CREATEDBY                                                         AS created_by_user_id,
    ibn.UPDATEDBY                                                         AS updated_by_user_id,

    -- Audit (denormalised user names from ossys_user)
    usr_create.NAME                                                       AS created_by,
    usr_update.NAME                                                       AS modified_by,
    UsrExt.CPR_NUMBER                                                     AS created_by_cpr,

    -- Dates with OS1 sentinel handling
    CASE WHEN ibn.CREATEDON = DATE '1900-01-01' THEN NULL
         ELSE ibn.CREATEDON END                                           AS created_on,
    CASE WHEN ibn.UPDATEDON = DATE '1900-01-01' THEN NULL
         ELSE ibn.UPDATEDON END                                           AS modified_on,

    -- Flags
    --CASE WHEN ibn.ISDEFAULT = 1 THEN TRUE ELSE FALSE END                  AS is_default,
	ibn.ISDEFAULT AS is_default,

    -- Standard trailing audit columns
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_IBAN ibn
-- INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_IBANTYPE ibntyp
--        ON ibntyp.ID = ibn.IBANTYPEID
-- INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_BANK bnk
--        ON bnk.ID = ibn.BANKID
-- INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_PX1_IBANSTATUS ibnSts
--        ON ibnSts.ID = ibn.IBANSTATUSID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER usr_create
       ON usr_create.ID = ibn.CREATEDBY
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSSYS_USER usr_update
       ON usr_update.ID = ibn.UPDATEDBY
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_MKZ_USEREXTENSION UsrExt
       ON UsrExt.USERID = usr_create.ID
),
iban_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : IBAN_FINANCE_NTP
Description : This model extracts and transforms IBAN and finance-related attributes
              from the NEO2 (NTP) source system Bronze Layer and loads into the
              IBAN_FINANCE target table as part of the Silver Layer data pipeline.
              It supports incremental loading with merge strategy and implements
              soft delete handling using a post-hook.

Source Tables : neo2.OSUSR_TLV_IBAN
                neo2.OSUSR_TLV_BANK
                neo2.OSUSR_TLV_IBANSTATUS
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_CUSTOMERTYPE
                neo2.OSUSR_ZMZ_INDIVIDUAL
                neo2.OSUSR_ZMZ_COMPANY

Target Table : IBAN_FINANCE
Load Type    : Incremental Load (Merge + Soft Delete)
Materialized : incremental
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-11 |    Abitha     | Initial version

================================================================================================= 
*/
WITH CTE_IBAN_FINANCE AS (
    SELECT
        IBAN.id,
        IBAN.ibannumber,
        IBST.LABEL                                                        AS iban_status,
        IBAN.customerprofileid,
        IBAN.portaluserid,
        IBAN.ibanstatusid,
        IBAN.bankid,
        IBAN.docchecklistguid,
        IBAN.updatedby,
        IBAN.isdefault,
        IBAN.isverifiedbytarabut,
        IBAN.issalaryiban,
        IBAN.externalbankid,
        IBAN.currency,
        IBAN.deactivatedon,
        IBAN.docdeactivateguid,
        IBAN.reasonsfordeactivation,
        CASE
            WHEN CusType.LABEL = 'Individual' THEN IND.CPRNUMBER
            ELSE CMP.CODE
        END                                                               AS payee_cpr_cr_license,
        CUS.NAMEEN                                                        AS customer_name_commercial_name_english,
        IBAN.EMAIL                                                        AS email,
        CONCAT('+', IBAN.MOBILECOUNTRYPREFIX, ' ', IBAN.MOBILENUMBER)    AS mobile_number,
        IBAN.ACCOUNTNAME                                                  AS account_name,
        BANK.BANKNAME                                                     AS bank_name,
        CASE
            WHEN IBAN.CREATEDON = CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)
            THEN NULL
            ELSE IBAN.CREATEDON + INTERVAL '3' HOUR
        END                                                               AS createdon,
        IBAN.CREATEDBY                                                    AS created_by,
        BANK.SWIFTBANKCODE                                                AS swift_code,
        BANK.BICCODE                                                      AS bic_code,
        CusType.LABEL                                                     AS customer_type,
        IBAN.UPDATEDON                                                    AS updatedon,
        CASE
            WHEN IBAN.IBANSTATUSID = 'VER' THEN IBAN.UPDATEDON
            ELSE NULL
        END                                                               AS verified_on,
        ROW_NUMBER() OVER (PARTITION BY IBAN.ID ORDER BY IBAN.CREATEDON DESC NULLS LAST, IBAN.UPDATEDON DESC NULLS LAST)                                                              AS rnk,
        FALSE                                                             AS is_deleted,
        'NEO2'                                                            AS source_system_name,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)          AS dbt_updated_at

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_TLV_IBAN                          IBAN
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_TLV_BANK                     BANK
           ON BANK.ID = IBAN.BANKID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_TLV_IBANSTATUS               IBST
           ON IBAN.IBANSTATUSID = IBST.CODE
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERPROFILE          CUSPROF
           ON CUSPROF.ID = IBAN.CUSTOMERPROFILEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER                 CUS
           ON CUSPROF.CUSTOMERID = CUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMERTYPE             CusType
           ON CusType.CODE = CUS.CUSTOMERTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_INDIVIDUAL               IND
           ON CUSPROF.CUSTOMERID = IND.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_COMPANY                  CMP
           ON CUSPROF.CUSTOMERID = CMP.ID
)

SELECT
    TRY_CAST(NULLIF(CAST(id AS STRING), '') AS BIGINT)                                          AS id,
    ibannumber,
    iban_status,
    customerprofileid,
    portaluserid,
    ibanstatusid,
    bankid,
    docchecklistguid,
    updatedby,
    isdefault,
    isverifiedbytarabut,
    issalaryiban,
    externalbankid,
    currency,
    deactivatedon,
    docdeactivateguid,
    reasonsfordeactivation,
    payee_cpr_cr_license,
    customer_name_commercial_name_english,
    email,
    mobile_number,
    account_name,
    bank_name,
    TRY_CAST(NULLIF(CAST(createdon AS STRING), '') AS TIMESTAMP)                           AS createdon,
    created_by,
    swift_code,
    bic_code,
    customer_type,
    TRY_CAST(NULLIF(CAST(updatedon AS STRING), '') AS TIMESTAMP)                               AS updatedon,
    TRY_CAST(NULLIF(CAST(verified_on AS STRING), '') AS TIMESTAMP)                               AS verified_on,
    is_deleted,
    UPPER(NULLIF(TRIM(CAST(source_system_name AS STRING)), ''))                         AS source_system_name,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS STRING), '') AS TIMESTAMP)                            AS dbt_updated_at

FROM CTE_IBAN_FINANCE
WHERE rnk = 1
)
select
    -- source markers
    mis_source_table,
    cast(null as STRING) as os1_source_table,

    -- common identifiers / iban fields
    cast(iban_id as STRING) as iban_id,
    iban_number,
    bank_name,
    created_by,
    modified_by,
    created_on,
    modified_on,

    -- mis columns
    iban_name,
    account_holder,
    branch_name,
    status_reason,
    state,
    owner_name,

    -- os1 columns
    cast(null as STRING) as account_name,
    cast(null as STRING) as iban_type,
    cast(null as STRING) as workflow_status,
    cast(null as bigint) as iban_type_id,
    cast(null as bigint) as bank_id,
    cast(null as bigint) as iban_status_id,
    cast(null as bigint) as created_by_user_id,
    cast(null as bigint) as updated_by_user_id,
    cast(null as STRING) as created_by_cpr,
    cast(null as boolean) as is_default,

    -- os2 columns
    cast(null as bigint) as customerprofileid,
    cast(null as bigint) as portaluserid,
    cast(null as STRING) as os2_ibanstatusid,
    cast(null as STRING) as docchecklistguid,
    cast(null as STRING) as os2_updatedby,
    cast(null as boolean) as isverifiedbytarabut,
    cast(null as boolean) as issalaryiban,
    cast(null as bigint) as externalbankid,
    cast(null as STRING) as currency,
    cast(null as timestamp) as deactivatedon,
    cast(null as STRING) as docdeactivateguid,
    cast(null as STRING) as reasonsfordeactivation,
    cast(null as STRING) as payee_cpr_cr_license,
    cast(null as STRING) as customer_name_commercial_name_english,
    cast(null as STRING) as email,
    cast(null as STRING) as mobile_number,
    cast(null as STRING) as swift_code,
    cast(null as STRING) as bic_code,
    cast(null as STRING) as customer_type,
    cast(null as timestamp) as verified_on,

    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    cast(created_on as timestamp) as createdon,
    cast(null as timestamp) as updatedon

from iban_base_mis_source

union all

select
    -- source markers
    cast(null as STRING) as mis_source_table,
    os1_source_table,

    -- common identifiers / iban fields
    cast(iban_id as STRING) as iban_id,
    iban_number,
    bank_name,
    created_by,
    modified_by,
    created_on,
    modified_on,

    -- mis placeholders
    cast(null as STRING) as iban_name,
    cast(null as STRING) as account_holder,
    cast(null as STRING) as branch_name,
    cast(null as STRING) as status_reason,
    cast(null as STRING) as state,
    cast(null as STRING) as owner_name,

    -- os1 columns
    account_name,
    iban_type,
    workflow_status,
    iban_type_id,
    bank_id,
    iban_status_id,
    created_by_user_id,
    updated_by_user_id,
    created_by_cpr,
    is_default,

    -- os2 placeholders
    cast(null as bigint) as customerprofileid,
    cast(null as bigint) as portaluserid,
    cast(null as STRING) as os2_ibanstatusid,
    cast(null as STRING) as docchecklistguid,
    cast(null as STRING) as os2_updatedby,
    cast(null as boolean) as isverifiedbytarabut,
    cast(null as boolean) as issalaryiban,
    cast(null as bigint) as externalbankid,
    cast(null as STRING) as currency,
    cast(null as timestamp) as deactivatedon,
    cast(null as STRING) as docdeactivateguid,
    cast(null as STRING) as reasonsfordeactivation,
    cast(null as STRING) as payee_cpr_cr_license,
    cast(null as STRING) as customer_name_commercial_name_english,
    cast(null as STRING) as email,
    cast(null as STRING) as mobile_number,
    cast(null as STRING) as swift_code,
    cast(null as STRING) as bic_code,
    cast(null as STRING) as customer_type,
    cast(null as timestamp) as verified_on,

    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    cast(created_on as timestamp) as createdon,
    cast(null as timestamp) as updatedon

from iban_base_os1_source

union all

select
    -- source markers
    cast(null as STRING) as mis_source_table,
    cast(null as STRING) as os1_source_table,

    -- common identifiers / iban fields
    cast(id as STRING) as iban_id,
    ibannumber as iban_number,
    bank_name as bank_name,
    created_by as created_by,
    cast(updatedby as STRING) as modified_by,
    createdon as created_on,
    updatedon as modified_on,

    -- mis placeholders
    cast(null as STRING) as iban_name,
    cast(null as STRING) as account_holder,
    cast(null as STRING) as branch_name,
    cast(null as STRING) as status_reason,
    cast(null as STRING) as state,
    cast(null as STRING) as owner_name,

    -- os1 / common columns
    account_name as account_name,
    cast(null as STRING) as iban_type,
    iban_status as workflow_status,
    cast(null as bigint) as iban_type_id,
    bankid as bank_id,
    cast(null as bigint) as iban_status_id,
    cast(null as bigint) as created_by_user_id,
    cast(null as bigint) as updated_by_user_id,
    cast(null as STRING) as created_by_cpr,
    isdefault as is_default,

    -- os2 columns
    customerprofileid as customerprofileid,
    portaluserid as portaluserid,
    cast(ibanstatusid as STRING) as os2_ibanstatusid,
    docchecklistguid as docchecklistguid,
    cast(updatedby as STRING) as os2_updatedby,
    isverifiedbytarabut as isverifiedbytarabut,
    issalaryiban as issalaryiban,
    externalbankid as externalbankid,
    currency as currency,
    deactivatedon as deactivatedon,
    docdeactivateguid as docdeactivateguid,
    reasonsfordeactivation as reasonsfordeactivation,
    payee_cpr_cr_license as payee_cpr_cr_license,
    customer_name_commercial_name_english as customer_name_commercial_name_english,
    email as email,
    mobile_number as mobile_number,
    swift_code as swift_code,
    bic_code as bic_code,
    customer_type as customer_type,
    verified_on as verified_on,

    source_system_name as source_system_name,
    is_deleted as is_deleted,
    current_date as report_date,
    dbt_updated_at,
    createdon,
    updatedon
from iban_base_os2_source
),

silver_layer AS (
SELECT
    mis_source_table,
    os1_source_table,
    iban_id,
    iban_number,
    bank_name,
    created_by,
    modified_by,
    created_on,
    modified_on,
    iban_name,
    account_holder,
    branch_name,
    status_reason,
    state,
    owner_name,
    account_name,
    iban_type,
    workflow_status,
    iban_type_id,
    bank_id,
    iban_status_id,
    created_by_user_id,
    updated_by_user_id,
    created_by_cpr,
    is_default,
    customerprofileid,
    portaluserid,
    os2_ibanstatusid,
    docchecklistguid,
    os2_updatedby,
    isverifiedbytarabut,
    issalaryiban,
    externalbankid,
    currency,
    deactivatedon,
    docdeactivateguid,
    reasonsfordeactivation,
    payee_cpr_cr_license,
    customer_name_commercial_name_english,
    email,
    mobile_number,
    swift_code,
    bic_code,
    customer_type,
    verified_on,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at,
    createdon,
    updatedon
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.iban_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'mis_source_table'),
        (2, 'os1_source_table'),
        (3, 'iban_id'),
        (4, 'iban_number'),
        (5, 'bank_name'),
        (6, 'created_by'),
        (7, 'modified_by'),
        (8, 'created_on'),
        (9, 'modified_on'),
        (10, 'iban_name'),
        (11, 'account_holder'),
        (12, 'branch_name'),
        (13, 'status_reason'),
        (14, 'state'),
        (15, 'owner_name'),
        (16, 'account_name'),
        (17, 'iban_type'),
        (18, 'workflow_status'),
        (19, 'iban_type_id'),
        (20, 'bank_id'),
        (21, 'iban_status_id'),
        (22, 'created_by_user_id'),
        (23, 'updated_by_user_id'),
        (24, 'created_by_cpr'),
        (25, 'is_default'),
        (26, 'customerprofileid'),
        (27, 'portaluserid'),
        (28, 'os2_ibanstatusid'),
        (29, 'docchecklistguid'),
        (30, 'os2_updatedby'),
        (31, 'isverifiedbytarabut'),
        (32, 'issalaryiban'),
        (33, 'externalbankid'),
        (34, 'currency'),
        (35, 'deactivatedon'),
        (36, 'docdeactivateguid'),
        (37, 'reasonsfordeactivation'),
        (38, 'payee_cpr_cr_license'),
        (39, 'customer_name_commercial_name_english'),
        (40, 'email'),
        (41, 'mobile_number'),
        (42, 'swift_code'),
        (43, 'bic_code'),
        (44, 'customer_type'),
        (45, 'verified_on'),
        (46, 'source_system_name'),
        (47, 'is_deleted'),
        (48, 'report_date'),
        (49, 'dbt_updated_at'),
        (50, 'createdon'),
        (51, 'updatedon')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'mis_source_table'),
        (2, 'os1_source_table'),
        (3, 'iban_id'),
        (4, 'iban_number'),
        (5, 'bank_name'),
        (6, 'created_by'),
        (7, 'modified_by'),
        (8, 'created_on'),
        (9, 'modified_on'),
        (10, 'iban_name'),
        (11, 'account_holder'),
        (12, 'branch_name'),
        (13, 'status_reason'),
        (14, 'state'),
        (15, 'owner_name'),
        (16, 'account_name'),
        (17, 'iban_type'),
        (18, 'workflow_status'),
        (19, 'iban_type_id'),
        (20, 'bank_id'),
        (21, 'iban_status_id'),
        (22, 'created_by_user_id'),
        (23, 'updated_by_user_id'),
        (24, 'created_by_cpr'),
        (25, 'is_default'),
        (26, 'customerprofileid'),
        (27, 'portaluserid'),
        (28, 'os2_ibanstatusid'),
        (29, 'docchecklistguid'),
        (30, 'os2_updatedby'),
        (31, 'isverifiedbytarabut'),
        (32, 'issalaryiban'),
        (33, 'externalbankid'),
        (34, 'currency'),
        (35, 'deactivatedon'),
        (36, 'docdeactivateguid'),
        (37, 'reasonsfordeactivation'),
        (38, 'payee_cpr_cr_license'),
        (39, 'customer_name_commercial_name_english'),
        (40, 'email'),
        (41, 'mobile_number'),
        (42, 'swift_code'),
        (43, 'bic_code'),
        (44, 'customer_type'),
        (45, 'verified_on'),
        (46, 'source_system_name'),
        (47, 'is_deleted'),
        (48, 'report_date'),
        (49, 'dbt_updated_at'),
        (50, 'createdon'),
        (51, 'updatedon')
),

bronze_normalized AS (
    SELECT
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`os1_source_table` AS STRING) AS `os1_source_table`,
        CAST(`iban_id` AS STRING) AS `iban_id`,
        CAST(`iban_number` AS STRING) AS `iban_number`,
        CAST(`bank_name` AS STRING) AS `bank_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`iban_name` AS STRING) AS `iban_name`,
        CAST(`account_holder` AS STRING) AS `account_holder`,
        CAST(`branch_name` AS STRING) AS `branch_name`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`account_name` AS STRING) AS `account_name`,
        CAST(`iban_type` AS STRING) AS `iban_type`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`iban_type_id` AS STRING) AS `iban_type_id`,
        CAST(`bank_id` AS STRING) AS `bank_id`,
        CAST(`iban_status_id` AS STRING) AS `iban_status_id`,
        CAST(`created_by_user_id` AS STRING) AS `created_by_user_id`,
        CAST(`updated_by_user_id` AS STRING) AS `updated_by_user_id`,
        CAST(`created_by_cpr` AS STRING) AS `created_by_cpr`,
        CAST(`is_default` AS STRING) AS `is_default`,
        CAST(`customerprofileid` AS STRING) AS `customerprofileid`,
        CAST(`portaluserid` AS STRING) AS `portaluserid`,
        CAST(`os2_ibanstatusid` AS STRING) AS `os2_ibanstatusid`,
        CAST(`docchecklistguid` AS STRING) AS `docchecklistguid`,
        CAST(`os2_updatedby` AS STRING) AS `os2_updatedby`,
        CAST(`isverifiedbytarabut` AS STRING) AS `isverifiedbytarabut`,
        CAST(`issalaryiban` AS STRING) AS `issalaryiban`,
        CAST(`externalbankid` AS STRING) AS `externalbankid`,
        CAST(`currency` AS STRING) AS `currency`,
        CAST(`deactivatedon` AS STRING) AS `deactivatedon`,
        CAST(`docdeactivateguid` AS STRING) AS `docdeactivateguid`,
        CAST(`reasonsfordeactivation` AS STRING) AS `reasonsfordeactivation`,
        CAST(`payee_cpr_cr_license` AS STRING) AS `payee_cpr_cr_license`,
        CAST(`customer_name_commercial_name_english` AS STRING) AS `customer_name_commercial_name_english`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`mobile_number` AS STRING) AS `mobile_number`,
        CAST(`swift_code` AS STRING) AS `swift_code`,
        CAST(`bic_code` AS STRING) AS `bic_code`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`verified_on` AS STRING) AS `verified_on`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`os1_source_table` AS STRING) AS `os1_source_table`,
        CAST(`iban_id` AS STRING) AS `iban_id`,
        CAST(`iban_number` AS STRING) AS `iban_number`,
        CAST(`bank_name` AS STRING) AS `bank_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`iban_name` AS STRING) AS `iban_name`,
        CAST(`account_holder` AS STRING) AS `account_holder`,
        CAST(`branch_name` AS STRING) AS `branch_name`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`account_name` AS STRING) AS `account_name`,
        CAST(`iban_type` AS STRING) AS `iban_type`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`iban_type_id` AS STRING) AS `iban_type_id`,
        CAST(`bank_id` AS STRING) AS `bank_id`,
        CAST(`iban_status_id` AS STRING) AS `iban_status_id`,
        CAST(`created_by_user_id` AS STRING) AS `created_by_user_id`,
        CAST(`updated_by_user_id` AS STRING) AS `updated_by_user_id`,
        CAST(`created_by_cpr` AS STRING) AS `created_by_cpr`,
        CAST(`is_default` AS STRING) AS `is_default`,
        CAST(`customerprofileid` AS STRING) AS `customerprofileid`,
        CAST(`portaluserid` AS STRING) AS `portaluserid`,
        CAST(`os2_ibanstatusid` AS STRING) AS `os2_ibanstatusid`,
        CAST(`docchecklistguid` AS STRING) AS `docchecklistguid`,
        CAST(`os2_updatedby` AS STRING) AS `os2_updatedby`,
        CAST(`isverifiedbytarabut` AS STRING) AS `isverifiedbytarabut`,
        CAST(`issalaryiban` AS STRING) AS `issalaryiban`,
        CAST(`externalbankid` AS STRING) AS `externalbankid`,
        CAST(`currency` AS STRING) AS `currency`,
        CAST(`deactivatedon` AS STRING) AS `deactivatedon`,
        CAST(`docdeactivateguid` AS STRING) AS `docdeactivateguid`,
        CAST(`reasonsfordeactivation` AS STRING) AS `reasonsfordeactivation`,
        CAST(`payee_cpr_cr_license` AS STRING) AS `payee_cpr_cr_license`,
        CAST(`customer_name_commercial_name_english` AS STRING) AS `customer_name_commercial_name_english`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`mobile_number` AS STRING) AS `mobile_number`,
        CAST(`swift_code` AS STRING) AS `swift_code`,
        CAST(`bic_code` AS STRING) AS `bic_code`,
        CAST(`customer_type` AS STRING) AS `customer_type`,
        CAST(`verified_on` AS STRING) AS `verified_on`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`
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
        'iban_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'iban_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'iban_base' AS table_name,
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
        'iban_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'iban_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
