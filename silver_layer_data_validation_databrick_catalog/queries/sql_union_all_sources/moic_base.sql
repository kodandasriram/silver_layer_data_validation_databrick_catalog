-- Compare bronze-layer query output with silver-layer table output for moic_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\Direct tables\moic_base_direct.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\moic_base_silver_layer.sql

WITH
bronze_layer AS (
-- Standalone Trino SQL generated from moic_base.sql.
-- Final column order aligned to silver_layer_query/moic_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : MOIC_BASE_OS2
   Description   : Unified MOIC Silver model for OS2 — combines the previously-
                   separated MOIC_DATA_BASE_OS2 (enterprise master from MOIC
                   CRDETAILS + LMRA + SIO) and MOIC_ACTIVITIES_BASE_OS2 (ISIC4
                   activity classification from MOIC CRACTIVITY) into a single
                   per-application MOIC enterprise picture.

                   Combines the business logic from two source stored procedures:
                     - RPT-237_MOIC_Data         (enterprise master + workforce)
                     - RPT-233_MOIC_Activities   (sector / activity codes)

                   The merge is a LEFT JOIN — not a UNION — because the two
                   sources describe DIFFERENT ATTRIBUTES of the same business
                   entity (the application's enterprise CR), not different rows
                   of the same kind.

   Grain         : One row per APPLICATIONID
                   (matching the deduplicated grain of MOIC_DATA_BASE_OS2 via
                   RNK=1 over UPDATEDON, CREATEDON)

   Source Tables : neo2.OSUSR_MYA_MOIC_CRDETAILS                (data anchor)
                   neo2.OSUSR_MYA_LMRA_DETAILS                  (workforce)
                   neo2.OSUSR_MYA_SIO_ESTABLISHMENTDETAILS      (salaries)
                   neo2.OSUSR_MYA_MOIC_CRACTIVITY               (activities)
                   neo2.OSUSR_3QQ_SECTORISIC3                   (sector lookup)
                   neo2.OSUSR_3QQ_SECTORISICACTIVITY3           (activity label)

   Target Table  : MOIC_BASE_OS2

   Load Type     : Full Load
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily

   Replaces      : moic_data_base_os2 and moic_activities_base_os2.
                   Both can be retired once downstream refs are updated to
                   point at this unified model.

   Revision History:
   ----------------------------------------------------------------
   Version | Date       | Author    | Description
   ----------------------------------------------------------------
   1.0     | 2026-05-18 | Claude    | Initial version merging moic_data_base_os2
                                     and moic_activities_base_os2 via APPLICATIONID
============================================================================= 
*/
WITH

-- =====================================================================
-- 1. MOIC data anchor — deduplicated to one row per application
--    (preserves the original moic_data_base_os2 logic)
-- =====================================================================
cte_moic AS (
    SELECT
        moic1.*,
        ROW_NUMBER() OVER (PARTITION BY moic1.APPLICATIONID ORDER BY moic1.UPDATEDON DESC NULLS LAST, moic1.CREATEDON DESC NULLS LAST) AS rnk
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CRDETAILS moic1
    WHERE moic1.PAYMENTREQUESTID = 0
      AND moic1.ELIGIBILITYCRITERIAREQUESTTY = 'ASS'
      AND moic1.CRNUMBER IS NOT NULL
      AND moic1.CRNUMBER <> ''
),

cte_lmra AS (
    SELECT *
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_LMRA_DETAILS
    WHERE PAYMENTREQUESTID = 0
      AND ELIGIBILITYCRITERIAREQUESTTY = 'ASS'
),

cte_sio AS (
    SELECT *
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_SIO_ESTABLISHMENTDETAILS
    WHERE PAYMENTREQUESTID = 0
      AND ELIGIBILITYCRITERIAREQUESTTY = 'ASS'
),

-- =====================================================================
-- 2. MOIC activities — aggregate sector/ISIC codes per (application, CR, createdon)
--    (preserves the original moic_activities_base_os2 array_agg/array_join logic)
-- =====================================================================
cte_activities_raw AS (
    SELECT
        moic.*,
        sectorisicactivity.LABEL AS sector_activity_label
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CRACTIVITY moic
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISIC3 sectorisic
        ON moic.BUSINESSACTIVITYCODE = sectorisic.ISICCODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISICACTIVITY3 sectorisicactivity
        ON sectorisicactivity.CODE = sectorisic.SECTORISICACTIVITYID
    WHERE moic.PAYMENTREQUESTID = 0
      AND moic.ELIGIBILITYCRITERIAREQUESTTY = 'ASS'
      AND moic.CRNUMBER IS NOT NULL
      AND moic.CRNUMBER <> ''
),

cte_activities_agg AS (
    SELECT
        APPLICATIONID,
        CRNUMBER,
        CREATEDON,

        array_join(
            array_agg(DISTINCT BUSINESSACTIVITYCODE)
                FILTER (WHERE BUSINESSACTIVITYCODE IS NOT NULL),
            ' | '
        )                                                                    AS isic_4_activity_code,

        array_join(
            array_agg(DISTINCT DESCRIPTIONEN)
                FILTER (WHERE DESCRIPTIONEN IS NOT NULL),
            ' | '
        )                                                                    AS isic_4_activities_en,

        array_join(
            array_agg(DISTINCT DESCRIPTIONAR)
                FILTER (WHERE DESCRIPTIONAR IS NOT NULL),
            ' | '
        )                                                                    AS isic_4_activities_ar,

        array_join(
            array_agg(DISTINCT sector_activity_label)
                FILTER (WHERE sector_activity_label IS NOT NULL),
            ' | '
        )                                                                    AS business_activity_code,

        array_join(
            array_agg(DISTINCT CAST(ID AS VARCHAR))
                FILTER (WHERE ID IS NOT NULL),
            ' | '
        )                                                                    AS activity_id_list,

        array_join(
            array_agg(DISTINCT CAST(PSMONITORINGID AS VARCHAR))
                FILTER (WHERE PSMONITORINGID IS NOT NULL),
            ' | '
        )                                                                    AS activity_psmonitoring_id_list,

        array_join(
            array_agg(DISTINCT CAST(AMENDMENTREQUESTID AS VARCHAR))
                FILTER (WHERE AMENDMENTREQUESTID IS NOT NULL),
            ' | '
        )                                                                    AS activity_amendment_request_id_list,

        array_join(
            array_agg(DISTINCT CAST(ISLASTVERSION AS VARCHAR))
                FILTER (WHERE ISLASTVERSION IS NOT NULL),
            ' | '
        )                                                                    AS activity_islastversion_list

    FROM cte_activities_raw
    GROUP BY APPLICATIONID, CRNUMBER, CREATEDON
),

-- =====================================================================
-- 3. Dedup activities to per-application grain
--    (CRACTIVITY can have multiple (CR, CREATEDON) rows per app; we keep
--     the LATEST CREATEDON for parity with moic_data's RNK=1 logic)
-- =====================================================================
cte_activities AS (
    SELECT
        APPLICATIONID,
        CRNUMBER,
        CREATEDON                                                            AS activity_created_on,
        isic_4_activity_code,
        isic_4_activities_en,
        isic_4_activities_ar,
        business_activity_code,
        activity_id_list,
        activity_psmonitoring_id_list,
        activity_amendment_request_id_list,
        activity_islastversion_list,
        ROW_NUMBER() OVER (
            PARTITION BY APPLICATIONID
            ORDER BY CREATEDON DESC
        )                                                                    AS rn
    FROM cte_activities_agg
)

-- =====================================================================
-- 4. Final merge — LEFT JOIN activities onto MOIC data
-- =====================================================================
SELECT
    CAST(current_timestamp AT TIME ZONE 'UTC' AS DATE)                       AS extract_date,
    moic1.ID                                                                 AS id,
    moic1.APPLICATIONID                                                      AS id_application,
    moic1.PAYMENTREQUESTID                                                   AS payment_request_id,
    moic1.ELIGIBILITYCRITERIAREQUESTTY                                       AS eligibility_criteria_request_ty,
    moic1.CRNUMBER                                                           AS cr_number,
    moic1.COMPANYCATEGORYID                                                  AS company_category_id,
    moic1.COMPANYTYPECODE                                                    AS company_type_code,
    moic1.NATIONALITYCODE                                                    AS nationality_code,
    moic1.PSMONITORINGID                                                     AS ps_monitoring_id,
    moic1.AMENDMENTREQUESTID                                                 AS amendment_request_id,
    moic1.COMNERCIALNAMEEN                                                   AS comnercial_name_en_raw,
    moic1.COMNERCIALNAMEAR                                                   AS comnercial_name_ar_raw,
    moic1.ENTERPRISEGENDERID                                                 AS enterprise_gender_id,
    moic1.ENTERPRISEAGE                                                      AS enterprise_age_raw,
    moic1.COMPANYTYPE                                                        AS company_type,
    moic1.ISVIRTUAL                                                          AS is_virtual_raw,
    moic1.REGISTRATIONDATE                                                   AS registration_date_raw,
    moic1.EXPIRATIONDATE                                                     AS expiration_date_raw,
    moic1.NATIONALITY                                                        AS nationality,
    moic1.STATUS                                                             AS status,
    moic1.ISSUEDCAPITAL                                                      AS issued_capital,
    moic1.LOCALINVESTMENT                                                    AS local_investment,
    moic1.FOREIGNINVESTMENT                                                  AS foreign_investment,
    moic1.GCCINVESTMENT                                                      AS gcc_investment,
    moic1.ADDRESSFLAT                                                        AS address_flat_raw,
    moic1.ADDRESSROAD                                                        AS address_road_raw,
    moic1.ADDRESSBUILDING                                                    AS address_building_raw,
    moic1.ADDRESSTOWN                                                        AS address_town_raw,
    moic1.ADDRESSBLOCK                                                       AS address_block_raw,
    UPPER(TRIM(moic1.COMNERCIALNAMEEN))                                      AS commercial_name_en,
    UPPER(TRIM(moic1.COMNERCIALNAMEAR))                                      AS commercial_name_ar,
    moic1.ENTERPRISEGENDERID                                                 AS enterprise_gender,
    CASE
        WHEN moic1.COMPANYCATEGORYID IS NULL OR moic1.COMPANYCATEGORYID = '' THEN
            CASE
                WHEN lmra.ACTIVEWORKERS + lmra.PARALLELEXPATS + sio.TOTALBAHRAINIWORKERS > 100 THEN 'LARGE'
                WHEN lmra.ACTIVEWORKERS + lmra.PARALLELEXPATS + sio.TOTALBAHRAINIWORKERS >  50 THEN 'MEDI'
                WHEN lmra.ACTIVEWORKERS + lmra.PARALLELEXPATS + sio.TOTALBAHRAINIWORKERS <  51 THEN 'SMALL'
                ELSE 'unclassified'
            END
        ELSE moic1.COMPANYCATEGORYID
    END                                                                      AS enterprise_size,
    moic1.ENTERPRISEAGE                                                      AS enterprise_age,
    moic1.COMPANYTYPE                                                        AS cr_type,
    CASE
        WHEN moic1.ISVIRTUAL = FALSE THEN 'No'
        WHEN moic1.ISVIRTUAL = TRUE  THEN 'Yes'
        ELSE NULL
    END                                                                      AS is_virtual,
    CAST(moic1.REGISTRATIONDATE AS DATE)                                     AS registration_date,
    CAST(moic1.EXPIRATIONDATE AS DATE)                                       AS expiration_date,
    moic1.NATIONALITY                                                        AS cr_nationality,
    moic1.STATUS                                                             AS cr_license_status,
    moic1.ISSUEDCAPITAL                                                      AS capital_issued,
    moic1.LOCALINVESTMENT                                                    AS capital_local_investment,
    moic1.FOREIGNINVESTMENT                                                  AS capital_foreign_investment,
    moic1.GCCINVESTMENT                                                      AS capital_gcc_investment,
    moic1.ADDRESSFLAT                                                        AS address_flat,
    moic1.ADDRESSROAD                                                        AS address_road,
    moic1.ADDRESSBUILDING                                                    AS address_building,
    moic1.ADDRESSTOWN                                                        AS address_area,
    moic1.ADDRESSBLOCK                                                       AS address_block,
    lmra.ID                                                                  AS lmra_id,
    lmra.APPLICATIONID                                                       AS lmra_application_id,
    lmra.PAYMENTREQUESTID                                                    AS lmra_payment_request_id,
    lmra.ELIGIBILITYCRITERIAREQUESTTY                                        AS lmra_eligibility_criteria_request_ty,
    lmra.CODE                                                                AS lmra_code,
    lmra.TOTALBAHRAINIDISABLEWORKERS                                         AS total_bahraini_disable_workers,
    lmra.ISSUBJECTTOBAHRAINIZATION                                           AS is_subject_to_bahrainization,
    lmra.BAHRAINIZATIONTARGETPCT                                             AS bahrainization_target_pct,
    lmra.BAHRAINIZATIONCURRENTPCT                                            AS bahrainization_current_pct,
    lmra.BAHRAINIZATIONRATEDIFFPCT                                           AS bahrainization_rate_diff_pct,
    lmra.NOOFINVESTORS                                                       AS no_of_investors,
    lmra.HWTOWORKS                                                           AS hwto_works,
    lmra.ACTIVEWORKERS                                                       AS active_workers,
    lmra.PARALLELEXPATS                                                      AS parallel_expats,
    lmra.INPROGRESSREQUESTS                                                  AS in_progress_requests,
    lmra.TOTALNOOFNONBAHRANIWORKS                                            AS total_no_of_non_bahraini_workers,
    lmra.NOOFNONBAHRAINIPARALLEL                                             AS no_of_non_bahraini_parallel,
    lmra.AMENDMENTREQUESTID                                                  AS lmra_amendment_request_id,
    TRY_CAST(NULLIF(CAST(lmra.CREATEDON AS VARCHAR), '') AS TIMESTAMP)                              AS lmra_created_on,
    TRY_CAST(NULLIF(CAST(lmra.UPDATEDON AS VARCHAR), '') AS TIMESTAMP)                              AS lmra_updated_on,
    sio.ID                                                                   AS sio_id,
    sio.APPLICATIONID                                                        AS sio_application_id,
    sio.PAYMENTREQUESTID                                                     AS sio_payment_request_id,
    sio.ELIGIBILITYCRITERIAREQUESTTY                                         AS sio_eligibility_criteria_request_ty,
    sio.CODE                                                                 AS sio_code,
    sio.TOTALBAHRAINIWORKERS                                                 AS total_bahraini_workers,
    sio.TOTALBAHRAINISALARIES                                                AS total_bahraini_salaries,
    sio.TOTALEXPATRIATESALARIES                                              AS total_expatriate_salaries,
    sio.TOTALBAHRAINISALARIES600                                             AS total_bahraini_salaries_600,
    sio.TOTALEXPATRIATESSALARIES600                                          AS total_expatriate_salaries_600,
    sio.SITEVISITMONITORINGID                                                AS site_visit_monitoring_id,
    sio.AMENDMENTREQUESTID                                                   AS sio_amendment_request_id,
    act.isic_4_activity_code                                                 AS isic_4_activity_code,
    act.isic_4_activities_en                                                 AS isic_4_activities_en,
    act.isic_4_activities_ar                                                 AS isic_4_activities_ar,
    act.business_activity_code                                               AS business_activity_code,
    act.activity_id_list                                                     AS activity_id_list,
    act.activity_psmonitoring_id_list                                        AS activity_psmonitoring_id_list,
    act.activity_amendment_request_id_list                                   AS activity_amendment_request_id_list,
    act.activity_islastversion_list                                          AS activity_islastversion_list,
    act.activity_created_on                                                  AS activity_created_on,
    TRY_CAST(NULLIF(CAST(moic1.CREATEDON AS VARCHAR), '') AS TIMESTAMP)                             AS created_on,
    TRY_CAST(NULLIF(CAST(moic1.UPDATEDON AS VARCHAR), '') AS TIMESTAMP)                             AS updated_on,
    FALSE                                                                    AS is_deleted,
    UPPER(NULLIF(TRIM(CAST('NEO2' AS VARCHAR)), ''))                                       AS source_system_name,
    CAST(current_timestamp AT TIME ZONE 'UTC' AS TIMESTAMP)                  AS dbt_updated_at
FROM cte_moic moic1

LEFT JOIN cte_lmra lmra
    ON lmra.APPLICATIONID = moic1.APPLICATIONID

LEFT JOIN cte_sio sio
    ON sio.APPLICATIONID = moic1.APPLICATIONID

LEFT JOIN cte_activities act
    ON act.APPLICATIONID = moic1.APPLICATIONID
   AND act.rn = 1

WHERE moic1.rnk = 1
),

silver_layer AS (
SELECT
    extract_date,
    id,
    id_application,
    payment_request_id,
    eligibility_criteria_request_ty,
    cr_number,
    company_category_id,
    company_type_code,
    nationality_code,
    ps_monitoring_id,
    amendment_request_id,
    comnercial_name_en_raw,
    comnercial_name_ar_raw,
    enterprise_gender_id,
    enterprise_age_raw,
    company_type,
    is_virtual_raw,
    registration_date_raw,
    expiration_date_raw,
    nationality,
    status,
    issued_capital,
    local_investment,
    foreign_investment,
    gcc_investment,
    address_flat_raw,
    address_road_raw,
    address_building_raw,
    address_town_raw,
    address_block_raw,
    commercial_name_en,
    commercial_name_ar,
    enterprise_gender,
    enterprise_size,
    enterprise_age,
    cr_type,
    is_virtual,
    registration_date,
    expiration_date,
    cr_nationality,
    cr_license_status,
    capital_issued,
    capital_local_investment,
    capital_foreign_investment,
    capital_gcc_investment,
    address_flat,
    address_road,
    address_building,
    address_area,
    address_block,
    lmra_id,
    lmra_application_id,
    lmra_payment_request_id,
    lmra_eligibility_criteria_request_ty,
    lmra_code,
    total_bahraini_disable_workers,
    is_subject_to_bahrainization,
    bahrainization_target_pct,
    bahrainization_current_pct,
    bahrainization_rate_diff_pct,
    no_of_investors,
    hwto_works,
    active_workers,
    parallel_expats,
    in_progress_requests,
    total_no_of_non_bahraini_workers,
    no_of_non_bahraini_parallel,
    lmra_amendment_request_id,
    lmra_created_on,
    lmra_updated_on,
    sio_id,
    sio_application_id,
    sio_payment_request_id,
    sio_eligibility_criteria_request_ty,
    sio_code,
    total_bahraini_workers,
    total_bahraini_salaries,
    total_expatriate_salaries,
    total_bahraini_salaries_600,
    total_expatriate_salaries_600,
    site_visit_monitoring_id,
    sio_amendment_request_id,
    isic_4_activity_code,
    isic_4_activities_en,
    isic_4_activities_ar,
    business_activity_code,
    activity_id_list,
    activity_psmonitoring_id_list,
    activity_amendment_request_id_list,
    activity_islastversion_list,
    activity_created_on,
    created_on,
    updated_on,
    is_deleted,
    source_system_name,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".moic_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'id'),
        (3, 'id_application'),
        (4, 'payment_request_id'),
        (5, 'eligibility_criteria_request_ty'),
        (6, 'cr_number'),
        (7, 'company_category_id'),
        (8, 'company_type_code'),
        (9, 'nationality_code'),
        (10, 'ps_monitoring_id'),
        (11, 'amendment_request_id'),
        (12, 'comnercial_name_en_raw'),
        (13, 'comnercial_name_ar_raw'),
        (14, 'enterprise_gender_id'),
        (15, 'enterprise_age_raw'),
        (16, 'company_type'),
        (17, 'is_virtual_raw'),
        (18, 'registration_date_raw'),
        (19, 'expiration_date_raw'),
        (20, 'nationality'),
        (21, 'status'),
        (22, 'issued_capital'),
        (23, 'local_investment'),
        (24, 'foreign_investment'),
        (25, 'gcc_investment'),
        (26, 'address_flat_raw'),
        (27, 'address_road_raw'),
        (28, 'address_building_raw'),
        (29, 'address_town_raw'),
        (30, 'address_block_raw'),
        (31, 'commercial_name_en'),
        (32, 'commercial_name_ar'),
        (33, 'enterprise_gender'),
        (34, 'enterprise_size'),
        (35, 'enterprise_age'),
        (36, 'cr_type'),
        (37, 'is_virtual'),
        (38, 'registration_date'),
        (39, 'expiration_date'),
        (40, 'cr_nationality'),
        (41, 'cr_license_status'),
        (42, 'capital_issued'),
        (43, 'capital_local_investment'),
        (44, 'capital_foreign_investment'),
        (45, 'capital_gcc_investment'),
        (46, 'address_flat'),
        (47, 'address_road'),
        (48, 'address_building'),
        (49, 'address_area'),
        (50, 'address_block'),
        (51, 'lmra_id'),
        (52, 'lmra_application_id'),
        (53, 'lmra_payment_request_id'),
        (54, 'lmra_eligibility_criteria_request_ty'),
        (55, 'lmra_code'),
        (56, 'total_bahraini_disable_workers'),
        (57, 'is_subject_to_bahrainization'),
        (58, 'bahrainization_target_pct'),
        (59, 'bahrainization_current_pct'),
        (60, 'bahrainization_rate_diff_pct'),
        (61, 'no_of_investors'),
        (62, 'hwto_works'),
        (63, 'active_workers'),
        (64, 'parallel_expats'),
        (65, 'in_progress_requests'),
        (66, 'total_no_of_non_bahraini_workers'),
        (67, 'no_of_non_bahraini_parallel'),
        (68, 'lmra_amendment_request_id'),
        (69, 'lmra_created_on'),
        (70, 'lmra_updated_on'),
        (71, 'sio_id'),
        (72, 'sio_application_id'),
        (73, 'sio_payment_request_id'),
        (74, 'sio_eligibility_criteria_request_ty'),
        (75, 'sio_code'),
        (76, 'total_bahraini_workers'),
        (77, 'total_bahraini_salaries'),
        (78, 'total_expatriate_salaries'),
        (79, 'total_bahraini_salaries_600'),
        (80, 'total_expatriate_salaries_600'),
        (81, 'site_visit_monitoring_id'),
        (82, 'sio_amendment_request_id'),
        (83, 'isic_4_activity_code'),
        (84, 'isic_4_activities_en'),
        (85, 'isic_4_activities_ar'),
        (86, 'business_activity_code'),
        (87, 'activity_id_list'),
        (88, 'activity_psmonitoring_id_list'),
        (89, 'activity_amendment_request_id_list'),
        (90, 'activity_islastversion_list'),
        (91, 'activity_created_on'),
        (92, 'created_on'),
        (93, 'updated_on'),
        (94, 'is_deleted'),
        (95, 'source_system_name'),
        (96, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'id'),
        (3, 'id_application'),
        (4, 'payment_request_id'),
        (5, 'eligibility_criteria_request_ty'),
        (6, 'cr_number'),
        (7, 'company_category_id'),
        (8, 'company_type_code'),
        (9, 'nationality_code'),
        (10, 'ps_monitoring_id'),
        (11, 'amendment_request_id'),
        (12, 'comnercial_name_en_raw'),
        (13, 'comnercial_name_ar_raw'),
        (14, 'enterprise_gender_id'),
        (15, 'enterprise_age_raw'),
        (16, 'company_type'),
        (17, 'is_virtual_raw'),
        (18, 'registration_date_raw'),
        (19, 'expiration_date_raw'),
        (20, 'nationality'),
        (21, 'status'),
        (22, 'issued_capital'),
        (23, 'local_investment'),
        (24, 'foreign_investment'),
        (25, 'gcc_investment'),
        (26, 'address_flat_raw'),
        (27, 'address_road_raw'),
        (28, 'address_building_raw'),
        (29, 'address_town_raw'),
        (30, 'address_block_raw'),
        (31, 'commercial_name_en'),
        (32, 'commercial_name_ar'),
        (33, 'enterprise_gender'),
        (34, 'enterprise_size'),
        (35, 'enterprise_age'),
        (36, 'cr_type'),
        (37, 'is_virtual'),
        (38, 'registration_date'),
        (39, 'expiration_date'),
        (40, 'cr_nationality'),
        (41, 'cr_license_status'),
        (42, 'capital_issued'),
        (43, 'capital_local_investment'),
        (44, 'capital_foreign_investment'),
        (45, 'capital_gcc_investment'),
        (46, 'address_flat'),
        (47, 'address_road'),
        (48, 'address_building'),
        (49, 'address_area'),
        (50, 'address_block'),
        (51, 'lmra_id'),
        (52, 'lmra_application_id'),
        (53, 'lmra_payment_request_id'),
        (54, 'lmra_eligibility_criteria_request_ty'),
        (55, 'lmra_code'),
        (56, 'total_bahraini_disable_workers'),
        (57, 'is_subject_to_bahrainization'),
        (58, 'bahrainization_target_pct'),
        (59, 'bahrainization_current_pct'),
        (60, 'bahrainization_rate_diff_pct'),
        (61, 'no_of_investors'),
        (62, 'hwto_works'),
        (63, 'active_workers'),
        (64, 'parallel_expats'),
        (65, 'in_progress_requests'),
        (66, 'total_no_of_non_bahraini_workers'),
        (67, 'no_of_non_bahraini_parallel'),
        (68, 'lmra_amendment_request_id'),
        (69, 'lmra_created_on'),
        (70, 'lmra_updated_on'),
        (71, 'sio_id'),
        (72, 'sio_application_id'),
        (73, 'sio_payment_request_id'),
        (74, 'sio_eligibility_criteria_request_ty'),
        (75, 'sio_code'),
        (76, 'total_bahraini_workers'),
        (77, 'total_bahraini_salaries'),
        (78, 'total_expatriate_salaries'),
        (79, 'total_bahraini_salaries_600'),
        (80, 'total_expatriate_salaries_600'),
        (81, 'site_visit_monitoring_id'),
        (82, 'sio_amendment_request_id'),
        (83, 'isic_4_activity_code'),
        (84, 'isic_4_activities_en'),
        (85, 'isic_4_activities_ar'),
        (86, 'business_activity_code'),
        (87, 'activity_id_list'),
        (88, 'activity_psmonitoring_id_list'),
        (89, 'activity_amendment_request_id_list'),
        (90, 'activity_islastversion_list'),
        (91, 'activity_created_on'),
        (92, 'created_on'),
        (93, 'updated_on'),
        (94, 'is_deleted'),
        (95, 'source_system_name'),
        (96, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("extract_date" AS VARCHAR) AS "extract_date",
        CAST("id" AS VARCHAR) AS "id",
        CAST("id_application" AS VARCHAR) AS "id_application",
        CAST("payment_request_id" AS VARCHAR) AS "payment_request_id",
        CAST("eligibility_criteria_request_ty" AS VARCHAR) AS "eligibility_criteria_request_ty",
        CAST("cr_number" AS VARCHAR) AS "cr_number",
        CAST("company_category_id" AS VARCHAR) AS "company_category_id",
        CAST("company_type_code" AS VARCHAR) AS "company_type_code",
        CAST("nationality_code" AS VARCHAR) AS "nationality_code",
        CAST("ps_monitoring_id" AS VARCHAR) AS "ps_monitoring_id",
        CAST("amendment_request_id" AS VARCHAR) AS "amendment_request_id",
        CAST("comnercial_name_en_raw" AS VARCHAR) AS "comnercial_name_en_raw",
        CAST("comnercial_name_ar_raw" AS VARCHAR) AS "comnercial_name_ar_raw",
        CAST("enterprise_gender_id" AS VARCHAR) AS "enterprise_gender_id",
        CAST("enterprise_age_raw" AS VARCHAR) AS "enterprise_age_raw",
        CAST("company_type" AS VARCHAR) AS "company_type",
        CAST("is_virtual_raw" AS VARCHAR) AS "is_virtual_raw",
        CAST("registration_date_raw" AS VARCHAR) AS "registration_date_raw",
        CAST("expiration_date_raw" AS VARCHAR) AS "expiration_date_raw",
        CAST("nationality" AS VARCHAR) AS "nationality",
        CAST("status" AS VARCHAR) AS "status",
        CAST("issued_capital" AS VARCHAR) AS "issued_capital",
        CAST("local_investment" AS VARCHAR) AS "local_investment",
        CAST("foreign_investment" AS VARCHAR) AS "foreign_investment",
        CAST("gcc_investment" AS VARCHAR) AS "gcc_investment",
        CAST("address_flat_raw" AS VARCHAR) AS "address_flat_raw",
        CAST("address_road_raw" AS VARCHAR) AS "address_road_raw",
        CAST("address_building_raw" AS VARCHAR) AS "address_building_raw",
        CAST("address_town_raw" AS VARCHAR) AS "address_town_raw",
        CAST("address_block_raw" AS VARCHAR) AS "address_block_raw",
        CAST("commercial_name_en" AS VARCHAR) AS "commercial_name_en",
        CAST("commercial_name_ar" AS VARCHAR) AS "commercial_name_ar",
        CAST("enterprise_gender" AS VARCHAR) AS "enterprise_gender",
        CAST("enterprise_size" AS VARCHAR) AS "enterprise_size",
        CAST("enterprise_age" AS VARCHAR) AS "enterprise_age",
        CAST("cr_type" AS VARCHAR) AS "cr_type",
        CAST("is_virtual" AS VARCHAR) AS "is_virtual",
        CAST("registration_date" AS VARCHAR) AS "registration_date",
        CAST("expiration_date" AS VARCHAR) AS "expiration_date",
        CAST("cr_nationality" AS VARCHAR) AS "cr_nationality",
        CAST("cr_license_status" AS VARCHAR) AS "cr_license_status",
        CAST("capital_issued" AS VARCHAR) AS "capital_issued",
        CAST("capital_local_investment" AS VARCHAR) AS "capital_local_investment",
        CAST("capital_foreign_investment" AS VARCHAR) AS "capital_foreign_investment",
        CAST("capital_gcc_investment" AS VARCHAR) AS "capital_gcc_investment",
        CAST("address_flat" AS VARCHAR) AS "address_flat",
        CAST("address_road" AS VARCHAR) AS "address_road",
        CAST("address_building" AS VARCHAR) AS "address_building",
        CAST("address_area" AS VARCHAR) AS "address_area",
        CAST("address_block" AS VARCHAR) AS "address_block",
        CAST("lmra_id" AS VARCHAR) AS "lmra_id",
        CAST("lmra_application_id" AS VARCHAR) AS "lmra_application_id",
        CAST("lmra_payment_request_id" AS VARCHAR) AS "lmra_payment_request_id",
        CAST("lmra_eligibility_criteria_request_ty" AS VARCHAR) AS "lmra_eligibility_criteria_request_ty",
        CAST("lmra_code" AS VARCHAR) AS "lmra_code",
        CAST("total_bahraini_disable_workers" AS VARCHAR) AS "total_bahraini_disable_workers",
        CAST("is_subject_to_bahrainization" AS VARCHAR) AS "is_subject_to_bahrainization",
        CAST("bahrainization_target_pct" AS VARCHAR) AS "bahrainization_target_pct",
        CAST("bahrainization_current_pct" AS VARCHAR) AS "bahrainization_current_pct",
        CAST("bahrainization_rate_diff_pct" AS VARCHAR) AS "bahrainization_rate_diff_pct",
        CAST("no_of_investors" AS VARCHAR) AS "no_of_investors",
        CAST("hwto_works" AS VARCHAR) AS "hwto_works",
        CAST("active_workers" AS VARCHAR) AS "active_workers",
        CAST("parallel_expats" AS VARCHAR) AS "parallel_expats",
        CAST("in_progress_requests" AS VARCHAR) AS "in_progress_requests",
        CAST("total_no_of_non_bahraini_workers" AS VARCHAR) AS "total_no_of_non_bahraini_workers",
        CAST("no_of_non_bahraini_parallel" AS VARCHAR) AS "no_of_non_bahraini_parallel",
        CAST("lmra_amendment_request_id" AS VARCHAR) AS "lmra_amendment_request_id",
        CAST("lmra_created_on" AS VARCHAR) AS "lmra_created_on",
        CAST("lmra_updated_on" AS VARCHAR) AS "lmra_updated_on",
        CAST("sio_id" AS VARCHAR) AS "sio_id",
        CAST("sio_application_id" AS VARCHAR) AS "sio_application_id",
        CAST("sio_payment_request_id" AS VARCHAR) AS "sio_payment_request_id",
        CAST("sio_eligibility_criteria_request_ty" AS VARCHAR) AS "sio_eligibility_criteria_request_ty",
        CAST("sio_code" AS VARCHAR) AS "sio_code",
        CAST("total_bahraini_workers" AS VARCHAR) AS "total_bahraini_workers",
        CAST("total_bahraini_salaries" AS VARCHAR) AS "total_bahraini_salaries",
        CAST("total_expatriate_salaries" AS VARCHAR) AS "total_expatriate_salaries",
        CAST("total_bahraini_salaries_600" AS VARCHAR) AS "total_bahraini_salaries_600",
        CAST("total_expatriate_salaries_600" AS VARCHAR) AS "total_expatriate_salaries_600",
        CAST("site_visit_monitoring_id" AS VARCHAR) AS "site_visit_monitoring_id",
        CAST("sio_amendment_request_id" AS VARCHAR) AS "sio_amendment_request_id",
        CAST("isic_4_activity_code" AS VARCHAR) AS "isic_4_activity_code",
        CAST("isic_4_activities_en" AS VARCHAR) AS "isic_4_activities_en",
        CAST("isic_4_activities_ar" AS VARCHAR) AS "isic_4_activities_ar",
        CAST("business_activity_code" AS VARCHAR) AS "business_activity_code",
        CAST("activity_id_list" AS VARCHAR) AS "activity_id_list",
        CAST("activity_psmonitoring_id_list" AS VARCHAR) AS "activity_psmonitoring_id_list",
        CAST("activity_amendment_request_id_list" AS VARCHAR) AS "activity_amendment_request_id_list",
        CAST("activity_islastversion_list" AS VARCHAR) AS "activity_islastversion_list",
        CAST("activity_created_on" AS VARCHAR) AS "activity_created_on",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("updated_on" AS VARCHAR) AS "updated_on",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("extract_date" AS VARCHAR) AS "extract_date",
        CAST("id" AS VARCHAR) AS "id",
        CAST("id_application" AS VARCHAR) AS "id_application",
        CAST("payment_request_id" AS VARCHAR) AS "payment_request_id",
        CAST("eligibility_criteria_request_ty" AS VARCHAR) AS "eligibility_criteria_request_ty",
        CAST("cr_number" AS VARCHAR) AS "cr_number",
        CAST("company_category_id" AS VARCHAR) AS "company_category_id",
        CAST("company_type_code" AS VARCHAR) AS "company_type_code",
        CAST("nationality_code" AS VARCHAR) AS "nationality_code",
        CAST("ps_monitoring_id" AS VARCHAR) AS "ps_monitoring_id",
        CAST("amendment_request_id" AS VARCHAR) AS "amendment_request_id",
        CAST("comnercial_name_en_raw" AS VARCHAR) AS "comnercial_name_en_raw",
        CAST("comnercial_name_ar_raw" AS VARCHAR) AS "comnercial_name_ar_raw",
        CAST("enterprise_gender_id" AS VARCHAR) AS "enterprise_gender_id",
        CAST("enterprise_age_raw" AS VARCHAR) AS "enterprise_age_raw",
        CAST("company_type" AS VARCHAR) AS "company_type",
        CAST("is_virtual_raw" AS VARCHAR) AS "is_virtual_raw",
        CAST("registration_date_raw" AS VARCHAR) AS "registration_date_raw",
        CAST("expiration_date_raw" AS VARCHAR) AS "expiration_date_raw",
        CAST("nationality" AS VARCHAR) AS "nationality",
        CAST("status" AS VARCHAR) AS "status",
        CAST("issued_capital" AS VARCHAR) AS "issued_capital",
        CAST("local_investment" AS VARCHAR) AS "local_investment",
        CAST("foreign_investment" AS VARCHAR) AS "foreign_investment",
        CAST("gcc_investment" AS VARCHAR) AS "gcc_investment",
        CAST("address_flat_raw" AS VARCHAR) AS "address_flat_raw",
        CAST("address_road_raw" AS VARCHAR) AS "address_road_raw",
        CAST("address_building_raw" AS VARCHAR) AS "address_building_raw",
        CAST("address_town_raw" AS VARCHAR) AS "address_town_raw",
        CAST("address_block_raw" AS VARCHAR) AS "address_block_raw",
        CAST("commercial_name_en" AS VARCHAR) AS "commercial_name_en",
        CAST("commercial_name_ar" AS VARCHAR) AS "commercial_name_ar",
        CAST("enterprise_gender" AS VARCHAR) AS "enterprise_gender",
        CAST("enterprise_size" AS VARCHAR) AS "enterprise_size",
        CAST("enterprise_age" AS VARCHAR) AS "enterprise_age",
        CAST("cr_type" AS VARCHAR) AS "cr_type",
        CAST("is_virtual" AS VARCHAR) AS "is_virtual",
        CAST("registration_date" AS VARCHAR) AS "registration_date",
        CAST("expiration_date" AS VARCHAR) AS "expiration_date",
        CAST("cr_nationality" AS VARCHAR) AS "cr_nationality",
        CAST("cr_license_status" AS VARCHAR) AS "cr_license_status",
        CAST("capital_issued" AS VARCHAR) AS "capital_issued",
        CAST("capital_local_investment" AS VARCHAR) AS "capital_local_investment",
        CAST("capital_foreign_investment" AS VARCHAR) AS "capital_foreign_investment",
        CAST("capital_gcc_investment" AS VARCHAR) AS "capital_gcc_investment",
        CAST("address_flat" AS VARCHAR) AS "address_flat",
        CAST("address_road" AS VARCHAR) AS "address_road",
        CAST("address_building" AS VARCHAR) AS "address_building",
        CAST("address_area" AS VARCHAR) AS "address_area",
        CAST("address_block" AS VARCHAR) AS "address_block",
        CAST("lmra_id" AS VARCHAR) AS "lmra_id",
        CAST("lmra_application_id" AS VARCHAR) AS "lmra_application_id",
        CAST("lmra_payment_request_id" AS VARCHAR) AS "lmra_payment_request_id",
        CAST("lmra_eligibility_criteria_request_ty" AS VARCHAR) AS "lmra_eligibility_criteria_request_ty",
        CAST("lmra_code" AS VARCHAR) AS "lmra_code",
        CAST("total_bahraini_disable_workers" AS VARCHAR) AS "total_bahraini_disable_workers",
        CAST("is_subject_to_bahrainization" AS VARCHAR) AS "is_subject_to_bahrainization",
        CAST("bahrainization_target_pct" AS VARCHAR) AS "bahrainization_target_pct",
        CAST("bahrainization_current_pct" AS VARCHAR) AS "bahrainization_current_pct",
        CAST("bahrainization_rate_diff_pct" AS VARCHAR) AS "bahrainization_rate_diff_pct",
        CAST("no_of_investors" AS VARCHAR) AS "no_of_investors",
        CAST("hwto_works" AS VARCHAR) AS "hwto_works",
        CAST("active_workers" AS VARCHAR) AS "active_workers",
        CAST("parallel_expats" AS VARCHAR) AS "parallel_expats",
        CAST("in_progress_requests" AS VARCHAR) AS "in_progress_requests",
        CAST("total_no_of_non_bahraini_workers" AS VARCHAR) AS "total_no_of_non_bahraini_workers",
        CAST("no_of_non_bahraini_parallel" AS VARCHAR) AS "no_of_non_bahraini_parallel",
        CAST("lmra_amendment_request_id" AS VARCHAR) AS "lmra_amendment_request_id",
        CAST("lmra_created_on" AS VARCHAR) AS "lmra_created_on",
        CAST("lmra_updated_on" AS VARCHAR) AS "lmra_updated_on",
        CAST("sio_id" AS VARCHAR) AS "sio_id",
        CAST("sio_application_id" AS VARCHAR) AS "sio_application_id",
        CAST("sio_payment_request_id" AS VARCHAR) AS "sio_payment_request_id",
        CAST("sio_eligibility_criteria_request_ty" AS VARCHAR) AS "sio_eligibility_criteria_request_ty",
        CAST("sio_code" AS VARCHAR) AS "sio_code",
        CAST("total_bahraini_workers" AS VARCHAR) AS "total_bahraini_workers",
        CAST("total_bahraini_salaries" AS VARCHAR) AS "total_bahraini_salaries",
        CAST("total_expatriate_salaries" AS VARCHAR) AS "total_expatriate_salaries",
        CAST("total_bahraini_salaries_600" AS VARCHAR) AS "total_bahraini_salaries_600",
        CAST("total_expatriate_salaries_600" AS VARCHAR) AS "total_expatriate_salaries_600",
        CAST("site_visit_monitoring_id" AS VARCHAR) AS "site_visit_monitoring_id",
        CAST("sio_amendment_request_id" AS VARCHAR) AS "sio_amendment_request_id",
        CAST("isic_4_activity_code" AS VARCHAR) AS "isic_4_activity_code",
        CAST("isic_4_activities_en" AS VARCHAR) AS "isic_4_activities_en",
        CAST("isic_4_activities_ar" AS VARCHAR) AS "isic_4_activities_ar",
        CAST("business_activity_code" AS VARCHAR) AS "business_activity_code",
        CAST("activity_id_list" AS VARCHAR) AS "activity_id_list",
        CAST("activity_psmonitoring_id_list" AS VARCHAR) AS "activity_psmonitoring_id_list",
        CAST("activity_amendment_request_id_list" AS VARCHAR) AS "activity_amendment_request_id_list",
        CAST("activity_islastversion_list" AS VARCHAR) AS "activity_islastversion_list",
        CAST("activity_created_on" AS VARCHAR) AS "activity_created_on",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("updated_on" AS VARCHAR) AS "updated_on",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
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
        'moic_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'moic_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'moic_base' AS table_name,
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
        'moic_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'moic_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
