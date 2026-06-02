WITH
bronze_layer AS (
    SELECT
        extract_month,
        cr_no,
        branch_no,
        cr_status,
        status_en,
        status_ar,
        commercial_name_en,
        commercial_name_ar,
        cr_type,
        company_type_ar,
        company_type_en,
        previous_company_type_ar,
        previous_company_type_en,
        type_change_date,
        reg_date,
        expiry_date,
        flat_shop_no AS address_flat_shop,
        building AS address_building,
        road_street AS address_road,
        block AS address_block,
        town AS address_town,
        municipality_en,
        municipality_ar,
        phone,
        mobile,
        email,
        isic4,
        activity_en,
        activity_ar,
        sector_en,
        sector_ar,
        cr_class,
        ownership,
        gender,
        issued_capital_bd,
        authorized_capital_bd,
        sijili_date,
        deletion_date,
        conventional_date,
        current_timestamp() AS dbt_updated_at,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_moic_active_and_deleted_crs
),

silver_layer AS (
    SELECT
        extract_month,
        cr_no,
        branch_no,
        cr_status,
        status_en,
        status_ar,
        commercial_name_en,
        commercial_name_ar,
        cr_type,
        company_type_ar,
        company_type_en,
        previous_company_type_ar,
        previous_company_type_en,
        type_change_date,
        reg_date,
        expiry_date,
        address_flat_shop,
        address_building,
        address_road,
        address_block,
        address_town,
        municipality_en,
        municipality_ar,
        phone,
        mobile,
        email,
        isic4,
        activity_en,
        activity_ar,
        sector_en,
        sector_ar,
        cr_class,
        ownership,
        gender,
        issued_capital_bd,
        authorized_capital_bd,
        sijili_date,
        deletion_date,
        conventional_date,
        dbt_updated_at,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_moic_active_and_deleted_crs
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_month'),
        (2, 'cr_no'),
        (3, 'branch_no'),
        (4, 'cr_status'),
        (5, 'status_en'),
        (6, 'status_ar'),
        (7, 'commercial_name_en'),
        (8, 'commercial_name_ar'),
        (9, 'cr_type'),
        (10, 'company_type_ar'),
        (11, 'company_type_en'),
        (12, 'previous_company_type_ar'),
        (13, 'previous_company_type_en'),
        (14, 'type_change_date'),
        (15, 'reg_date'),
        (16, 'expiry_date'),
        (17, 'address_flat_shop'),
        (18, 'address_building'),
        (19, 'address_road'),
        (20, 'address_block'),
        (21, 'address_town'),
        (22, 'municipality_en'),
        (23, 'municipality_ar'),
        (24, 'phone'),
        (25, 'mobile'),
        (26, 'email'),
        (27, 'isic4'),
        (28, 'activity_en'),
        (29, 'activity_ar'),
        (30, 'sector_en'),
        (31, 'sector_ar'),
        (32, 'cr_class'),
        (33, 'ownership'),
        (34, 'gender'),
        (35, 'issued_capital_bd'),
        (36, 'authorized_capital_bd'),
        (37, 'sijili_date'),
        (38, 'deletion_date'),
        (39, 'conventional_date'),
        (40, 'dbt_updated_at'),
        (41, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_month'),
        (2, 'cr_no'),
        (3, 'branch_no'),
        (4, 'cr_status'),
        (5, 'status_en'),
        (6, 'status_ar'),
        (7, 'commercial_name_en'),
        (8, 'commercial_name_ar'),
        (9, 'cr_type'),
        (10, 'company_type_ar'),
        (11, 'company_type_en'),
        (12, 'previous_company_type_ar'),
        (13, 'previous_company_type_en'),
        (14, 'type_change_date'),
        (15, 'reg_date'),
        (16, 'expiry_date'),
        (17, 'address_flat_shop'),
        (18, 'address_building'),
        (19, 'address_road'),
        (20, 'address_block'),
        (21, 'address_town'),
        (22, 'municipality_en'),
        (23, 'municipality_ar'),
        (24, 'phone'),
        (25, 'mobile'),
        (26, 'email'),
        (27, 'isic4'),
        (28, 'activity_en'),
        (29, 'activity_ar'),
        (30, 'sector_en'),
        (31, 'sector_ar'),
        (32, 'cr_class'),
        (33, 'ownership'),
        (34, 'gender'),
        (35, 'issued_capital_bd'),
        (36, 'authorized_capital_bd'),
        (37, 'sijili_date'),
        (38, 'deletion_date'),
        (39, 'conventional_date'),
        (40, 'dbt_updated_at'),
        (41, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_month AS STRING) AS extract_month,
        CAST(cr_no AS STRING) AS cr_no,
        CAST(branch_no AS STRING) AS branch_no,
        CAST(cr_status AS STRING) AS cr_status,
        CAST(status_en AS STRING) AS status_en,
        CAST(status_ar AS STRING) AS status_ar,
        CAST(commercial_name_en AS STRING) AS commercial_name_en,
        CAST(commercial_name_ar AS STRING) AS commercial_name_ar,
        CAST(cr_type AS STRING) AS cr_type,
        CAST(company_type_ar AS STRING) AS company_type_ar,
        CAST(company_type_en AS STRING) AS company_type_en,
        CAST(previous_company_type_ar AS STRING) AS previous_company_type_ar,
        CAST(previous_company_type_en AS STRING) AS previous_company_type_en,
        CAST(type_change_date AS STRING) AS type_change_date,
        CAST(reg_date AS STRING) AS reg_date,
        CAST(expiry_date AS STRING) AS expiry_date,
        CAST(address_flat_shop AS STRING) AS address_flat_shop,
        CAST(address_building AS STRING) AS address_building,
        CAST(address_road AS STRING) AS address_road,
        CAST(address_block AS STRING) AS address_block,
        CAST(address_town AS STRING) AS address_town,
        CAST(municipality_en AS STRING) AS municipality_en,
        CAST(municipality_ar AS STRING) AS municipality_ar,
        CAST(phone AS STRING) AS phone,
        CAST(mobile AS STRING) AS mobile,
        CAST(email AS STRING) AS email,
        CAST(isic4 AS STRING) AS isic4,
        CAST(activity_en AS STRING) AS activity_en,
        CAST(activity_ar AS STRING) AS activity_ar,
        CAST(sector_en AS STRING) AS sector_en,
        CAST(sector_ar AS STRING) AS sector_ar,
        CAST(cr_class AS STRING) AS cr_class,
        CAST(ownership AS STRING) AS ownership,
        CAST(gender AS STRING) AS gender,
        CAST(issued_capital_bd AS STRING) AS issued_capital_bd,
        CAST(authorized_capital_bd AS STRING) AS authorized_capital_bd,
        CAST(sijili_date AS STRING) AS sijili_date,
        CAST(deletion_date AS STRING) AS deletion_date,
        CAST(conventional_date AS STRING) AS conventional_date,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_month AS STRING) AS extract_month,
        CAST(cr_no AS STRING) AS cr_no,
        CAST(branch_no AS STRING) AS branch_no,
        CAST(cr_status AS STRING) AS cr_status,
        CAST(status_en AS STRING) AS status_en,
        CAST(status_ar AS STRING) AS status_ar,
        CAST(commercial_name_en AS STRING) AS commercial_name_en,
        CAST(commercial_name_ar AS STRING) AS commercial_name_ar,
        CAST(cr_type AS STRING) AS cr_type,
        CAST(company_type_ar AS STRING) AS company_type_ar,
        CAST(company_type_en AS STRING) AS company_type_en,
        CAST(previous_company_type_ar AS STRING) AS previous_company_type_ar,
        CAST(previous_company_type_en AS STRING) AS previous_company_type_en,
        CAST(type_change_date AS STRING) AS type_change_date,
        CAST(reg_date AS STRING) AS reg_date,
        CAST(expiry_date AS STRING) AS expiry_date,
        CAST(address_flat_shop AS STRING) AS address_flat_shop,
        CAST(address_building AS STRING) AS address_building,
        CAST(address_road AS STRING) AS address_road,
        CAST(address_block AS STRING) AS address_block,
        CAST(address_town AS STRING) AS address_town,
        CAST(municipality_en AS STRING) AS municipality_en,
        CAST(municipality_ar AS STRING) AS municipality_ar,
        CAST(phone AS STRING) AS phone,
        CAST(mobile AS STRING) AS mobile,
        CAST(email AS STRING) AS email,
        CAST(isic4 AS STRING) AS isic4,
        CAST(activity_en AS STRING) AS activity_en,
        CAST(activity_ar AS STRING) AS activity_ar,
        CAST(sector_en AS STRING) AS sector_en,
        CAST(sector_ar AS STRING) AS sector_ar,
        CAST(cr_class AS STRING) AS cr_class,
        CAST(ownership AS STRING) AS ownership,
        CAST(gender AS STRING) AS gender,
        CAST(issued_capital_bd AS STRING) AS issued_capital_bd,
        CAST(authorized_capital_bd AS STRING) AS authorized_capital_bd,
        CAST(sijili_date AS STRING) AS sijili_date,
        CAST(deletion_date AS STRING) AS deletion_date,
        CAST(conventional_date AS STRING) AS conventional_date,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at,
        CAST(source_system_name AS STRING) AS source_system_name
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
        'offline_sheets_moic_active_and_deleted_crs' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE
            WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer)
                THEN 'PASS'
            ELSE 'FAIL'
        END AS validation_status

    UNION ALL

    SELECT
        'offline_sheets_moic_active_and_deleted_crs' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE
            WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns)
                THEN 'PASS'
            ELSE 'FAIL'
        END AS validation_status

    UNION ALL

    SELECT
        'offline_sheets_moic_active_and_deleted_crs' AS table_name,
        'column_names_match' AS validation_point,
        CAST((
            SELECT COUNT(*)
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL
               OR s.column_name IS NULL
        ) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM bronze_columns b
                FULL OUTER JOIN silver_columns s
                  ON b.column_position = s.column_position
                 AND b.column_name = s.column_name
                WHERE b.column_name IS NULL
                   OR s.column_name IS NULL
            ) THEN 'PASS'
            ELSE 'FAIL'
        END AS validation_status

    UNION ALL

    SELECT
        'offline_sheets_moic_active_and_deleted_crs' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE
            WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0
                THEN 'PASS'
            ELSE 'FAIL'
        END AS validation_status

    UNION ALL

    SELECT
        'offline_sheets_moic_active_and_deleted_crs' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE
            WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0
                THEN 'PASS'
            ELSE 'FAIL'
        END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
