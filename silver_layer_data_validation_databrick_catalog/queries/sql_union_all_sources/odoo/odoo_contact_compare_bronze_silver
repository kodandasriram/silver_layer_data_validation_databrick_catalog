WITH
bronze_layer AS (
    SELECT
        name,
        country,
        `company id` AS cr_number,
        `sector team` AS sector_team,
        salesperson AS relationship_manager,
        `moic issued capital` AS moic_issued_capital,
        `# meetings` AS number_of_meetings,
        `global rm` AS global_rm,
        activities,
        `created on` AS created_on,
        CAST('ODOO' AS STRING) AS source_system_name,
        current_timestamp() AS dbt_updated_at
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.odoo_contacts
),

silver_layer AS (
    SELECT
        name,
        country,
        cr_number,
        sector_team,
        relationship_manager,
        moic_issued_capital,
        number_of_meetings,
        global_rm,
        activities,
        created_on,
        source_system_name,
        dbt_updated_at
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.odoo_contact_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'name'),
        (2, 'country'),
        (3, 'cr_number'),
        (4, 'sector_team'),
        (5, 'relationship_manager'),
        (6, 'moic_issued_capital'),
        (7, 'number_of_meetings'),
        (8, 'global_rm'),
        (9, 'activities'),
        (10, 'created_on'),
        (11, 'source_system_name'),
        (12, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'name'),
        (2, 'country'),
        (3, 'cr_number'),
        (4, 'sector_team'),
        (5, 'relationship_manager'),
        (6, 'moic_issued_capital'),
        (7, 'number_of_meetings'),
        (8, 'global_rm'),
        (9, 'activities'),
        (10, 'created_on'),
        (11, 'source_system_name'),
        (12, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(name AS STRING) AS name,
        CAST(country AS STRING) AS country,
        CAST(cr_number AS STRING) AS cr_number,
        CAST(sector_team AS STRING) AS sector_team,
        CAST(relationship_manager AS STRING) AS relationship_manager,
        CAST(moic_issued_capital AS STRING) AS moic_issued_capital,
        CAST(number_of_meetings AS STRING) AS number_of_meetings,
        CAST(global_rm AS STRING) AS global_rm,
        CAST(activities AS STRING) AS activities,
        CAST(created_on AS STRING) AS created_on,
        CAST(source_system_name AS STRING) AS source_system_name,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(name AS STRING) AS name,
        CAST(country AS STRING) AS country,
        CAST(cr_number AS STRING) AS cr_number,
        CAST(sector_team AS STRING) AS sector_team,
        CAST(relationship_manager AS STRING) AS relationship_manager,
        CAST(moic_issued_capital AS STRING) AS moic_issued_capital,
        CAST(number_of_meetings AS STRING) AS number_of_meetings,
        CAST(global_rm AS STRING) AS global_rm,
        CAST(activities AS STRING) AS activities,
        CAST(created_on AS STRING) AS created_on,
        CAST(source_system_name AS STRING) AS source_system_name,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at
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
        'odoo_contact_base' AS table_name,
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
        'odoo_contact_base' AS table_name,
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
        'odoo_contact_base' AS table_name,
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
        'odoo_contact_base' AS table_name,
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
        'odoo_contact_base' AS table_name,
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
