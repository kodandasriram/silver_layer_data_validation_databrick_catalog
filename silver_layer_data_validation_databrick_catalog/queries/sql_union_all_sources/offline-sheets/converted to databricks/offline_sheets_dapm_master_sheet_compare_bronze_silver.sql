WITH
bronze_layer AS (
    SELECT
        extract_date,
        seq_num,
        project,
        project_segment,
        on_system as is_on_system,
        collection_status,
        project_status,
        comments,
        'OFFLINE_SHEETS' as source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_dapm_master_sheet
),

silver_layer AS (
    SELECT
        extract_date,
        seq_num,
        project,
        project_segment,
        is_on_system,
        collection_status,
        project_status,
        comments,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_dapm_master_sheet
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'seq_num'),
        (3, 'project'),
        (4, 'project_segment'),
        (5, 'is_on_system'),
        (6, 'collection_status'),
        (7, 'project_status'),
        (8, 'comments'),
        (9, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'seq_num'),
        (3, 'project'),
        (4, 'project_segment'),
        (5, 'is_on_system'),
        (6, 'collection_status'),
        (7, 'project_status'),
        (8, 'comments'),
        (9, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(seq_num AS STRING) AS seq_num,
        CAST(project AS STRING) AS project,
        CAST(project_segment AS STRING) AS project_segment,
        CAST(is_on_system AS STRING) AS is_on_system,
        CAST(collection_status AS STRING) AS collection_status,
        CAST(project_status AS STRING) AS project_status,
        CAST(comments AS STRING) AS comments,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(seq_num AS STRING) AS seq_num,
        CAST(project AS STRING) AS project,
        CAST(project_segment AS STRING) AS project_segment,
        CAST(is_on_system AS STRING) AS is_on_system,
        CAST(collection_status AS STRING) AS collection_status,
        CAST(project_status AS STRING) AS project_status,
        CAST(comments AS STRING) AS comments,
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
        'offline_sheets_dapm_master_sheet' AS table_name,
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
        'offline_sheets_dapm_master_sheet' AS table_name,
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
        'offline_sheets_dapm_master_sheet' AS table_name,
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
        'offline_sheets_dapm_master_sheet' AS table_name,
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
        'offline_sheets_dapm_master_sheet' AS table_name,
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
