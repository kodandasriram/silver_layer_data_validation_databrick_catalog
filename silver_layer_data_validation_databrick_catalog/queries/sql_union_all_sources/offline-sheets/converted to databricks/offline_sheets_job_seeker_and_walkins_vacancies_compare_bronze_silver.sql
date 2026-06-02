WITH
bronze_layer AS (
    SELECT
        extract_date,
        job_center,
        date_of_update,
        id AS vacancy_id,
        company_name,
        industry_sector,
        roles_positions,
        no_of_vacancies,
        salary_bhd,
        job_application_status,
        date_of_employment,
        salary_ranges_bhd,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_job_seeker_and_walkins_vacancies
),

silver_layer AS (
    SELECT
        extract_date,
        job_center,
        date_of_update,
        vacancy_id,
        company_name,
        industry_sector,
        roles_positions,
        no_of_vacancies,
        salary_bhd,
        job_application_status,
        date_of_employment,
        salary_ranges_bhd,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_job_seeker_and_walkins_vacancies
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'job_center'),
        (3, 'date_of_update'),
        (4, 'vacancy_id'),
        (5, 'company_name'),
        (6, 'industry_sector'),
        (7, 'roles_positions'),
        (8, 'no_of_vacancies'),
        (9, 'salary_bhd'),
        (10, 'job_application_status'),
        (11, 'date_of_employment'),
        (12, 'salary_ranges_bhd'),
        (13, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'job_center'),
        (3, 'date_of_update'),
        (4, 'vacancy_id'),
        (5, 'company_name'),
        (6, 'industry_sector'),
        (7, 'roles_positions'),
        (8, 'no_of_vacancies'),
        (9, 'salary_bhd'),
        (10, 'job_application_status'),
        (11, 'date_of_employment'),
        (12, 'salary_ranges_bhd'),
        (13, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(job_center AS STRING) AS job_center,
        CAST(date_of_update AS STRING) AS date_of_update,
        CAST(vacancy_id AS STRING) AS vacancy_id,
        CAST(company_name AS STRING) AS company_name,
        CAST(industry_sector AS STRING) AS industry_sector,
        CAST(roles_positions AS STRING) AS roles_positions,
        CAST(no_of_vacancies AS STRING) AS no_of_vacancies,
        CAST(salary_bhd AS STRING) AS salary_bhd,
        CAST(job_application_status AS STRING) AS job_application_status,
        CAST(date_of_employment AS STRING) AS date_of_employment,
        CAST(salary_ranges_bhd AS STRING) AS salary_ranges_bhd,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(job_center AS STRING) AS job_center,
        CAST(date_of_update AS STRING) AS date_of_update,
        CAST(vacancy_id AS STRING) AS vacancy_id,
        CAST(company_name AS STRING) AS company_name,
        CAST(industry_sector AS STRING) AS industry_sector,
        CAST(roles_positions AS STRING) AS roles_positions,
        CAST(no_of_vacancies AS STRING) AS no_of_vacancies,
        CAST(salary_bhd AS STRING) AS salary_bhd,
        CAST(job_application_status AS STRING) AS job_application_status,
        CAST(date_of_employment AS STRING) AS date_of_employment,
        CAST(salary_ranges_bhd AS STRING) AS salary_ranges_bhd,
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
        'offline_sheets_job_seeker_and_walkins_vacancies' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_vacancies' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_vacancies' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_vacancies' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_vacancies' AS table_name,
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
