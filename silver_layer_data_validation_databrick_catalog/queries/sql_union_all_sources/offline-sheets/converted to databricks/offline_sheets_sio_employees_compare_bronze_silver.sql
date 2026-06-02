WITH
bronze_layer AS (
    SELECT
        extract_month,
        branch_cr_number,
        branch_cr_number_main_cr,
        branch_cr_number_branch_cr,
        employer_number,
        branch_number,
        employer_insurance_type_description_english AS employer_insurance_type,
        branch_name_english AS branch_name,
        branch_nationality_english_description AS branch_nationality,
        economic_activity_english_description AS economic_activity,
        worker_cpr,
        nationality_b_nb AS is_bahraini,
        worker_date_of_birth,
        worker_insurance_start_date,
        worker_gender_description AS worker_gender,
        job_position_description_english AS job_position,
        worker_qualification_english_description AS worker_qualification,
        job_start_date_employer_and_branch AS job_start_date,
        job_end_date,
        job_terminiation_code,
        job_terminiation_english AS job_termination_reason,
        sio_salary_ranges_tamkeen AS sio_salary_range,
        number_of_months_approximation,
        current_timestamp() AS dbt_updated_on,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_external_data_sio_employees
),

silver_layer AS (
    SELECT
        extract_month,
        branch_cr_number,
        branch_cr_number_main_cr,
        branch_cr_number_branch_cr,
        employer_number,
        branch_number,
        employer_insurance_type,
        branch_name,
        branch_nationality,
        economic_activity,
        worker_cpr,
        is_bahraini,
        worker_date_of_birth,
        worker_insurance_start_date,
        worker_gender,
        job_position,
        worker_qualification,
        job_start_date,
        job_end_date,
        job_terminiation_code,
        job_termination_reason,
        sio_salary_range,
        number_of_months_approximation,
        dbt_updated_on,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_sio_employees
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_month'),
        (2, 'branch_cr_number'),
        (3, 'branch_cr_number_main_cr'),
        (4, 'branch_cr_number_branch_cr'),
        (5, 'employer_number'),
        (6, 'branch_number'),
        (7, 'employer_insurance_type'),
        (8, 'branch_name'),
        (9, 'branch_nationality'),
        (10, 'economic_activity'),
        (11, 'worker_cpr'),
        (12, 'is_bahraini'),
        (13, 'worker_date_of_birth'),
        (14, 'worker_insurance_start_date'),
        (15, 'worker_gender'),
        (16, 'job_position'),
        (17, 'worker_qualification'),
        (18, 'job_start_date'),
        (19, 'job_end_date'),
        (20, 'job_terminiation_code'),
        (21, 'job_termination_reason'),
        (22, 'sio_salary_range'),
        (23, 'number_of_months_approximation'),
        (24, 'dbt_updated_on'),
        (25, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_month'),
        (2, 'branch_cr_number'),
        (3, 'branch_cr_number_main_cr'),
        (4, 'branch_cr_number_branch_cr'),
        (5, 'employer_number'),
        (6, 'branch_number'),
        (7, 'employer_insurance_type'),
        (8, 'branch_name'),
        (9, 'branch_nationality'),
        (10, 'economic_activity'),
        (11, 'worker_cpr'),
        (12, 'is_bahraini'),
        (13, 'worker_date_of_birth'),
        (14, 'worker_insurance_start_date'),
        (15, 'worker_gender'),
        (16, 'job_position'),
        (17, 'worker_qualification'),
        (18, 'job_start_date'),
        (19, 'job_end_date'),
        (20, 'job_terminiation_code'),
        (21, 'job_termination_reason'),
        (22, 'sio_salary_range'),
        (23, 'number_of_months_approximation'),
        (24, 'dbt_updated_on'),
        (25, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_month AS STRING) AS extract_month,
        CAST(branch_cr_number AS STRING) AS branch_cr_number,
        CAST(branch_cr_number_main_cr AS STRING) AS branch_cr_number_main_cr,
        CAST(branch_cr_number_branch_cr AS STRING) AS branch_cr_number_branch_cr,
        CAST(employer_number AS STRING) AS employer_number,
        CAST(branch_number AS STRING) AS branch_number,
        CAST(employer_insurance_type AS STRING) AS employer_insurance_type,
        CAST(branch_name AS STRING) AS branch_name,
        CAST(branch_nationality AS STRING) AS branch_nationality,
        CAST(economic_activity AS STRING) AS economic_activity,
        CAST(worker_cpr AS STRING) AS worker_cpr,
        CAST(is_bahraini AS STRING) AS is_bahraini,
        CAST(worker_date_of_birth AS STRING) AS worker_date_of_birth,
        CAST(worker_insurance_start_date AS STRING) AS worker_insurance_start_date,
        CAST(worker_gender AS STRING) AS worker_gender,
        CAST(job_position AS STRING) AS job_position,
        CAST(worker_qualification AS STRING) AS worker_qualification,
        CAST(job_start_date AS STRING) AS job_start_date,
        CAST(job_end_date AS STRING) AS job_end_date,
        CAST(job_terminiation_code AS STRING) AS job_terminiation_code,
        CAST(job_termination_reason AS STRING) AS job_termination_reason,
        CAST(sio_salary_range AS STRING) AS sio_salary_range,
        CAST(number_of_months_approximation AS STRING) AS number_of_months_approximation,
        CAST(dbt_updated_on AS STRING) AS dbt_updated_on,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_month AS STRING) AS extract_month,
        CAST(branch_cr_number AS STRING) AS branch_cr_number,
        CAST(branch_cr_number_main_cr AS STRING) AS branch_cr_number_main_cr,
        CAST(branch_cr_number_branch_cr AS STRING) AS branch_cr_number_branch_cr,
        CAST(employer_number AS STRING) AS employer_number,
        CAST(branch_number AS STRING) AS branch_number,
        CAST(employer_insurance_type AS STRING) AS employer_insurance_type,
        CAST(branch_name AS STRING) AS branch_name,
        CAST(branch_nationality AS STRING) AS branch_nationality,
        CAST(economic_activity AS STRING) AS economic_activity,
        CAST(worker_cpr AS STRING) AS worker_cpr,
        CAST(is_bahraini AS STRING) AS is_bahraini,
        CAST(worker_date_of_birth AS STRING) AS worker_date_of_birth,
        CAST(worker_insurance_start_date AS STRING) AS worker_insurance_start_date,
        CAST(worker_gender AS STRING) AS worker_gender,
        CAST(job_position AS STRING) AS job_position,
        CAST(worker_qualification AS STRING) AS worker_qualification,
        CAST(job_start_date AS STRING) AS job_start_date,
        CAST(job_end_date AS STRING) AS job_end_date,
        CAST(job_terminiation_code AS STRING) AS job_terminiation_code,
        CAST(job_termination_reason AS STRING) AS job_termination_reason,
        CAST(sio_salary_range AS STRING) AS sio_salary_range,
        CAST(number_of_months_approximation AS STRING) AS number_of_months_approximation,
        CAST(dbt_updated_on AS STRING) AS dbt_updated_on,
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
        'offline_sheets_sio_employees' AS table_name,
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
        'offline_sheets_sio_employees' AS table_name,
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
        'offline_sheets_sio_employees' AS table_name,
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
        'offline_sheets_sio_employees' AS table_name,
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
        'offline_sheets_sio_employees' AS table_name,
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
