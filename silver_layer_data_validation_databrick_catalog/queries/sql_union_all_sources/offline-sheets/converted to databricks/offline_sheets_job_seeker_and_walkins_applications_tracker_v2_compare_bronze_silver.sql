WITH
bronze_layer AS (
    SELECT
        extract_date,
        id,
        application_no,
        application_id,
        cpr,
        individual_name,
        joining_date,
        mol_registered,
        job_center,
        basic_salary,
        allowances,
        position,
        company,
        cr_license_no,
        contract_type,
        application_status,
        contract_signed,
        additional_comments,
        mol_registered_jobseeker_for_3plus_years AS mol_registered_3plus_years,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_job_seeker_and_walkins_applications_tracker_v2
),

silver_layer AS (
    SELECT
        extract_date,
        id,
        application_no,
        application_id,
        cpr,
        individual_name,
        joining_date,
        mol_registered,
        job_center,
        basic_salary,
        allowances,
        position,
        company,
        cr_license_no,
        contract_type,
        application_status,
        contract_signed,
        additional_comments,
        mol_registered_3plus_years,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_job_seeker_and_walkins_applications_tracker_v2
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'id'),
        (3, 'application_no'),
        (4, 'application_id'),
        (5, 'cpr'),
        (6, 'individual_name'),
        (7, 'joining_date'),
        (8, 'mol_registered'),
        (9, 'job_center'),
        (10, 'basic_salary'),
        (11, 'allowances'),
        (12, 'position'),
        (13, 'company'),
        (14, 'cr_license_no'),
        (15, 'contract_type'),
        (16, 'application_status'),
        (17, 'contract_signed'),
        (18, 'additional_comments'),
        (19, 'mol_registered_3plus_years'),
        (20, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'id'),
        (3, 'application_no'),
        (4, 'application_id'),
        (5, 'cpr'),
        (6, 'individual_name'),
        (7, 'joining_date'),
        (8, 'mol_registered'),
        (9, 'job_center'),
        (10, 'basic_salary'),
        (11, 'allowances'),
        (12, 'position'),
        (13, 'company'),
        (14, 'cr_license_no'),
        (15, 'contract_type'),
        (16, 'application_status'),
        (17, 'contract_signed'),
        (18, 'additional_comments'),
        (19, 'mol_registered_3plus_years'),
        (20, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(id AS STRING) AS id,
        CAST(application_no AS STRING) AS application_no,
        CAST(application_id AS STRING) AS application_id,
        CAST(cpr AS STRING) AS cpr,
        CAST(individual_name AS STRING) AS individual_name,
        CAST(joining_date AS STRING) AS joining_date,
        CAST(mol_registered AS STRING) AS mol_registered,
        CAST(job_center AS STRING) AS job_center,
        CAST(basic_salary AS STRING) AS basic_salary,
        CAST(allowances AS STRING) AS allowances,
        CAST(position AS STRING) AS position,
        CAST(company AS STRING) AS company,
        CAST(cr_license_no AS STRING) AS cr_license_no,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(application_status AS STRING) AS application_status,
        CAST(contract_signed AS STRING) AS contract_signed,
        CAST(additional_comments AS STRING) AS additional_comments,
        CAST(mol_registered_3plus_years AS STRING) AS mol_registered_3plus_years,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(id AS STRING) AS id,
        CAST(application_no AS STRING) AS application_no,
        CAST(application_id AS STRING) AS application_id,
        CAST(cpr AS STRING) AS cpr,
        CAST(individual_name AS STRING) AS individual_name,
        CAST(joining_date AS STRING) AS joining_date,
        CAST(mol_registered AS STRING) AS mol_registered,
        CAST(job_center AS STRING) AS job_center,
        CAST(basic_salary AS STRING) AS basic_salary,
        CAST(allowances AS STRING) AS allowances,
        CAST(position AS STRING) AS position,
        CAST(company AS STRING) AS company,
        CAST(cr_license_no AS STRING) AS cr_license_no,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(application_status AS STRING) AS application_status,
        CAST(contract_signed AS STRING) AS contract_signed,
        CAST(additional_comments AS STRING) AS additional_comments,
        CAST(mol_registered_3plus_years AS STRING) AS mol_registered_3plus_years,
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
        'offline_sheets_job_seeker_and_walkins_applications_tracker_v2' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_applications_tracker_v2' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_applications_tracker_v2' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_applications_tracker_v2' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_applications_tracker_v2' AS table_name,
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
