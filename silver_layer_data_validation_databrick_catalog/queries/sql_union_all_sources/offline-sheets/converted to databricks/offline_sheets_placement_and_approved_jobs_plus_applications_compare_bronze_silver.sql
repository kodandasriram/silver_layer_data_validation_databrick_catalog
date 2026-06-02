WITH
bronze_layer AS (
    SELECT
        extract_date,
        application_no,
        validation_note,
        cpr,
        individual_name,
        application_status,
        application_approved_on,
        joining_date,
        mol_registered,
        job_center,
        basic_salary,
        allowances,
        total_salary,
        job_title,
        company,
        cr_license_no,
        contract_type,
        contract_signed,
        contract_link,
        assessor_name,
        approval_name,
        ad_approval,
        additional_comments,
        current_timestamp() AS dbt_updated_at,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_placement_and_approved_jobs_plus_applications
),

silver_layer AS (
    SELECT
        extract_date,
        application_no,
        validation_note,
        cpr,
        individual_name,
        application_status,
        application_approved_on,
        joining_date,
        mol_registered,
        job_center,
        basic_salary,
        allowances,
        total_salary,
        job_title,
        company,
        cr_license_no,
        contract_type,
        contract_signed,
        contract_link,
        assessor_name,
        approval_name,
        ad_approval,
        additional_comments,
        dbt_updated_at,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_placement_and_approved_jobs_plus_applications
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'application_no'),
        (3, 'validation_note'),
        (4, 'cpr'),
        (5, 'individual_name'),
        (6, 'application_status'),
        (7, 'application_approved_on'),
        (8, 'joining_date'),
        (9, 'mol_registered'),
        (10, 'job_center'),
        (11, 'basic_salary'),
        (12, 'allowances'),
        (13, 'total_salary'),
        (14, 'job_title'),
        (15, 'company'),
        (16, 'cr_license_no'),
        (17, 'contract_type'),
        (18, 'contract_signed'),
        (19, 'contract_link'),
        (20, 'assessor_name'),
        (21, 'approval_name'),
        (22, 'ad_approval'),
        (23, 'additional_comments'),
        (24, 'dbt_updated_at'),
        (25, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'application_no'),
        (3, 'validation_note'),
        (4, 'cpr'),
        (5, 'individual_name'),
        (6, 'application_status'),
        (7, 'application_approved_on'),
        (8, 'joining_date'),
        (9, 'mol_registered'),
        (10, 'job_center'),
        (11, 'basic_salary'),
        (12, 'allowances'),
        (13, 'total_salary'),
        (14, 'job_title'),
        (15, 'company'),
        (16, 'cr_license_no'),
        (17, 'contract_type'),
        (18, 'contract_signed'),
        (19, 'contract_link'),
        (20, 'assessor_name'),
        (21, 'approval_name'),
        (22, 'ad_approval'),
        (23, 'additional_comments'),
        (24, 'dbt_updated_at'),
        (25, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(application_no AS STRING) AS application_no,
        CAST(validation_note AS STRING) AS validation_note,
        CAST(cpr AS STRING) AS cpr,
        CAST(individual_name AS STRING) AS individual_name,
        CAST(application_status AS STRING) AS application_status,
        CAST(application_approved_on AS STRING) AS application_approved_on,
        CAST(joining_date AS STRING) AS joining_date,
        CAST(mol_registered AS STRING) AS mol_registered,
        CAST(job_center AS STRING) AS job_center,
        CAST(basic_salary AS STRING) AS basic_salary,
        CAST(allowances AS STRING) AS allowances,
        CAST(total_salary AS STRING) AS total_salary,
        CAST(job_title AS STRING) AS job_title,
        CAST(company AS STRING) AS company,
        CAST(cr_license_no AS STRING) AS cr_license_no,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(contract_signed AS STRING) AS contract_signed,
        CAST(contract_link AS STRING) AS contract_link,
        CAST(assessor_name AS STRING) AS assessor_name,
        CAST(approval_name AS STRING) AS approval_name,
        CAST(ad_approval AS STRING) AS ad_approval,
        CAST(additional_comments AS STRING) AS additional_comments,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(application_no AS STRING) AS application_no,
        CAST(validation_note AS STRING) AS validation_note,
        CAST(cpr AS STRING) AS cpr,
        CAST(individual_name AS STRING) AS individual_name,
        CAST(application_status AS STRING) AS application_status,
        CAST(application_approved_on AS STRING) AS application_approved_on,
        CAST(joining_date AS STRING) AS joining_date,
        CAST(mol_registered AS STRING) AS mol_registered,
        CAST(job_center AS STRING) AS job_center,
        CAST(basic_salary AS STRING) AS basic_salary,
        CAST(allowances AS STRING) AS allowances,
        CAST(total_salary AS STRING) AS total_salary,
        CAST(job_title AS STRING) AS job_title,
        CAST(company AS STRING) AS company,
        CAST(cr_license_no AS STRING) AS cr_license_no,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(contract_signed AS STRING) AS contract_signed,
        CAST(contract_link AS STRING) AS contract_link,
        CAST(assessor_name AS STRING) AS assessor_name,
        CAST(approval_name AS STRING) AS approval_name,
        CAST(ad_approval AS STRING) AS ad_approval,
        CAST(additional_comments AS STRING) AS additional_comments,
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
        'offline_sheets_placement_and_approved_jobs_plus_applications' AS table_name,
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
        'offline_sheets_placement_and_approved_jobs_plus_applications' AS table_name,
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
        'offline_sheets_placement_and_approved_jobs_plus_applications' AS table_name,
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
        'offline_sheets_placement_and_approved_jobs_plus_applications' AS table_name,
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
        'offline_sheets_placement_and_approved_jobs_plus_applications' AS table_name,
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
