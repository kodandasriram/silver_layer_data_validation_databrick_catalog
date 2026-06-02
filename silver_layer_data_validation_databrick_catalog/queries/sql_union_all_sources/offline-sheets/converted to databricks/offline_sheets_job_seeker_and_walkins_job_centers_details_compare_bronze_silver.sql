WITH
bronze_layer AS (
    SELECT
        extract_date,
        job_center,
        date AS activity_date,
        walk_ins,
        walk_ins_comments_if_any AS walk_ins_comments,
        appointments_and_calls_mol_list_total AS appointments_mol_list_total,
        appointments_mol_list_virtual,
        appointments_mol_list_physical,
        appointments_and_calls_tamkeen_system_total AS appointments_tamkeen_system_total,
        appointments_tamkeen_system_virtual AS appointments_tamkeen_virtual,
        appointments_tamkeen_system_physical AS appointments_tamkeen_physical,
        appointments_comments_if_any AS appointments_comments,
        job_interviews_conducted_mol_list_total AS interviews_mol_total,
        job_interviews_conducted_mol_list_virtual AS interviews_mol_virtual,
        job_interviews_conducted_mol_list_physical AS interviews_mol_physical,
        mol_details_of_the_interviews_company_role_etc AS interviews_mol_details,
        job_interviews_conducted_tamkeen_system_total AS interviews_tamkeen_total,
        job_interviews_conducted_tamkeen_system_virtual AS interviews_tamkeen_virtual,
        job_interviews_conducted_tamkeen_system_physical AS interviews_tamkeen_physical,
        tamkeen_details_of_the_interviews_company_role_etc AS interviews_tamkeen_details,
        number_of_trainings_conducted_on_the_premises AS no_of_trainings_on_premises,
        type_of_training_subject_quality_participants_etc AS training_type_details,
        number_of_job_matches,
        issues_to_flag_in_general AS issues_to_flag,
        start_time,
        completion_time,
        email AS officer_email,
        name AS officer_name,
        id AS record_id,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_job_seeker_and_walkins_job_centers_details
),

silver_layer AS (
    SELECT
        extract_date,
        job_center,
        activity_date,
        walk_ins,
        walk_ins_comments,
        appointments_mol_list_total,
        appointments_mol_list_virtual,
        appointments_mol_list_physical,
        appointments_tamkeen_system_total,
        appointments_tamkeen_virtual,
        appointments_tamkeen_physical,
        appointments_comments,
        interviews_mol_total,
        interviews_mol_virtual,
        interviews_mol_physical,
        interviews_mol_details,
        interviews_tamkeen_total,
        interviews_tamkeen_virtual,
        interviews_tamkeen_physical,
        interviews_tamkeen_details,
        no_of_trainings_on_premises,
        training_type_details,
        number_of_job_matches,
        issues_to_flag,
        start_time,
        completion_time,
        officer_email,
        officer_name,
        record_id,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_job_seeker_and_walkins_job_centers_details
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'job_center'),
        (3, 'activity_date'),
        (4, 'walk_ins'),
        (5, 'walk_ins_comments'),
        (6, 'appointments_mol_list_total'),
        (7, 'appointments_mol_list_virtual'),
        (8, 'appointments_mol_list_physical'),
        (9, 'appointments_tamkeen_system_total'),
        (10, 'appointments_tamkeen_virtual'),
        (11, 'appointments_tamkeen_physical'),
        (12, 'appointments_comments'),
        (13, 'interviews_mol_total'),
        (14, 'interviews_mol_virtual'),
        (15, 'interviews_mol_physical'),
        (16, 'interviews_mol_details'),
        (17, 'interviews_tamkeen_total'),
        (18, 'interviews_tamkeen_virtual'),
        (19, 'interviews_tamkeen_physical'),
        (20, 'interviews_tamkeen_details'),
        (21, 'no_of_trainings_on_premises'),
        (22, 'training_type_details'),
        (23, 'number_of_job_matches'),
        (24, 'issues_to_flag'),
        (25, 'start_time'),
        (26, 'completion_time'),
        (27, 'officer_email'),
        (28, 'officer_name'),
        (29, 'record_id'),
        (30, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'job_center'),
        (3, 'activity_date'),
        (4, 'walk_ins'),
        (5, 'walk_ins_comments'),
        (6, 'appointments_mol_list_total'),
        (7, 'appointments_mol_list_virtual'),
        (8, 'appointments_mol_list_physical'),
        (9, 'appointments_tamkeen_system_total'),
        (10, 'appointments_tamkeen_virtual'),
        (11, 'appointments_tamkeen_physical'),
        (12, 'appointments_comments'),
        (13, 'interviews_mol_total'),
        (14, 'interviews_mol_virtual'),
        (15, 'interviews_mol_physical'),
        (16, 'interviews_mol_details'),
        (17, 'interviews_tamkeen_total'),
        (18, 'interviews_tamkeen_virtual'),
        (19, 'interviews_tamkeen_physical'),
        (20, 'interviews_tamkeen_details'),
        (21, 'no_of_trainings_on_premises'),
        (22, 'training_type_details'),
        (23, 'number_of_job_matches'),
        (24, 'issues_to_flag'),
        (25, 'start_time'),
        (26, 'completion_time'),
        (27, 'officer_email'),
        (28, 'officer_name'),
        (29, 'record_id'),
        (30, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(job_center AS STRING) AS job_center,
        CAST(activity_date AS STRING) AS activity_date,
        CAST(walk_ins AS STRING) AS walk_ins,
        CAST(walk_ins_comments AS STRING) AS walk_ins_comments,
        CAST(appointments_mol_list_total AS STRING) AS appointments_mol_list_total,
        CAST(appointments_mol_list_virtual AS STRING) AS appointments_mol_list_virtual,
        CAST(appointments_mol_list_physical AS STRING) AS appointments_mol_list_physical,
        CAST(appointments_tamkeen_system_total AS STRING) AS appointments_tamkeen_system_total,
        CAST(appointments_tamkeen_virtual AS STRING) AS appointments_tamkeen_virtual,
        CAST(appointments_tamkeen_physical AS STRING) AS appointments_tamkeen_physical,
        CAST(appointments_comments AS STRING) AS appointments_comments,
        CAST(interviews_mol_total AS STRING) AS interviews_mol_total,
        CAST(interviews_mol_virtual AS STRING) AS interviews_mol_virtual,
        CAST(interviews_mol_physical AS STRING) AS interviews_mol_physical,
        CAST(interviews_mol_details AS STRING) AS interviews_mol_details,
        CAST(interviews_tamkeen_total AS STRING) AS interviews_tamkeen_total,
        CAST(interviews_tamkeen_virtual AS STRING) AS interviews_tamkeen_virtual,
        CAST(interviews_tamkeen_physical AS STRING) AS interviews_tamkeen_physical,
        CAST(interviews_tamkeen_details AS STRING) AS interviews_tamkeen_details,
        CAST(no_of_trainings_on_premises AS STRING) AS no_of_trainings_on_premises,
        CAST(training_type_details AS STRING) AS training_type_details,
        CAST(number_of_job_matches AS STRING) AS number_of_job_matches,
        CAST(issues_to_flag AS STRING) AS issues_to_flag,
        CAST(start_time AS STRING) AS start_time,
        CAST(completion_time AS STRING) AS completion_time,
        CAST(officer_email AS STRING) AS officer_email,
        CAST(officer_name AS STRING) AS officer_name,
        CAST(record_id AS STRING) AS record_id,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(job_center AS STRING) AS job_center,
        CAST(activity_date AS STRING) AS activity_date,
        CAST(walk_ins AS STRING) AS walk_ins,
        CAST(walk_ins_comments AS STRING) AS walk_ins_comments,
        CAST(appointments_mol_list_total AS STRING) AS appointments_mol_list_total,
        CAST(appointments_mol_list_virtual AS STRING) AS appointments_mol_list_virtual,
        CAST(appointments_mol_list_physical AS STRING) AS appointments_mol_list_physical,
        CAST(appointments_tamkeen_system_total AS STRING) AS appointments_tamkeen_system_total,
        CAST(appointments_tamkeen_virtual AS STRING) AS appointments_tamkeen_virtual,
        CAST(appointments_tamkeen_physical AS STRING) AS appointments_tamkeen_physical,
        CAST(appointments_comments AS STRING) AS appointments_comments,
        CAST(interviews_mol_total AS STRING) AS interviews_mol_total,
        CAST(interviews_mol_virtual AS STRING) AS interviews_mol_virtual,
        CAST(interviews_mol_physical AS STRING) AS interviews_mol_physical,
        CAST(interviews_mol_details AS STRING) AS interviews_mol_details,
        CAST(interviews_tamkeen_total AS STRING) AS interviews_tamkeen_total,
        CAST(interviews_tamkeen_virtual AS STRING) AS interviews_tamkeen_virtual,
        CAST(interviews_tamkeen_physical AS STRING) AS interviews_tamkeen_physical,
        CAST(interviews_tamkeen_details AS STRING) AS interviews_tamkeen_details,
        CAST(no_of_trainings_on_premises AS STRING) AS no_of_trainings_on_premises,
        CAST(training_type_details AS STRING) AS training_type_details,
        CAST(number_of_job_matches AS STRING) AS number_of_job_matches,
        CAST(issues_to_flag AS STRING) AS issues_to_flag,
        CAST(start_time AS STRING) AS start_time,
        CAST(completion_time AS STRING) AS completion_time,
        CAST(officer_email AS STRING) AS officer_email,
        CAST(officer_name AS STRING) AS officer_name,
        CAST(record_id AS STRING) AS record_id,
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
        'offline_sheets_job_seeker_and_walkins_job_centers_details' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_job_centers_details' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_job_centers_details' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_job_centers_details' AS table_name,
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
        'offline_sheets_job_seeker_and_walkins_job_centers_details' AS table_name,
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
