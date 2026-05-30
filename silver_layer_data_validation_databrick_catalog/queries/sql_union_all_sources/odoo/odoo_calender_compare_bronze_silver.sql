WITH
bronze_layer AS (
    SELECT
        `meeting subject` AS meeting_subject,
        `start` AS start_datetime,
        `stop` AS end_datetime,
        attendees,
        `sector team` AS sector_team,
        `location` AS location,
        `duration` AS duration_hours,
        company AS company_name,
        `program feedback notes` AS program_feedback_notes,
        `created on` AS created_on,
        CAST('ODOO' AS STRING) AS source_system_name,
        current_timestamp() AS dbt_updated_at
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.odoo_calendar
),

silver_layer AS (
    SELECT
        meeting_subject,
        start_datetime,
        end_datetime,
        attendees,
        sector_team,
        location,
        duration_hours,
        company_name,
        program_feedback_notes,
        created_on,
        source_system_name,
        dbt_updated_at
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.odoo_calendar_event_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'meeting_subject'),
        (2, 'start_datetime'),
        (3, 'end_datetime'),
        (4, 'attendees'),
        (5, 'sector_team'),
        (6, 'location'),
        (7, 'duration_hours'),
        (8, 'company_name'),
        (9, 'program_feedback_notes'),
        (10, 'created_on'),
        (11, 'source_system_name'),
        (12, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'meeting_subject'),
        (2, 'start_datetime'),
        (3, 'end_datetime'),
        (4, 'attendees'),
        (5, 'sector_team'),
        (6, 'location'),
        (7, 'duration_hours'),
        (8, 'company_name'),
        (9, 'program_feedback_notes'),
        (10, 'created_on'),
        (11, 'source_system_name'),
        (12, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(meeting_subject AS STRING) AS meeting_subject,
        CAST(start_datetime AS STRING) AS start_datetime,
        CAST(end_datetime AS STRING) AS end_datetime,
        CAST(attendees AS STRING) AS attendees,
        CAST(sector_team AS STRING) AS sector_team,
        CAST(location AS STRING) AS location,
        CAST(duration_hours AS STRING) AS duration_hours,
        CAST(company_name AS STRING) AS company_name,
        CAST(program_feedback_notes AS STRING) AS program_feedback_notes,
        CAST(created_on AS STRING) AS created_on,
        CAST(source_system_name AS STRING) AS source_system_name,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(meeting_subject AS STRING) AS meeting_subject,
        CAST(start_datetime AS STRING) AS start_datetime,
        CAST(end_datetime AS STRING) AS end_datetime,
        CAST(attendees AS STRING) AS attendees,
        CAST(sector_team AS STRING) AS sector_team,
        CAST(location AS STRING) AS location,
        CAST(duration_hours AS STRING) AS duration_hours,
        CAST(company_name AS STRING) AS company_name,
        CAST(program_feedback_notes AS STRING) AS program_feedback_notes,
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
        'odoo_calendar_event_base' AS table_name,
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
        'odoo_calendar_event_base' AS table_name,
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
        'odoo_calendar_event_base' AS table_name,
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
        'odoo_calendar_event_base' AS table_name,
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
        'odoo_calendar_event_base' AS table_name,
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
