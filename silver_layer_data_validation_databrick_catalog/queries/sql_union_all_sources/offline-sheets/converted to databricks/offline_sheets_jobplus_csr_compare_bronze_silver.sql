WITH
bronze_layer AS (
    SELECT
        extract_date,
        campaign,
        time_interval_day AS report_date,
        no_of_offered_inbound_interactions,
        no_of_handled_inbound_interactions,
        inbound_handled_calls_below_upper_service_level AS inbound_handled_below_service_level,
        no_of_abandoned_inbound_interactions,
        no_of_inbound_abandoned_calls_above_lower_service_level AS inbound_abandoned_above_service_level,
        total_time_in_talk_state,
        service_level,
        no_of_handled_outbound_interactions,
        average_time_in_handling_state,
        maximum_time_in_inbound_talk_state,
        average_time_in_waiting_state,
        average_no_of_agents_in_ready_state,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_jobplus_csr
),

silver_layer AS (
    SELECT
        extract_date,
        campaign,
        report_date,
        no_of_offered_inbound_interactions,
        no_of_handled_inbound_interactions,
        inbound_handled_below_service_level,
        no_of_abandoned_inbound_interactions,
        inbound_abandoned_above_service_level,
        total_time_in_talk_state,
        service_level,
        no_of_handled_outbound_interactions,
        average_time_in_handling_state,
        maximum_time_in_inbound_talk_state,
        average_time_in_waiting_state,
        average_no_of_agents_in_ready_state,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_jobplus_csr
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'campaign'),
        (3, 'report_date'),
        (4, 'no_of_offered_inbound_interactions'),
        (5, 'no_of_handled_inbound_interactions'),
        (6, 'inbound_handled_below_service_level'),
        (7, 'no_of_abandoned_inbound_interactions'),
        (8, 'inbound_abandoned_above_service_level'),
        (9, 'total_time_in_talk_state'),
        (10, 'service_level'),
        (11, 'no_of_handled_outbound_interactions'),
        (12, 'average_time_in_handling_state'),
        (13, 'maximum_time_in_inbound_talk_state'),
        (14, 'average_time_in_waiting_state'),
        (15, 'average_no_of_agents_in_ready_state'),
        (16, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'campaign'),
        (3, 'report_date'),
        (4, 'no_of_offered_inbound_interactions'),
        (5, 'no_of_handled_inbound_interactions'),
        (6, 'inbound_handled_below_service_level'),
        (7, 'no_of_abandoned_inbound_interactions'),
        (8, 'inbound_abandoned_above_service_level'),
        (9, 'total_time_in_talk_state'),
        (10, 'service_level'),
        (11, 'no_of_handled_outbound_interactions'),
        (12, 'average_time_in_handling_state'),
        (13, 'maximum_time_in_inbound_talk_state'),
        (14, 'average_time_in_waiting_state'),
        (15, 'average_no_of_agents_in_ready_state'),
        (16, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(campaign AS STRING) AS campaign,
        CAST(report_date AS STRING) AS report_date,
        CAST(no_of_offered_inbound_interactions AS STRING) AS no_of_offered_inbound_interactions,
        CAST(no_of_handled_inbound_interactions AS STRING) AS no_of_handled_inbound_interactions,
        CAST(inbound_handled_below_service_level AS STRING) AS inbound_handled_below_service_level,
        CAST(no_of_abandoned_inbound_interactions AS STRING) AS no_of_abandoned_inbound_interactions,
        CAST(inbound_abandoned_above_service_level AS STRING) AS inbound_abandoned_above_service_level,
        CAST(total_time_in_talk_state AS STRING) AS total_time_in_talk_state,
        CAST(service_level AS STRING) AS service_level,
        CAST(no_of_handled_outbound_interactions AS STRING) AS no_of_handled_outbound_interactions,
        CAST(average_time_in_handling_state AS STRING) AS average_time_in_handling_state,
        CAST(maximum_time_in_inbound_talk_state AS STRING) AS maximum_time_in_inbound_talk_state,
        CAST(average_time_in_waiting_state AS STRING) AS average_time_in_waiting_state,
        CAST(average_no_of_agents_in_ready_state AS STRING) AS average_no_of_agents_in_ready_state,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(campaign AS STRING) AS campaign,
        CAST(report_date AS STRING) AS report_date,
        CAST(no_of_offered_inbound_interactions AS STRING) AS no_of_offered_inbound_interactions,
        CAST(no_of_handled_inbound_interactions AS STRING) AS no_of_handled_inbound_interactions,
        CAST(inbound_handled_below_service_level AS STRING) AS inbound_handled_below_service_level,
        CAST(no_of_abandoned_inbound_interactions AS STRING) AS no_of_abandoned_inbound_interactions,
        CAST(inbound_abandoned_above_service_level AS STRING) AS inbound_abandoned_above_service_level,
        CAST(total_time_in_talk_state AS STRING) AS total_time_in_talk_state,
        CAST(service_level AS STRING) AS service_level,
        CAST(no_of_handled_outbound_interactions AS STRING) AS no_of_handled_outbound_interactions,
        CAST(average_time_in_handling_state AS STRING) AS average_time_in_handling_state,
        CAST(maximum_time_in_inbound_talk_state AS STRING) AS maximum_time_in_inbound_talk_state,
        CAST(average_time_in_waiting_state AS STRING) AS average_time_in_waiting_state,
        CAST(average_no_of_agents_in_ready_state AS STRING) AS average_no_of_agents_in_ready_state,
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
        'offline_sheets_jobplus_csr' AS table_name,
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
        'offline_sheets_jobplus_csr' AS table_name,
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
        'offline_sheets_jobplus_csr' AS table_name,
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
        'offline_sheets_jobplus_csr' AS table_name,
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
        'offline_sheets_jobplus_csr' AS table_name,
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
