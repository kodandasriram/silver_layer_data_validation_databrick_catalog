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
    --current_timestamp AS dbt_updated_on,
    CAST('OFFLINE_SHEETS' AS VARCHAR) AS source_system_name
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".offline_sheets_jobplus_csr;


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
    --dbt_updated_on,
    source_system_name
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".offline_sheets_jobplus_csr;