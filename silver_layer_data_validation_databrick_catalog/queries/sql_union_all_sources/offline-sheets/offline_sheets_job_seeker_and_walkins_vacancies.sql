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
    --current_timestamp AS dbt_updated_on,
    CAST('OFFLINE_SHEETS' AS VARCHAR) AS source_system_name
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".offline_sheets_job_seeker_and_walkins_vacancies;


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
    --dbt_updated_on,
    source_system_name
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".offline_sheets_job_seeker_and_walkins_vacancies;