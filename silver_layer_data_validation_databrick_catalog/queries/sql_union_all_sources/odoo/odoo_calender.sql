SELECT
    `meeting subject`,
	`start`,
	`stop`,
	attendees,
	`sector team`,
	`location`,
	`duration`,
	company,
	`program feedback notes`,
	`created on`,
    CAST('ODOO' AS STRING) AS source_system_name,
    current_timestamp() AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.odoo_calendar ;

select 
meeting_subject,
start_datetime  ,
end_datetime ,
attendees,
sector_team,
location,
duration_hours,
company_name ,
program_feedback_notes,
created_on,
source_system_name,
dbt_updated_at
from 
`tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.odoo_calendar_event_base _base