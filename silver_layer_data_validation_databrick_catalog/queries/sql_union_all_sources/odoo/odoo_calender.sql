SELECT
    "meeting subject",
	"start",
	"stop",
	attendees,
	"sector team",
	"location",
	"duration",
	company,
	"program feedback notes",
	"created on",
    CAST('ODOO' AS VARCHAR) AS source_system_name,
    current_timestamp AS dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".odoo_calendar ;

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
dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".odoo_calendar_event_base _base