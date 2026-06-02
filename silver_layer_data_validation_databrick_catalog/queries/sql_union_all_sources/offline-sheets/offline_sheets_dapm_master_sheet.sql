select
extract_date,
seq_num,
project,
project_segment,
on_system as is_on_system,
collection_status,
project_status,
comments,
'OFFLINE_SHEETS' as source_system_name
from 
dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".offline_sheets_dapm_master_sheet;

select  
extract_date,
seq_num,
project,
project_segment,
is_on_system,
collection_status,
project_status,
comments,
--dbt_updated_on,
source_system_name
from 
dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".offline_sheets_dapm_master_sheet
