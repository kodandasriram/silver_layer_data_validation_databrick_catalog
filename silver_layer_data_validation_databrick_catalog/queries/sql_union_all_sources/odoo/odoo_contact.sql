SELECT
    name,
    country,
    `company id`,
    `sector team`,
    salesperson,
    `moic issued capital`,
    `# meetings`,
    `global rm`,
    activities,
    `created on`,
    CAST('ODOO' AS STRING) AS source_system_name,
    current_timestamp() AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.odoo_contacts;

select 
name,
country,
cr_number,
sector_team,
relationship_manager,
moic_issued_capital,
number_of_meetings,
global_rm,
activities,
created_on ,
source_system_name,
dbt_updated_at
from 
`tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.odoo_contact_base _base