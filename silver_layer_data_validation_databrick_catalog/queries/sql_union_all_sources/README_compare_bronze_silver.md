# Bronze vs Silver Comparison Scripts

Generated comparison SQL files for bronze-layer query outputs against silver-layer table outputs. Each file returns validation rows for record count, column count, column name/order match, and mismatch counts in both directions after casting compared columns to VARCHAR.

| table | bronze columns | silver columns | compared columns | note | file |
|---|---:|---:|---:|---|---|
| amendment_base | 59 | 58 | 58 | COLUMN_DIFF | amendment_base_compare_bronze_silver.sql |
| application_base | 98 | 98 | 98 | BRONZE_METADATA_ALIGNED | application_base_compare_bronze_silver.sql |
| application_support_base | 29 | 29 | 29 | OK | application_support_base_compare_bronze_silver.sql |
| assessment_base | 64 | 64 | 64 | OK | assessment_base_compare_bronze_silver.sql |
| certification_base | 73 | 73 | 73 | OK | certification_base_compare_bronze_silver.sql |
| customer_enterprise_base | 194 | 194 | 194 | OK | customer_enterprise_base_compare_bronze_silver.sql |
| customer_individual_base | 136 | 136 | 136 | OK | customer_individual_base_compare_bronze_silver.sql |
| employee_base | 50 | 50 | 50 | OK | employee_base_compare_bronze_silver.sql |
| financing_base | 47 | 47 | 47 | OK | financing_base_compare_bronze_silver.sql |
| iban_base | 51 | 51 | 51 | OK | iban_base_compare_bronze_silver.sql |
| moic_base | 96 | 96 | 96 | OK | moic_base_compare_bronze_silver.sql |
| payment_assessment_base | 59 | 59 | 59 | OK | payment_assessment_base_compare_bronze_silver.sql |
| payment_base | 148 | 148 | 148 | OK | payment_base_compare_bronze_silver.sql |
| payment_plan_base | 74 | 74 | 74 | OK | payment_plan_base_compare_bronze_silver.sql |
| payment_support_base | 42 | 42 | 42 | OK | payment_support_base_compare_bronze_silver.sql |
| product_services_base | 121 | 121 | 120 | COLUMN_DIFF | product_services_base_compare_bronze_silver.sql |
| program_base | 25 | 25 | 23 | COLUMN_DIFF | program_base_compare_bronze_silver.sql |
| sector_isic_base | 6 | 6 | 6 | OK | sector_isic_base_compare_bronze_silver.sql |
| security_base | 47 | 47 | 47 | OK | security_base_compare_bronze_silver.sql |
| service_fee_base | 6 | 6 | 6 | OK | service_fee_base_compare_bronze_silver.sql |
| special_condition_base | 35 | 35 | 35 | OK | special_condition_base_compare_bronze_silver.sql |
| support_structure_base | 19 | 19 | 19 | OK | support_structure_base_compare_bronze_silver.sql |
| ticket_base | 131 | 131 | 131 | OK | ticket_base_compare_bronze_silver.sql |
| training_base | 111 | 111 | 111 | BRONZE_METADATA_ALIGNED | training_base_compare_bronze_silver.sql |
| unstructured_questions_base | 62 | 62 | 62 | OK | unstructured_questions_base_compare_bronze_silver.sql |
| user_base | 48 | 48 | 48 | OK | user_base_compare_bronze_silver.sql |
| wage_base | 91 | 91 | 91 | OK | wage_base_compare_bronze_silver.sql |
| workflow_base | 85 | 85 | 85 | BRONZE_METADATA_ALIGNED | workflow_base_compare_bronze_silver.sql |
