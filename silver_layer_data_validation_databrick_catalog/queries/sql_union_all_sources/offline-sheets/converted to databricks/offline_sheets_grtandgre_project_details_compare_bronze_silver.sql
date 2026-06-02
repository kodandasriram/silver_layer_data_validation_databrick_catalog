WITH
bronze_layer AS (
    SELECT
        extract_date,
        project_details_project_owner AS project_owner,
        project_details_project_id AS project_id,
        project_details_project_name AS project_name,
        project_details_project_manager AS project_manager,
        project_details_project_segment AS project_segment,
        project_details_support_area AS support_area,
        project_details_responsible_department AS responsible_department,
        project_details_project_brief AS project_brief,
        project_details_project_objectives AS project_objectives,
        project_details_requested_human_capital AS requested_human_capital,
        project_details_requested_amount AS requested_amount,
        project_details_workflow_status AS workflow_status,
        project_details_gl_account AS gl_account,
        partner_details_cr_license_number AS cr_license_number,
        partner_details_commercial_name AS commercial_name,
        partner_details_partner_location AS partner_location,
        partner_details_partner_region AS partner_region,
        partner_details_partner_country AS partner_country,
        partner_details_company_type AS company_type,
        partner_details_registration_date AS registration_date,
        partner_details_enterprise_gender AS enterprise_gender,
        partner_details_enterprise_type AS enterprise_type,
        partner_details_sector AS sector,
        partner_details_conducting_business_overseas AS conducting_business_overseas,
        employment_and_salary_details_no_of_bahraini_employees AS no_of_bahraini_employees,
        employment_and_salary_details_no_of_non_bahriani_employees AS no_of_non_bahraini_employees,
        employment_and_salary_details_total_no_of_employees AS total_no_of_employees,
        employment_and_salary_details_no_of_bahraini_female_employees AS no_of_bahraini_female_employees,
        employment_and_salary_details_total_bahraini_salaries AS total_bahraini_salaries,
        employment_and_salary_details_total_non_bahraini_salaries AS total_non_bahraini_salaries,
        employment_and_salary_details_total_salaries AS total_salaries,
        contract_details_contract_reference AS contract_reference,
        contract_details_target_intake AS target_intake,
        contract_details_target_segment AS target_segment,
        contract_details_target_sector AS target_sector,
        contract_details_start_date AS contract_start_date,
        contract_details_end_date AS contract_end_date,
        contract_details_total_budget AS total_budget,
        contract_details_utilized_budget AS utilized_budget,
        contract_details_remaining_budget AS remaining_budget,
        enterprise_contact_details_name AS contact_name,
        enterprise_contact_details_designation AS contact_designation,
        enterprise_contact_details_mobile AS contact_mobile,
        enterprise_contact_details_office AS contact_office,
        enterprise_contact_details_email AS contact_email,
        partner_address_flat AS address_flat,
        partner_address_building AS address_building,
        partner_address_road AS address_road,
        partner_address_block AS address_block,
        partner_address_area AS address_area,
        admin_created_on AS created_on,
        admin_created_by AS created_by,
        admin_approved_on AS approved_on,
        admin_approved_by AS approved_by,
        CAST('OFFLINE_SHEETS' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.offline_sheets_grtandgre_project_details
),

silver_layer AS (
    SELECT
        extract_date,
        project_owner,
        project_id,
        project_name,
        project_manager,
        project_segment,
        support_area,
        responsible_department,
        project_brief,
        project_objectives,
        requested_human_capital,
        requested_amount,
        workflow_status,
        gl_account,
        cr_license_number,
        commercial_name,
        partner_location,
        partner_region,
        partner_country,
        company_type,
        registration_date,
        enterprise_gender,
        enterprise_type,
        sector,
        conducting_business_overseas,
        no_of_bahraini_employees,
        no_of_non_bahraini_employees,
        total_no_of_employees,
        no_of_bahraini_female_employees,
        total_bahraini_salaries,
        total_non_bahraini_salaries,
        total_salaries,
        contract_reference,
        target_intake,
        target_segment,
        target_sector,
        contract_start_date,
        contract_end_date,
        total_budget,
        utilized_budget,
        remaining_budget,
        contact_name,
        contact_designation,
        contact_mobile,
        contact_office,
        contact_email,
        address_flat,
        address_building,
        address_road,
        address_block,
        address_area,
        created_on,
        created_by,
        approved_on,
        approved_by,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.offline_sheets_grtandgre_project_details
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'project_owner'),
        (3, 'project_id'),
        (4, 'project_name'),
        (5, 'project_manager'),
        (6, 'project_segment'),
        (7, 'support_area'),
        (8, 'responsible_department'),
        (9, 'project_brief'),
        (10, 'project_objectives'),
        (11, 'requested_human_capital'),
        (12, 'requested_amount'),
        (13, 'workflow_status'),
        (14, 'gl_account'),
        (15, 'cr_license_number'),
        (16, 'commercial_name'),
        (17, 'partner_location'),
        (18, 'partner_region'),
        (19, 'partner_country'),
        (20, 'company_type'),
        (21, 'registration_date'),
        (22, 'enterprise_gender'),
        (23, 'enterprise_type'),
        (24, 'sector'),
        (25, 'conducting_business_overseas'),
        (26, 'no_of_bahraini_employees'),
        (27, 'no_of_non_bahraini_employees'),
        (28, 'total_no_of_employees'),
        (29, 'no_of_bahraini_female_employees'),
        (30, 'total_bahraini_salaries'),
        (31, 'total_non_bahraini_salaries'),
        (32, 'total_salaries'),
        (33, 'contract_reference'),
        (34, 'target_intake'),
        (35, 'target_segment'),
        (36, 'target_sector'),
        (37, 'contract_start_date'),
        (38, 'contract_end_date'),
        (39, 'total_budget'),
        (40, 'utilized_budget'),
        (41, 'remaining_budget'),
        (42, 'contact_name'),
        (43, 'contact_designation'),
        (44, 'contact_mobile'),
        (45, 'contact_office'),
        (46, 'contact_email'),
        (47, 'address_flat'),
        (48, 'address_building'),
        (49, 'address_road'),
        (50, 'address_block'),
        (51, 'address_area'),
        (52, 'created_on'),
        (53, 'created_by'),
        (54, 'approved_on'),
        (55, 'approved_by'),
        (56, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'project_owner'),
        (3, 'project_id'),
        (4, 'project_name'),
        (5, 'project_manager'),
        (6, 'project_segment'),
        (7, 'support_area'),
        (8, 'responsible_department'),
        (9, 'project_brief'),
        (10, 'project_objectives'),
        (11, 'requested_human_capital'),
        (12, 'requested_amount'),
        (13, 'workflow_status'),
        (14, 'gl_account'),
        (15, 'cr_license_number'),
        (16, 'commercial_name'),
        (17, 'partner_location'),
        (18, 'partner_region'),
        (19, 'partner_country'),
        (20, 'company_type'),
        (21, 'registration_date'),
        (22, 'enterprise_gender'),
        (23, 'enterprise_type'),
        (24, 'sector'),
        (25, 'conducting_business_overseas'),
        (26, 'no_of_bahraini_employees'),
        (27, 'no_of_non_bahraini_employees'),
        (28, 'total_no_of_employees'),
        (29, 'no_of_bahraini_female_employees'),
        (30, 'total_bahraini_salaries'),
        (31, 'total_non_bahraini_salaries'),
        (32, 'total_salaries'),
        (33, 'contract_reference'),
        (34, 'target_intake'),
        (35, 'target_segment'),
        (36, 'target_sector'),
        (37, 'contract_start_date'),
        (38, 'contract_end_date'),
        (39, 'total_budget'),
        (40, 'utilized_budget'),
        (41, 'remaining_budget'),
        (42, 'contact_name'),
        (43, 'contact_designation'),
        (44, 'contact_mobile'),
        (45, 'contact_office'),
        (46, 'contact_email'),
        (47, 'address_flat'),
        (48, 'address_building'),
        (49, 'address_road'),
        (50, 'address_block'),
        (51, 'address_area'),
        (52, 'created_on'),
        (53, 'created_by'),
        (54, 'approved_on'),
        (55, 'approved_by'),
        (56, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(project_owner AS STRING) AS project_owner,
        CAST(project_id AS STRING) AS project_id,
        CAST(project_name AS STRING) AS project_name,
        CAST(project_manager AS STRING) AS project_manager,
        CAST(project_segment AS STRING) AS project_segment,
        CAST(support_area AS STRING) AS support_area,
        CAST(responsible_department AS STRING) AS responsible_department,
        CAST(project_brief AS STRING) AS project_brief,
        CAST(project_objectives AS STRING) AS project_objectives,
        CAST(requested_human_capital AS STRING) AS requested_human_capital,
        CAST(requested_amount AS STRING) AS requested_amount,
        CAST(workflow_status AS STRING) AS workflow_status,
        CAST(gl_account AS STRING) AS gl_account,
        CAST(cr_license_number AS STRING) AS cr_license_number,
        CAST(commercial_name AS STRING) AS commercial_name,
        CAST(partner_location AS STRING) AS partner_location,
        CAST(partner_region AS STRING) AS partner_region,
        CAST(partner_country AS STRING) AS partner_country,
        CAST(company_type AS STRING) AS company_type,
        CAST(registration_date AS STRING) AS registration_date,
        CAST(enterprise_gender AS STRING) AS enterprise_gender,
        CAST(enterprise_type AS STRING) AS enterprise_type,
        CAST(sector AS STRING) AS sector,
        CAST(conducting_business_overseas AS STRING) AS conducting_business_overseas,
        CAST(no_of_bahraini_employees AS STRING) AS no_of_bahraini_employees,
        CAST(no_of_non_bahraini_employees AS STRING) AS no_of_non_bahraini_employees,
        CAST(total_no_of_employees AS STRING) AS total_no_of_employees,
        CAST(no_of_bahraini_female_employees AS STRING) AS no_of_bahraini_female_employees,
        CAST(total_bahraini_salaries AS STRING) AS total_bahraini_salaries,
        CAST(total_non_bahraini_salaries AS STRING) AS total_non_bahraini_salaries,
        CAST(total_salaries AS STRING) AS total_salaries,
        CAST(contract_reference AS STRING) AS contract_reference,
        CAST(target_intake AS STRING) AS target_intake,
        CAST(target_segment AS STRING) AS target_segment,
        CAST(target_sector AS STRING) AS target_sector,
        CAST(contract_start_date AS STRING) AS contract_start_date,
        CAST(contract_end_date AS STRING) AS contract_end_date,
        CAST(total_budget AS STRING) AS total_budget,
        CAST(utilized_budget AS STRING) AS utilized_budget,
        CAST(remaining_budget AS STRING) AS remaining_budget,
        CAST(contact_name AS STRING) AS contact_name,
        CAST(contact_designation AS STRING) AS contact_designation,
        CAST(contact_mobile AS STRING) AS contact_mobile,
        CAST(contact_office AS STRING) AS contact_office,
        CAST(contact_email AS STRING) AS contact_email,
        CAST(address_flat AS STRING) AS address_flat,
        CAST(address_building AS STRING) AS address_building,
        CAST(address_road AS STRING) AS address_road,
        CAST(address_block AS STRING) AS address_block,
        CAST(address_area AS STRING) AS address_area,
        CAST(created_on AS STRING) AS created_on,
        CAST(created_by AS STRING) AS created_by,
        CAST(approved_on AS STRING) AS approved_on,
        CAST(approved_by AS STRING) AS approved_by,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(extract_date AS STRING) AS extract_date,
        CAST(project_owner AS STRING) AS project_owner,
        CAST(project_id AS STRING) AS project_id,
        CAST(project_name AS STRING) AS project_name,
        CAST(project_manager AS STRING) AS project_manager,
        CAST(project_segment AS STRING) AS project_segment,
        CAST(support_area AS STRING) AS support_area,
        CAST(responsible_department AS STRING) AS responsible_department,
        CAST(project_brief AS STRING) AS project_brief,
        CAST(project_objectives AS STRING) AS project_objectives,
        CAST(requested_human_capital AS STRING) AS requested_human_capital,
        CAST(requested_amount AS STRING) AS requested_amount,
        CAST(workflow_status AS STRING) AS workflow_status,
        CAST(gl_account AS STRING) AS gl_account,
        CAST(cr_license_number AS STRING) AS cr_license_number,
        CAST(commercial_name AS STRING) AS commercial_name,
        CAST(partner_location AS STRING) AS partner_location,
        CAST(partner_region AS STRING) AS partner_region,
        CAST(partner_country AS STRING) AS partner_country,
        CAST(company_type AS STRING) AS company_type,
        CAST(registration_date AS STRING) AS registration_date,
        CAST(enterprise_gender AS STRING) AS enterprise_gender,
        CAST(enterprise_type AS STRING) AS enterprise_type,
        CAST(sector AS STRING) AS sector,
        CAST(conducting_business_overseas AS STRING) AS conducting_business_overseas,
        CAST(no_of_bahraini_employees AS STRING) AS no_of_bahraini_employees,
        CAST(no_of_non_bahraini_employees AS STRING) AS no_of_non_bahraini_employees,
        CAST(total_no_of_employees AS STRING) AS total_no_of_employees,
        CAST(no_of_bahraini_female_employees AS STRING) AS no_of_bahraini_female_employees,
        CAST(total_bahraini_salaries AS STRING) AS total_bahraini_salaries,
        CAST(total_non_bahraini_salaries AS STRING) AS total_non_bahraini_salaries,
        CAST(total_salaries AS STRING) AS total_salaries,
        CAST(contract_reference AS STRING) AS contract_reference,
        CAST(target_intake AS STRING) AS target_intake,
        CAST(target_segment AS STRING) AS target_segment,
        CAST(target_sector AS STRING) AS target_sector,
        CAST(contract_start_date AS STRING) AS contract_start_date,
        CAST(contract_end_date AS STRING) AS contract_end_date,
        CAST(total_budget AS STRING) AS total_budget,
        CAST(utilized_budget AS STRING) AS utilized_budget,
        CAST(remaining_budget AS STRING) AS remaining_budget,
        CAST(contact_name AS STRING) AS contact_name,
        CAST(contact_designation AS STRING) AS contact_designation,
        CAST(contact_mobile AS STRING) AS contact_mobile,
        CAST(contact_office AS STRING) AS contact_office,
        CAST(contact_email AS STRING) AS contact_email,
        CAST(address_flat AS STRING) AS address_flat,
        CAST(address_building AS STRING) AS address_building,
        CAST(address_road AS STRING) AS address_road,
        CAST(address_block AS STRING) AS address_block,
        CAST(address_area AS STRING) AS address_area,
        CAST(created_on AS STRING) AS created_on,
        CAST(created_by AS STRING) AS created_by,
        CAST(approved_on AS STRING) AS approved_on,
        CAST(approved_by AS STRING) AS approved_by,
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
        'offline_sheets_grtandgre_project_details' AS table_name,
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
        'offline_sheets_grtandgre_project_details' AS table_name,
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
        'offline_sheets_grtandgre_project_details' AS table_name,
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
        'offline_sheets_grtandgre_project_details' AS table_name,
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
        'offline_sheets_grtandgre_project_details' AS table_name,
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
