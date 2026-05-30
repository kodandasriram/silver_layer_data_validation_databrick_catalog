WITH
bronze_layer AS (
    SELECT
        contract AS contract_id,
        description AS contract_description,
        contracttype AS contract_type,
        name AS contract_name,
        vendor AS vendor_id,
        vendorname AS vendor_name,
        reference1 AS vendor_reference,
        derivedaccountingunit AS accounting_unit_code,
        description_full AS accounting_unit_description,
        onhold AS is_on_hold,
        status AS contract_status,
        effectivedate AS effective_date,
        expirationdate AS expiration_date,
        maximumamount AS maximum_amount,
        amountordered AS amount_ordered,
        derivedcommitmentamountremaining AS commitment_amount_remaining,
        performancebondpercentage AS performance_bond_pct,
        primarycontact AS primary_contact,
        timestamp AS updated_on,
        current_timestamp() AS dbt_updated_at,
        CAST('ODOO' AS STRING) AS source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.infor_specialprojectcontracts
),

silver_layer AS (
    SELECT
        contract_id,
        contract_description,
        contract_type,
        contract_name,
        vendor_id,
        vendor_name,
        vendor_reference,
        accounting_unit_code,
        accounting_unit_description,
        is_on_hold,
        contract_status,
        effective_date,
        expiration_date,
        maximum_amount,
        amount_ordered,
        commitment_amount_remaining,
        performance_bond_pct,
        primary_contact,
        updated_on,
        dbt_updated_at,
        source_system_name
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.special_projects_agreement_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'contract_id'),
        (2, 'contract_description'),
        (3, 'contract_type'),
        (4, 'contract_name'),
        (5, 'vendor_id'),
        (6, 'vendor_name'),
        (7, 'vendor_reference'),
        (8, 'accounting_unit_code'),
        (9, 'accounting_unit_description'),
        (10, 'is_on_hold'),
        (11, 'contract_status'),
        (12, 'effective_date'),
        (13, 'expiration_date'),
        (14, 'maximum_amount'),
        (15, 'amount_ordered'),
        (16, 'commitment_amount_remaining'),
        (17, 'performance_bond_pct'),
        (18, 'primary_contact'),
        (19, 'updated_on'),
        (20, 'dbt_updated_at'),
        (21, 'source_system_name')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'contract_id'),
        (2, 'contract_description'),
        (3, 'contract_type'),
        (4, 'contract_name'),
        (5, 'vendor_id'),
        (6, 'vendor_name'),
        (7, 'vendor_reference'),
        (8, 'accounting_unit_code'),
        (9, 'accounting_unit_description'),
        (10, 'is_on_hold'),
        (11, 'contract_status'),
        (12, 'effective_date'),
        (13, 'expiration_date'),
        (14, 'maximum_amount'),
        (15, 'amount_ordered'),
        (16, 'commitment_amount_remaining'),
        (17, 'performance_bond_pct'),
        (18, 'primary_contact'),
        (19, 'updated_on'),
        (20, 'dbt_updated_at'),
        (21, 'source_system_name')
),

bronze_normalized AS (
    SELECT
        CAST(contract_id AS STRING) AS contract_id,
        CAST(contract_description AS STRING) AS contract_description,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(contract_name AS STRING) AS contract_name,
        CAST(vendor_id AS STRING) AS vendor_id,
        CAST(vendor_name AS STRING) AS vendor_name,
        CAST(vendor_reference AS STRING) AS vendor_reference,
        CAST(accounting_unit_code AS STRING) AS accounting_unit_code,
        CAST(accounting_unit_description AS STRING) AS accounting_unit_description,
        CAST(is_on_hold AS STRING) AS is_on_hold,
        CAST(contract_status AS STRING) AS contract_status,
        CAST(effective_date AS STRING) AS effective_date,
        CAST(expiration_date AS STRING) AS expiration_date,
        CAST(maximum_amount AS STRING) AS maximum_amount,
        CAST(amount_ordered AS STRING) AS amount_ordered,
        CAST(commitment_amount_remaining AS STRING) AS commitment_amount_remaining,
        CAST(performance_bond_pct AS STRING) AS performance_bond_pct,
        CAST(primary_contact AS STRING) AS primary_contact,
        CAST(updated_on AS STRING) AS updated_on,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at,
        CAST(source_system_name AS STRING) AS source_system_name
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(contract_id AS STRING) AS contract_id,
        CAST(contract_description AS STRING) AS contract_description,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(contract_name AS STRING) AS contract_name,
        CAST(vendor_id AS STRING) AS vendor_id,
        CAST(vendor_name AS STRING) AS vendor_name,
        CAST(vendor_reference AS STRING) AS vendor_reference,
        CAST(accounting_unit_code AS STRING) AS accounting_unit_code,
        CAST(accounting_unit_description AS STRING) AS accounting_unit_description,
        CAST(is_on_hold AS STRING) AS is_on_hold,
        CAST(contract_status AS STRING) AS contract_status,
        CAST(effective_date AS STRING) AS effective_date,
        CAST(expiration_date AS STRING) AS expiration_date,
        CAST(maximum_amount AS STRING) AS maximum_amount,
        CAST(amount_ordered AS STRING) AS amount_ordered,
        CAST(commitment_amount_remaining AS STRING) AS commitment_amount_remaining,
        CAST(performance_bond_pct AS STRING) AS performance_bond_pct,
        CAST(primary_contact AS STRING) AS primary_contact,
        CAST(updated_on AS STRING) AS updated_on,
        CAST(dbt_updated_at AS STRING) AS dbt_updated_at,
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
        'special_projects_agreement_base' AS table_name,
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
        'special_projects_agreement_base' AS table_name,
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
        'special_projects_agreement_base' AS table_name,
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
        'special_projects_agreement_base' AS table_name,
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
        'special_projects_agreement_base' AS table_name,
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
