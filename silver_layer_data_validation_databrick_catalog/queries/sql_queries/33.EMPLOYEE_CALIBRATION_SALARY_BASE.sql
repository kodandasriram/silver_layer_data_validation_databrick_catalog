---Note no table in the silver layer, because this table has zero records in source 

WITH bronze_layer AS (
    SELECT
        id
		,salarycalibrationrequestid
		,employeeid
		,siowage
		,needscalibration
		,wagechangedate
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEECALIBRATIONSALARY
),

silver_layer AS (
   SELECT 
       	id
		,salarycalibrationrequestid
		,employeeid
		,siowage
		,needscalibration
		,wagechangedate
   from  dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".EMPLOYEE_CALIBRATION_SALARY_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
) t

