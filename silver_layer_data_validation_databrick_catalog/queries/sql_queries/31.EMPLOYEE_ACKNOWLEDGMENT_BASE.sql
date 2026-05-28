WITH bronze_layer AS (
    SELECT
        empack.id,
        empack.processid,
        empackstatus.label AS employeeacknowledgmentstatus,
        empack.expirationdate,
        empack.employeesubmissiondate,
        empack.applicationid,
        empack.portaluserid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEEACKNOWLEDGMENT empack
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2da_EmployeeAcknowledgmentStatus empackstatus 
        ON empackstatus.code  = empack.employeeacknowledgmentstatus
),

silver_layer AS (

   SELECT 
        id,
        processid,
        employeeacknowledgmentstatus,
        expirationdate,
        employeesubmissiondate,
        applicationid,
        portaluserid
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".EMPLOYEE_ACKNOWLEDGMENT_BASE
   
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

