WITH bronze_layer AS (
    SELECT
      id
	--,guid
	,referencenumber
	,processid
	,applicationid
	,appstatus.label as amendmentstatus --amendmentstatusid --jon with OSUSR_398_APPLICATIONSTATUS(CODE)
	--,supportareaid --join with OSUSR_MM5_SUPPORTAREA(CODE) Note: table is missing hence commented
	--,docinstancechecklistguid
	--,instanceformguid
	,amendmentno
	,utilizedamount
	,unutilizedamount
	,totalapprovedamount	
	--,isactive
	--,createdby
	--,createdon
	--,updatedby
	--,updatedon
	,submittedon
	,approvedon
	,totalavailableamt
	,utilizedamt
	,unutilizedamt
	,tkshareamt
	,customershareamt
	,haswagesupportmolemployees
	--,internaldocchecklistguid
	,amreq.bronze_created_on
	,amreq.bronze_updated_on
	,false as is_deleted
    FROM 
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_AMENDMENTREQUEST as amreq 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_APPLICATIONSTATUS as appstatus 
        ON appstatus.code = amreq.amendmentstatusid 
  
        ),

silver_layer AS (
   SELECT 
   	id
	,referencenumber
	,processid
	,applicationid
	,amendmentstatus
	--,supportarea
	,amendmentno
	,utilizedamount
	,unutilizedamount
	,totalapprovedamount
	,submittedon
	,approvedon
	,totalavailableamt
	,utilizedamt
	,unutilizedamt
	,tkshareamt
	,customershareamt
	,haswagesupportmolemployees
	,bronze_created_on
	,bronze_updated_on
	,is_deleted
	--,source_system_name
	--,dbt_updated_at        
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".AMENDMENT_REQUEST_BASE
)

-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- Duplicate Bronze
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id 
    FROM bronze_layer 
    GROUP BY id 
    HAVING COUNT(*) > 1
)

UNION ALL

-- Duplicate Silver
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id 
    FROM silver_layer 
    GROUP BY id 
    HAVING COUNT(*) > 1
)

UNION ALL

-- ✅ COLUMN MISMATCH (CORRECTED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.referencenumber AS VARCHAR), '') <> COALESCE(CAST(s.referencenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.processid AS VARCHAR), '') <> COALESCE(CAST(s.processid AS VARCHAR), '')
     OR COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.amendmentstatus AS VARCHAR), '') <> COALESCE(CAST(s.amendmentstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.amendmentno AS VARCHAR), '') <> COALESCE(CAST(s.amendmentno AS VARCHAR), '')
     OR COALESCE(CAST(b.utilizedamount AS VARCHAR), '') <> COALESCE(CAST(s.utilizedamount AS VARCHAR), '')
     OR COALESCE(CAST(b.unutilizedamount AS VARCHAR), '') <> COALESCE(CAST(s.unutilizedamount AS VARCHAR), '')
     OR COALESCE(CAST(b.totalapprovedamount AS VARCHAR), '') <> COALESCE(CAST(s.totalapprovedamount AS VARCHAR), '')
     OR COALESCE(CAST(b.submittedon AS VARCHAR), '') <> COALESCE(CAST(s.submittedon AS VARCHAR), '')
     OR COALESCE(CAST(b.approvedon AS VARCHAR), '') <> COALESCE(CAST(s.approvedon AS VARCHAR), '')
     OR COALESCE(CAST(b.totalavailableamt AS VARCHAR), '') <> COALESCE(CAST(s.totalavailableamt AS VARCHAR), '')
     OR COALESCE(CAST(b.utilizedamt AS VARCHAR), '') <> COALESCE(CAST(s.utilizedamt AS VARCHAR), '')
     OR COALESCE(CAST(b.unutilizedamt AS VARCHAR), '') <> COALESCE(CAST(s.unutilizedamt AS VARCHAR), '')
     OR COALESCE(CAST(b.tkshareamt AS VARCHAR), '') <> COALESCE(CAST(s.tkshareamt AS VARCHAR), '')
     OR COALESCE(CAST(b.customershareamt AS VARCHAR), '') <> COALESCE(CAST(s.customershareamt AS VARCHAR), '')
     OR COALESCE(CAST(b.haswagesupportmolemployees AS VARCHAR), '') <> COALESCE(CAST(s.haswagesupportmolemployees AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(referencenumber AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(amendmentstatus AS VARCHAR),
        CAST(amendmentno AS VARCHAR),
        CAST(utilizedamount AS VARCHAR),
        CAST(unutilizedamount AS VARCHAR),
        CAST(totalapprovedamount AS VARCHAR),
        CAST(submittedon AS VARCHAR),
        CAST(approvedon AS VARCHAR),
        CAST(totalavailableamt AS VARCHAR),
        CAST(utilizedamt AS VARCHAR),
        CAST(unutilizedamt AS VARCHAR),
        CAST(tkshareamt AS VARCHAR),
        CAST(customershareamt AS VARCHAR),
        CAST(haswagesupportmolemployees AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(referencenumber AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(amendmentstatus AS VARCHAR),
        CAST(amendmentno AS VARCHAR),
        CAST(utilizedamount AS VARCHAR),
        CAST(unutilizedamount AS VARCHAR),
        CAST(totalapprovedamount AS VARCHAR),
        CAST(submittedon AS VARCHAR),
        CAST(approvedon AS VARCHAR),
        CAST(totalavailableamt AS VARCHAR),
        CAST(utilizedamt AS VARCHAR),
        CAST(unutilizedamt AS VARCHAR),
        CAST(tkshareamt AS VARCHAR),
        CAST(customershareamt AS VARCHAR),
        CAST(haswagesupportmolemployees AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(referencenumber AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(amendmentstatus AS VARCHAR),
        CAST(amendmentno AS VARCHAR),
        CAST(utilizedamount AS VARCHAR),
        CAST(unutilizedamount AS VARCHAR),
        CAST(totalapprovedamount AS VARCHAR),
        CAST(submittedon AS VARCHAR),
        CAST(approvedon AS VARCHAR),
        CAST(totalavailableamt AS VARCHAR),
        CAST(utilizedamt AS VARCHAR),
        CAST(unutilizedamt AS VARCHAR),
        CAST(tkshareamt AS VARCHAR),
        CAST(customershareamt AS VARCHAR),
        CAST(haswagesupportmolemployees AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(referencenumber AS VARCHAR),
        CAST(processid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(amendmentstatus AS VARCHAR),
        CAST(amendmentno AS VARCHAR),
        CAST(utilizedamount AS VARCHAR),
        CAST(unutilizedamount AS VARCHAR),
        CAST(totalapprovedamount AS VARCHAR),
        CAST(submittedon AS VARCHAR),
        CAST(approvedon AS VARCHAR),
        CAST(totalavailableamt AS VARCHAR),
        CAST(utilizedamt AS VARCHAR),
        CAST(unutilizedamt AS VARCHAR),
        CAST(tkshareamt AS VARCHAR),
        CAST(customershareamt AS VARCHAR),
        CAST(haswagesupportmolemployees AS VARCHAR)
    FROM bronze_layer
) t;