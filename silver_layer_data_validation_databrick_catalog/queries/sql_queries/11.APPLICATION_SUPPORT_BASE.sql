WITH bronze_layer AS (

    SELECT
        id
		,parentapplicationsupportid
		,amendmentrequestid
		,applicationid
		,supporttypeid
		,applicationsupportstatusid
		,pricecheckstatusid
		,individualid
		,providerid
		,externalproviderid
		,providertypeid
		,ownerid
		,typeofpaymentid
		,activestatusid
		,applicationsupportactionid
		,isactive
		,facilityid
		,referencenumber
		,remarks
		,spendingperiodduedate
		,completenessstatusid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT
),

silver_layer AS (

    SELECT 
        id
		,parentapplicationsupportid
		,amendmentrequestid
		,applicationid
		,supporttypeid
		,applicationsupportstatusid
		,pricecheckstatusid
		,individualid
		,providerid
		,externalproviderid
		,providertypeid
		,ownerid
		,typeofpaymentid
		,activestatusid
		,applicationsupportactionid
		,isactive
		,facilityid
		,referencenumber
		,remarks
		,spendingperiodduedate
		,completenessstatusid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_SUPPORT_BASE
)

-- =========================================

-- NULL PK
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- COUNT
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- DUPLICATES
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1)

UNION ALL

-- ✅ FULL COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.parentapplicationsupportid AS VARCHAR), '') <> COALESCE(CAST(s.parentapplicationsupportid AS VARCHAR), '')
     OR COALESCE(CAST(b.amendmentrequestid AS VARCHAR), '') <> COALESCE(CAST(s.amendmentrequestid AS VARCHAR), '')
     OR COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.supporttypeid AS VARCHAR), '') <> COALESCE(CAST(s.supporttypeid AS VARCHAR), '')
     OR COALESCE(CAST(b.applicationsupportstatusid AS VARCHAR), '') <> COALESCE(CAST(s.applicationsupportstatusid AS VARCHAR), '')
     OR COALESCE(CAST(b.pricecheckstatusid AS VARCHAR), '') <> COALESCE(CAST(s.pricecheckstatusid AS VARCHAR), '')
     OR COALESCE(CAST(b.individualid AS VARCHAR), '') <> COALESCE(CAST(s.individualid AS VARCHAR), '')
     OR COALESCE(CAST(b.providerid AS VARCHAR), '') <> COALESCE(CAST(s.providerid AS VARCHAR), '')
     OR COALESCE(CAST(b.externalproviderid AS VARCHAR), '') <> COALESCE(CAST(s.externalproviderid AS VARCHAR), '')
     OR COALESCE(CAST(b.providertypeid AS VARCHAR), '') <> COALESCE(CAST(s.providertypeid AS VARCHAR), '')
     OR COALESCE(CAST(b.ownerid AS VARCHAR), '') <> COALESCE(CAST(s.ownerid AS VARCHAR), '')
     OR COALESCE(CAST(b.typeofpaymentid AS VARCHAR), '') <> COALESCE(CAST(s.typeofpaymentid AS VARCHAR), '')
     OR COALESCE(CAST(b.activestatusid AS VARCHAR), '') <> COALESCE(CAST(s.activestatusid AS VARCHAR), '')
     OR COALESCE(CAST(b.applicationsupportactionid AS VARCHAR), '') <> COALESCE(CAST(s.applicationsupportactionid AS VARCHAR), '')
     OR COALESCE(CAST(b.isactive AS VARCHAR), '') <> COALESCE(CAST(s.isactive AS VARCHAR), '')
     OR COALESCE(CAST(b.facilityid AS VARCHAR), '') <> COALESCE(CAST(s.facilityid AS VARCHAR), '')
     OR COALESCE(CAST(b.referencenumber AS VARCHAR), '') <> COALESCE(CAST(s.referencenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.remarks AS VARCHAR), '') <> COALESCE(CAST(s.remarks AS VARCHAR), '')
     OR COALESCE(CAST(b.spendingperiodduedate AS VARCHAR), '') <> COALESCE(CAST(s.spendingperiodduedate AS VARCHAR), '')
     OR COALESCE(CAST(b.completenessstatusid AS VARCHAR), '') <> COALESCE(CAST(s.completenessstatusid AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(parentapplicationsupportid AS VARCHAR),
        CAST(amendmentrequestid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(applicationsupportstatusid AS VARCHAR),
        CAST(pricecheckstatusid AS VARCHAR),
        CAST(individualid AS VARCHAR),
        CAST(providerid AS VARCHAR),
        CAST(externalproviderid AS VARCHAR),
        CAST(providertypeid AS VARCHAR),
        CAST(ownerid AS VARCHAR),
        CAST(typeofpaymentid AS VARCHAR),
        CAST(activestatusid AS VARCHAR),
        CAST(applicationsupportactionid AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(facilityid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(spendingperiodduedate AS VARCHAR),
        CAST(completenessstatusid AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(parentapplicationsupportid AS VARCHAR),
        CAST(amendmentrequestid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(applicationsupportstatusid AS VARCHAR),
        CAST(pricecheckstatusid AS VARCHAR),
        CAST(individualid AS VARCHAR),
        CAST(providerid AS VARCHAR),
        CAST(externalproviderid AS VARCHAR),
        CAST(providertypeid AS VARCHAR),
        CAST(ownerid AS VARCHAR),
        CAST(typeofpaymentid AS VARCHAR),
        CAST(activestatusid AS VARCHAR),
        CAST(applicationsupportactionid AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(facilityid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(spendingperiodduedate AS VARCHAR),
        CAST(completenessstatusid AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(parentapplicationsupportid AS VARCHAR),
        CAST(amendmentrequestid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(applicationsupportstatusid AS VARCHAR),
        CAST(pricecheckstatusid AS VARCHAR),
        CAST(individualid AS VARCHAR),
        CAST(providerid AS VARCHAR),
        CAST(externalproviderid AS VARCHAR),
        CAST(providertypeid AS VARCHAR),
        CAST(ownerid AS VARCHAR),
        CAST(typeofpaymentid AS VARCHAR),
        CAST(activestatusid AS VARCHAR),
        CAST(applicationsupportactionid AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(facilityid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(spendingperiodduedate AS VARCHAR),
        CAST(completenessstatusid AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(parentapplicationsupportid AS VARCHAR),
        CAST(amendmentrequestid AS VARCHAR),
        CAST(applicationid AS VARCHAR),
        CAST(supporttypeid AS VARCHAR),
        CAST(applicationsupportstatusid AS VARCHAR),
        CAST(pricecheckstatusid AS VARCHAR),
        CAST(individualid AS VARCHAR),
        CAST(providerid AS VARCHAR),
        CAST(externalproviderid AS VARCHAR),
        CAST(providertypeid AS VARCHAR),
        CAST(ownerid AS VARCHAR),
        CAST(typeofpaymentid AS VARCHAR),
        CAST(activestatusid AS VARCHAR),
        CAST(applicationsupportactionid AS VARCHAR),
        CAST(isactive AS VARCHAR),
        CAST(facilityid AS VARCHAR),
        CAST(referencenumber AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(spendingperiodduedate AS VARCHAR),
        CAST(completenessstatusid AS VARCHAR)
    FROM bronze_layer
) t;