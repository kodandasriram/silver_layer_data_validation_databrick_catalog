WITH bronze_layer AS (
    SELECT
        id
		,portaluserid 				--join with OSUSR_QM6_PORTALUSER(ID)
		,status.label as recoveryuserstatus  ---tatusrecoveryuserstatusid 		-- join OSUSR_QM6_USERSTATUS(CODE)   			
		,otpcorrelationkey
		--,requestguid
		--,docchecklistguid
		,email
		,isemailverified
		,emailverifiedon
		,mobilephone
		,mobilecountryprefix
		,ismobileverified
		,mobileverifiedon
		,isverified
		,noofverificationattempts
		,linkid
		,referenceid
		--,createdon
		--,updatedon
		--,iskycrequest
		--,bronze_created_on
		--,bronze_updated_on
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_ACCOUNTRECOVERY3 rebase 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_USERSTATUS status 
        ON status.code = rebase.recoveryuserstatusid
),
silver_layer AS (
   SELECT 
        id
		,portaluserid
		,recoveryuserstatus
		,otpcorrelationkey
		,email
		,isemailverified
		,emailverifiedon
		,mobilephone
		,mobilecountryprefix
		,ismobileverified
		,mobileverifiedon
		,isverified
		,noofverificationattempts
		,linkid
		,referenceid
		--,is_deleted
		--,source_system_name
		--,updatedon
		--,createdon
		--,dbt_updated_at 
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".ACCOUNT_RECOVERY_BASE
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

-- ✅ COLUMN MISMATCH (FIXED FOR THIS TABLE)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s 
        ON b.id = s.id
    WHERE
        COALESCE(CAST(b.portaluserid AS VARCHAR), '') <> COALESCE(CAST(s.portaluserid AS VARCHAR), '')
     OR COALESCE(CAST(b.recoveryuserstatus AS VARCHAR), '') <> COALESCE(CAST(s.recoveryuserstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.otpcorrelationkey AS VARCHAR), '') <> COALESCE(CAST(s.otpcorrelationkey AS VARCHAR), '')
     OR COALESCE(CAST(b.email AS VARCHAR), '') <> COALESCE(CAST(s.email AS VARCHAR), '')
     OR COALESCE(CAST(b.isemailverified AS VARCHAR), '') <> COALESCE(CAST(s.isemailverified AS VARCHAR), '')
     OR COALESCE(CAST(b.emailverifiedon AS VARCHAR), '') <> COALESCE(CAST(s.emailverifiedon AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilephone AS VARCHAR), '') <> COALESCE(CAST(s.mobilephone AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilecountryprefix AS VARCHAR), '') <> COALESCE(CAST(s.mobilecountryprefix AS VARCHAR), '')
     OR COALESCE(CAST(b.ismobileverified AS VARCHAR), '') <> COALESCE(CAST(s.ismobileverified AS VARCHAR), '')
     OR COALESCE(CAST(b.mobileverifiedon AS VARCHAR), '') <> COALESCE(CAST(s.mobileverifiedon AS VARCHAR), '')
     OR COALESCE(CAST(b.isverified AS VARCHAR), '') <> COALESCE(CAST(s.isverified AS VARCHAR), '')
     OR COALESCE(CAST(b.noofverificationattempts AS VARCHAR), '') <> COALESCE(CAST(s.noofverificationattempts AS VARCHAR), '')
     OR COALESCE(CAST(b.linkid AS VARCHAR), '') <> COALESCE(CAST(s.linkid AS VARCHAR), '')
     OR COALESCE(CAST(b.referenceid AS VARCHAR), '') <> COALESCE(CAST(s.referenceid AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(portaluserid AS VARCHAR),
        CAST(recoveryuserstatus AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(isverified AS VARCHAR),
        CAST(noofverificationattempts AS VARCHAR),
        CAST(linkid AS VARCHAR),
        CAST(referenceid AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(portaluserid AS VARCHAR),
        CAST(recoveryuserstatus AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(isverified AS VARCHAR),
        CAST(noofverificationattempts AS VARCHAR),
        CAST(linkid AS VARCHAR),
        CAST(referenceid AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(portaluserid AS VARCHAR),
        CAST(recoveryuserstatus AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(isverified AS VARCHAR),
        CAST(noofverificationattempts AS VARCHAR),
        CAST(linkid AS VARCHAR),
        CAST(referenceid AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(portaluserid AS VARCHAR),
        CAST(recoveryuserstatus AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(isverified AS VARCHAR),
        CAST(noofverificationattempts AS VARCHAR),
        CAST(linkid AS VARCHAR),
        CAST(referenceid AS VARCHAR)
    FROM bronze_layer
) t;