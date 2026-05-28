WITH bronze_layer AS (
    SELECT
        zmz.id,
        zmz.individualid,
        zmz.customerprofileid,
        zmzstatus.label AS crcheckrequeststatus,
        zmz.isadmin,
        zmz.companymembersroleid,
        compy.label AS companymemberspermissions,   -- ✅ label used
        zmz.cpr,
        zmz.individualnamear,
        zmz.individualnameen,
        zmz.isacknowledgechecked,
        zmz.issioregistered,
        zmz.submittedon
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CRCHECKREQUEST zmz
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CRCHECKREQUESTSTATUS zmzstatus 
        ON zmz.crcheckrequeststatusid = zmzstatus.id
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANYMEMBERSPERMISSIONS compy 
        ON compy.code = zmz.companymemberspermissionsid
),

silver_layer AS (
   SELECT 
        id,
        individualid,
        customerprofileid,
        crcheckrequeststatus,
        isadmin,
        companymembersroleid,
        companymemberspermissions,
        cpr,
        individualnamear,
        individualnameen,
        isacknowledgechecked,
        issioregistered,
        submittedon
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".CRCHECK_REQUEST_BASE
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT
    'COUNT_VALIDATION' AS validation_type,
    COUNT(*) AS bronze_count,
    (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer

UNION ALL

-- Duplicate Bronze
SELECT
    'DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM bronze_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- Duplicate Silver
SELECT
    'DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT id
    FROM silver_layer
    GROUP BY id
    HAVING COUNT(*) > 1
) t

UNION ALL

-- ✅ Column Mismatch Count (FINAL FIX)
SELECT
    'COLUMN_MISMATCH_COUNT',
    COUNT(*),
    NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
        ON b.id = s.id

    WHERE
        COALESCE(CAST(b.individualid AS VARCHAR), '') <> COALESCE(CAST(s.individualid AS VARCHAR), '')
     OR COALESCE(CAST(b.customerprofileid AS VARCHAR), '') <> COALESCE(CAST(s.customerprofileid AS VARCHAR), '')
     OR COALESCE(CAST(b.crcheckrequeststatus AS VARCHAR), '') <> COALESCE(CAST(s.crcheckrequeststatus AS VARCHAR), '')
     OR COALESCE(CAST(b.isadmin AS VARCHAR), '') <> COALESCE(CAST(s.isadmin AS VARCHAR), '')
     OR COALESCE(CAST(b.companymembersroleid AS VARCHAR), '') <> COALESCE(CAST(s.companymembersroleid AS VARCHAR), '')
     OR COALESCE(CAST(b.companymemberspermissions AS VARCHAR), '') <> COALESCE(CAST(s.companymemberspermissions AS VARCHAR), '')
     OR COALESCE(CAST(b.cpr AS VARCHAR), '') <> COALESCE(CAST(s.cpr AS VARCHAR), '')
     OR COALESCE(CAST(b.individualnamear AS VARCHAR), '') <> COALESCE(CAST(s.individualnamear AS VARCHAR), '')
     OR COALESCE(CAST(b.individualnameen AS VARCHAR), '') <> COALESCE(CAST(s.individualnameen AS VARCHAR), '')
     OR COALESCE(CAST(b.isacknowledgechecked AS VARCHAR), '') <> COALESCE(CAST(s.isacknowledgechecked AS VARCHAR), '')
     OR COALESCE(CAST(b.issioregistered AS VARCHAR), '') <> COALESCE(CAST(s.issioregistered AS VARCHAR), '')
     OR COALESCE(CAST(b.submittedon AS VARCHAR), '') <> COALESCE(CAST(s.submittedon AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM bronze_layer
    EXCEPT
    SELECT * FROM silver_layer
) t

UNION ALL

-- Silver not in Bronze
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT * FROM silver_layer
    EXCEPT
    SELECT * FROM bronze_layer
) t;