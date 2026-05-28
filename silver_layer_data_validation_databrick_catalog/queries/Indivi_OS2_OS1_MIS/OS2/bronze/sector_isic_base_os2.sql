WITH cte_sector AS (
    SELECT
        a.ID,
        a.ISICCODE,
        b.LABEL AS SECTORISICACTIVITY,
        'NEO2' AS SOURCE_SYSTEM_NAME,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISIC3 a
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISICACTIVITY3 b
        ON a.SECTORISICACTIVITYID = b.CODE
)
SELECT
    CAST(ID AS integer) AS ID,
    ISICCODE AS ISICCODE,
    SECTORISICACTIVITY,
    UPPER(TRIM(SOURCE_SYSTEM_NAME)) AS SOURCE_SYSTEM_NAME,
    CAST(DBT_UPDATED_AT AS timestamp) AS DBT_UPDATED_AT
FROM cte_sector;
