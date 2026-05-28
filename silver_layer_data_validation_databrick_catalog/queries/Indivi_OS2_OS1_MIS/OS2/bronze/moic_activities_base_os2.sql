WITH CTE_MOIC_ACTIVITIES AS (

    SELECT
        MOIC.*,
        SECTORISICACTIVITY.LABEL,
        FALSE AS IS_DELETED,
        'Neo2' AS SOURCE_SYSTEM_NAME,
        CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp) AS DBT_UPDATED_AT

    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MYA_MOIC_CRACTIVITY MOIC

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISIC3 SECTORISIC
        ON MOIC.BUSINESSACTIVITYCODE = SECTORISIC.ISICCODE

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_SECTORISICACTIVITY3 SECTORISICACTIVITY
        ON SECTORISICACTIVITY.CODE = SECTORISIC.SECTORISICACTIVITYID

    WHERE MOIC.PAYMENTREQUESTID = 0
      AND MOIC.ELIGIBILITYCRITERIAREQUESTTY = 'ASS'
      AND MOIC.CRNUMBER IS NOT NULL
      AND MOIC.CRNUMBER <> ''

),

FINAL AS (

    SELECT
        CAST(current_timestamp AT TIME ZONE 'UTC' AS date) AS EXTRACT_DATE,

        APPLICATIONID,
        CRNUMBER,

        array_join(
            array_agg(DISTINCT BUSINESSACTIVITYCODE)
                FILTER (WHERE BUSINESSACTIVITYCODE IS NOT NULL),
            ' | '
        ) AS ISIC_4_ACTIVITY_CODE,

        array_join(
            array_agg(DISTINCT DESCRIPTIONEN)
                FILTER (WHERE DESCRIPTIONEN IS NOT NULL),
            ' | '
        ) AS ISIC_4_ACTIVITIES_EN,

        array_join(
            array_agg(DISTINCT DESCRIPTIONAR)
                FILTER (WHERE DESCRIPTIONAR IS NOT NULL),
            ' | '
        ) AS ISIC_4_ACTIVITIES_AR,

        array_join(
            array_agg(DISTINCT LABEL)
                FILTER (WHERE LABEL IS NOT NULL),
            ' | '
        ) AS BUSINESS_ACTIVITY_CODE,

        array_join(
            array_agg(DISTINCT CAST(ID AS VARCHAR))
                FILTER (WHERE ID IS NOT NULL),
            ' | '
        ) AS ID,

        array_join(
            array_agg(DISTINCT CAST(PAYMENTREQUESTID AS VARCHAR))
                FILTER (WHERE PAYMENTREQUESTID IS NOT NULL),
            ' | '
        ) AS PAYMENTREQUESTID,

        array_join(
            array_agg(DISTINCT ELIGIBILITYCRITERIAREQUESTTY)
                FILTER (WHERE ELIGIBILITYCRITERIAREQUESTTY IS NOT NULL),
            ' | '
        ) AS ELIGIBILITYCRITERIAREQUESTTY,

        array_join(
            array_agg(DISTINCT CAST(PSMONITORINGID AS VARCHAR))
                FILTER (WHERE PSMONITORINGID IS NOT NULL),
            ' | '
        ) AS PSMONITORINGID,

        array_join(
            array_agg(DISTINCT CAST(AMENDMENTREQUESTID AS VARCHAR))
                FILTER (WHERE AMENDMENTREQUESTID IS NOT NULL),
            ' | '
        ) AS AMENDMENTREQUESTID,

        array_join(
            array_agg(DISTINCT CAST(ISLASTVERSION AS VARCHAR))
                FILTER (WHERE ISLASTVERSION IS NOT NULL),
            ' | '
        ) AS ISLASTVERSION,

        CAST(date_add('hour', 3, CREATEDON) AS timestamp) AS CREATED_ON,
        CAST(UPDATEDON AS timestamp) AS UPDATEDON,
        CAST(CREATEDON AS timestamp) AS CREATEDON,

        IS_DELETED,
        UPPER(TRIM(SOURCE_SYSTEM_NAME)) AS SOURCE_SYSTEM_NAME,
        DBT_UPDATED_AT

    FROM CTE_MOIC_ACTIVITIES

    GROUP BY
        APPLICATIONID,
        CRNUMBER,
        CREATEDON,
        UPDATEDON,
        IS_DELETED,
        SOURCE_SYSTEM_NAME,
        DBT_UPDATED_AT

)

SELECT
    EXTRACT_DATE,
    APPLICATIONID,
    CRNUMBER,
    ISIC_4_ACTIVITY_CODE,
    ISIC_4_ACTIVITIES_EN,
    ISIC_4_ACTIVITIES_AR,
    BUSINESS_ACTIVITY_CODE,
    ID,
    PAYMENTREQUESTID,
    ELIGIBILITYCRITERIAREQUESTTY,
    PSMONITORINGID,
    AMENDMENTREQUESTID,
    ISLASTVERSION,
    CREATED_ON,
    UPDATEDON,
    CREATEDON,
    IS_DELETED,
    SOURCE_SYSTEM_NAME,
    DBT_UPDATED_AT

FROM FINAL F;
