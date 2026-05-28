WITH CTE_WAGE_SUPPORT_PLAN AS
(
    SELECT
        CAST(date_add('hour', 3, current_timestamp) AS DATE)      AS EXTRACT_DATE,
        WSP.ID                                                    AS ID_PAYMENT_SCHEDULE,
        WSP.WAGEID                                                AS ID_APPLICATION_SUPPORT,
        WSP.MONTH                                                 AS MONTH_NO,
        CAST(WSP.DATE AS DATE)                                    AS DUE_DATE,
        CASE
            WHEN WSP.MONTHPAYMENTDATE = TIMESTAMP  '1900-01-01 00:00:00.000'
                THEN NULL
            ELSE date_add('hour', 3, WSP.MONTHPAYMENTDATE)
        END                                                       AS PAID_ON,
        WSP.CURRENTWAGE                                           AS WAGE_CURRENT,
        WSP.ITEMCAP                                               AS WAGE_SUPPORT_CAP,
        WSP.TKSHAREAMT                                            AS WAGE_SUPPORT_TAMKEEN_SHARE,
        WSP.CUSTOMERSHAREAMT                                      AS WAGE_SUPPORT_CUSTOMER_SHARE,
        WSP.TKSHAREACTUALPCT                                      AS WAGE_SUPPORT_TAMKEEN_SHARE_PERCENT,
        WSP.CUSTOMERSHAREPCT                                      AS WAGE_SUPPORT_CUSTOMER_SHARE_PERCENT,
        TRANSACTIONSTATUS.LABEL                                   AS WORKFLOW_STATUS,
        TRANSACTIONSTATUS.DESCRIPTION                             AS WORKFLOW_STATUS_DESCRIPTION,
        FALSE                                                     AS IS_DELETED,
        'Neo2'                                                    AS SOURCE_SYSTEM_NAME,
        WSP.BRONZE_CREATED_ON,
        WSP.BRONZE_UPDATED_ON,
        CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)   AS DBT_UPDATED_AT
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VYW_WAGESUPPORTPLAN WSP

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_TRANSACTIONSTATUS TRANSACTIONSTATUS
        ON TRANSACTIONSTATUS.CODE = WSP.TRANSACTIONSTATUSID

    WHERE WSP.WAGEID IN
    (
        SELECT ASP.ID
        FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_APPLICATIONSUPPORT ASP

        LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
            ON APP.ID = ASP.APPLICATIONID

        WHERE APP.APPLICATIONSTATUSID <> 'PM'
    )
)

SELECT
     CAST(EXTRACT_DATE AS DATE)                         AS EXTRACT_DATE,
    CAST(ID_PAYMENT_SCHEDULE AS INTEGER)                AS ID_PAYMENT_SCHEDULE,
    CAST(ID_APPLICATION_SUPPORT AS INTEGER)             AS ID_APPLICATION_SUPPORT,
    MONTH_NO                                                      AS MONTH_NO,
    CAST(DUE_DATE AS DATE)                              AS DUE_DATE,
    CAST(PAID_ON AS TIMESTAMP)                          AS PAID_ON,
    WAGE_CURRENT                                                  AS WAGE_CURRENT,
    WAGE_SUPPORT_CAP                                              AS WAGE_SUPPORT_CAP,
    WAGE_SUPPORT_TAMKEEN_SHARE                                    AS WAGE_SUPPORT_TAMKEEN_SHARE,
    WAGE_SUPPORT_CUSTOMER_SHARE                                   AS WAGE_SUPPORT_CUSTOMER_SHARE,
    WAGE_SUPPORT_TAMKEEN_SHARE_PERCENT                            AS WAGE_SUPPORT_TAMKEEN_SHARE_PERCENT,
    WAGE_SUPPORT_CUSTOMER_SHARE_PERCENT                           AS WAGE_SUPPORT_CUSTOMER_SHARE_PERCENT,
    WORKFLOW_STATUS                                               AS WORKFLOW_STATUS,
    WORKFLOW_STATUS_DESCRIPTION                                   AS WORKFLOW_STATUS_DESCRIPTION,
    IS_DELETED                                                    AS IS_DELETED,
    UPPER(TRIM(SOURCE_SYSTEM_NAME))                AS SOURCE_SYSTEM_NAME,
    CAST(BRONZE_CREATED_ON AS TIMESTAMP)                AS BRONZE_CREATED_ON,
    CAST(BRONZE_UPDATED_ON AS TIMESTAMP)                AS BRONZE_UPDATED_ON,
    CAST(DBT_UPDATED_AT AS TIMESTAMP)                   AS DBT_UPDATED_AT

FROM CTE_WAGE_SUPPORT_PLAN
