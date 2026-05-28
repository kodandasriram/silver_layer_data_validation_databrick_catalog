WITH bronze_layer AS (
    SELECT
        a.ID,
        a.USERID,
        b.LABEL as LOGINTYPE,
        c.LABEL AS USERSTATUS,
        a.CUSTOMERID,
        d.LABEL as GENDER,
        g.COUNTRYNAME as COUNTRY,
        a.EMAIL,
        a.NAME,
        a.USERNAME,
        a.MOBILEPHONE,
        a.MOBILECOUNTRYPREFIX,
        a.BIRTHDATE,
        a.ISEMAILVERIFIED,
        a.EMAILVERIFIEDON,
        a.ISMOBILEVERIFIED,
        a.MOBILEVERIFIEDON,
        a.OTPCORRELATIONKEY,
        a.NRATTEMPTS,
        a.LASTLOGINATTEMPT,
        a.LASTLOGINOTPVERIFIEDON,
        a.ISAUTHETICATIONOTPLOCKED,
        a.LASTLOGINON,
        a.CREATEDON,
        a.UPDATEDON,
        a.ISMANUALVERIFICATION,
        a.LINKID,
        a.NOOFVERIFICATIONATTEMPTS,
        a.REFERENCEID,
        h.COUNTRYNAME as COUNTRYOFORIGIN,
        a.PASSPORTNUMBER,
        e.LABEL as SECONDARYIDTYPE,
        a.SECONDARYIDNUMBER,
        f.LABEL as REGISTRATIONTYPE,
        a.ISREGISTEREDBYEKEY2,
        c.IS_ACTIVE,
        c.COLORID
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_PORTALUSER a
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_LOGINTYPE b
        ON a.LOGINTYPEID = b.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_QM6_USERSTATUS c
        ON a.USERSTATUSID = c.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_GENDER d
        ON a.GENDERID = d.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_SECONDARYIDTYPE e
        ON a.SECONDARYIDTYPEID = e.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_REGISTRATIONTYPE f
        ON a.REGISTRATIONTYPEID = f.CODE
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_COUNTRY4 g
        ON a.COUNTRYID = g.ID
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_COUNTRY4 h
        ON a.COUNTRYOFORIGIN = h.ID
),

silver_layer AS (
    SELECT
        id,
        userid,
        logintype,
        userstatus,
        customerid,
        gender,
        country,
        email,
        name,
        username,
        mobilephone,
        mobilecountryprefix,
        birthdate,
        isemailverified,
        emailverifiedon,
        ismobileverified,
        mobileverifiedon,
        otpcorrelationkey,
        nrattempts,
        lastloginattempt,
        lastloginotpverifiedon,
        isautheticationotplocked,
        lastloginon,
        createdon,
        updatedon,
        ismanualverification,
        linkid,
        noofverificationattempts,
        referenceid,
        countryoforigin,
        passportnumber,
        secondaryidtype,
        secondaryidnumber,
        registrationtype,
        isregisteredbyekey2,
        is_active,
        colorid
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".portaluser_base
)

-- =========================================
-- VALIDATION
-- =========================================

-- 1. COUNT VALIDATION
SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

-- 2. DUPLICATE BRONZE
SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE SILVER
SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- 4. PK NULL CHECK
SELECT 'PK_NULL_BRONZE', COUNT(*), NULL
FROM bronze_layer WHERE id IS NULL

UNION ALL

SELECT 'PK_NULL_SILVER', COUNT(*), NULL
FROM silver_layer WHERE id IS NULL

UNION ALL

-- 5. BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(userid AS VARCHAR),
        CAST(logintype AS VARCHAR),
        CAST(userstatus AS VARCHAR),
        CAST(customerid AS VARCHAR),
        CAST(gender AS VARCHAR),
        CAST(country AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(username AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(birthdate AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(nrattempts AS VARCHAR),
        CAST(lastloginattempt AS VARCHAR),
        CAST(lastloginotpverifiedon AS VARCHAR),
        CAST(isautheticationotplocked AS VARCHAR),
        CAST(lastloginon AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(userid AS VARCHAR),
        CAST(logintype AS VARCHAR),
        CAST(userstatus AS VARCHAR),
        CAST(customerid AS VARCHAR),
        CAST(gender AS VARCHAR),
        CAST(country AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(username AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(birthdate AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(nrattempts AS VARCHAR),
        CAST(lastloginattempt AS VARCHAR),
        CAST(lastloginotpverifiedon AS VARCHAR),
        CAST(isautheticationotplocked AS VARCHAR),
        CAST(lastloginon AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 6. SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(userid AS VARCHAR),
        CAST(logintype AS VARCHAR),
        CAST(userstatus AS VARCHAR),
        CAST(customerid AS VARCHAR),
        CAST(gender AS VARCHAR),
        CAST(country AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(username AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(birthdate AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(nrattempts AS VARCHAR),
        CAST(lastloginattempt AS VARCHAR),
        CAST(lastloginotpverifiedon AS VARCHAR),
        CAST(isautheticationotplocked AS VARCHAR),
        CAST(lastloginon AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(userid AS VARCHAR),
        CAST(logintype AS VARCHAR),
        CAST(userstatus AS VARCHAR),
        CAST(customerid AS VARCHAR),
        CAST(gender AS VARCHAR),
        CAST(country AS VARCHAR),
        CAST(email AS VARCHAR),
        CAST(name AS VARCHAR),
        CAST(username AS VARCHAR),
        CAST(mobilephone AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(birthdate AS VARCHAR),
        CAST(isemailverified AS VARCHAR),
        CAST(emailverifiedon AS VARCHAR),
        CAST(ismobileverified AS VARCHAR),
        CAST(mobileverifiedon AS VARCHAR),
        CAST(otpcorrelationkey AS VARCHAR),
        CAST(nrattempts AS VARCHAR),
        CAST(lastloginattempt AS VARCHAR),
        CAST(lastloginotpverifiedon AS VARCHAR),
        CAST(isautheticationotplocked AS VARCHAR),
        CAST(lastloginon AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- 7. COLUMN MISMATCH
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.userid AS VARCHAR), '') <> COALESCE(CAST(s.userid AS VARCHAR), '')
     OR COALESCE(CAST(b.logintype AS VARCHAR), '') <> COALESCE(CAST(s.logintype AS VARCHAR), '')
     OR COALESCE(CAST(b.userstatus AS VARCHAR), '') <> COALESCE(CAST(s.userstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.email AS VARCHAR), '') <> COALESCE(CAST(s.email AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilephone AS VARCHAR), '') <> COALESCE(CAST(s.mobilephone AS VARCHAR), '')
     OR COALESCE(CAST(b.lastloginon AS VARCHAR), '') <> COALESCE(CAST(s.lastloginon AS VARCHAR), '')
) t;