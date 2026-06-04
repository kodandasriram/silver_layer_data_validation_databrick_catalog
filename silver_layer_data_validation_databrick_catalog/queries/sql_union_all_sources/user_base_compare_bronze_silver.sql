-- Compare bronze-layer query output with silver-layer table output for user_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\Silver_layer_03-June-2026\converted db script to databricks\Databricks_union of all sources\user_base.sql
-- Silver source: converted db script to databricks\silver_layer scripts\user_base_silver_layer.sql

WITH
bronze_layer AS (
/*
Generated Databricks union layer for user_base.
Column order and typed NULL placeholders follow dbt model: user_base.sql.
Source transformations are embedded whole from the converted Databricks OS1/OS2/MIS scripts.
dbt macros expanded to Databricks TRY_CAST / string cleanup expressions.
*/

/*
 =================================================================================================

Name        : USER_BASE
Description : This model consolidates and standardizes amendment-related attributes
              from OS1 and OS2 base models into a unified schema. It aligns column
              structures across both sources using NULL placeholders where attributes
              are not available and combines the datasets using UNION ALL.

              The model ensures consistent column naming and structure for downstream
              consumption in the Silver Layer.

Source Tables : user_base_os2
                user_base_os1

				

Target Table : USER_BASE
Load Type    : Full Load (Table)
Materialized : table
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-13 | Kaviya  | Initial version

================================================================================================= 
*/



WITH
    user_base_os2 AS (
/* =============================================================================
   Name          : USER_BASE_OS2
   Description   : This model extracts and transforms user-level data from the
                   NEO2 (OS2) Bronze Layer and loads it into the USER_BASE_OS2
                   target table as part of the Silver Layer data pipeline.

                   It captures user identity, authentication, contact details,
                   personal information, verification status, and login activity.

                   The model also enriches user data by joining user extension
                   and user file reference tables to bring selfie and document
                   metadata (front/back ID files).

   Source Tables : neo2.OSSYS_USER
                   neo2.OSUSR_MKZ_USEREXTENSION
                   neo2.OSUSR_MKZ_USERFILE

   Target Table  : USER_BASE_OS2

   Load Type     : Incremental / Full Load (as per pipeline design)
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author       | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-05-12   | siva          | Initial Development
   ---------------------------------------------------------------------------
============================================================================= */

WITH source_data_base AS (
SELECT
    a.ID,
    a.TENANT_ID,
    a.IS_ACTIVE,
    a.CREATION_DATE,
    a.LAST_LOGIN,
    a.NAME,
    a.MOBILEPHONE,
    a.EMAIL,
    a.USERNAME,
    a.PASSWORD,
    a.EXTERNAL_ID,
    b.CPR_NUMBER,
    b.DATEOFBIRTH,
    b.GENDER,
    b.NATIONALITY,
    b.LOGINATTEMPTSFAILED,
    b.PHONECOUNTRYCODE,
    b.ISVERIFIED,
    c.NAME AS SELFIEFILENAME,
    d.NAME AS FONTSIDEDOCFILENAME,
    e.NAME AS BACKSIDEDOCFILENAME,
    b.USEOUTSYSTEMSLOGIN,
    b.USEOUTSYSTEMSLOGICSOURCE,
    b.USEOUTSYSTEMSLOGICDATETIME,
    b.OLDPORTAL_TAMKEENIDENTITYID,
    b.ISEMAILVERIFIED,
    b.EMAILVERIFIED_DATETIME,
    b.ISMOBILEVERIFIED,
    b.MOBILEVERIFIED_DATETIME,
    b.ISREJECTED,
    b.REJECTIONREMARK,
    b.SECONDARYPHONENUMBER,
    b.RESETPASSREQUIRED,
    'NEO2' AS SOURCE_SYSTEM_NAME,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS DBT_UPDATED_AT,
    b.USERID,
    b.SELFIEFILEID,
    b.FONTSIDEDOCFILEID,
    b.BACKSIDEDOCFILEID,
    b.ISFROMOLDPORTAL,
    b.OLDPORTAL_TAMKEENPWDHASH,
    b.OLDPORTAL_TAMKEENPWDSALT,
    b.LASTLOGINOTPVERIFIEDDATETIME,
    b.ISOTPLOCKED,
    b.OTPLOCKEND_DATETIME
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER` a
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USEREXTENSION` b
    ON a.ID = b.USERID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USERFILE` c
    ON b.SELFIEFILEID = c.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USERFILE` d
    ON b.FONTSIDEDOCFILEID = d.ID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USERFILE` e
    ON b.BACKSIDEDOCFILEID = e.ID
)

SELECT
 TRY_CAST(ID AS BIGINT) AS id,
TRY_CAST(TENANT_ID AS BIGINT) AS tenant_id,
IS_ACTIVE AS is_active,
TRY_CAST(CREATION_DATE AS TIMESTAMP) AS creation_date,
TRY_CAST(LAST_LOGIN AS TIMESTAMP) AS last_login,
NAME AS name,
MOBILEPHONE AS mobilephone,
EMAIL AS email,
USERNAME AS username,
PASSWORD AS password,
EXTERNAL_ID AS external_id,
CPR_NUMBER AS cpr_number,
TRY_CAST(DATEOFBIRTH AS TIMESTAMP) AS dateofbirth,
GENDER AS gender,
NATIONALITY AS nationality,
TRY_CAST(LOGINATTEMPTSFAILED AS BIGINT) AS loginattemptsfailed,
PHONECOUNTRYCODE AS phonecountrycode,
ISVERIFIED AS isverified,
SELFIEFILENAME AS selfiefilename,
FONTSIDEDOCFILENAME AS fontsidedocfilename,
BACKSIDEDOCFILENAME AS backsidedocfilename,
USEOUTSYSTEMSLOGIN AS useoutsystemslogin,
USEOUTSYSTEMSLOGICSOURCE AS useoutsystemslogicsource,
TRY_CAST(USEOUTSYSTEMSLOGICDATETIME AS TIMESTAMP) AS useoutsystemslogicdatetime,
OLDPORTAL_TAMKEENIDENTITYID AS oldportal_tamkeenidentityid,
ISEMAILVERIFIED AS isemailverified,
TRY_CAST(EMAILVERIFIED_DATETIME AS TIMESTAMP) AS emailverified_datetime,
ISMOBILEVERIFIED AS ismobileverified,
TRY_CAST(MOBILEVERIFIED_DATETIME AS TIMESTAMP) AS mobileverified_datetime,
ISREJECTED AS isrejected,
REJECTIONREMARK AS rejectionremark,
SECONDARYPHONENUMBER AS secondaryphonenumber,
RESETPASSREQUIRED AS resetpassrequired,
UPPER(NULLIF(TRIM(SOURCE_SYSTEM_NAME), '')) AS source_system_name,
TRY_CAST(DBT_UPDATED_AT AS TIMESTAMP) AS dbt_updated_at,
FALSE AS is_deleted,
TRY_CAST(USERID AS BIGINT) AS userid,
TRY_CAST(SELFIEFILEID AS BIGINT) AS selfiefileid,
TRY_CAST(FONTSIDEDOCFILEID AS BIGINT) AS fontsidedocfileid,
TRY_CAST(BACKSIDEDOCFILEID AS BIGINT) AS backsidedocfileid,
ISFROMOLDPORTAL AS isfromoldportal,
OLDPORTAL_TAMKEENPWDHASH AS oldportal_tamkeenpwdhash,
OLDPORTAL_TAMKEENPWDSALT AS oldportal_tamkeenpwdsalt,
TRY_CAST(LASTLOGINOTPVERIFIEDDATETIME AS TIMESTAMP) AS lastloginotpverifieddatetime,
ISOTPLOCKED AS isotplocked,
TRY_CAST(OTPLOCKEND_DATETIME AS TIMESTAMP) AS otplockend_datetime
FROM source_data_base
),
    user_base_os1 AS (
/*
============================================================================
silver_user_os1.sql
============================================================================
Per-source intermediate Silver model for the User domain â€” OS1 only.

Sources (User domain entities):
  â˜… ossys_User                  â†’ standard OutSystems user table (anchor)
    OSUSR_MKZ_USEREXTENSION     â†’ user-extension data (CPR number, etc.)

Reference SPs:
  - Used implicitly in 14 of 20 OS1 SPs as a JOIN target for created-by /
    modified-by user references. Not a primary report subject in any SP.
  - OSUSR_MKZ_USEREXTENSION is referenced in 7 SPs to surface CPR and
    flag customer vs. staff users (a row in USEREXTENSION typically means
    the user is a customer; a NULL JOIN means the user is internal staff).

Approach: anchor on ossys_User and LEFT JOIN OSUSR_MKZ_USEREXTENSION. A
non-null extension row signals `customer`; otherwise the user is staff.
A computed `user_type` column makes this explicit.

Cross-domain note: this is a thin reference-style Silver model. Domain
Silver models that need user details (Cheque, IBAN, Workflow, etc.) keep
the user FK column on their rows for downstream re-joining at unified
Silver / Gold level â€” they don't replicate user details inline (apart from
the few cases where the source SP already denormalises them).

Note on OS1 user extension semantics: the `'Customer: ' + name` prefix
pattern in RPT-178 / RPT-187 is a downstream report-level concern, not a
Silver concern. Silver exposes the raw `name` and the `is_customer` flag;
the prefix decoration belongs at Gold/AGG.
============================================================================
*/

SELECT
    'OSSYS_USER' AS os1_source_table,

    -- IDENTIFIERS
    USR.ID                                                                AS user_id,
    USR.NAME                                                              AS user_name,

    -- JOINED: USER EXTENSION (CUSTOMER-SPECIFIC DATA)
    USREXT.CPR_NUMBER                                                     AS cpr_number,

    -- DERIVED: USER TYPE CLASSIFICATION
    CASE WHEN USREXT.USERID IS NOT NULL THEN 'CUSTOMER' ELSE 'STAFF' END  AS user_type,
    CASE WHEN USREXT.USERID IS NOT NULL THEN TRUE       ELSE FALSE   END  AS is_customer,

    -- STANDARD TRAILING AUDIT COLUMNS
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER` usr
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USEREXTENSION` UsrExt
       ON UsrExt.USERID = usr.ID
)
SELECT DISTINCT
    -- COMMON STRUCTURE
    CAST(id AS STRING)                                  AS id,
    CAST(tenant_id AS STRING)                           AS tenant_id,
    CAST(is_active AS BOOLEAN)                           AS is_active,
    TRY_CAST(creation_date AS TIMESTAMP)           AS creation_date,
    TRY_CAST(last_login AS TIMESTAMP)              AS last_login,
    CAST(name AS STRING)                                AS name,
    CAST(mobilephone AS STRING)                         AS mobilephone,
    CAST(email AS STRING)                               AS email,
    CAST(username AS STRING)                            AS username,
    CAST(password AS STRING)                            AS password,
    CAST(external_id AS STRING)                         AS external_id,
    CAST(cpr_number AS STRING)                          AS cpr_number,
    TRY_CAST(dateofbirth AS DATE)                  AS dateofbirth,
    CAST(gender AS STRING)                              AS gender,
    CAST(nationality AS STRING)                         AS nationality,
    TRY_CAST(loginattemptsfailed AS BIGINT)        AS loginattemptsfailed,
    CAST(phonecountrycode AS STRING)                    AS phonecountrycode,
    CAST(isverified AS BOOLEAN)                          AS isverified,
    CAST(selfiefilename AS STRING)                      AS selfiefilename,
    CAST(fontsidedocfilename AS STRING)                 AS fontsidedocfilename,
    CAST(backsidedocfilename AS STRING)                 AS backsidedocfilename,
    CAST(useoutsystemslogin AS BOOLEAN)                  AS useoutsystemslogin,
    CAST(useoutsystemslogicsource AS STRING)            AS useoutsystemslogicsource,
    TRY_CAST(useoutsystemslogicdatetime AS TIMESTAMP) AS useoutsystemslogicdatetime,
    CAST(oldportal_tamkeenidentityid AS STRING)         AS oldportal_tamkeenidentityid,
    CAST(isemailverified AS BOOLEAN)                     AS isemailverified,
    TRY_CAST(emailverified_datetime AS TIMESTAMP)   AS emailverified_datetime,
    CAST(ismobileverified AS BOOLEAN)                    AS ismobileverified,
    TRY_CAST(mobileverified_datetime AS TIMESTAMP)  AS mobileverified_datetime,
    CAST(isrejected AS BOOLEAN)                          AS isrejected,
    CAST(rejectionremark AS STRING)                     AS rejectionremark,
    CAST(secondaryphonenumber AS STRING)                AS secondaryphonenumber,
    CAST(resetpassrequired AS BOOLEAN)                   AS resetpassrequired,
    source_system_name,
    dbt_updated_at,
    is_deleted,
    CAST(userid AS STRING)                              AS userid,
    CAST(selfiefileid AS STRING)                        AS selfiefileid,
    CAST(fontsidedocfileid AS STRING)                   AS fontsidedocfileid,
    CAST(backsidedocfileid AS STRING)                   AS backsidedocfileid,
    CAST(isfromoldportal AS BOOLEAN)                     AS isfromoldportal,
    CAST(oldportal_tamkeenpwdhash AS STRING)            AS oldportal_tamkeenpwdhash,
    CAST(oldportal_tamkeenpwdsalt AS STRING)            AS oldportal_tamkeenpwdsalt,
    TRY_CAST(lastloginotpverifieddatetime AS TIMESTAMP) AS lastloginotpverifieddatetime,
    CAST(isotplocked AS BOOLEAN)                         AS isotplocked,
    TRY_CAST(otplockend_datetime AS TIMESTAMP)      AS otplockend_datetime,

    -- OS1 EXTRA COLUMNS
    CAST(NULL AS STRING)                                AS user_type,
    CAST(NULL AS BOOLEAN)                                AS is_customer

FROM user_base_os2

UNION ALL

SELECT DISTINCT
    -- COMMON STRUCTURE
    CAST(user_id AS STRING)                             AS id,
    CAST(NULL AS STRING)                                AS tenant_id,
    CAST(NULL AS BOOLEAN)                                AS is_active,
    CAST(NULL AS TIMESTAMP)                              AS creation_date,
    CAST(NULL AS TIMESTAMP)                              AS last_login,
    CAST(NULL AS STRING)                                AS name,
    CAST(NULL AS STRING)                                AS mobilephone,
    CAST(NULL AS STRING)                                AS email,
    CAST(USER_NAME AS STRING)                           AS username,
    CAST(NULL AS STRING)                                AS password,
    CAST(NULL AS STRING)                                AS external_id,
    CAST(cpr_number AS STRING)                          AS cpr_number,
    CAST(NULL AS DATE)                                   AS dateofbirth,
    CAST(NULL AS STRING)                                AS gender,
    CAST(NULL AS STRING)                                AS nationality,
    CAST(NULL AS BIGINT)                                 AS loginattemptsfailed,
    CAST(NULL AS STRING)                                AS phonecountrycode,
    CAST(NULL AS BOOLEAN)                                AS isverified,
    CAST(NULL AS STRING)                                AS selfiefilename,
    CAST(NULL AS STRING)                                AS fontsidedocfilename,
    CAST(NULL AS STRING)                                AS backsidedocfilename,
    CAST(NULL AS BOOLEAN)                                AS useoutsystemslogin,
    CAST(NULL AS STRING)                                AS useoutsystemslogicsource,
    CAST(NULL AS TIMESTAMP)                              AS useoutsystemslogicdatetime,
    CAST(NULL AS STRING)                                AS oldportal_tamkeenidentityid,
    CAST(NULL AS BOOLEAN)                                AS isemailverified,
    CAST(NULL AS TIMESTAMP)                              AS emailverified_datetime,
    CAST(NULL AS BOOLEAN)                                AS ismobileverified,
    CAST(NULL AS TIMESTAMP)                              AS mobileverified_datetime,
    CAST(NULL AS BOOLEAN)                                AS isrejected,
    CAST(NULL AS STRING)                                AS rejectionremark,
    CAST(NULL AS STRING)                                AS secondaryphonenumber,
    CAST(NULL AS BOOLEAN)                                AS resetpassrequired,
    source_system_name,
    dbt_updated_at,
    is_deleted,
    CAST(NULL AS STRING)                                AS userid,
    CAST(NULL AS STRING)                                AS selfiefileid,
    CAST(NULL AS STRING)                                AS fontsidedocfileid,
    CAST(NULL AS STRING)                                AS backsidedocfileid,
    CAST(NULL AS BOOLEAN)                                AS isfromoldportal,
    CAST(NULL AS STRING)                                AS oldportal_tamkeenpwdhash,
    CAST(NULL AS STRING)                                AS oldportal_tamkeenpwdsalt,
    CAST(NULL AS TIMESTAMP)                              AS lastloginotpverifieddatetime,
    CAST(NULL AS BOOLEAN)                                AS isotplocked,
    CAST(NULL AS TIMESTAMP)                              AS otplockend_datetime,

    -- OS1 EXTRA COLUMNS
    CAST(user_type AS STRING)                           AS user_type,
    CAST(is_customer AS BOOLEAN)                         AS is_customer

FROM user_base_os1
),

silver_layer AS (
SELECT
    `id`,
    `tenant_id`,
    `is_active`,
    `creation_date`,
    `last_login`,
    `name`,
    `mobilephone`,
    `email`,
    `username`,
    `password`,
    `external_id`,
    `cpr_number`,
    `dateofbirth`,
    `gender`,
    `nationality`,
    `loginattemptsfailed`,
    `phonecountrycode`,
    `isverified`,
    `selfiefilename`,
    `fontsidedocfilename`,
    `backsidedocfilename`,
    `useoutsystemslogin`,
    `useoutsystemslogicsource`,
    `useoutsystemslogicdatetime`,
    `oldportal_tamkeenidentityid`,
    `isemailverified`,
    `emailverified_datetime`,
    `ismobileverified`,
    `mobileverified_datetime`,
    `isrejected`,
    `rejectionremark`,
    `secondaryphonenumber`,
    `resetpassrequired`,
    `source_system_name`,
    `dbt_updated_at`,
    `is_deleted`,
    `userid`,
    `selfiefileid`,
    `fontsidedocfileid`,
    `backsidedocfileid`,
    `isfromoldportal`,
    `oldportal_tamkeenpwdhash`,
    `oldportal_tamkeenpwdsalt`,
    `lastloginotpverifieddatetime`,
    `isotplocked`,
    `otplockend_datetime`,
    `user_type`,
    `is_customer`
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`user_base`
),

bronze_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'id'),
        (2, 'tenant_id'),
        (3, 'is_active'),
        (4, 'creation_date'),
        (5, 'last_login'),
        (6, 'name'),
        (7, 'mobilephone'),
        (8, 'email'),
        (9, 'username'),
        (10, 'password'),
        (11, 'external_id'),
        (12, 'cpr_number'),
        (13, 'dateofbirth'),
        (14, 'gender'),
        (15, 'nationality'),
        (16, 'loginattemptsfailed'),
        (17, 'phonecountrycode'),
        (18, 'isverified'),
        (19, 'selfiefilename'),
        (20, 'fontsidedocfilename'),
        (21, 'backsidedocfilename'),
        (22, 'useoutsystemslogin'),
        (23, 'useoutsystemslogicsource'),
        (24, 'useoutsystemslogicdatetime'),
        (25, 'oldportal_tamkeenidentityid'),
        (26, 'isemailverified'),
        (27, 'emailverified_datetime'),
        (28, 'ismobileverified'),
        (29, 'mobileverified_datetime'),
        (30, 'isrejected'),
        (31, 'rejectionremark'),
        (32, 'secondaryphonenumber'),
        (33, 'resetpassrequired'),
        (34, 'source_system_name'),
        (35, 'dbt_updated_at'),
        (36, 'is_deleted'),
        (37, 'userid'),
        (38, 'selfiefileid'),
        (39, 'fontsidedocfileid'),
        (40, 'backsidedocfileid'),
        (41, 'isfromoldportal'),
        (42, 'oldportal_tamkeenpwdhash'),
        (43, 'oldportal_tamkeenpwdsalt'),
        (44, 'lastloginotpverifieddatetime'),
        (45, 'isotplocked'),
        (46, 'otplockend_datetime'),
        (47, 'user_type'),
        (48, 'is_customer')
    ) AS t(column_position, column_name)
),

silver_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'id'),
        (2, 'tenant_id'),
        (3, 'is_active'),
        (4, 'creation_date'),
        (5, 'last_login'),
        (6, 'name'),
        (7, 'mobilephone'),
        (8, 'email'),
        (9, 'username'),
        (10, 'password'),
        (11, 'external_id'),
        (12, 'cpr_number'),
        (13, 'dateofbirth'),
        (14, 'gender'),
        (15, 'nationality'),
        (16, 'loginattemptsfailed'),
        (17, 'phonecountrycode'),
        (18, 'isverified'),
        (19, 'selfiefilename'),
        (20, 'fontsidedocfilename'),
        (21, 'backsidedocfilename'),
        (22, 'useoutsystemslogin'),
        (23, 'useoutsystemslogicsource'),
        (24, 'useoutsystemslogicdatetime'),
        (25, 'oldportal_tamkeenidentityid'),
        (26, 'isemailverified'),
        (27, 'emailverified_datetime'),
        (28, 'ismobileverified'),
        (29, 'mobileverified_datetime'),
        (30, 'isrejected'),
        (31, 'rejectionremark'),
        (32, 'secondaryphonenumber'),
        (33, 'resetpassrequired'),
        (34, 'source_system_name'),
        (35, 'dbt_updated_at'),
        (36, 'is_deleted'),
        (37, 'userid'),
        (38, 'selfiefileid'),
        (39, 'fontsidedocfileid'),
        (40, 'backsidedocfileid'),
        (41, 'isfromoldportal'),
        (42, 'oldportal_tamkeenpwdhash'),
        (43, 'oldportal_tamkeenpwdsalt'),
        (44, 'lastloginotpverifieddatetime'),
        (45, 'isotplocked'),
        (46, 'otplockend_datetime'),
        (47, 'user_type'),
        (48, 'is_customer')
    ) AS t(column_position, column_name)
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`tenant_id` AS STRING) AS `tenant_id`,
        CAST(`is_active` AS STRING) AS `is_active`,
        CAST(`creation_date` AS STRING) AS `creation_date`,
        CAST(`last_login` AS STRING) AS `last_login`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`mobilephone` AS STRING) AS `mobilephone`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`username` AS STRING) AS `username`,
        CAST(`password` AS STRING) AS `password`,
        CAST(`external_id` AS STRING) AS `external_id`,
        CAST(`cpr_number` AS STRING) AS `cpr_number`,
        CAST(`dateofbirth` AS STRING) AS `dateofbirth`,
        CAST(`gender` AS STRING) AS `gender`,
        CAST(`nationality` AS STRING) AS `nationality`,
        CAST(`loginattemptsfailed` AS STRING) AS `loginattemptsfailed`,
        CAST(`phonecountrycode` AS STRING) AS `phonecountrycode`,
        CAST(`isverified` AS STRING) AS `isverified`,
        CAST(`selfiefilename` AS STRING) AS `selfiefilename`,
        CAST(`fontsidedocfilename` AS STRING) AS `fontsidedocfilename`,
        CAST(`backsidedocfilename` AS STRING) AS `backsidedocfilename`,
        CAST(`useoutsystemslogin` AS STRING) AS `useoutsystemslogin`,
        CAST(`useoutsystemslogicsource` AS STRING) AS `useoutsystemslogicsource`,
        CAST(`useoutsystemslogicdatetime` AS STRING) AS `useoutsystemslogicdatetime`,
        CAST(`oldportal_tamkeenidentityid` AS STRING) AS `oldportal_tamkeenidentityid`,
        CAST(`isemailverified` AS STRING) AS `isemailverified`,
        CAST(`emailverified_datetime` AS STRING) AS `emailverified_datetime`,
        CAST(`ismobileverified` AS STRING) AS `ismobileverified`,
        CAST(`mobileverified_datetime` AS STRING) AS `mobileverified_datetime`,
        CAST(`isrejected` AS STRING) AS `isrejected`,
        CAST(`rejectionremark` AS STRING) AS `rejectionremark`,
        CAST(`secondaryphonenumber` AS STRING) AS `secondaryphonenumber`,
        CAST(`resetpassrequired` AS STRING) AS `resetpassrequired`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`userid` AS STRING) AS `userid`,
        CAST(`selfiefileid` AS STRING) AS `selfiefileid`,
        CAST(`fontsidedocfileid` AS STRING) AS `fontsidedocfileid`,
        CAST(`backsidedocfileid` AS STRING) AS `backsidedocfileid`,
        CAST(`isfromoldportal` AS STRING) AS `isfromoldportal`,
        CAST(`oldportal_tamkeenpwdhash` AS STRING) AS `oldportal_tamkeenpwdhash`,
        CAST(`oldportal_tamkeenpwdsalt` AS STRING) AS `oldportal_tamkeenpwdsalt`,
        CAST(`lastloginotpverifieddatetime` AS STRING) AS `lastloginotpverifieddatetime`,
        CAST(`isotplocked` AS STRING) AS `isotplocked`,
        CAST(`otplockend_datetime` AS STRING) AS `otplockend_datetime`,
        CAST(`user_type` AS STRING) AS `user_type`,
        CAST(`is_customer` AS STRING) AS `is_customer`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`tenant_id` AS STRING) AS `tenant_id`,
        CAST(`is_active` AS STRING) AS `is_active`,
        CAST(`creation_date` AS STRING) AS `creation_date`,
        CAST(`last_login` AS STRING) AS `last_login`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`mobilephone` AS STRING) AS `mobilephone`,
        CAST(`email` AS STRING) AS `email`,
        CAST(`username` AS STRING) AS `username`,
        CAST(`password` AS STRING) AS `password`,
        CAST(`external_id` AS STRING) AS `external_id`,
        CAST(`cpr_number` AS STRING) AS `cpr_number`,
        CAST(`dateofbirth` AS STRING) AS `dateofbirth`,
        CAST(`gender` AS STRING) AS `gender`,
        CAST(`nationality` AS STRING) AS `nationality`,
        CAST(`loginattemptsfailed` AS STRING) AS `loginattemptsfailed`,
        CAST(`phonecountrycode` AS STRING) AS `phonecountrycode`,
        CAST(`isverified` AS STRING) AS `isverified`,
        CAST(`selfiefilename` AS STRING) AS `selfiefilename`,
        CAST(`fontsidedocfilename` AS STRING) AS `fontsidedocfilename`,
        CAST(`backsidedocfilename` AS STRING) AS `backsidedocfilename`,
        CAST(`useoutsystemslogin` AS STRING) AS `useoutsystemslogin`,
        CAST(`useoutsystemslogicsource` AS STRING) AS `useoutsystemslogicsource`,
        CAST(`useoutsystemslogicdatetime` AS STRING) AS `useoutsystemslogicdatetime`,
        CAST(`oldportal_tamkeenidentityid` AS STRING) AS `oldportal_tamkeenidentityid`,
        CAST(`isemailverified` AS STRING) AS `isemailverified`,
        CAST(`emailverified_datetime` AS STRING) AS `emailverified_datetime`,
        CAST(`ismobileverified` AS STRING) AS `ismobileverified`,
        CAST(`mobileverified_datetime` AS STRING) AS `mobileverified_datetime`,
        CAST(`isrejected` AS STRING) AS `isrejected`,
        CAST(`rejectionremark` AS STRING) AS `rejectionremark`,
        CAST(`secondaryphonenumber` AS STRING) AS `secondaryphonenumber`,
        CAST(`resetpassrequired` AS STRING) AS `resetpassrequired`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`userid` AS STRING) AS `userid`,
        CAST(`selfiefileid` AS STRING) AS `selfiefileid`,
        CAST(`fontsidedocfileid` AS STRING) AS `fontsidedocfileid`,
        CAST(`backsidedocfileid` AS STRING) AS `backsidedocfileid`,
        CAST(`isfromoldportal` AS STRING) AS `isfromoldportal`,
        CAST(`oldportal_tamkeenpwdhash` AS STRING) AS `oldportal_tamkeenpwdhash`,
        CAST(`oldportal_tamkeenpwdsalt` AS STRING) AS `oldportal_tamkeenpwdsalt`,
        CAST(`lastloginotpverifieddatetime` AS STRING) AS `lastloginotpverifieddatetime`,
        CAST(`isotplocked` AS STRING) AS `isotplocked`,
        CAST(`otplockend_datetime` AS STRING) AS `otplockend_datetime`,
        CAST(`user_type` AS STRING) AS `user_type`,
        CAST(`is_customer` AS STRING) AS `is_customer`
    FROM silver_layer
),

bronze_minus_silver AS (
    SELECT * FROM bronze_normalized
    EXCEPT ALL
    SELECT * FROM silver_normalized
),

silver_minus_bronze AS (
    SELECT * FROM silver_normalized
    EXCEPT ALL
    SELECT * FROM bronze_normalized
),

validation_results AS (
    SELECT
        'user_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'user_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'user_base' AS table_name,
        'column_names_match' AS validation_point,
        CAST((
            SELECT COUNT(*)
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL OR s.column_name IS NULL
        ) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN NOT EXISTS (
            SELECT 1
            FROM bronze_columns b
            FULL OUTER JOIN silver_columns s
              ON b.column_position = s.column_position
             AND b.column_name = s.column_name
            WHERE b.column_name IS NULL OR s.column_name IS NULL
        ) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'user_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'user_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
