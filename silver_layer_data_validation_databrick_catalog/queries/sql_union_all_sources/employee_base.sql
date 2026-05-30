WITH
bronze_layer AS (
-- Standalone Trino SQL generated from employee_base.sql.
-- Final column order aligned to silver_layer_query/employee_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =============================================================================
   Name          : EMPLOYEE_BASE
   Description   : This model extracts and transforms employee-level data 
                   from the NEO2 (OS2) source system Bronze Layer and loads 
                   it into the EMPLOYEE_BASE target table as part of the 
                   Silver Layer data pipeline.

                   It captures employee personal, academic, and employment-
                   related attributes including contact details, education, 
                   job details, salary information, employment type, contract 
                   details, and employer information.

                   The model enriches employee data by joining multiple 
                   reference tables such as academic degree, country, job 
                   level, salary status, employment type, applicant segment, 
                   employer details, removal reasons, disability status, and 
                   work arrangement types.

   Source Tables : neo2.OSUSR_2DA_EMPLOYEE
                   neo2.OSUSR_2DA_EMPLOYEECONTRACTTYPE
                   neo2.OSUSR_398_ACADEMICDEGREE
                   neo2.OSUSR_398_COUNTRY
                   neo2.OSUSR_2DA_JOBLEVEL
                   neo2.OSUSR_2DA_SALARYSTATUS
                   neo2.OSUSR_2DA_EMPLOYMENTTYPE
                   neo2.OSUSR_3QQ_APPLICANTSEGMENT
                   neo2.OSUSR_ZMZ_CUSTOMER
                   neo2.OSUSR_2DA_REMOVEEMPLOYEEREASONS
                   neo2.OSUSR_MM5_YESNOOPTION4
                   neo2.OSUSR_2DA_TYPEOFWORKARRANGEMENT

   Target Table  : EMPLOYEE_BASE_OS2

   Load Type     : Full Load
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author       | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-03-24   | Pandian     | Initial Development
   ---------------------------------------------------------------------------
============================================================================= 
*/
with cte_OSUSR_2DA_EMPLOYEE AS(

SELECT
    A.applicationsupportid,
    A.emailaddress,
    A.mobilecountryprefix,
    A.mobilenumber,
    c2.LABEL as academicdegree,
    A.universityname,
    C3.COUNTRYNAME as universitylocation,
    A.degreespecialization,
    TRY_CAST(NULLIF(CAST(A.DATEDEGREEGRADUATION AS STRING), '') AS DATE) as datedegreegraduation,
    C4.LABEL as joblevel,
    A.jobtitle,
    A.jobdepartment,
    A.jobresponsabilities,
    A.jobcurrentwage,
    C5.LABEL as  salarystatus,
    TRY_CAST(NULLIF(CAST(A.JOININGDATE AS STRING), '') AS DATE) as joiningdate,
    A.currentmonthsexperience,
    A.totalmonthsexperience,
    C6.LABEL as employmenttype,
    C1.LABEL as employeecontracttype,
    TRY_CAST(NULLIF(CAST(A.EMPLOYMENTCONTRACTSTARTDATE AS STRING), '') AS DATE) as employmentcontractstartdate,
    TRY_CAST(NULLIF(CAST(A.EMPLOYMENTCONTRACTENDDATE AS STRING), '') AS DATE) as employmentcontractenddate,
    C7.LABEL as segmenttype ,
    A.frequencyofpayment,
    A.universitylocations,
    C8.NAMEEN as employer,
    A.employerid ,
    TRY_CAST(NULLIF(CAST(A.LASTWORKINGDAYDATE AS STRING), '') AS DATE) as lastworkingdaydate,
    A.employercode,
    C9.LABEL AS removeemployeereasons,
    A.removeemployeereasonsother,
    A.employername,
    A.iban,
    C10.LABEL AS isphysicallydisabled,
    A.jobcode,
    A.siosalary,
    C11.LABEL as typeofworkarrangement,
    A.isrelatedtocrowner,
    A.relation,
    A.isoutsourced,
    A.isreplacingnonbahraini,
    A.employeetypeid,
    A.contracttype,
    TRY_CAST(NULLIF(CAST(A.CONTRACTSTARTDATE AS STRING), '') AS DATE) as contractstartdate,
    TRY_CAST(NULLIF(CAST(A.CONTRACTENDDATE AS STRING), '') AS DATE) as contractenddate,
    FALSE as is_deleted,
    'NEO2' AS source_system_name,
    cast(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at,
    A.createdon,
    A.updatedon,
    ROW_NUMBER() OVER (PARTITION BY A.APPLICATIONSUPPORTID ORDER BY A.UPDATEDON DESC NULLS LAST, A.CREATEDON DESC NULLS LAST) AS rnk
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_EMPLOYEE A
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_EMPLOYEECONTRACTTYPE C1
    ON C1.CODE = A.EMPLOYEECONTRACTTYPEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_ACADEMICDEGREE C2
    ON C2.CODE = A.ACADEMICDEGREEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_398_COUNTRY C3
    ON C3.ID = TRY_CAST(A.UNIVERSITYLOCATION AS BIGINT)
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_JOBLEVEL C4
    ON C4.CODE = A.JOBLEVELID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_SALARYSTATUS C5
    ON C5.CODE = A.SALARYSTATUSID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_EMPLOYMENTTYPE C6
    ON C6.CODE = A.EMPLOYMENTTYPEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_3QQ_APPLICANTSEGMENT C7
    ON C7.CODE = A.SEGMENTTYPEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_ZMZ_CUSTOMER C8
    ON C8.ID = A.EMPLOYERID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_REMOVEEMPLOYEEREASONS C9
    ON C9.CODE = A.REMOVEEMPLOYEEREASONSID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_MM5_YESNOOPTION4 C10
    ON C10.ID = A.ISPHYSICALLYDISABLED
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_2DA_TYPEOFWORKARRANGEMENT C11
    ON C11.ID = A.TYPEOFWORKARRANGEMENTID

)
SELECT
    applicationsupportid,
    emailaddress,
    mobilecountryprefix,
    mobilenumber,
    academicdegree,
    universityname,
    universitylocation,
    degreespecialization,
    datedegreegraduation,
    joblevel,
    jobtitle,
    jobdepartment,
    jobresponsabilities,
    jobcurrentwage,
    salarystatus,
    joiningdate,
    currentmonthsexperience,
    totalmonthsexperience,
    employmenttype,
    employeecontracttype,
    employmentcontractstartdate,
    employmentcontractenddate,
    segmenttype,
    frequencyofpayment,
    universitylocations,
    employer,
    employerid,
    lastworkingdaydate,
    employercode,
    removeemployeereasons,
    removeemployeereasonsother,
    employername,
    iban,
    isphysicallydisabled,
    jobcode,
    siosalary,
    typeofworkarrangement,
    isrelatedtocrowner,
    relation,
    isoutsourced,
    isreplacingnonbahraini,
    employeetypeid,
    contracttype,
    contractstartdate,
    contractenddate,
    is_deleted,
    source_system_name,
    dbt_updated_at,
    createdon,
    updatedon
FROM cte_OSUSR_2DA_EMPLOYEE app
WHERE rnk = 1
),

silver_layer AS (
SELECT
    applicationsupportid,
    emailaddress,
    mobilecountryprefix,
    mobilenumber,
    academicdegree,
    universityname,
    universitylocation,
    degreespecialization,
    datedegreegraduation,
    joblevel,
    jobtitle,
    jobdepartment,
    jobresponsabilities,
    jobcurrentwage,
    salarystatus,
    joiningdate,
    currentmonthsexperience,
    totalmonthsexperience,
    employmenttype,
    employeecontracttype,
    employmentcontractstartdate,
    employmentcontractenddate,
    segmenttype,
    frequencyofpayment,
    universitylocations,
    employer,
    employerid,
    lastworkingdaydate,
    employercode,
    removeemployeereasons,
    removeemployeereasonsother,
    employername,
    iban,
    isphysicallydisabled,
    jobcode,
    siosalary,
    typeofworkarrangement,
    isrelatedtocrowner,
    relation,
    isoutsourced,
    isreplacingnonbahraini,
    employeetypeid,
    contracttype,
    contractstartdate,
    contractenddate,
    is_deleted,
    source_system_name,
    dbt_updated_at,
    createdon,
    updatedon
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.employee_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'applicationsupportid'),
        (2, 'emailaddress'),
        (3, 'mobilecountryprefix'),
        (4, 'mobilenumber'),
        (5, 'academicdegree'),
        (6, 'universityname'),
        (7, 'universitylocation'),
        (8, 'degreespecialization'),
        (9, 'datedegreegraduation'),
        (10, 'joblevel'),
        (11, 'jobtitle'),
        (12, 'jobdepartment'),
        (13, 'jobresponsabilities'),
        (14, 'jobcurrentwage'),
        (15, 'salarystatus'),
        (16, 'joiningdate'),
        (17, 'currentmonthsexperience'),
        (18, 'totalmonthsexperience'),
        (19, 'employmenttype'),
        (20, 'employeecontracttype'),
        (21, 'employmentcontractstartdate'),
        (22, 'employmentcontractenddate'),
        (23, 'segmenttype'),
        (24, 'frequencyofpayment'),
        (25, 'universitylocations'),
        (26, 'employer'),
        (27, 'employerid'),
        (28, 'lastworkingdaydate'),
        (29, 'employercode'),
        (30, 'removeemployeereasons'),
        (31, 'removeemployeereasonsother'),
        (32, 'employername'),
        (33, 'iban'),
        (34, 'isphysicallydisabled'),
        (35, 'jobcode'),
        (36, 'siosalary'),
        (37, 'typeofworkarrangement'),
        (38, 'isrelatedtocrowner'),
        (39, 'relation'),
        (40, 'isoutsourced'),
        (41, 'isreplacingnonbahraini'),
        (42, 'employeetypeid'),
        (43, 'contracttype'),
        (44, 'contractstartdate'),
        (45, 'contractenddate'),
        (46, 'is_deleted'),
        (47, 'source_system_name'),
        (48, 'dbt_updated_at'),
        (49, 'createdon'),
        (50, 'updatedon')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'applicationsupportid'),
        (2, 'emailaddress'),
        (3, 'mobilecountryprefix'),
        (4, 'mobilenumber'),
        (5, 'academicdegree'),
        (6, 'universityname'),
        (7, 'universitylocation'),
        (8, 'degreespecialization'),
        (9, 'datedegreegraduation'),
        (10, 'joblevel'),
        (11, 'jobtitle'),
        (12, 'jobdepartment'),
        (13, 'jobresponsabilities'),
        (14, 'jobcurrentwage'),
        (15, 'salarystatus'),
        (16, 'joiningdate'),
        (17, 'currentmonthsexperience'),
        (18, 'totalmonthsexperience'),
        (19, 'employmenttype'),
        (20, 'employeecontracttype'),
        (21, 'employmentcontractstartdate'),
        (22, 'employmentcontractenddate'),
        (23, 'segmenttype'),
        (24, 'frequencyofpayment'),
        (25, 'universitylocations'),
        (26, 'employer'),
        (27, 'employerid'),
        (28, 'lastworkingdaydate'),
        (29, 'employercode'),
        (30, 'removeemployeereasons'),
        (31, 'removeemployeereasonsother'),
        (32, 'employername'),
        (33, 'iban'),
        (34, 'isphysicallydisabled'),
        (35, 'jobcode'),
        (36, 'siosalary'),
        (37, 'typeofworkarrangement'),
        (38, 'isrelatedtocrowner'),
        (39, 'relation'),
        (40, 'isoutsourced'),
        (41, 'isreplacingnonbahraini'),
        (42, 'employeetypeid'),
        (43, 'contracttype'),
        (44, 'contractstartdate'),
        (45, 'contractenddate'),
        (46, 'is_deleted'),
        (47, 'source_system_name'),
        (48, 'dbt_updated_at'),
        (49, 'createdon'),
        (50, 'updatedon')
),

bronze_normalized AS (
    SELECT
        CAST(`applicationsupportid` AS STRING) AS `applicationsupportid`,
        CAST(`emailaddress` AS STRING) AS `emailaddress`,
        CAST(`mobilecountryprefix` AS STRING) AS `mobilecountryprefix`,
        CAST(`mobilenumber` AS STRING) AS `mobilenumber`,
        CAST(`academicdegree` AS STRING) AS `academicdegree`,
        CAST(`universityname` AS STRING) AS `universityname`,
        CAST(`universitylocation` AS STRING) AS `universitylocation`,
        CAST(`degreespecialization` AS STRING) AS `degreespecialization`,
        CAST(`datedegreegraduation` AS STRING) AS `datedegreegraduation`,
        CAST(`joblevel` AS STRING) AS `joblevel`,
        CAST(`jobtitle` AS STRING) AS `jobtitle`,
        CAST(`jobdepartment` AS STRING) AS `jobdepartment`,
        CAST(`jobresponsabilities` AS STRING) AS `jobresponsabilities`,
        CAST(`jobcurrentwage` AS STRING) AS `jobcurrentwage`,
        CAST(`salarystatus` AS STRING) AS `salarystatus`,
        CAST(`joiningdate` AS STRING) AS `joiningdate`,
        CAST(`currentmonthsexperience` AS STRING) AS `currentmonthsexperience`,
        CAST(`totalmonthsexperience` AS STRING) AS `totalmonthsexperience`,
        CAST(`employmenttype` AS STRING) AS `employmenttype`,
        CAST(`employeecontracttype` AS STRING) AS `employeecontracttype`,
        CAST(`employmentcontractstartdate` AS STRING) AS `employmentcontractstartdate`,
        CAST(`employmentcontractenddate` AS STRING) AS `employmentcontractenddate`,
        CAST(`segmenttype` AS STRING) AS `segmenttype`,
        CAST(`frequencyofpayment` AS STRING) AS `frequencyofpayment`,
        CAST(`universitylocations` AS STRING) AS `universitylocations`,
        CAST(`employer` AS STRING) AS `employer`,
        CAST(`employerid` AS STRING) AS `employerid`,
        CAST(`lastworkingdaydate` AS STRING) AS `lastworkingdaydate`,
        CAST(`employercode` AS STRING) AS `employercode`,
        CAST(`removeemployeereasons` AS STRING) AS `removeemployeereasons`,
        CAST(`removeemployeereasonsother` AS STRING) AS `removeemployeereasonsother`,
        CAST(`employername` AS STRING) AS `employername`,
        CAST(`iban` AS STRING) AS `iban`,
        CAST(`isphysicallydisabled` AS STRING) AS `isphysicallydisabled`,
        CAST(`jobcode` AS STRING) AS `jobcode`,
        CAST(`siosalary` AS STRING) AS `siosalary`,
        CAST(`typeofworkarrangement` AS STRING) AS `typeofworkarrangement`,
        CAST(`isrelatedtocrowner` AS STRING) AS `isrelatedtocrowner`,
        CAST(`relation` AS STRING) AS `relation`,
        CAST(`isoutsourced` AS STRING) AS `isoutsourced`,
        CAST(`isreplacingnonbahraini` AS STRING) AS `isreplacingnonbahraini`,
        CAST(`employeetypeid` AS STRING) AS `employeetypeid`,
        CAST(`contracttype` AS STRING) AS `contracttype`,
        CAST(`contractstartdate` AS STRING) AS `contractstartdate`,
        CAST(`contractenddate` AS STRING) AS `contractenddate`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`applicationsupportid` AS STRING) AS `applicationsupportid`,
        CAST(`emailaddress` AS STRING) AS `emailaddress`,
        CAST(`mobilecountryprefix` AS STRING) AS `mobilecountryprefix`,
        CAST(`mobilenumber` AS STRING) AS `mobilenumber`,
        CAST(`academicdegree` AS STRING) AS `academicdegree`,
        CAST(`universityname` AS STRING) AS `universityname`,
        CAST(`universitylocation` AS STRING) AS `universitylocation`,
        CAST(`degreespecialization` AS STRING) AS `degreespecialization`,
        CAST(`datedegreegraduation` AS STRING) AS `datedegreegraduation`,
        CAST(`joblevel` AS STRING) AS `joblevel`,
        CAST(`jobtitle` AS STRING) AS `jobtitle`,
        CAST(`jobdepartment` AS STRING) AS `jobdepartment`,
        CAST(`jobresponsabilities` AS STRING) AS `jobresponsabilities`,
        CAST(`jobcurrentwage` AS STRING) AS `jobcurrentwage`,
        CAST(`salarystatus` AS STRING) AS `salarystatus`,
        CAST(`joiningdate` AS STRING) AS `joiningdate`,
        CAST(`currentmonthsexperience` AS STRING) AS `currentmonthsexperience`,
        CAST(`totalmonthsexperience` AS STRING) AS `totalmonthsexperience`,
        CAST(`employmenttype` AS STRING) AS `employmenttype`,
        CAST(`employeecontracttype` AS STRING) AS `employeecontracttype`,
        CAST(`employmentcontractstartdate` AS STRING) AS `employmentcontractstartdate`,
        CAST(`employmentcontractenddate` AS STRING) AS `employmentcontractenddate`,
        CAST(`segmenttype` AS STRING) AS `segmenttype`,
        CAST(`frequencyofpayment` AS STRING) AS `frequencyofpayment`,
        CAST(`universitylocations` AS STRING) AS `universitylocations`,
        CAST(`employer` AS STRING) AS `employer`,
        CAST(`employerid` AS STRING) AS `employerid`,
        CAST(`lastworkingdaydate` AS STRING) AS `lastworkingdaydate`,
        CAST(`employercode` AS STRING) AS `employercode`,
        CAST(`removeemployeereasons` AS STRING) AS `removeemployeereasons`,
        CAST(`removeemployeereasonsother` AS STRING) AS `removeemployeereasonsother`,
        CAST(`employername` AS STRING) AS `employername`,
        CAST(`iban` AS STRING) AS `iban`,
        CAST(`isphysicallydisabled` AS STRING) AS `isphysicallydisabled`,
        CAST(`jobcode` AS STRING) AS `jobcode`,
        CAST(`siosalary` AS STRING) AS `siosalary`,
        CAST(`typeofworkarrangement` AS STRING) AS `typeofworkarrangement`,
        CAST(`isrelatedtocrowner` AS STRING) AS `isrelatedtocrowner`,
        CAST(`relation` AS STRING) AS `relation`,
        CAST(`isoutsourced` AS STRING) AS `isoutsourced`,
        CAST(`isreplacingnonbahraini` AS STRING) AS `isreplacingnonbahraini`,
        CAST(`employeetypeid` AS STRING) AS `employeetypeid`,
        CAST(`contracttype` AS STRING) AS `contracttype`,
        CAST(`contractstartdate` AS STRING) AS `contractstartdate`,
        CAST(`contractenddate` AS STRING) AS `contractenddate`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`
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
        'employee_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'employee_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'employee_base' AS table_name,
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
        'employee_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'employee_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
