--note table OSUSR_MM5_ACADEMICDEGREE4 will change dynamically
WITH bronze_layer as
(

-- =========================
-- FIRST SELECT (UNCHANGED)
-- =========================
select 
ntpapp.applicationcustomerid
,ntpapp.mobilecountryprefix
,ntpapp.mobilenumber
,ntpapp.emailaddress
,ntpapp.addressflat
,ntpapp.addressbuilding
,ntpapp.addressroad
,ntpapp.addressblock
,ntpapp.addressarea
,track.label  as trainingtrack
,ntpapp.employername
,ntpapp.jobtitle
,ntpapp.joiningdate
,ntpapp.currentmonthsexperience
,ntpapp.totalmonthsexperience
,ntpapp.currentwage
,ntpapp.currentdegreepursued
,ntpapp.expectedgraduationdate
,degree.label as academicdegree
,ntpapp.universityname
,country.countryname as universitylocations
,ntpapp.degreespecialization
,ntpapp.datedegreegraduation
,segment.label as applicantsegment
,ntpapp.ismolnominatedjobseeker
,ntpapp.isemployeesio
,ntpapp.isentrepreneur
,CAST(NULL AS VARCHAR)    AS MIS_INDIVIDUALAPPLICATIONID
,CAST(NULL AS VARCHAR)    AS MIS_ID
,CAST(NULL AS TIMESTAMP)  AS MIS_APPLICATIONDATE
,CAST(NULL AS VARCHAR)    AS MIS_INDIVIDUALID
,CAST(NULL AS BIGINT)    AS TMKN_SEGMENT
,CAST(NULL AS VARCHAR)    AS MIS_CERTIFICATENAME
,CAST(NULL AS VARCHAR)    AS MIS_TRAINING_PROVIDER
,CAST(NULL AS TIMESTAMP)  AS CREATEDON
,CAST(NULL AS TIMESTAMP)  AS MODIFIEDON

from 
dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMERINDIVIDUAL as ntpapp 
left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_TRAININGTRACK as track 
    on track.id = ntpapp.trainingtrackid  
left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_ACADEMICDEGREE4 as degree 
    on degree.code = ntpapp.academicdegreeid  
left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_COUNTRY as country 
    on country.id = ntpapp.universitylocations  
left join dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_APPLICANTSEGMENT as segment 
    on segment.code = ntpapp.applicantsegmentid  

UNION ALL

select
    CAST(NULL AS BIGINT)     AS APPLICATIONCUSTOMERID,
    CAST(NULL AS VARCHAR)    AS MOBILECOUNTRYPREFIX,
    CAST(NULL AS VARCHAR)    AS MOBILENUMBER,
    CAST(NULL AS VARCHAR)    AS EMAILADDRESS,
    CAST(NULL AS VARCHAR)    AS ADDRESSFLAT,
    CAST(NULL AS VARCHAR)    AS ADDRESSBUILDING,
    CAST(NULL AS BIGINT)    AS ADDRESSROAD,
    CAST(NULL AS BIGINT)    AS ADDRESSBLOCK,
    CAST(NULL AS VARCHAR)    AS ADDRESSAREA,
    CAST(NULL AS VARCHAR)    AS TRAININGTRACK,
    CAST(NULL AS VARCHAR)    AS EMPLOYERNAME,
    CAST(NULL AS VARCHAR)    AS JOBTITLE,
    CAST(NULL AS TIMESTAMP)  AS JOININGDATE,
    CAST(NULL AS BIGINT)     AS CURRENTMONTHSEXPERIENCE,
    CAST(NULL AS BIGINT)     AS TOTALMONTHSEXPERIENCE,
    CAST(NULL AS DOUBLE)     AS CURRENTWAGE,
    CAST(NULL AS VARCHAR)    AS CURRENTDEGREEPURSUED,
    CAST(NULL AS TIMESTAMP)  AS EXPECTEDGRADUATIONDATE,
    CAST(NULL AS VARCHAR)    AS ACADEMICDEGREE,
    CAST(NULL AS VARCHAR)    AS UNIVERSITYNAME,
    CAST(NULL AS VARCHAR)    AS UNIVERSITYLOCATIONS,
    CAST(NULL AS VARCHAR)    AS DEGREESPECIALIZATION,
    CAST(NULL AS TIMESTAMP)  AS DATEDEGREEGRADUATION,
    CAST(NULL AS VARCHAR)    AS APPLICANTSEGMENT,
    CAST(NULL AS BOOLEAN)    AS ISMOLNOMINATEDJOBSEEKER,
    CAST(NULL AS BOOLEAN)    AS ISEMPLOYEESIO,
    CAST(NULL AS BOOLEAN)    AS ISENTREPRENEUR,
    MIS_INDIVIDUALAPPLICATIONID,
    MIS_ID,
    MIS_APPLICATIONDATE,
    MIS_INDIVIDUALID,
    TMKN_SEGMENT,
    MIS_CERTIFICATENAME,
    MIS_TRAINING_PROVIDER,
    CREATEDON,
    MODIFIEDON
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".MIS_INDIVIDUALAPPLICATIONBASE

),
silver_layer AS 

(

select
applicationcustomerid
,mobilecountryprefix
,mobilenumber
,emailaddress
,addressflat
,addressbuilding
,addressroad
,addressblock
,addressarea
,trainingtrack
,employername
,jobtitle
,joiningdate
,currentmonthsexperience
,totalmonthsexperience
,currentwage
,currentdegreepursued
,expectedgraduationdate
,academicdegree
--,academicdegreelevel
,universityname
,universitylocations
--,universitycountry
,degreespecialization
,datedegreegraduation
,applicantsegment
,ismolnominatedjobseeker
,isemployeesio
,isentrepreneur
--,disability
--,createdby
,mis_individualapplicationid
,mis_id
,mis_applicationdate
,mis_individualid
,tmkn_segment
,mis_certificatename
,mis_training_provider
,createdon
,modifiedon
--,source_system_name
--,dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_CUSTOMER_INDIVIDUAL_BASE

)


SELECT
    'COUNT_VALIDATION' AS validation_type,
    COUNT(*) AS bronze_count,
    (SELECT COUNT(*) FROM silver_layer) AS silver_count
FROM bronze_layer
/**
UNION ALL

-- 2. DUPLICATE CHECK - BRONZE
SELECT
    'DUPLICATE_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT applicationcustomerid
    FROM bronze_layer
    GROUP BY applicationcustomerid
    HAVING COUNT(*) > 1
)

UNION ALL

-- 3. DUPLICATE CHECK - SILVER
SELECT
    'DUPLICATE_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT applicationcustomerid
    FROM silver_layer
    GROUP BY applicationcustomerid
    HAVING COUNT(*) > 1
)
**/
UNION ALL

-- 4. BRONZE NOT IN SILVER
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT 
        CAST(applicationcustomerid AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(addressflat AS VARCHAR),
        CAST(addressbuilding AS VARCHAR),
        CAST(addressroad AS VARCHAR),
        CAST(addressblock AS VARCHAR),
        CAST(addressarea AS VARCHAR),
        CAST(trainingtrack AS VARCHAR),
        CAST(employername AS VARCHAR),
        CAST(jobtitle AS VARCHAR),
        CAST(joiningdate AS VARCHAR),
        CAST(currentmonthsexperience AS VARCHAR),
        CAST(totalmonthsexperience AS VARCHAR),
        CAST(currentwage AS VARCHAR),
        CAST(currentdegreepursued AS VARCHAR),
        CAST(expectedgraduationdate AS VARCHAR),
        CAST(academicdegree AS VARCHAR),
        CAST(universityname AS VARCHAR),
        CAST(universitylocations AS VARCHAR),
        CAST(degreespecialization AS VARCHAR),
        CAST(datedegreegraduation AS VARCHAR),
        CAST(applicantsegment AS VARCHAR),
        CAST(ismolnominatedjobseeker AS VARCHAR),
        CAST(isemployeesio AS VARCHAR),
        CAST(isentrepreneur AS VARCHAR),
        CAST(mis_individualapplicationid AS VARCHAR),
        CAST(mis_id AS VARCHAR),
        CAST(mis_applicationdate AS VARCHAR),
        CAST(mis_individualid AS VARCHAR),
        CAST(tmkn_segment AS VARCHAR),
        CAST(mis_certificatename AS VARCHAR),
        CAST(mis_training_provider AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(modifiedon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(applicationcustomerid AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(addressflat AS VARCHAR),
        CAST(addressbuilding AS VARCHAR),
        CAST(addressroad AS VARCHAR),
        CAST(addressblock AS VARCHAR),
        CAST(addressarea AS VARCHAR),
        CAST(trainingtrack AS VARCHAR),
        CAST(employername AS VARCHAR),
        CAST(jobtitle AS VARCHAR),
        CAST(joiningdate AS VARCHAR),
        CAST(currentmonthsexperience AS VARCHAR),
        CAST(totalmonthsexperience AS VARCHAR),
        CAST(currentwage AS VARCHAR),
        CAST(currentdegreepursued AS VARCHAR),
        CAST(expectedgraduationdate AS VARCHAR),
        CAST(academicdegree AS VARCHAR),
        CAST(universityname AS VARCHAR),
        CAST(universitylocations AS VARCHAR),
        CAST(degreespecialization AS VARCHAR),
        CAST(datedegreegraduation AS VARCHAR),
        CAST(applicantsegment AS VARCHAR),
        CAST(ismolnominatedjobseeker AS VARCHAR),
        CAST(isemployeesio AS VARCHAR),
        CAST(isentrepreneur AS VARCHAR),
        CAST(mis_individualapplicationid AS VARCHAR),
        CAST(mis_id AS VARCHAR),
        CAST(mis_applicationdate AS VARCHAR),
        CAST(mis_individualid AS VARCHAR),
        CAST(tmkn_segment AS VARCHAR),
        CAST(mis_certificatename AS VARCHAR),
        CAST(mis_training_provider AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(modifiedon AS VARCHAR)
    FROM silver_layer
)

UNION ALL

-- 5. SILVER NOT IN BRONZE
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT 
        CAST(applicationcustomerid AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(addressflat AS VARCHAR),
        CAST(addressbuilding AS VARCHAR),
        CAST(addressroad AS VARCHAR),
        CAST(addressblock AS VARCHAR),
        CAST(addressarea AS VARCHAR),
        CAST(trainingtrack AS VARCHAR),
        CAST(employername AS VARCHAR),
        CAST(jobtitle AS VARCHAR),
        CAST(joiningdate AS VARCHAR),
        CAST(currentmonthsexperience AS VARCHAR),
        CAST(totalmonthsexperience AS VARCHAR),
        CAST(currentwage AS VARCHAR),
        CAST(currentdegreepursued AS VARCHAR),
        CAST(expectedgraduationdate AS VARCHAR),
        CAST(academicdegree AS VARCHAR),
        CAST(universityname AS VARCHAR),
        CAST(universitylocations AS VARCHAR),
        CAST(degreespecialization AS VARCHAR),
        CAST(datedegreegraduation AS VARCHAR),
        CAST(applicantsegment AS VARCHAR),
        CAST(ismolnominatedjobseeker AS VARCHAR),
        CAST(isemployeesio AS VARCHAR),
        CAST(isentrepreneur AS VARCHAR),
        CAST(mis_individualapplicationid AS VARCHAR),
        CAST(mis_id AS VARCHAR),
        CAST(mis_applicationdate AS VARCHAR),
        CAST(mis_individualid AS VARCHAR),
        CAST(tmkn_segment AS VARCHAR),
        CAST(mis_certificatename AS VARCHAR),
        CAST(mis_training_provider AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(modifiedon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(applicationcustomerid AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(addressflat AS VARCHAR),
        CAST(addressbuilding AS VARCHAR),
        CAST(addressroad AS VARCHAR),
        CAST(addressblock AS VARCHAR),
        CAST(addressarea AS VARCHAR),
        CAST(trainingtrack AS VARCHAR),
        CAST(employername AS VARCHAR),
        CAST(jobtitle AS VARCHAR),
        CAST(joiningdate AS VARCHAR),
        CAST(currentmonthsexperience AS VARCHAR),
        CAST(totalmonthsexperience AS VARCHAR),
        CAST(currentwage AS VARCHAR),
        CAST(currentdegreepursued AS VARCHAR),
        CAST(expectedgraduationdate AS VARCHAR),
        CAST(academicdegree AS VARCHAR),
        CAST(universityname AS VARCHAR),
        CAST(universitylocations AS VARCHAR),
        CAST(degreespecialization AS VARCHAR),
        CAST(datedegreegraduation AS VARCHAR),
        CAST(applicantsegment AS VARCHAR),
        CAST(ismolnominatedjobseeker AS VARCHAR),
        CAST(isemployeesio AS VARCHAR),
        CAST(isentrepreneur AS VARCHAR),
        CAST(mis_individualapplicationid AS VARCHAR),
        CAST(mis_id AS VARCHAR),
        CAST(mis_applicationdate AS VARCHAR),
        CAST(mis_individualid AS VARCHAR),
        CAST(tmkn_segment AS VARCHAR),
        CAST(mis_certificatename AS VARCHAR),
        CAST(mis_training_provider AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(modifiedon AS VARCHAR)
    FROM bronze_layer
)

UNION ALL

-- 6. COLUMN LEVEL MISMATCH
SELECT
    'COLUMN_MISMATCH_COUNT',
    COUNT(*),
    NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
        ON b.applicationcustomerid = s.applicationcustomerid

    WHERE
        COALESCE(CAST(b.mobilecountryprefix AS VARCHAR), '') <> COALESCE(CAST(s.mobilecountryprefix AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilenumber AS VARCHAR), '') <> COALESCE(CAST(s.mobilenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.emailaddress AS VARCHAR), '') <> COALESCE(CAST(s.emailaddress AS VARCHAR), '')
     OR COALESCE(CAST(b.addressflat AS VARCHAR), '') <> COALESCE(CAST(s.addressflat AS VARCHAR), '')
     OR COALESCE(CAST(b.addressbuilding AS VARCHAR), '') <> COALESCE(CAST(s.addressbuilding AS VARCHAR), '')
     OR COALESCE(CAST(b.addressroad AS VARCHAR), '') <> COALESCE(CAST(s.addressroad AS VARCHAR), '')
     OR COALESCE(CAST(b.addressblock AS VARCHAR), '') <> COALESCE(CAST(s.addressblock AS VARCHAR), '')
     OR COALESCE(CAST(b.addressarea AS VARCHAR), '') <> COALESCE(CAST(s.addressarea AS VARCHAR), '')
     OR COALESCE(CAST(b.trainingtrack AS VARCHAR), '') <> COALESCE(CAST(s.trainingtrack AS VARCHAR), '')
     OR COALESCE(CAST(b.employername AS VARCHAR), '') <> COALESCE(CAST(s.employername AS VARCHAR), '')
     OR COALESCE(CAST(b.jobtitle AS VARCHAR), '') <> COALESCE(CAST(s.jobtitle AS VARCHAR), '')
     OR COALESCE(CAST(b.joiningdate AS VARCHAR), '') <> COALESCE(CAST(s.joiningdate AS VARCHAR), '')
     OR COALESCE(CAST(b.currentmonthsexperience AS VARCHAR), '') <> COALESCE(CAST(s.currentmonthsexperience AS VARCHAR), '')
     OR COALESCE(CAST(b.totalmonthsexperience AS VARCHAR), '') <> COALESCE(CAST(s.totalmonthsexperience AS VARCHAR), '')
     OR COALESCE(CAST(b.currentwage AS VARCHAR), '') <> COALESCE(CAST(s.currentwage AS VARCHAR), '')
     OR COALESCE(CAST(b.currentdegreepursued AS VARCHAR), '') <> COALESCE(CAST(s.currentdegreepursued AS VARCHAR), '')
     OR COALESCE(CAST(b.expectedgraduationdate AS VARCHAR), '') <> COALESCE(CAST(s.expectedgraduationdate AS VARCHAR), '')
     OR COALESCE(CAST(b.academicdegree AS VARCHAR), '') <> COALESCE(CAST(s.academicdegree AS VARCHAR), '')
     OR COALESCE(CAST(b.universityname AS VARCHAR), '') <> COALESCE(CAST(s.universityname AS VARCHAR), '')
     OR COALESCE(CAST(b.universitylocations AS VARCHAR), '') <> COALESCE(CAST(s.universitylocations AS VARCHAR), '')
     OR COALESCE(CAST(b.degreespecialization AS VARCHAR), '') <> COALESCE(CAST(s.degreespecialization AS VARCHAR), '')
     OR COALESCE(CAST(b.datedegreegraduation AS VARCHAR), '') <> COALESCE(CAST(s.datedegreegraduation AS VARCHAR), '')
     OR COALESCE(CAST(b.applicantsegment AS VARCHAR), '') <> COALESCE(CAST(s.applicantsegment AS VARCHAR), '')
     OR COALESCE(CAST(b.ismolnominatedjobseeker AS VARCHAR), '') <> COALESCE(CAST(s.ismolnominatedjobseeker AS VARCHAR), '')
     OR COALESCE(CAST(b.isemployeesio AS VARCHAR), '') <> COALESCE(CAST(s.isemployeesio AS VARCHAR), '')
     OR COALESCE(CAST(b.isentrepreneur AS VARCHAR), '') <> COALESCE(CAST(s.isentrepreneur AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_individualapplicationid AS VARCHAR), '') <> COALESCE(CAST(s.mis_individualapplicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_id AS VARCHAR), '') <> COALESCE(CAST(s.mis_id AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_applicationdate AS VARCHAR), '') <> COALESCE(CAST(s.mis_applicationdate AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_individualid AS VARCHAR), '') <> COALESCE(CAST(s.mis_individualid AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_segment AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_segment AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_certificatename AS VARCHAR), '') <> COALESCE(CAST(s.mis_certificatename AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_training_provider AS VARCHAR), '') <> COALESCE(CAST(s.mis_training_provider AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.modifiedon AS VARCHAR), '') <> COALESCE(CAST(s.modifiedon AS VARCHAR), '')
)
