WITH bronze_layer AS (


    SELECT
        CAST(applicationsupportid AS VARCHAR) AS applicationsupportid
		,emailaddress
		,mobilecountryprefix
		,mobilenumber
		,degree.label as academicdegree
		,universityname
		,degreespecialization
		,datedegreegraduation
		,joblevel.label as joblevel
		,jobtitle
		,jobdepartment
		,jobresponsabilities
		,jobcurrentwage
		,salstatus.label as salarystatus 
		,joiningdate
		,currentmonthsexperience
		,totalmonthsexperience
		,emptype.label  as employmenttype
		,employmentcontractstartdate
		,employmentcontractenddate
		,appsegment.label as segmenttype
		,frequencyofpayment
		,universitylocations
		,CAST(employerid AS VARCHAR) AS employerid
		,lastworkingdaydate
		,employercode
		,empreason.label  as removeemployeereasons
		,removeemployeereasonsother
		,employername
		,iban
		,CAST(NULL AS VARCHAR) AS isphysicallydisabled   -- VARCHAR
		,jobcode
		,siosalary
		,workarrg.label  as typeofworkarrangement
		,isrelatedtocrowner
		,relation
		,isoutsourced
		,isreplacingnonbahraini
		,employeetypeid
		,contracttype
		,contractstartdate
		,contractenddate
		,CAST(NULL AS VARCHAR) AS TWS_EMPLOYEEAPPLICATIONID,
	    CAST(NULL AS VARCHAR) AS TMKN_MAINCOMPANY,
	    CAST(NULL AS VARCHAR) AS TWS_NAME,
	    CAST(NULL AS TIMESTAMP) AS TWS_APPLICATION_DATE,
	    CAST(NULL AS TIMESTAMP) AS MIS_CREATEDON,
	    CAST(NULL AS TIMESTAMP) AS TWS_JOINING_DATE,
	    CAST(NULL AS TIMESTAMP) AS TWS_SUBMITTED_ON,
	    CAST(NULL AS INTEGER) AS TWS_CURRENTWORKEXPERIENCEMONTHS,
	    CAST(NULL AS INTEGER) AS TWS_PREVIOUSEXPERIENCEMONTHS,
	    CAST(NULL AS INTEGER) AS TWS_CURRENT_EMPLOYER_CONTRIBUTION_MONTHS,
	    CAST(NULL AS VARCHAR) AS TWS_LM_EMAIL,
	    CAST(NULL AS BOOLEAN) AS TMKN_ISPARTTIME,
	    CAST(NULL AS BOOLEAN) AS TMKN_ISMIGRATED
    FROM 
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYEE as emp left join 
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_398_ACADEMICDEGREE as degree on degree.code = emp.academicdegreeid left join
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_JOBLEVEL as joblevel on joblevel.code = emp.joblevelid left join
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_SALARYSTATUS as salstatus on salstatus.code = emp.salarystatusid left join
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_EMPLOYMENTTYPE as emptype on emptype.code = emp.employmenttypeid left join
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3qq_ApplicantSegment as appsegment on appsegment.code = emp.segmenttypeid left join
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_REMOVEEMPLOYEEREASONS as empreason on empreason.code =emp.removeemployeereasonsid left join
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_2DA_TYPEOFWORKARRANGEMENT as workarrg on workarrg.id = emp.typeofworkarrangementid 
    
    union all
    
    select 
    	CAST(TWS_EMPLOYEEAPPLICATIONID AS VARCHAR) AS applicationsupportid,
	    TWS_LM_EMAIL AS emailaddress,
	    CAST(NULL AS VARCHAR) AS mobilecountryprefix,
	    CAST(NULL AS VARCHAR) AS mobilenumber,
	    CAST(NULL AS VARCHAR) AS academicdegree,
	    CAST(NULL AS VARCHAR) AS universityname,
	    --CAST(NULL AS VARCHAR) AS universitylocation_country,
	    CAST(NULL AS VARCHAR) AS degreespecialization,
	    CAST(NULL AS TIMESTAMP) AS datedegreegraduation,
	    CAST(NULL AS VARCHAR) AS joblevel,
	    CAST(NULL AS VARCHAR) AS jobtitle,
	    CAST(NULL AS VARCHAR) AS jobdepartment,
	    CAST(NULL AS VARCHAR) AS jobresponsabilities,
	    CAST(NULL AS DECIMAL) AS jobcurrentwage,
	    CAST(NULL AS VARCHAR) AS salarystatus,
	    TWS_JOINING_DATE AS joiningdate,
	    cast(TWS_CURRENTWORKEXPERIENCEMONTHS as integer) AS currentmonthsexperience,
	    cast( TWS_PREVIOUSEXPERIENCEMONTHS as integer)  AS totalmonthsexperience,
	    CAST(NULL AS VARCHAR) AS employmenttype,
	    --CAST(NULL AS VARCHAR) AS employeecontracttypeid,
	    CAST(NULL AS timestamp) AS employmentcontractstartdate,
	    CAST(NULL AS timestamp) AS employmentcontractenddate,
	    CAST(NULL AS VARCHAR) AS segmenttype,
	    CAST(NULL AS VARCHAR) AS frequencyofpayment,
	    CAST(NULL AS bigint) AS universitylocations,
	    cast(TMKN_MAINCOMPANY as varchar) AS employerid,
	    CAST(NULL AS timestamp) AS lastworkingdaydate,
	    CAST(NULL AS VARCHAR) AS employercode,
	    CAST(NULL AS VARCHAR) AS removeemployeereasons,
	    CAST(NULL AS VARCHAR) AS removeemployeereasonsother,
	    CAST(NULL AS VARCHAR) AS employername,
	    CAST(NULL AS VARCHAR) AS iban,
	    CAST(NULL AS VARCHAR) AS isphysicallydisabled,
	    CAST(NULL AS VARCHAR) AS jobcode,
	    CAST(NULL AS DECIMAL) AS siosalary,
	    CAST(NULL AS VARCHAR) AS typeofworkarrangement,
	    CAST(NULL AS BOOLEAN) AS isrelatedtocrowner,
	    CAST(NULL AS VARCHAR) AS relation,
	    CAST(NULL AS BOOLEAN) AS isoutsourced,
	    CAST(NULL AS BOOLEAN) AS isreplacingnonbahraini,
	    CAST(NULL AS VARCHAR) AS employeetypeid,
	    CAST(NULL AS VARCHAR) AS contracttype,
	    CAST(NULL AS timestamp) AS contractstartdate,
	    CAST(NULL AS timestamp) AS contractenddate,
    	tws_employeeapplicationid
		,tmkn_maincompany
		,tws_name
		,tws_application_date
		,createdon  as mis_createdon
		,tws_joining_date
		,tws_submitted_on
		,tws_currentworkexperiencemonths
		,tws_previousexperiencemonths
		,tws_current_employer_contribution_months
		,tws_lm_email
		,tmkn_isparttime
		,tmkn_ismigrated
    from    
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TWS_EMPLOYEEAPPLICATIONBASE
    
    
    
),

silver_layer AS (



   SELECT 
        CAST(applicationsupportid AS VARCHAR) AS applicationsupportid
		,emailaddress
		,mobilecountryprefix
		,mobilenumber
		,academicdegree
		,universityname
		,degreespecialization
		,datedegreegraduation
		,joblevel
		,jobtitle
		,jobdepartment
		,jobresponsabilities
		,jobcurrentwage
		,salarystatus
		,joiningdate
		,currentmonthsexperience
		,totalmonthsexperience
		,employmenttype
		,employmentcontractstartdate
		,employmentcontractenddate
		,segmenttype
		,frequencyofpayment
		,universitylocations
		,employerid
		,lastworkingdaydate
		,employercode
		,removeemployeereasons
		,removeemployeereasonsother
		,employername
		,iban
		,CAST(isphysicallydisabled AS VARCHAR) AS isphysicallydisabled
		,jobcode
		,siosalary
		,typeofworkarrangement
		,isrelatedtocrowner
		,relation
		,isoutsourced
		,isreplacingnonbahraini
		,employeetypeid
		,contracttype
		,contractstartdate
		,contractenddate
		,tws_employeeapplicationid
		,tmkn_maincompany
		,tws_name
		,tws_application_date
		,mis_createdon
		,tws_joining_date
		,tws_submitted_on
		,tws_currentworkexperiencemonths
		,tws_previousexperiencemonths
		,tws_current_employer_contribution_months
		,tws_lm_email
		,tmkn_isparttime
		,tmkn_ismigrated
		--,source_system_name
		--,dbt_updated_at
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".EMPLOYEE_BASE
   
)

-- =========================================
-- FINAL VALIDATION OUTPUT
-- =========================================

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

