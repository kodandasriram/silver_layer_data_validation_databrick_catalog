WITH bronze_layer AS (

    SELECT
    CAST(company.id AS VARCHAR) AS id,
    company.code,
    company.maincode,
    comtype.label AS companyidtype,
    company.registrationdate,
    company.createdon,
    company.createdby,
    company.updatedon,
    company.updatedby,
    company.ownercpr,
    company.establishmentyear,
    CAST(NULL AS VARCHAR) AS TMKN_COMPANYID,
    CAST(NULL AS VARCHAR) AS TMKN_CR,
    CAST(NULL AS VARCHAR) AS TMKN_COMMERCIALNAMEENGLISH,
    CAST(NULL AS VARCHAR) AS TMKN_COMMERCIALNAMEARABIC,
    CAST(NULL AS DECIMAL) AS TMKN_AUDITDURATION,
    CAST(NULL AS DECIMAL) AS TMKN_ISSUEDCAPTIAL,
    CAST(NULL AS DECIMAL) AS TMKN_ANNUALREVENUE,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTFIRSTNAME,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTLASTNAME,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTCPR,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTEMAIL,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTMOBILENUMBER,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTOFFICENUMBER,
    CAST(NULL AS VARCHAR) AS TMKN_CONTACTDESIGNATION,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTFIRSTNAME,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTLASTNAME,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTCPR,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTEMAIL,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTMOBILENUMBER,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTOFFICENUMBER,
    CAST(NULL AS VARCHAR) AS TMKN_SECONDARYCONTACTDESIGNATION,
    CAST(NULL AS DECIMAL) AS TMKN_CURRENTBAHRAINIZATIONRATE,
    CAST(NULL AS DECIMAL) AS TMKN_TARGETBAHRAINIZATIONRATE,
    CAST(NULL AS DECIMAL) AS TMKN_BAHRAINIZATIONRATEDIFFERENCE,
    CAST(NULL AS DECIMAL) AS TMKN_TOTALEXPATRIATESSALARIES,
    CAST(NULL AS DECIMAL) AS TMKN_INPROGRESSREQUESTS,
    CAST(NULL AS DECIMAL) AS TMKN_HWTOWORK,
    CAST(NULL AS DECIMAL) AS TMKN_ACTIVEWORKERS,
    CAST(NULL AS DECIMAL) AS TMKN_PARALLELEXPAT,
    CAST(NULL AS VARCHAR) AS TMKN_ADDRESSBUILDING,
    CAST(NULL AS VARCHAR) AS TMKN_ADDRESSFLAT,
    CAST(NULL AS VARCHAR) AS TMKN_ADDRESSROADSTREET,
    CAST(NULL AS TIMESTAMP) AS MIS_CREATEDON,
    CAST(NULL AS TIMESTAMP) AS MIS_MODIFIEDON

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY company
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_COMPANYIDTYPE4 comtype
    ON comtype.id = company.companyidtypeid

UNION ALL

SELECT
    CAST(tmkn_companyid AS VARCHAR) AS id,
    CAST(NULL AS VARCHAR) AS code,
    CAST(NULL AS VARCHAR) AS maincode,
    CAST(NULL AS VARCHAR) AS companyidtype,
    CAST(NULL AS TIMESTAMP) AS registrationdate,
    CAST(NULL AS TIMESTAMP) AS createdon,
    CAST(NULL AS VARCHAR) AS createdby,
    CAST(NULL AS TIMESTAMP) AS updatedon,
    CAST(NULL AS VARCHAR) AS updatedby,
    CAST(NULL AS VARCHAR) AS ownercpr,
    CAST(NULL AS INTEGER) AS establishmentyear,
    tmkn_companyid,
    tmkn_cr,
    tmkn_commercialnameenglish,
    tmkn_commercialnamearabic,
    tmkn_auditduration,
    tmkn_issuedcaptial,
    tmkn_annualrevenue,
    tmkn_contactfirstname,
    tmkn_contactlastname,
    tmkn_contactcpr,
    tmkn_contactemail,
    tmkn_contactmobilenumber,
    tmkn_contactofficenumber,
    tmkn_contactdesignation,
    tmkn_secondarycontactfirstname,
    tmkn_secondarycontactlastname,
    tmkn_secondarycontactcpr,
    tmkn_secondarycontactemail,
    tmkn_secondarycontactmobilenumber,
    tmkn_secondarycontactofficenumber,
    tmkn_secondarycontactdesignation,
    tmkn_currentbahrainizationrate,
    tmkn_targetbahrainizationrate,
    tmkn_bahrainizationratedifference,
    tmkn_totalexpatriatessalaries,
    tmkn_inprogressrequests,
    tmkn_hwtowork,
    tmkn_activeworkers,
    tmkn_parallelexpat,
    tmkn_addressbuilding,
    tmkn_addressflat,
    tmkn_addressroadstreet,
    createdon AS MIS_CREATEDON,
    modifiedon AS MIS_MODIFIEDON

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".TMKN_COMPANYBASE
    
),

silver_layer AS (

    SELECT 
        id
		,code
		,maincode
		,companytype
		,registrationdate
		,createdon
		,createdby
		,updatedon
		,updatedby
		,ownercpr
		,establishmentyear
		,tmkn_companyid
		,tmkn_cr
		,tmkn_commercialnameenglish
		,tmkn_commercialnamearabic
		,tmkn_auditduration
		,tmkn_issuedcaptial
		,tmkn_annualrevenue
		,tmkn_contactfirstname
		,tmkn_contactlastname
		,tmkn_contactcpr
		,tmkn_contactemail
		,tmkn_contactmobilenumber
		,tmkn_contactofficenumber
		,tmkn_contactdesignation
		,tmkn_secondarycontactfirstname
		,tmkn_secondarycontactlastname
		,tmkn_secondarycontactcpr
		,tmkn_secondarycontactemail
		,tmkn_secondarycontactmobilenumber
		,tmkn_secondarycontactofficenumber
		,tmkn_secondarycontactdesignation
		,tmkn_currentbahrainizationrate
		,tmkn_targetbahrainizationrate
		,tmkn_bahrainizationratedifference
		,tmkn_totalexpatriatessalaries
		,tmkn_inprogressrequests
		,tmkn_hwtowork
		,tmkn_activeworkers
		,tmkn_parallelexpat
		,tmkn_addressbuilding
		,tmkn_addressflat
		,tmkn_addressroadstreet
		,mis_createdon
		,mis_modifiedon
		--,source_system_name
		--,dbt_updated_at
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".COMPANY_BASE
    
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

-- ✅ Column Mismatch Count (FIXED)
SELECT
    'COLUMN_MISMATCH_COUNT',
    COUNT(*),
    NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s
          ON CAST(b.id AS VARCHAR) = CAST(s.id AS VARCHAR)

    WHERE
        COALESCE(CAST(b.code AS VARCHAR), '') <> COALESCE(CAST(s.code AS VARCHAR), '')
     OR COALESCE(CAST(b.maincode AS VARCHAR), '') <> COALESCE(CAST(s.maincode AS VARCHAR), '')
     OR COALESCE(CAST(b.companyidtype AS VARCHAR), '') <> COALESCE(CAST(s.companytype AS VARCHAR), '')
     OR COALESCE(CAST(b.registrationdate AS VARCHAR), '') <> COALESCE(CAST(s.registrationdate AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.createdby AS VARCHAR), '') <> COALESCE(CAST(s.createdby AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedby AS VARCHAR), '') <> COALESCE(CAST(s.updatedby AS VARCHAR), '')
     OR COALESCE(CAST(b.ownercpr AS VARCHAR), '') <> COALESCE(CAST(s.ownercpr AS VARCHAR), '')
     OR COALESCE(CAST(b.establishmentyear AS VARCHAR), '') <> COALESCE(CAST(s.establishmentyear AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_companyid AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_companyid AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_cr AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_cr AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_commercialnameenglish AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_commercialnameenglish AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_commercialnamearabic AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_commercialnamearabic AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_auditduration AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_auditduration AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_issuedcaptial AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_issuedcaptial AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_annualrevenue AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_annualrevenue AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_contactfirstname AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactfirstname AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_contactlastname AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactlastname AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_contactcpr AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactcpr AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_contactemail AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactemail AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_contactmobilenumber AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactmobilenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_contactofficenumber AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactofficenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_contactdesignation AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_contactdesignation AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_secondarycontactfirstname AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_secondarycontactfirstname AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_secondarycontactlastname AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_secondarycontactlastname AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_secondarycontactemail AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_secondarycontactemail AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_currentbahrainizationrate AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_currentbahrainizationrate AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_targetbahrainizationrate AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_targetbahrainizationrate AS VARCHAR), '')

     OR COALESCE(CAST(b.tmkn_addressbuilding AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_addressbuilding AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_addressflat AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_addressflat AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_addressroadstreet AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_addressroadstreet AS VARCHAR), '')

     OR COALESCE(CAST(b.mis_createdon AS VARCHAR), '') <> COALESCE(CAST(s.mis_createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_modifiedon AS VARCHAR), '') <> COALESCE(CAST(s.mis_modifiedon AS VARCHAR), '')
) t;

