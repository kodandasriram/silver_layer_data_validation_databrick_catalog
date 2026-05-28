WITH bronze_layer AS (

   SELECT
     CAST(vw9cert.ID AS VARCHAR) AS ID
    ,vw9cert.TRAININGPROGRAMID
    ,vw9cert.TRAININGHOURS
    ,vw9cert.TRAININGPROGRAMTYPEid
    ,vw9cert.ISALLOWPAPERS  
    ,vw9cert.createdon
    ,vw9cert.updatedon 
    ,CAST(false AS BOOLEAN) AS is_deleted
    ,CAST(NULL AS VARCHAR) AS TMKN_ID
    ,CAST(NULL AS VARCHAR) AS MIS_NAME
    ,CAST(NULL AS INTEGER) AS MIS_TYPE
    ,CAST(NULL AS DECIMAL(18,6)) AS MIS_CAP
    ,CAST(NULL AS INTEGER) AS MIS_CAP_TYPE
    ,CAST(NULL AS INTEGER) AS TMKN_CERTIFICATETYPE
    ,CAST(NULL AS BOOLEAN) AS MIS_BS
    ,CAST(NULL AS BOOLEAN) AS MIS_TPCS
    ,CAST(NULL AS BOOLEAN) AS MIS_TWS
    ,CAST(NULL AS BOOLEAN) AS TMKN_TARGETEDLEARNERS_EMPLOYEE
    ,CAST(NULL AS BOOLEAN) AS TMKN_TARGETEDLEARNERS_ENTREPRENEUR
    ,CAST(NULL AS BOOLEAN) AS TMKN_TARGETEDLEARNERS_JOBSEEKER
    ,CAST(NULL AS BOOLEAN) AS TMKN_TARGETEDLEARNERS_STUDENT
    ,CAST(NULL AS BOOLEAN) AS TMKN_STUDYTYPE_SELFSTUDY
    ,CAST(NULL AS BOOLEAN) AS TMKN_STUDYTYPE_ONLINE
    ,CAST(NULL AS BOOLEAN) AS TMKN_STUDYTYPE_BLENDEDLEARNING
    ,CAST(NULL AS BOOLEAN) AS TMKN_STUDYTYPE_LOCALTRAININGPROVIDER
    ,CAST(NULL AS BOOLEAN) AS TMKN_STUDYTYPE_INHOUSE
    ,CAST(NULL AS BOOLEAN) AS MIS_EMPLOYMENTREQUIRED
    ,CAST(NULL AS BOOLEAN) AS MIS_CERTIFICATESTATUS
    ,CAST(NULL AS BOOLEAN) AS MIS_ONJOBTRAINING
    ,CAST(NULL AS BOOLEAN) AS NFC_DEACTIVATEFLAG
    ,CAST(NULL AS INTEGER) AS MIS_AVERAGECONTACTHOURS
    ,CAST(NULL AS INTEGER) AS NFC_NOOFAPPLICANTS
    ,CAST(NULL AS INTEGER) AS NFC_TAMKEENSUPPORT
    ,CAST(NULL AS DECIMAL(18,6)) AS MIS_PRICE
    ,CAST(NULL AS DECIMAL(18,6)) AS MIS_PRICEPERHOURPERPERSON
    ,CAST(NULL AS VARCHAR) AS TMKN_ANALYSTNOTE
    ,CAST(NULL AS VARCHAR) AS MIS_OVERVIEW
    ,CAST(NULL AS VARCHAR) AS MIS_TAMKEENELIGIBILITYCRITERIA
    ,CAST(NULL AS VARCHAR) AS MIS_CRT
    ,CAST(NULL AS VARCHAR) AS MIS_DURATION
    ,CAST(NULL AS VARCHAR) AS MIS_NOOFHOURS
    ,CAST(NULL AS VARCHAR) AS MIS_CERTIFICATEWEBSITE
    ,CAST(NULL AS VARCHAR) AS MIS_LEVELS
    ,CAST(NULL AS VARCHAR) AS MIS_AVAILABILITYOFASSESSMENT
    ,CAST(NULL AS INTEGER) AS MIS_PAYMENT_STRUCTURE
    ,CAST(NULL AS INTEGER) AS MIS_LEVEL
    ,CAST(NULL AS INTEGER) AS MIS_LEVELQCF
    ,CAST(NULL AS INTEGER) AS NFC_TOTALNOOFINDIVIDUALAPPLICATIONS_STATE
    ,CAST(NULL AS INTEGER) AS NFC_TOTALNOOFINDIVIDUALAPPLICATIONS
    ,CAST(NULL AS INTEGER) AS NFC_TOTALNOOFTWSENROLLMENTS
    ,CAST(NULL AS INTEGER) AS NFC_TOTALNOOFTWSENROLLMENTS_STATE
    ,CAST(NULL AS INTEGER) AS NFC_TOTALNOOFAPPLICANTS
    ,CAST(NULL AS TIMESTAMP) AS NFC_TOTALNOOFTWSENROLLMENTS_DATE
    ,CAST(NULL AS TIMESTAMP) AS NFC_TOTALNOOFINDIVIDUALAPPLICATIONS_DATE
    ,CAST(NULL AS INTEGER) AS STATUSCODE
    ,CAST(NULL AS DECIMAL) AS EXCHANGERATE
    ,CAST(NULL AS TIMESTAMP) AS MIS_MODIFIEDON
    ,CAST(NULL AS INTEGER) AS STATECODE
    ,CAST(NULL AS TIMESTAMP) AS MIS_CREATEDON

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_CERTIFICATION vw9cert


UNION ALL


SELECT
    CAST(MIS_CERTIFICATEID AS VARCHAR)     AS ID
   ,CAST(NULL AS BIGINT)     AS TRAININGPROGRAMID      -- ✅ FIXED
   ,CAST(NULL AS INTEGER)    AS TRAININGHOURS          -- ✅ FIXED
   ,CAST(NULL AS VARCHAR)    AS TRAININGPROGRAMTYPEID  -- ✅ FIXED
   ,CAST(NULL AS BOOLEAN)    AS ISALLOWPAPERS
   ,CAST(createdon AS TIMESTAMP)  AS createdon
   ,CAST(modifiedon AS TIMESTAMP)  AS updatedon
   ,CAST(false AS BOOLEAN)   AS is_deleted
   ,CAST(TMKN_ID AS VARCHAR)
   ,CAST(MIS_NAME AS VARCHAR)
   ,CAST(MIS_TYPE AS INTEGER)
   ,CAST(MIS_CAP AS DECIMAL(18,6))
   ,CAST(MIS_CAP_TYPE AS INTEGER)
   ,CAST(TMKN_CERTIFICATETYPE AS INTEGER)
   ,CAST(MIS_BS AS BOOLEAN)
   ,CAST(MIS_TPCS AS BOOLEAN)
   ,CAST(MIS_TWS AS BOOLEAN)
   ,CAST(TMKN_TARGETEDLEARNERS_EMPLOYEE AS BOOLEAN)
   ,CAST(TMKN_TARGETEDLEARNERS_ENTREPRENEUR AS BOOLEAN)
   ,CAST(TMKN_TARGETEDLEARNERS_JOBSEEKER AS BOOLEAN)
   ,CAST(TMKN_TARGETEDLEARNERS_STUDENT AS BOOLEAN)
   ,CAST(TMKN_STUDYTYPE_SELFSTUDY AS BOOLEAN)
   ,CAST(TMKN_STUDYTYPE_ONLINE AS BOOLEAN)
   ,CAST(TMKN_STUDYTYPE_BLENDEDLEARNING AS BOOLEAN)
   ,CAST(TMKN_STUDYTYPE_LOCALTRAININGPROVIDER AS BOOLEAN)
   ,CAST(TMKN_STUDYTYPE_INHOUSE AS BOOLEAN)
   ,CAST(MIS_EMPLOYMENTREQUIRED AS BOOLEAN)
   ,CAST(MIS_CERTIFICATESTATUS AS BOOLEAN)
   ,CAST(MIS_ONJOBTRAINING AS BOOLEAN)
   ,CAST(NFC_DEACTIVATEFLAG AS BOOLEAN)
   ,CAST(MIS_AVERAGECONTACTHOURS AS INTEGER)
   ,CAST(NFC_NOOFAPPLICANTS AS INTEGER)
   ,CAST(NFC_TAMKEENSUPPORT AS INTEGER)
   ,CAST(MIS_PRICE AS DECIMAL(18,6))
   ,CAST(MIS_PRICEPERHOURPERPERSON AS DECIMAL(18,6))
   ,CAST(TMKN_ANALYSTNOTE AS VARCHAR)
   ,CAST(MIS_OVERVIEW AS VARCHAR)
   ,CAST(MIS_TAMKEENELIGIBILITYCRITERIA AS VARCHAR)
   ,CAST(MIS_CRT AS VARCHAR)
   ,CAST(MIS_DURATION AS VARCHAR)
   ,CAST(MIS_NOOFHOURS AS VARCHAR)
   ,CAST(MIS_CERTIFICATEWEBSITE AS VARCHAR)
   ,CAST(MIS_LEVELS AS VARCHAR)
   ,CAST(MIS_AVAILABILITYOFASSESSMENT AS VARCHAR)
   ,CAST(MIS_PAYMENT_STRUCTURE AS INTEGER)
   ,CAST(MIS_LEVEL AS INTEGER)
   ,CAST(MIS_LEVELQCF AS INTEGER)
   ,CAST(NFC_TOTALNOOFINDIVIDUALAPPLICATIONS_STATE AS INTEGER)
   ,CAST(NFC_TOTALNOOFINDIVIDUALAPPLICATIONS AS INTEGER)
   ,CAST(NFC_TOTALNOOFTWSENROLLMENTS AS INTEGER)
   ,CAST(NFC_TOTALNOOFTWSENROLLMENTS_STATE AS INTEGER)
   ,CAST(NFC_TOTALNOOFAPPLICANTS AS INTEGER)
   ,CAST(NFC_TOTALNOOFTWSENROLLMENTS_DATE AS TIMESTAMP)
   ,CAST(NFC_TOTALNOOFINDIVIDUALAPPLICATIONS_DATE AS TIMESTAMP)
   ,CAST(STATUSCODE AS INTEGER)
   ,CAST(EXCHANGERATE AS DECIMAL)
   ,CAST(modifiedon AS TIMESTAMP)
   ,CAST(STATECODE AS INTEGER)
   ,CAST(createdon AS TIMESTAMP)

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".MIS_CERTIFICATEBASE


),

silver_layer AS (

    SELECT 
        id
		,trainingprogramid
		,traininghours
		,trainingprogramtypeid --as per mapping doc name is trainingprogramtype
		--APPLICATIONID --present in mapping doc and missing in the db
		,isallowpapers
		,createdon --getting this columns from table OSUSR_VW9_CERTIFICATION
		,updatedon --getting this columns from table OSUSR_VW9_CERTIFICATION
		,is_deleted
		,tmkn_id
		,mis_name
		,mis_type
		,mis_cap
		,mis_cap_type
		,tmkn_certificatetype
		,mis_bs
		,mis_tpcs
		,mis_tws
		,tmkn_targetedlearners_employee
		,tmkn_targetedlearners_entrepreneur
		,tmkn_targetedlearners_jobseeker
		,tmkn_targetedlearners_student
		,tmkn_studytype_selfstudy
		,tmkn_studytype_online
		,tmkn_studytype_blendedlearning
		,tmkn_studytype_localtrainingprovider
		,tmkn_studytype_inhouse
		,mis_employmentrequired
		,mis_certificatestatus
		,mis_onjobtraining
		,nfc_deactivateflag
		,mis_averagecontacthours
		,nfc_noofapplicants
		,nfc_tamkeensupport
		,mis_price
		,mis_priceperhourperperson
		,tmkn_analystnote
		,mis_overview
		,mis_tamkeeneligibilitycriteria
		,mis_crt
		,mis_duration
		,mis_noofhours
		,mis_certificatewebsite
		,mis_levels
		,mis_availabilityofassessment
		,mis_payment_structure
		,mis_level
		,mis_levelqcf
		,nfc_totalnoofindividualapplications_state
		,nfc_totalnoofindividualapplications
		,nfc_totalnooftwsenrollments
		,nfc_totalnooftwsenrollments_state
		,nfc_totalnoofapplicants
		,nfc_totalnooftwsenrollments_date
		,nfc_totalnoofindividualapplications_date
		,statuscode
		,exchangerate
		,mis_modifiedon
		,statecode
		,mis_createdon
		--,source_system_name
		--,dbt_updated_at
    from dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".CERTIFICATION_BASE
    
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

-- ✅ Column Mismatch Count (FIXED FOR CERTIFICATE)
-- ✅ COLUMN MISMATCH COUNT (CORRECTED)
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
        COALESCE(CAST(b.trainingprogramid AS VARCHAR), '') <> COALESCE(CAST(s.trainingprogramid AS VARCHAR), '')
     OR COALESCE(CAST(b.traininghours AS VARCHAR), '') <> COALESCE(CAST(s.traininghours AS VARCHAR), '')
     OR COALESCE(CAST(b.trainingprogramtypeid AS VARCHAR), '') <> COALESCE(CAST(s.trainingprogramtypeid AS VARCHAR), '')
     OR COALESCE(CAST(b.isallowpapers AS VARCHAR), '') <> COALESCE(CAST(s.isallowpapers AS VARCHAR), '')
     OR COALESCE(CAST(b.createdon AS VARCHAR), '') <> COALESCE(CAST(s.createdon AS VARCHAR), '')
     OR COALESCE(CAST(b.updatedon AS VARCHAR), '') <> COALESCE(CAST(s.updatedon AS VARCHAR), '')
     OR COALESCE(CAST(b.is_deleted AS VARCHAR), '') <> COALESCE(CAST(s.is_deleted AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_id AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_id AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_name AS VARCHAR), '') <> COALESCE(CAST(s.mis_name AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_type AS VARCHAR), '') <> COALESCE(CAST(s.mis_type AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_cap AS VARCHAR), '') <> COALESCE(CAST(s.mis_cap AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_cap_type AS VARCHAR), '') <> COALESCE(CAST(s.mis_cap_type AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_certificatetype AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_certificatetype AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_bs AS VARCHAR), '') <> COALESCE(CAST(s.mis_bs AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_tpcs AS VARCHAR), '') <> COALESCE(CAST(s.mis_tpcs AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_tws AS VARCHAR), '') <> COALESCE(CAST(s.mis_tws AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_targetedlearners_employee AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_targetedlearners_employee AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_targetedlearners_entrepreneur AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_targetedlearners_entrepreneur AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_targetedlearners_jobseeker AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_targetedlearners_jobseeker AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_targetedlearners_student AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_targetedlearners_student AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_studytype_selfstudy AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_studytype_selfstudy AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_studytype_online AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_studytype_online AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_studytype_blendedlearning AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_studytype_blendedlearning AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_studytype_localtrainingprovider AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_studytype_localtrainingprovider AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_studytype_inhouse AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_studytype_inhouse AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_employmentrequired AS VARCHAR), '') <> COALESCE(CAST(s.mis_employmentrequired AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_certificatestatus AS VARCHAR), '') <> COALESCE(CAST(s.mis_certificatestatus AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_onjobtraining AS VARCHAR), '') <> COALESCE(CAST(s.mis_onjobtraining AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_deactivateflag AS VARCHAR), '') <> COALESCE(CAST(s.nfc_deactivateflag AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_averagecontacthours AS VARCHAR), '') <> COALESCE(CAST(s.mis_averagecontacthours AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_noofapplicants AS VARCHAR), '') <> COALESCE(CAST(s.nfc_noofapplicants AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_tamkeensupport AS VARCHAR), '') <> COALESCE(CAST(s.nfc_tamkeensupport AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_price AS VARCHAR), '') <> COALESCE(CAST(s.mis_price AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_priceperhourperperson AS VARCHAR), '') <> COALESCE(CAST(s.mis_priceperhourperperson AS VARCHAR), '')
     OR COALESCE(CAST(b.tmkn_analystnote AS VARCHAR), '') <> COALESCE(CAST(s.tmkn_analystnote AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_overview AS VARCHAR), '') <> COALESCE(CAST(s.mis_overview AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_tamkeeneligibilitycriteria AS VARCHAR), '') <> COALESCE(CAST(s.mis_tamkeeneligibilitycriteria AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_crt AS VARCHAR), '') <> COALESCE(CAST(s.mis_crt AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_duration AS VARCHAR), '') <> COALESCE(CAST(s.mis_duration AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_noofhours AS VARCHAR), '') <> COALESCE(CAST(s.mis_noofhours AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_certificatewebsite AS VARCHAR), '') <> COALESCE(CAST(s.mis_certificatewebsite AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_levels AS VARCHAR), '') <> COALESCE(CAST(s.mis_levels AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_availabilityofassessment AS VARCHAR), '') <> COALESCE(CAST(s.mis_availabilityofassessment AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_payment_structure AS VARCHAR), '') <> COALESCE(CAST(s.mis_payment_structure AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_level AS VARCHAR), '') <> COALESCE(CAST(s.mis_level AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_levelqcf AS VARCHAR), '') <> COALESCE(CAST(s.mis_levelqcf AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnoofindividualapplications_state AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnoofindividualapplications_state AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnoofindividualapplications AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnoofindividualapplications AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnooftwsenrollments AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnooftwsenrollments AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnooftwsenrollments_state AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnooftwsenrollments_state AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnoofapplicants AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnoofapplicants AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnooftwsenrollments_date AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnooftwsenrollments_date AS VARCHAR), '')
     OR COALESCE(CAST(b.nfc_totalnoofindividualapplications_date AS VARCHAR), '') <> COALESCE(CAST(s.nfc_totalnoofindividualapplications_date AS VARCHAR), '')
     OR COALESCE(CAST(b.statuscode AS VARCHAR), '') <> COALESCE(CAST(s.statuscode AS VARCHAR), '')
     OR COALESCE(CAST(b.exchangerate AS VARCHAR), '') <> COALESCE(CAST(s.exchangerate AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_modifiedon AS VARCHAR), '') <> COALESCE(CAST(s.mis_modifiedon AS VARCHAR), '')
     OR COALESCE(CAST(b.statecode AS VARCHAR), '') <> COALESCE(CAST(s.statecode AS VARCHAR), '')
     OR COALESCE(CAST(b.mis_createdon AS VARCHAR), '') <> COALESCE(CAST(s.mis_createdon AS VARCHAR), '')
) t

UNION ALL

-- Bronze not in Silver
SELECT
    'BRONZE_NOT_IN_SILVER',
    COUNT(*),
    NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(traininghours AS VARCHAR),
        CAST(trainingprogramtypeid AS VARCHAR),
        CAST(isallowpapers AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(is_deleted AS VARCHAR),
        CAST(tmkn_id AS VARCHAR),
        CAST(mis_name AS VARCHAR),
        CAST(mis_type AS VARCHAR),
        CAST(mis_cap AS VARCHAR),
        CAST(mis_cap_type AS VARCHAR),
        CAST(tmkn_certificatetype AS VARCHAR),
        CAST(mis_bs AS VARCHAR),
        CAST(mis_tpcs AS VARCHAR),
        CAST(mis_tws AS VARCHAR),
        CAST(tmkn_targetedlearners_employee AS VARCHAR),
        CAST(tmkn_targetedlearners_entrepreneur AS VARCHAR),
        CAST(tmkn_targetedlearners_jobseeker AS VARCHAR),
        CAST(tmkn_targetedlearners_student AS VARCHAR),
        CAST(tmkn_studytype_selfstudy AS VARCHAR),
        CAST(tmkn_studytype_online AS VARCHAR),
        CAST(tmkn_studytype_blendedlearning AS VARCHAR),
        CAST(tmkn_studytype_localtrainingprovider AS VARCHAR),
        CAST(tmkn_studytype_inhouse AS VARCHAR),
        CAST(mis_employmentrequired AS VARCHAR),
        CAST(mis_certificatestatus AS VARCHAR),
        CAST(mis_onjobtraining AS VARCHAR),
        CAST(nfc_deactivateflag AS VARCHAR),
        CAST(mis_averagecontacthours AS VARCHAR),
        CAST(nfc_noofapplicants AS VARCHAR),
        CAST(nfc_tamkeensupport AS VARCHAR),
        CAST(mis_price AS VARCHAR),
        CAST(mis_priceperhourperperson AS VARCHAR),
        CAST(tmkn_analystnote AS VARCHAR),
        CAST(mis_overview AS VARCHAR),
        CAST(mis_tamkeeneligibilitycriteria AS VARCHAR),
        CAST(mis_crt AS VARCHAR),
        CAST(mis_duration AS VARCHAR),
        CAST(mis_noofhours AS VARCHAR),
        CAST(mis_certificatewebsite AS VARCHAR),
        CAST(mis_levels AS VARCHAR),
        CAST(mis_availabilityofassessment AS VARCHAR),
        CAST(mis_payment_structure AS VARCHAR),
        CAST(mis_level AS VARCHAR),
        CAST(mis_levelqcf AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_state AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_state AS VARCHAR),
        CAST(nfc_totalnoofapplicants AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_date AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_date AS VARCHAR),
        CAST(statuscode AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(mis_modifiedon AS VARCHAR),
        CAST(statecode AS VARCHAR),
        CAST(mis_createdon AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(traininghours AS VARCHAR),
        CAST(trainingprogramtypeid AS VARCHAR),
        CAST(isallowpapers AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(is_deleted AS VARCHAR),
        CAST(tmkn_id AS VARCHAR),
        CAST(mis_name AS VARCHAR),
        CAST(mis_type AS VARCHAR),
        CAST(mis_cap AS VARCHAR),
        CAST(mis_cap_type AS VARCHAR),
        CAST(tmkn_certificatetype AS VARCHAR),
        CAST(mis_bs AS VARCHAR),
        CAST(mis_tpcs AS VARCHAR),
        CAST(mis_tws AS VARCHAR),
        CAST(tmkn_targetedlearners_employee AS VARCHAR),
        CAST(tmkn_targetedlearners_entrepreneur AS VARCHAR),
        CAST(tmkn_targetedlearners_jobseeker AS VARCHAR),
        CAST(tmkn_targetedlearners_student AS VARCHAR),
        CAST(tmkn_studytype_selfstudy AS VARCHAR),
        CAST(tmkn_studytype_online AS VARCHAR),
        CAST(tmkn_studytype_blendedlearning AS VARCHAR),
        CAST(tmkn_studytype_localtrainingprovider AS VARCHAR),
        CAST(tmkn_studytype_inhouse AS VARCHAR),
        CAST(mis_employmentrequired AS VARCHAR),
        CAST(mis_certificatestatus AS VARCHAR),
        CAST(mis_onjobtraining AS VARCHAR),
        CAST(nfc_deactivateflag AS VARCHAR),
        CAST(mis_averagecontacthours AS VARCHAR),
        CAST(nfc_noofapplicants AS VARCHAR),
        CAST(nfc_tamkeensupport AS VARCHAR),
        CAST(mis_price AS VARCHAR),
        CAST(mis_priceperhourperperson AS VARCHAR),
        CAST(tmkn_analystnote AS VARCHAR),
        CAST(mis_overview AS VARCHAR),
        CAST(mis_tamkeeneligibilitycriteria AS VARCHAR),
        CAST(mis_crt AS VARCHAR),
        CAST(mis_duration AS VARCHAR),
        CAST(mis_noofhours AS VARCHAR),
        CAST(mis_certificatewebsite AS VARCHAR),
        CAST(mis_levels AS VARCHAR),
        CAST(mis_availabilityofassessment AS VARCHAR),
        CAST(mis_payment_structure AS VARCHAR),
        CAST(mis_level AS VARCHAR),
        CAST(mis_levelqcf AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_state AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_state AS VARCHAR),
        CAST(nfc_totalnoofapplicants AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_date AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_date AS VARCHAR),
        CAST(statuscode AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(mis_modifiedon AS VARCHAR),
        CAST(statecode AS VARCHAR),
        CAST(mis_createdon AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- Silver not in Bronze
SELECT
    'SILVER_NOT_IN_BRONZE',
    COUNT(*),
    NULL
FROM (
    SELECT 
        CAST(id AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(traininghours AS VARCHAR),
        CAST(trainingprogramtypeid AS VARCHAR),
        CAST(isallowpapers AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(is_deleted AS VARCHAR),
        CAST(tmkn_id AS VARCHAR),
        CAST(mis_name AS VARCHAR),
        CAST(mis_type AS VARCHAR),
        CAST(mis_cap AS VARCHAR),
        CAST(mis_cap_type AS VARCHAR),
        CAST(tmkn_certificatetype AS VARCHAR),
        CAST(mis_bs AS VARCHAR),
        CAST(mis_tpcs AS VARCHAR),
        CAST(mis_tws AS VARCHAR),
        CAST(tmkn_targetedlearners_employee AS VARCHAR),
        CAST(tmkn_targetedlearners_entrepreneur AS VARCHAR),
        CAST(tmkn_targetedlearners_jobseeker AS VARCHAR),
        CAST(tmkn_targetedlearners_student AS VARCHAR),
        CAST(tmkn_studytype_selfstudy AS VARCHAR),
        CAST(tmkn_studytype_online AS VARCHAR),
        CAST(tmkn_studytype_blendedlearning AS VARCHAR),
        CAST(tmkn_studytype_localtrainingprovider AS VARCHAR),
        CAST(tmkn_studytype_inhouse AS VARCHAR),
        CAST(mis_employmentrequired AS VARCHAR),
        CAST(mis_certificatestatus AS VARCHAR),
        CAST(mis_onjobtraining AS VARCHAR),
        CAST(nfc_deactivateflag AS VARCHAR),
        CAST(mis_averagecontacthours AS VARCHAR),
        CAST(nfc_noofapplicants AS VARCHAR),
        CAST(nfc_tamkeensupport AS VARCHAR),
        CAST(mis_price AS VARCHAR),
        CAST(mis_priceperhourperperson AS VARCHAR),
        CAST(tmkn_analystnote AS VARCHAR),
        CAST(mis_overview AS VARCHAR),
        CAST(mis_tamkeeneligibilitycriteria AS VARCHAR),
        CAST(mis_crt AS VARCHAR),
        CAST(mis_duration AS VARCHAR),
        CAST(mis_noofhours AS VARCHAR),
        CAST(mis_certificatewebsite AS VARCHAR),
        CAST(mis_levels AS VARCHAR),
        CAST(mis_availabilityofassessment AS VARCHAR),
        CAST(mis_payment_structure AS VARCHAR),
        CAST(mis_level AS VARCHAR),
        CAST(mis_levelqcf AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_state AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_state AS VARCHAR),
        CAST(nfc_totalnoofapplicants AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_date AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_date AS VARCHAR),
        CAST(statuscode AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(mis_modifiedon AS VARCHAR),
        CAST(statecode AS VARCHAR),
        CAST(mis_createdon AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        CAST(id AS VARCHAR),
        CAST(trainingprogramid AS VARCHAR),
        CAST(traininghours AS VARCHAR),
        CAST(trainingprogramtypeid AS VARCHAR),
        CAST(isallowpapers AS VARCHAR),
        CAST(createdon AS VARCHAR),
        CAST(updatedon AS VARCHAR),
        CAST(is_deleted AS VARCHAR),
        CAST(tmkn_id AS VARCHAR),
        CAST(mis_name AS VARCHAR),
        CAST(mis_type AS VARCHAR),
        CAST(mis_cap AS VARCHAR),
        CAST(mis_cap_type AS VARCHAR),
        CAST(tmkn_certificatetype AS VARCHAR),
        CAST(mis_bs AS VARCHAR),
        CAST(mis_tpcs AS VARCHAR),
        CAST(mis_tws AS VARCHAR),
        CAST(tmkn_targetedlearners_employee AS VARCHAR),
        CAST(tmkn_targetedlearners_entrepreneur AS VARCHAR),
        CAST(tmkn_targetedlearners_jobseeker AS VARCHAR),
        CAST(tmkn_targetedlearners_student AS VARCHAR),
        CAST(tmkn_studytype_selfstudy AS VARCHAR),
        CAST(tmkn_studytype_online AS VARCHAR),
        CAST(tmkn_studytype_blendedlearning AS VARCHAR),
        CAST(tmkn_studytype_localtrainingprovider AS VARCHAR),
        CAST(tmkn_studytype_inhouse AS VARCHAR),
        CAST(mis_employmentrequired AS VARCHAR),
        CAST(mis_certificatestatus AS VARCHAR),
        CAST(mis_onjobtraining AS VARCHAR),
        CAST(nfc_deactivateflag AS VARCHAR),
        CAST(mis_averagecontacthours AS VARCHAR),
        CAST(nfc_noofapplicants AS VARCHAR),
        CAST(nfc_tamkeensupport AS VARCHAR),
        CAST(mis_price AS VARCHAR),
        CAST(mis_priceperhourperperson AS VARCHAR),
        CAST(tmkn_analystnote AS VARCHAR),
        CAST(mis_overview AS VARCHAR),
        CAST(mis_tamkeeneligibilitycriteria AS VARCHAR),
        CAST(mis_crt AS VARCHAR),
        CAST(mis_duration AS VARCHAR),
        CAST(mis_noofhours AS VARCHAR),
        CAST(mis_certificatewebsite AS VARCHAR),
        CAST(mis_levels AS VARCHAR),
        CAST(mis_availabilityofassessment AS VARCHAR),
        CAST(mis_payment_structure AS VARCHAR),
        CAST(mis_level AS VARCHAR),
        CAST(mis_levelqcf AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_state AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_state AS VARCHAR),
        CAST(nfc_totalnoofapplicants AS VARCHAR),
        CAST(nfc_totalnooftwsenrollments_date AS VARCHAR),
        CAST(nfc_totalnoofindividualapplications_date AS VARCHAR),
        CAST(statuscode AS VARCHAR),
        CAST(exchangerate AS VARCHAR),
        CAST(mis_modifiedon AS VARCHAR),
        CAST(statecode AS VARCHAR),
        CAST(mis_createdon AS VARCHAR)
    FROM bronze_layer
) t;
