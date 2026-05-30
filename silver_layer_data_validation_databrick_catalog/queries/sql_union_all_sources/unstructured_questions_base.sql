WITH
bronze_layer AS (
-- Standalone Trino SQL generated from unstructured_questions_base.sql.
-- Final column order aligned to silver_layer_query/unstructured_questions_base_silver_layer.sql.
-- Standalone Trino SQL converted from dbt model.
/*
 =================================================================================================

Name        : RPT_235_UNSTRUCTURED_QUESTIONS
Description : This model extracts and transforms unstructured question/answer data
              from multiple form instance types in the NTP application system and
              loads into the RPT_235_UNSTRUCTURED_QUESTIONS target table as part of
              the Silver Layer data pipeline.

Source Tables : tmkn_aws_landing.OSUSR_NTP_APPLICATION
                tmkn_aws_landing.OSUSR_QFS_FORMINSTANCE
                tmkn_aws_landing.OSUSR_QFS_FORM
                tmkn_aws_landing.OSUSR_QFS_SECTION
                tmkn_aws_landing.OSUSR_QFS_SECTIONFIELD
                tmkn_aws_landing.OSUSR_QFS_FIELD
                tmkn_aws_landing.OSUSR_QFS_FIELDVALUE

Target Table : RPT_235_UNSTRUCTURED_QUESTIONS
Load Type    : Incremental Load
Materialized : incremental
Format       : PARQUET
Tags         : tmkn, daily, reporting

Revision History:
--------------------------------------------------------------

Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-12 | DBT     | Initial version - converted from stored procedure

================================================================================================= 
*/
WITH cte_analysis_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'ANALYSIS' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.ANALYSISINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_application_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'APPLICATION' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.APPLICATIONINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_customer_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'CUSTOMER' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.CUSTOMERINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_evc_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'EVC' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.EVCINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_findata_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'FINDATA' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.FINDATAINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_grantcalc_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'GRANTCALC' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.GRANTCALCINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_hipo_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'HIPO' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.HIPOINSTANCEFORMGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_profiling_form AS (

    SELECT
        CAST(current_timestamp() + INTERVAL '3' HOUR AS DATE)      AS EXTRACT_DATE,
        APP.ID AS ID_APPLICATION,
        APP.GUID AS ID_GUID,
        APP.REFERENCENUMBER AS APPLICATION_NO,
        FIELD.NAME AS QUESTION,
        FIELDVALUE.VALUETEXT AS ANSWER_TEXT,
        FIELDVALUE.VALUEBOOLEAN AS ANSWER_BOOLEAN,
        FIELDVALUE.VALUEDATE AS ANSWER_DATE,
        FIELDVALUE.VALUENUMBER AS ANSWER_NUMBER,
        FIELD.CODE AS FIELD_CODE,
        SECTION.ID AS ID_SECTION,
        SECTION.FORMID AS FORMID_SECTION,
        SECTION.NAME AS NAME_SECTION,
        SECTION.DESCRIPTION AS DESCRIPTION_SECTION,
        SECTION.ISVISIBLE AS ISVISIBLE_SECTION,
        SECTION.WEIGHT AS WEIGHT_SECTION,

        SECFIELD.ID AS ID_SECFIELD,
        SECFIELD.SECTIONID AS SECTIONID_SECFIELD,
        SECFIELD.FIELDID AS FIELDID_SECFIELD,
        SECFIELD.ROWPOSITION,
        SECFIELD.COLUMNPOSITION,
        SECFIELD.NUMBERCOLUMNS,
        SECFIELD.ISVISIBLE AS ISVISIBLE_SECFIELD,
        SECFIELD.ISENABLED AS ISENABLED_SECFIELD,
        SECFIELD.ISMANDATORY AS ISMANDATORY_SECFIELD,
        SECFIELD.WEIGHT AS WEIGHT_SECFIELD,
        SECFIELD.HASMANUALVERIFICATION,

        FORMINST.ID AS ID_FORMINST,
        FORMINST.FORMID AS FORMID_FORMINST,
        FORMINST.ISVALID AS ISVALID_FORMINST,

        FORM.ID AS ID_FORM,
        FORM.DOMAINID AS DOMAINID_FORM,
        FORM.NAME AS NAME_FORM,
        FORM.DESCRIPTION AS DESCRIPTION_FORM,
        FORM.BASEURL,
        FORM.URLPATH,
        FORM.ISACTIVE AS ISACTIVE_FORM,

        FIELDVALUE.ISVALID AS ISVALID_FIELDVALUE,
        FIELDVALUE.ISMANUALVERIFIED AS ISMANUALVERIFIED_FIELDVALUE,

        FIELD.ID AS ID_FIELD,
        FIELD.ISACTIVE AS ISACTIVE_FIELD,
        FIELD.DOMAINID AS DOMAINID_FIELD,
        FIELD.DATATYPEID,
        FIELD.FIELDINPUTTYPEID,
        FIELD.DESCRIPTION AS DESCRIPTION_FIELD,
        FIELD.REGEX,
        FIELD.DEFAULTTEXT,
        FIELD.MINIMUMTEXTLENGTH,
        FIELD.MAXIMUMTEXTLENGTH,
        FIELD.DEFAULTDATE,
        FIELD.MINIMUMDATE,
        FIELD.MAXIMUMDATE,
        FIELD.GROUPSEPARATOR,
        FIELD.DECIMALSEPARATOR,
        FIELD.DECIMALPLACES,
        FIELD.DEFAULTNUMBER,
        FIELD.MINIMUMNUMBER,
        FIELD.MAXIMUMNUMBER,
        FIELD.EXPRESSION
        --'PROFILING' AS FORM_TYPE

    FROM  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_NTP_APPLICATION AS APP
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORMINSTANCE AS FORMINST 
        ON FORMINST.GUID = APP.PROFILINGINSTANCEGUID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FORM AS FORM 
        ON FORM.ID = FORMINST.FORMID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTION AS SECTION 
        ON SECTION.FORMID= FORM.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_SECTIONFIELD AS SECFIELD 
        ON SECFIELD.SECTIONID = SECTION.ID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELD AS FIELD 
        ON FIELD.ID = SECFIELD.FIELDID
    INNER JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_QFS_FIELDVALUE AS FIELDVALUE 
        ON FIELDVALUE.FORMINSTANCEID = FORMINST.ID 
        AND FIELDVALUE.SECTIONFIELDID = SECFIELD.ID

),

cte_combined AS (

    SELECT * FROM cte_analysis_form
    UNION ALL
    SELECT * FROM cte_application_form
    UNION ALL
    SELECT * FROM cte_customer_form
    UNION ALL
    SELECT * FROM cte_evc_form
    UNION ALL
    SELECT * FROM cte_findata_form
    UNION ALL
    SELECT * FROM cte_grantcalc_form
    UNION ALL
    SELECT * FROM cte_hipo_form
    UNION ALL
    SELECT * FROM cte_profiling_form

),

cte_filtered AS (

    SELECT
        EXTRACT_DATE,
        ID_APPLICATION,
        ID_GUID,
        APPLICATION_NO,

        QUESTION,
        ANSWER_TEXT,
        ANSWER_BOOLEAN,
        ANSWER_DATE,
        ANSWER_NUMBER,
        FIELD_CODE,

        ID_SECTION,
        FORMID_SECTION,
        NAME_SECTION,
        DESCRIPTION_SECTION,
        ISVISIBLE_SECTION,
        WEIGHT_SECTION,

        ID_SECFIELD,
        SECTIONID_SECFIELD,
        FIELDID_SECFIELD,
        ROWPOSITION,
        COLUMNPOSITION,
        NUMBERCOLUMNS,
        ISVISIBLE_SECFIELD,
        ISENABLED_SECFIELD,
        ISMANDATORY_SECFIELD,
        WEIGHT_SECFIELD,
        HASMANUALVERIFICATION,

        ID_FORMINST,
        FORMID_FORMINST,
        ISVALID_FORMINST,

        ID_FORM,
        DOMAINID_FORM,
        NAME_FORM,
        DESCRIPTION_FORM,
        BASEURL,
        URLPATH,
        ISACTIVE_FORM,

        ISVALID_FIELDVALUE,
        ISMANUALVERIFIED_FIELDVALUE,

        ID_FIELD,
        ISACTIVE_FIELD,
        DOMAINID_FIELD,
        DATATYPEID,
        FIELDINPUTTYPEID,
        DESCRIPTION_FIELD,
        REGEX,
        DEFAULTTEXT,
        MINIMUMTEXTLENGTH,
        MAXIMUMTEXTLENGTH,
        DEFAULTDATE,
        MINIMUMDATE,
        MAXIMUMDATE,
        GROUPSEPARATOR,
        DECIMALSEPARATOR,
        DECIMALPLACES,
        DEFAULTNUMBER,
        MINIMUMNUMBER,
        MAXIMUMNUMBER,
        EXPRESSION,
		FALSE                                                   AS IS_DELETED,
        'NEO2'                                                  AS SOURCE_SYSTEM_NAME,
		CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS DBT_UPDATED_AT
        --BRONZE_CREATED_ON,
        --BRONZE_UPDATED_ON

    FROM cte_combined

    WHERE FIELD_CODE IN (
        'INT.ENT.17', 'INT.ENT.17-Old', 'INT.ENT.17-Old-Old',
        'INT.ENT.18', 'INT.ENT.18-Old', 'INT.ENT.18-Old-Old',
        'INT.ENT.19', 'INT.ENT.19-Old', 'INT.ENT.19-Old-Old',
        'INT.ENT.15', 'INT.ENT.15-Old', 'INT.ENT.15-Old-Old',
        'EXT.ENT.Sector', 'EXT.ENT.Sector-Old', 'EXT.ENT.Sector-Old-Old',
        'EXT.HC.WorkArrangement', 'EXT.HC.WorkArrangement-Old', 'EXT.HC.WorkArrangement-Old-Old',
        'EXT.ENT.InOperation', 'EXT.ENT.InOperation-Old', 'EXT.ENT.InOperation-Old-Old',
        'EXT.ENT.Sector.Other', 'EXT.ENT.Sector.Other-Old', 'EXT.ENT.Sector.Other-Old-Old',
        'AS.HIPO.SECTOR1', 'AS.HIPO.SECTOR1-Old', 'AS.HIPO.SECTOR1-Old-Old',
        'AS.HIPO.COMPANY', 'AS.HIPO.COMPANY-Old', 'AS.HIPO.COMPANY-Old-Old',
        'INT.ENT.Target.Date', 'INT.ENT.Target.Date-Old', 'INT.ENT.Target.Date-Old-Old',
        'INT.ENT.Scalable', 'INT.ENT.Scalable-Old', 'INT.ENT.Scalable-Old-Old',
        'INT.ENT.IPs', 'INT.ENT.IPs-Old', 'INT.ENT.IPs-Old-Old',
        'INT.ENT.Main.Obj', 'INT.ENT.Main.Obj-Old', 'INT.ENT.Main.Obj-Old-Old',
        'INT.ENT.Detailed.Obj', 'INT.ENT.Detailed.Obj-Old', 'INT.ENT.Detailed.Obj-Old-Old',
        'INT.ENT.Prod.Inno', 'INT.ENT.Prod.Inno-Old', 'INT.ENT.Prod.Inno-Old-Old',
        'INT.ENT.20', 'INT.ENT.20-Old', 'INT.ENT.20-Old-Old',
        'Profile02.a', 'Profile02.a-Old', 'Profile02.a-Old-Old',
        'Profile.NEP', 'Profile.NEP-Old', 'Profile.NEP-Old-Old'
    )

)

SELECT
    TRY_CAST(NULLIF(CAST(EXTRACT_DATE AS STRING), '') AS DATE) AS extract_date,
    TRY_CAST(NULLIF(CAST(ID_APPLICATION AS STRING), '') AS BIGINT) AS id_application,
    ID_GUID AS id_guid,
    APPLICATION_NO AS application_no,
    QUESTION AS question,
    ANSWER_TEXT AS answer_text,
    ANSWER_BOOLEAN AS answer_boolean,
    TRY_CAST(NULLIF(CAST(ANSWER_DATE AS STRING), '') AS TIMESTAMP) AS answer_date,
    TRY_CAST(NULLIF(CAST(ANSWER_NUMBER AS STRING), '') AS BIGINT) AS answer_number,
    FIELD_CODE AS field_code,
    TRY_CAST(NULLIF(CAST(ID_SECTION AS STRING), '') AS BIGINT) AS id_section,
    TRY_CAST(NULLIF(CAST(FORMID_SECTION AS STRING), '') AS BIGINT) AS formid_section,
    NAME_SECTION AS name_section,
    DESCRIPTION_SECTION AS description_section,
    ISVISIBLE_SECTION AS isvisible_section,
    TRY_CAST(NULLIF(CAST(WEIGHT_SECTION AS STRING), '') AS BIGINT) AS weight_section,
    TRY_CAST(NULLIF(CAST(ID_SECFIELD AS STRING), '') AS BIGINT) AS id_secfield,
    TRY_CAST(NULLIF(CAST(SECTIONID_SECFIELD AS STRING), '') AS BIGINT) AS sectionid_secfield,
    TRY_CAST(NULLIF(CAST(FIELDID_SECFIELD AS STRING), '') AS BIGINT) AS fieldid_secfield,
    TRY_CAST(NULLIF(CAST(ROWPOSITION AS STRING), '') AS BIGINT) AS rowposition,
    TRY_CAST(NULLIF(CAST(COLUMNPOSITION AS STRING), '') AS BIGINT) AS columnposition,
    TRY_CAST(NULLIF(CAST(NUMBERCOLUMNS AS STRING), '') AS BIGINT) AS numbercolumns,
    ISVISIBLE_SECFIELD AS isvisible_secfield,
    ISENABLED_SECFIELD AS isenabled_secfield,
    ISMANDATORY_SECFIELD AS ismandatory_secfield,
    TRY_CAST(NULLIF(CAST(WEIGHT_SECFIELD AS STRING), '') AS BIGINT) AS weight_secfield,
    HASMANUALVERIFICATION AS hasmanualverification,
    TRY_CAST(NULLIF(CAST(ID_FORMINST AS STRING), '') AS BIGINT) AS id_forminst,
    TRY_CAST(NULLIF(CAST(FORMID_FORMINST AS STRING), '') AS BIGINT) AS formid_forminst,
    ISVALID_FORMINST AS isvalid_forminst,
    TRY_CAST(NULLIF(CAST(ID_FORM AS STRING), '') AS BIGINT) AS id_form,
    TRY_CAST(NULLIF(CAST(DOMAINID_FORM AS STRING), '') AS BIGINT) AS domainid_form,
    NAME_FORM AS name_form,
    DESCRIPTION_FORM AS description_form,
    BASEURL AS baseurl,
    URLPATH AS urlpath,
    ISACTIVE_FORM AS isactive_form,
    ISVALID_FIELDVALUE AS isvalid_fieldvalue,
    ISMANUALVERIFIED_FIELDVALUE AS ismanualverified_fieldvalue,
    TRY_CAST(NULLIF(CAST(ID_FIELD AS STRING), '') AS BIGINT) AS id_field,
    ISACTIVE_FIELD AS isactive_field,
    TRY_CAST(NULLIF(CAST(DOMAINID_FIELD AS STRING), '') AS BIGINT) AS domainid_field,
    DATATYPEID AS datatypeid,
    FIELDINPUTTYPEID AS fieldinputtypeid,
    DESCRIPTION_FIELD AS description_field,
    REGEX AS regex,
    DEFAULTTEXT AS defaulttext,
    TRY_CAST(NULLIF(CAST(MINIMUMTEXTLENGTH AS STRING), '') AS BIGINT) AS minimumtextlength,
    TRY_CAST(NULLIF(CAST(MAXIMUMTEXTLENGTH AS STRING), '') AS BIGINT) AS maximumtextlength,
    TRY_CAST(NULLIF(CAST(DEFAULTDATE AS STRING), '') AS TIMESTAMP) AS defaultdate,
    TRY_CAST(NULLIF(CAST(MINIMUMDATE AS STRING), '') AS TIMESTAMP) AS minimumdate,
    TRY_CAST(NULLIF(CAST(MAXIMUMDATE AS STRING), '') AS TIMESTAMP) AS maximumdate,
    GROUPSEPARATOR AS groupseparator,
    DECIMALSEPARATOR AS decimalseparator,
    TRY_CAST(NULLIF(CAST(DECIMALPLACES AS STRING), '') AS BIGINT) AS decimalplaces,
    TRY_CAST(NULLIF(CAST(DEFAULTNUMBER AS STRING), '') AS BIGINT) AS defaultnumber,
    TRY_CAST(NULLIF(CAST(MINIMUMNUMBER AS STRING), '') AS BIGINT) AS minimumnumber,
    TRY_CAST(NULLIF(CAST(MAXIMUMNUMBER AS STRING), '') AS BIGINT) AS maximumnumber,
    EXPRESSION AS expression,
    IS_DELETED AS is_deleted,
    UPPER(NULLIF(TRIM(CAST(SOURCE_SYSTEM_NAME AS STRING)), '')) AS source_system_name,
    TRY_CAST(NULLIF(CAST(DBT_UPDATED_AT AS STRING), '') AS TIMESTAMP) AS dbt_updated_at
FROM cte_filtered
),

silver_layer AS (
SELECT
    extract_date,
    id_application,
    id_guid,
    application_no,
    question,
    answer_text,
    answer_boolean,
    answer_date,
    answer_number,
    field_code,
    id_section,
    formid_section,
    name_section,
    description_section,
    isvisible_section,
    weight_section,
    id_secfield,
    sectionid_secfield,
    fieldid_secfield,
    rowposition,
    columnposition,
    numbercolumns,
    isvisible_secfield,
    isenabled_secfield,
    ismandatory_secfield,
    weight_secfield,
    hasmanualverification,
    id_forminst,
    formid_forminst,
    isvalid_forminst,
    id_form,
    domainid_form,
    name_form,
    description_form,
    baseurl,
    urlpath,
    isactive_form,
    isvalid_fieldvalue,
    ismanualverified_fieldvalue,
    id_field,
    isactive_field,
    domainid_field,
    datatypeid,
    fieldinputtypeid,
    description_field,
    regex,
    defaulttext,
    minimumtextlength,
    maximumtextlength,
    defaultdate,
    minimumdate,
    maximumdate,
    groupseparator,
    decimalseparator,
    decimalplaces,
    defaultnumber,
    minimumnumber,
    maximumnumber,
    expression,
    is_deleted,
    source_system_name,
    dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.unstructured_questions_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'id_application'),
        (3, 'id_guid'),
        (4, 'application_no'),
        (5, 'question'),
        (6, 'answer_text'),
        (7, 'answer_boolean'),
        (8, 'answer_date'),
        (9, 'answer_number'),
        (10, 'field_code'),
        (11, 'id_section'),
        (12, 'formid_section'),
        (13, 'name_section'),
        (14, 'description_section'),
        (15, 'isvisible_section'),
        (16, 'weight_section'),
        (17, 'id_secfield'),
        (18, 'sectionid_secfield'),
        (19, 'fieldid_secfield'),
        (20, 'rowposition'),
        (21, 'columnposition'),
        (22, 'numbercolumns'),
        (23, 'isvisible_secfield'),
        (24, 'isenabled_secfield'),
        (25, 'ismandatory_secfield'),
        (26, 'weight_secfield'),
        (27, 'hasmanualverification'),
        (28, 'id_forminst'),
        (29, 'formid_forminst'),
        (30, 'isvalid_forminst'),
        (31, 'id_form'),
        (32, 'domainid_form'),
        (33, 'name_form'),
        (34, 'description_form'),
        (35, 'baseurl'),
        (36, 'urlpath'),
        (37, 'isactive_form'),
        (38, 'isvalid_fieldvalue'),
        (39, 'ismanualverified_fieldvalue'),
        (40, 'id_field'),
        (41, 'isactive_field'),
        (42, 'domainid_field'),
        (43, 'datatypeid'),
        (44, 'fieldinputtypeid'),
        (45, 'description_field'),
        (46, 'regex'),
        (47, 'defaulttext'),
        (48, 'minimumtextlength'),
        (49, 'maximumtextlength'),
        (50, 'defaultdate'),
        (51, 'minimumdate'),
        (52, 'maximumdate'),
        (53, 'groupseparator'),
        (54, 'decimalseparator'),
        (55, 'decimalplaces'),
        (56, 'defaultnumber'),
        (57, 'minimumnumber'),
        (58, 'maximumnumber'),
        (59, 'expression'),
        (60, 'is_deleted'),
        (61, 'source_system_name'),
        (62, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'extract_date'),
        (2, 'id_application'),
        (3, 'id_guid'),
        (4, 'application_no'),
        (5, 'question'),
        (6, 'answer_text'),
        (7, 'answer_boolean'),
        (8, 'answer_date'),
        (9, 'answer_number'),
        (10, 'field_code'),
        (11, 'id_section'),
        (12, 'formid_section'),
        (13, 'name_section'),
        (14, 'description_section'),
        (15, 'isvisible_section'),
        (16, 'weight_section'),
        (17, 'id_secfield'),
        (18, 'sectionid_secfield'),
        (19, 'fieldid_secfield'),
        (20, 'rowposition'),
        (21, 'columnposition'),
        (22, 'numbercolumns'),
        (23, 'isvisible_secfield'),
        (24, 'isenabled_secfield'),
        (25, 'ismandatory_secfield'),
        (26, 'weight_secfield'),
        (27, 'hasmanualverification'),
        (28, 'id_forminst'),
        (29, 'formid_forminst'),
        (30, 'isvalid_forminst'),
        (31, 'id_form'),
        (32, 'domainid_form'),
        (33, 'name_form'),
        (34, 'description_form'),
        (35, 'baseurl'),
        (36, 'urlpath'),
        (37, 'isactive_form'),
        (38, 'isvalid_fieldvalue'),
        (39, 'ismanualverified_fieldvalue'),
        (40, 'id_field'),
        (41, 'isactive_field'),
        (42, 'domainid_field'),
        (43, 'datatypeid'),
        (44, 'fieldinputtypeid'),
        (45, 'description_field'),
        (46, 'regex'),
        (47, 'defaulttext'),
        (48, 'minimumtextlength'),
        (49, 'maximumtextlength'),
        (50, 'defaultdate'),
        (51, 'minimumdate'),
        (52, 'maximumdate'),
        (53, 'groupseparator'),
        (54, 'decimalseparator'),
        (55, 'decimalplaces'),
        (56, 'defaultnumber'),
        (57, 'minimumnumber'),
        (58, 'maximumnumber'),
        (59, 'expression'),
        (60, 'is_deleted'),
        (61, 'source_system_name'),
        (62, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`id_application` AS STRING) AS `id_application`,
        CAST(`id_guid` AS STRING) AS `id_guid`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`question` AS STRING) AS `question`,
        CAST(`answer_text` AS STRING) AS `answer_text`,
        CAST(`answer_boolean` AS STRING) AS `answer_boolean`,
        CAST(`answer_date` AS STRING) AS `answer_date`,
        CAST(`answer_number` AS STRING) AS `answer_number`,
        CAST(`field_code` AS STRING) AS `field_code`,
        CAST(`id_section` AS STRING) AS `id_section`,
        CAST(`formid_section` AS STRING) AS `formid_section`,
        CAST(`name_section` AS STRING) AS `name_section`,
        CAST(`description_section` AS STRING) AS `description_section`,
        CAST(`isvisible_section` AS STRING) AS `isvisible_section`,
        CAST(`weight_section` AS STRING) AS `weight_section`,
        CAST(`id_secfield` AS STRING) AS `id_secfield`,
        CAST(`sectionid_secfield` AS STRING) AS `sectionid_secfield`,
        CAST(`fieldid_secfield` AS STRING) AS `fieldid_secfield`,
        CAST(`rowposition` AS STRING) AS `rowposition`,
        CAST(`columnposition` AS STRING) AS `columnposition`,
        CAST(`numbercolumns` AS STRING) AS `numbercolumns`,
        CAST(`isvisible_secfield` AS STRING) AS `isvisible_secfield`,
        CAST(`isenabled_secfield` AS STRING) AS `isenabled_secfield`,
        CAST(`ismandatory_secfield` AS STRING) AS `ismandatory_secfield`,
        CAST(`weight_secfield` AS STRING) AS `weight_secfield`,
        CAST(`hasmanualverification` AS STRING) AS `hasmanualverification`,
        CAST(`id_forminst` AS STRING) AS `id_forminst`,
        CAST(`formid_forminst` AS STRING) AS `formid_forminst`,
        CAST(`isvalid_forminst` AS STRING) AS `isvalid_forminst`,
        CAST(`id_form` AS STRING) AS `id_form`,
        CAST(`domainid_form` AS STRING) AS `domainid_form`,
        CAST(`name_form` AS STRING) AS `name_form`,
        CAST(`description_form` AS STRING) AS `description_form`,
        CAST(`baseurl` AS STRING) AS `baseurl`,
        CAST(`urlpath` AS STRING) AS `urlpath`,
        CAST(`isactive_form` AS STRING) AS `isactive_form`,
        CAST(`isvalid_fieldvalue` AS STRING) AS `isvalid_fieldvalue`,
        CAST(`ismanualverified_fieldvalue` AS STRING) AS `ismanualverified_fieldvalue`,
        CAST(`id_field` AS STRING) AS `id_field`,
        CAST(`isactive_field` AS STRING) AS `isactive_field`,
        CAST(`domainid_field` AS STRING) AS `domainid_field`,
        CAST(`datatypeid` AS STRING) AS `datatypeid`,
        CAST(`fieldinputtypeid` AS STRING) AS `fieldinputtypeid`,
        CAST(`description_field` AS STRING) AS `description_field`,
        CAST(`regex` AS STRING) AS `regex`,
        CAST(`defaulttext` AS STRING) AS `defaulttext`,
        CAST(`minimumtextlength` AS STRING) AS `minimumtextlength`,
        CAST(`maximumtextlength` AS STRING) AS `maximumtextlength`,
        CAST(`defaultdate` AS STRING) AS `defaultdate`,
        CAST(`minimumdate` AS STRING) AS `minimumdate`,
        CAST(`maximumdate` AS STRING) AS `maximumdate`,
        CAST(`groupseparator` AS STRING) AS `groupseparator`,
        CAST(`decimalseparator` AS STRING) AS `decimalseparator`,
        CAST(`decimalplaces` AS STRING) AS `decimalplaces`,
        CAST(`defaultnumber` AS STRING) AS `defaultnumber`,
        CAST(`minimumnumber` AS STRING) AS `minimumnumber`,
        CAST(`maximumnumber` AS STRING) AS `maximumnumber`,
        CAST(`expression` AS STRING) AS `expression`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`id_application` AS STRING) AS `id_application`,
        CAST(`id_guid` AS STRING) AS `id_guid`,
        CAST(`application_no` AS STRING) AS `application_no`,
        CAST(`question` AS STRING) AS `question`,
        CAST(`answer_text` AS STRING) AS `answer_text`,
        CAST(`answer_boolean` AS STRING) AS `answer_boolean`,
        CAST(`answer_date` AS STRING) AS `answer_date`,
        CAST(`answer_number` AS STRING) AS `answer_number`,
        CAST(`field_code` AS STRING) AS `field_code`,
        CAST(`id_section` AS STRING) AS `id_section`,
        CAST(`formid_section` AS STRING) AS `formid_section`,
        CAST(`name_section` AS STRING) AS `name_section`,
        CAST(`description_section` AS STRING) AS `description_section`,
        CAST(`isvisible_section` AS STRING) AS `isvisible_section`,
        CAST(`weight_section` AS STRING) AS `weight_section`,
        CAST(`id_secfield` AS STRING) AS `id_secfield`,
        CAST(`sectionid_secfield` AS STRING) AS `sectionid_secfield`,
        CAST(`fieldid_secfield` AS STRING) AS `fieldid_secfield`,
        CAST(`rowposition` AS STRING) AS `rowposition`,
        CAST(`columnposition` AS STRING) AS `columnposition`,
        CAST(`numbercolumns` AS STRING) AS `numbercolumns`,
        CAST(`isvisible_secfield` AS STRING) AS `isvisible_secfield`,
        CAST(`isenabled_secfield` AS STRING) AS `isenabled_secfield`,
        CAST(`ismandatory_secfield` AS STRING) AS `ismandatory_secfield`,
        CAST(`weight_secfield` AS STRING) AS `weight_secfield`,
        CAST(`hasmanualverification` AS STRING) AS `hasmanualverification`,
        CAST(`id_forminst` AS STRING) AS `id_forminst`,
        CAST(`formid_forminst` AS STRING) AS `formid_forminst`,
        CAST(`isvalid_forminst` AS STRING) AS `isvalid_forminst`,
        CAST(`id_form` AS STRING) AS `id_form`,
        CAST(`domainid_form` AS STRING) AS `domainid_form`,
        CAST(`name_form` AS STRING) AS `name_form`,
        CAST(`description_form` AS STRING) AS `description_form`,
        CAST(`baseurl` AS STRING) AS `baseurl`,
        CAST(`urlpath` AS STRING) AS `urlpath`,
        CAST(`isactive_form` AS STRING) AS `isactive_form`,
        CAST(`isvalid_fieldvalue` AS STRING) AS `isvalid_fieldvalue`,
        CAST(`ismanualverified_fieldvalue` AS STRING) AS `ismanualverified_fieldvalue`,
        CAST(`id_field` AS STRING) AS `id_field`,
        CAST(`isactive_field` AS STRING) AS `isactive_field`,
        CAST(`domainid_field` AS STRING) AS `domainid_field`,
        CAST(`datatypeid` AS STRING) AS `datatypeid`,
        CAST(`fieldinputtypeid` AS STRING) AS `fieldinputtypeid`,
        CAST(`description_field` AS STRING) AS `description_field`,
        CAST(`regex` AS STRING) AS `regex`,
        CAST(`defaulttext` AS STRING) AS `defaulttext`,
        CAST(`minimumtextlength` AS STRING) AS `minimumtextlength`,
        CAST(`maximumtextlength` AS STRING) AS `maximumtextlength`,
        CAST(`defaultdate` AS STRING) AS `defaultdate`,
        CAST(`minimumdate` AS STRING) AS `minimumdate`,
        CAST(`maximumdate` AS STRING) AS `maximumdate`,
        CAST(`groupseparator` AS STRING) AS `groupseparator`,
        CAST(`decimalseparator` AS STRING) AS `decimalseparator`,
        CAST(`decimalplaces` AS STRING) AS `decimalplaces`,
        CAST(`defaultnumber` AS STRING) AS `defaultnumber`,
        CAST(`minimumnumber` AS STRING) AS `minimumnumber`,
        CAST(`maximumnumber` AS STRING) AS `maximumnumber`,
        CAST(`expression` AS STRING) AS `expression`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
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
        'unstructured_questions_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'unstructured_questions_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'unstructured_questions_base' AS table_name,
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
        'unstructured_questions_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'unstructured_questions_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
