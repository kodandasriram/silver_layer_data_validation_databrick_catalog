-- Compare bronze-layer query output with silver-layer table output for assessment_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\Silver_layer_03-June-2026\converted db script to databricks\Databricks_union of all sources\assessment_base.sql
-- Silver source: converted db script to databricks\silver_layer scripts\assessment_base_silver_layer.sql

WITH
bronze_layer AS (
/*
Generated Databricks union layer for assessment_base.
Column order and typed NULL placeholders follow dbt model: assessment_base.sql.
Source transformations are embedded whole from the converted Databricks OS1/OS2/MIS scripts.
dbt macros expanded to Databricks TRY_CAST / string cleanup expressions.
*/

WITH
    assessment_base_os2 AS (
/* =============================================================================
   Name          : ASSESSMENT_BASE
   Description   : This model extracts and transforms assessment-level data
                   from the NEO2 (OS2) Bronze Layer and loads it into the
                   ASSESSMENT_BASE target table as part of the Silver Layer
                   data pipeline.

                   The model captures assessment workflow and operational
                   details related to applications, including assessment,
                   review, approval, and monitoring roles and teams.

                   The model enriches assessment data by joining reference
                   tables such as assessment status and team master tables
                   to retrieve descriptive team names and status labels.

                   The model also applies data cleansing, timestamp
                   standardization, and deduplication logic to ensure only
                   the latest version of each assessment record is retained.

   Source Tables : neo2.OSUSR_1AT_ASSESSMENT
                   neo2.OSUSR_1AT_ASSESSMENTSTATUS
                   neo2.OSUSR_KUO_TEAM

   Target Table  : ASSESSMENT_BASE

   Load Type     : Full Load
   Materialized  : Table
   Format        : PARQUET
   Tags          : neo2, daily

   Business Rules:
   ---------------------------------------------------------------------------
   1. Latest assessment record is retained using deduplication logic:
        - Partition By : ass.ID
        - Order By     : ass.UPDATEDON DESC,
                         ass.CREATEDON DESC

   2. Team names are enriched from OSUSR_KUO_TEAM:
        - Assessment Team 1
        - Assessment Team 2
        - Review Team 1
        - Approve Team 1
        - Assessment Team MOL

   3. Assessment status description is enriched from:
        - OSUSR_1AT_ASSESSMENTSTATUS

   4. String cleansing is applied using:
        - clean_string()
        - clean_string_upper()

   5. Timestamp fields are standardized using:
        - safe_cast_timestamp()

   6. Source system is hardcoded as:
        - 'NEO2'

   Revision History:
   ---------------------------------------------------------------------------
   Version  | Date         | Author        | Description
   ---------------------------------------------------------------------------
   1.0      | 2026-05-12   | siva       | Initial Development
   ---------------------------------------------------------------------------
============================================================================= */


with CTE_OSUSR_1AT_ASSESSMENT AS
(
SELECT
        ass.id,
        ass.applicationid,
        ass.amendmentrequestid,
        ass.assessmentrole1,
        ass.assessmentrole2,
        ass.reviewrole,
        ass.approverole,
        ass.processid,
        KT1.name as assessmentteam1_name,
        KT2.name as assessmentteam2_name,
        KT3.name as reviewteam1_name,
        KT4.name as approveteam1_name,
        assessmentstatus.LABEL as assessmentstatusid,
        ass.assessmentrolemol,
        KT5.name as assessmentteammol_name,
        ass.reviewrole1,
        ass.reviewrole2,
        ass.reviewteam2,
        ass.monitoringrole1,
        ass.monitoringrole2,
        ass.monitoringteam1,
        ass.monitoringteam2,
        FALSE as is_deleted,
        'NEO2' AS source_system_name,
        ass.updatedon,
        ass.createdon,
        cast(to_utc_timestamp(current_timestamp(), current_timezone()) AS timestamp) AS dbt_updated_at,
        ROW_NUMBER() OVER (

    PARTITION BY ass.id

    ORDER BY ass.updatedon DESC, ass.createdon DESC

  ) AS rnk
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_1AT_ASSESSMENT` ass

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_1AT_ASSESSMENTSTATUS`  assessmentstatus
        ON ass.ASSESSMENTSTATUSID = assessmentstatus.CODE
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_KUO_TEAM` KT1
        ON  ass.ASSESSMENTTEAM1 = KT1.ID  
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_KUO_TEAM` KT2
        ON  ass.ASSESSMENTTEAM2 = KT2.ID 
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_KUO_TEAM` KT3
        ON  ass.REVIEWTEAM1 = KT3.ID 
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_KUO_TEAM` KT4
        ON  ass.APPROVETEAM1 = KT4.ID 
    LEFT join `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_KUO_TEAM` KT5
        ON  ass.ASSESSMENTTEAMMOL = KT5.ID 
)
SELECT id,
       applicationid,
       amendmentrequestid,
       assessmentrole1,
       assessmentrole2,
       reviewrole,
       approverole,
       processid,
       NULLIF(TRIM(assessmentteam1_name), '') AS assessmentteam1_name,
       NULLIF(TRIM(assessmentteam2_name), '') AS assessmentteam2_name,
       NULLIF(TRIM(reviewteam1_name), '') AS reviewteam1_name,
       NULLIF(TRIM(approveteam1_name), '') AS approveteam1_name,
       assessmentstatusid,
       assessmentrolemol,
       assessmentteammol_name,
       reviewrole1,
       reviewrole2,
       reviewteam2,
       monitoringrole1,
       monitoringrole2,
       monitoringteam1,
       monitoringteam2,
       is_deleted,
       UPPER(NULLIF(TRIM(SOURCE_SYSTEM_NAME), '')) AS source_system_name,
       TRY_CAST(UPDATEDON AS TIMESTAMP) AS updatedon,
       TRY_CAST(CREATEDON AS TIMESTAMP) AS createdon,
       TRY_CAST(DBT_UPDATED_AT AS TIMESTAMP) AS dbt_updated_at
FROM CTE_OSUSR_1AT_ASSESSMENT ass
WHERE rnk = 1
),
    assessment_base_mis AS (
/*
============================================================================
silver_assessment_mis.sql
============================================================================
Per-source intermediate Silver model for the Assessment domain â€” MIS only.

Sources (Assessment domain entities):
  â˜… tmkn_sitevisit       â€” physical site visit records
  â˜… tmkn_virtualvisit    â€” virtual visit records (linked to a site visit)
  â˜… tmkn_esmonitoring    â€” ES (Enterprise Support) monitoring assessments

Reference SPs:
  - RPT-038_ES_Site_Visits      (anchor on tmkn_sitevisit)
  - RPT-039_Virtual_Visits      (anchor on tmkn_virtualvisit, joined to sitevisit)
  - RPT-037_ES_Monitoring       (anchor on tmkn_esmonitoring)
  - RPT-034_ES_Payment_Request  (uses tmkn_sitevisit as a reference)

Structure decision:
  - Three parallel anchor entities â€” UNIONed (not joined together).
  - Site visits and virtual visits are conceptually related (virtual visit
    references its parent site visit), but we keep them as separate UNION
    branches to preserve their independent lifecycles. The site_visit_id FK
    is preserved on virtual visit rows for downstream re-joining if needed.
  - tmkn_esmonitoring is a separate type of assessment (financial monitoring)
    and shares no structural overlap with site/virtual visits â€” natural UNION.

Cross-domain note: RPT-037, RPT-038, and RPT-039 all join to tmkn_application
and tmkn_company. Those joins are NOT performed here â€” they belong in the
Application and Customer Enterprise domains. Application/Company FKs are
preserved here for downstream re-joining.

The assessment_subtype column identifies which sub-type each row is:
  - SITE_VISIT
  - VIRTUAL_VISIT
  - ES_MONITORING
============================================================================
*/


-- ============================================================================
-- Pre-aggregated site visit / monitoring status history (replaces SP cursor)
-- ============================================================================
WITH sv_status_history AS (
    SELECT
        sh.tmkn_ref                                      AS reference_id,
        sh.tmkn_StatusReport                             AS status_report_id,
        COUNT(sh.tmkn_svshid)                            AS occurrence_count,
        MIN(sh.createdon)                                AS first_created_on,
        MAX(sh.createdon)                                AS last_created_on
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_SVSHBASE` sh
    WHERE sh.tmkn_ref IS NOT NULL
      AND sh.statecode = 0
    GROUP BY
        sh.tmkn_ref,
        sh.tmkn_StatusReport
),

mon_status_history AS (
    SELECT
        sh.tmkn_ref                                      AS reference_id,
        sh.tmkn_StatusReport                             AS status_report_id,
        COUNT(sh.tmkn_monitoringstatushistoryid)         AS occurrence_count,
        MIN(sh.createdon)                                AS first_created_on,
        MAX(sh.createdon)                                AS last_created_on
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_MONITORINGSTATUSHISTORYBASE` sh
    WHERE sh.tmkn_ref IS NOT NULL
      AND sh.statecode = 0
    GROUP BY
        sh.tmkn_ref,
        sh.tmkn_StatusReport
)


-- ============================================================================
-- SUB-TYPE 1: Site Visits
-- Anchor: tmkn_sitevisit
-- ============================================================================
SELECT
    'SITE_VISIT' AS assessment_subtype,
    'tmkn_sitevisit' AS mis_source_table,
    -- Identifiers
    CAST(sv.tmkn_sitevisitid AS STRING)                 AS assessment_id,
    sv.tmkn_name                                         AS assessment_no,
    -- Foreign keys (preserved for downstream cross-domain joins)
    CAST(sv.tmkn_applicationref AS STRING)              AS application_id,
    CAST(sv.tmkn_sitevisitsid AS STRING)                AS monitoring_id,
    CAST(NULL AS STRING)                                AS site_visit_parent_id,
    CAST(NULL AS STRING)                                AS company_id,
    -- Display names
      sv.tmkn_applicationref                             AS application_no_name,
      sv.tmkn_sitevisitsid                               AS monitoring_ref_name,
      sv.tmkn_svref                                      AS payment_ref_name,
      sv.ownerid                                         AS owner_name,
    CAST(NULL AS STRING)                                AS sp_name,
    -- Site visit specific fields
    sv.tmkn_sitevisitdate                                AS site_visit_date,
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_sitevisit')

      AND LOWER(sm.attributename) = LOWER('tmkn_type')

      AND CAST(sm.attributevalue AS STRING) = CAST(sv.tmkn_type AS STRING)

)              AS site_visit_type, 
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_sitevisit')

      AND LOWER(sm.attributename) = LOWER('tmkn_virtuallyverified')

      AND CAST(sm.attributevalue AS STRING) = CAST(sv.tmkn_virtuallyverified AS STRING)

) AS virtually_verified, 
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_sitevisit')

      AND LOWER(sm.attributename) = LOWER('mis_onhold')

      AND CAST(sm.attributevalue AS STRING) = CAST(sv.mis_onhold AS STRING)

)             AS on_hold, 
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_sitevisit')

      AND LOWER(sm.attributename) = LOWER('tmkn_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(sv.tmkn_workflowstatus AS STRING)

)    AS workflow_status, 
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_sitevisit')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(sv.statecode AS STRING)

)              AS state,               
    -- Status history milestones (for site visit)
    sh_submit_request.first_created_on                   AS submit_request_on,
    --sh_submit_request.first_created_by                   AS submit_request_by,
    CAST(NULL AS STRING) AS submit_request_by,   ---NEWLY ADDED
    sh_submit_results.first_created_on                   AS submit_results_on,
    --sh_submit_results.first_created_by                   AS submit_results_by,
    CAST(NULL AS STRING) AS submit_results_by, ---NEWLY ADDED
    sh_close_request.last_created_on                     AS close_request_on,
    --sh_close_request.last_created_by                     AS close_request_by,
    CAST(NULL AS STRING) AS close_request_by, ---NEWLY ADDED
    -- Monitoring-specific placeholders (NULL for this branch)
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_revenue_t1,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_revenue_t,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_profit_t1,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_profit_t,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_reward_percentage,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_reward_score,
    CAST(NULL AS INTEGER)                                AS monitoring_total_employees,
    CAST(NULL AS INTEGER)                                AS monitoring_bahrainis_count,
    CAST(NULL AS INTEGER)                                AS monitoring_non_bahrainis_count,
    CAST(NULL AS INTEGER)                                AS monitoring_disabled_bahrainis,
    CAST(NULL AS BOOLEAN)                                AS monitoring_eligible,
    CAST(NULL AS BOOLEAN)                                AS monitoring_audited_financial,
    -- Audit
    sv.createdon                                         AS created_on,
    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_SITEVISITBASE` sv
LEFT JOIN sv_status_history sh_submit_request
       ON sh_submit_request.reference_id = sv.tmkn_sitevisitid
      AND sh_submit_request.status_report_id = 810800000  -- Submit Request
LEFT JOIN sv_status_history sh_submit_results
       ON sh_submit_results.reference_id = sv.tmkn_sitevisitid
      AND sh_submit_results.status_report_id = 810800005  -- Submit Results to Requester
LEFT JOIN sv_status_history sh_close_request
       ON sh_close_request.reference_id = sv.tmkn_sitevisitid
      AND sh_close_request.status_report_id = 810800003  -- Close Request


UNION ALL


-- ============================================================================
-- SUB-TYPE 2: Virtual Visits
-- Anchor: tmkn_virtualvisit (FK back to parent site visit preserved)
-- ============================================================================
SELECT
    'VIRTUAL_VISIT' AS assessment_subtype,
    'tmkn_virtualvisit' AS mis_source_table,

    -- Identifiers
    CAST(vv.tmkn_virtualvisitid AS STRING)              AS assessment_id,
    vv.tmkn_name                                         AS assessment_no,

    -- Foreign keys
    CAST(NULL AS STRING)                                AS application_id,
    CAST(NULL AS STRING)                                AS monitoring_id,
    CAST(vv.tmkn_svref AS STRING)                       AS site_visit_parent_id,
    CAST(NULL AS STRING)                                AS company_id,

    -- Display names
    CAST(NULL AS STRING)                                AS application_no_name,
    CAST(NULL AS STRING)                                AS monitoring_ref_name,
    vv.tmkn_svref                                        AS payment_ref_name,
    CAST(NULL AS STRING)                                AS owner_name,
    vv.tmkn_sp                                           AS sp_name,

    -- Site visit specific (NULL for virtual)
    CAST(NULL AS TIMESTAMP)                              AS site_visit_date,
    CAST(NULL AS STRING)                                AS site_visit_type,
    CAST(NULL AS STRING)                                AS virtually_verified,
    CAST(NULL AS STRING)                                AS on_hold,
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_virtualvisit')

      AND LOWER(sm.attributename) = LOWER('tmkn_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(vv.tmkn_workflowstatus AS STRING)

) AS workflow_status, 
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_virtualvisit')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(vv.statecode AS STRING)

)           AS state,            

    -- Status history (NULL for virtual visits â€” they don't carry milestones in source SP)
    CAST(NULL AS TIMESTAMP)                              AS submit_request_on,
    CAST(NULL AS STRING)                                AS submit_request_by, ---UNCOMMENTED
    CAST(NULL AS TIMESTAMP)                              AS submit_results_on,
    CAST(NULL AS STRING)                                AS submit_results_by, ---UNCOMMENTED
    CAST(NULL AS TIMESTAMP)                              AS close_request_on,
    CAST(NULL AS STRING)                                AS close_request_by, ---UNCOMMENTED

    -- Monitoring-specific placeholders
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_revenue_t1,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_revenue_t,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_profit_t1,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_profit_t,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_reward_percentage,
    CAST(NULL AS DECIMAL(18, 2))                         AS monitoring_reward_score,
    CAST(NULL AS INTEGER)                                AS monitoring_total_employees,
    CAST(NULL AS INTEGER)                                AS monitoring_bahrainis_count,
    CAST(NULL AS INTEGER)                                AS monitoring_non_bahrainis_count,
    CAST(NULL AS INTEGER)                                AS monitoring_disabled_bahrainis,
    CAST(NULL AS BOOLEAN)                                AS monitoring_eligible,
    CAST(NULL AS BOOLEAN)                                AS monitoring_audited_financial,

    -- Audit
    vv.createdon                                         AS created_on,

    -- Standard trailing
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_VIRTUALVISITBASE` vv


UNION ALL


-- ============================================================================
-- SUB-TYPE 3: ES Monitoring
-- Anchor: tmkn_esmonitoring (financial monitoring assessment)
-- ============================================================================
SELECT
    'ES_MONITORING' AS assessment_subtype,
    'tmkn_esmonitoring' AS mis_source_table,

    -- Identifiers
    CAST(esmon.tmkn_esmonitoringid AS STRING)           AS assessment_id,
    esmon.tmkn_id                                        AS assessment_no,

    -- Foreign keys
    CAST(esmon.tmkn_esappllication AS STRING)           AS application_id,
    CAST(esmon.tmkn_esmonitoringid AS STRING)           AS monitoring_id,
    CAST(NULL AS STRING)                                AS site_visit_parent_id,
    CAST(NULL AS STRING)                                AS company_id,

    -- Display names
    esmon.tmkn_esappllication                            AS application_no_name,
    CAST(NULL AS STRING)                                AS monitoring_ref_name,
    CAST(NULL AS STRING)                                AS payment_ref_name,
    esmon.ownerid                                        AS owner_name,
    CAST(NULL AS STRING)                                AS sp_name,

    -- Site visit fields (NULL for monitoring)
    CAST(NULL AS TIMESTAMP)                              AS site_visit_date,
    CAST(NULL AS STRING)                                AS site_visit_type,
    CAST(NULL AS STRING)                                AS virtually_verified,
    CAST(NULL AS STRING)                                AS on_hold,
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_esmonitoring')

      AND LOWER(sm.attributename) = LOWER('tmkn_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(esmon.tmkn_workflowstatus AS STRING)

) AS workflow_status, 
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_esmonitoring')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(esmon.statecode AS STRING)

)           AS state, 
    -- Status history milestones for monitoring
    sh_send_sv.last_created_on                           AS submit_request_on,
    --sh_send_sv.last_created_by                           AS submit_request_by, 
    CAST(NULL AS STRING) as submit_request_by,                                    ---NEWLY ADDED
    sh_analyst.last_created_on                           AS submit_results_on,
    --sh_analyst.last_created_by                           AS submit_results_by, 
    CAST(NULL AS STRING) as submit_results_by,                                     ---NEWLY ADDED
    sh_director.last_created_on                          AS close_request_on,
    --sh_director.last_created_by                          AS close_request_by, 
    CAST(NULL AS STRING) as close_request_by,                                       ---NEWLY ADDED

    -- Monitoring-specific financial fields
    esmon.tmkn_revenue                                   AS monitoring_revenue_t1,
    esmon.tmkn_totalrevenuenew                           AS monitoring_revenue_t,
    esmon.tmkn_profit                                    AS monitoring_profit_t1,
    esmon.tmkn_totalprofitnew                            AS monitoring_profit_t,
    esmon.tmkn_rewardpercentage                          AS monitoring_reward_percentage,
    esmon.tmkn_rewardscore                               AS monitoring_reward_score,
    esmon.tmkn_totalemp                                  AS monitoring_total_employees,
    esmon.tmkn_noofbahrainis                             AS monitoring_bahrainis_count,
    esmon.tmkn_noofnonbahrainis                          AS monitoring_non_bahrainis_count,
    esmon.tmkn_noofdisabledbah                           AS monitoring_disabled_bahrainis,
    esmon.tmkn_eligible                                  AS monitoring_eligible,
    esmon.tmkn_auditedfinancial                          AS monitoring_audited_financial,

    -- Audit
    esmon.createdon                                      AS created_on,

    -- Standard trailing
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_ESMONITORINGBASE` esmon
LEFT JOIN mon_status_history sh_send_sv
       ON sh_send_sv.reference_id = esmon.tmkn_esmonitoringid
      AND sh_send_sv.status_report_id = 810800011  -- Send For Site Visit
LEFT JOIN mon_status_history sh_analyst
       ON sh_analyst.reference_id = esmon.tmkn_esmonitoringid
      AND sh_analyst.status_report_id = 810800001   -- Submit for Analyst confirmation
LEFT JOIN mon_status_history sh_director
       ON sh_director.reference_id = esmon.tmkn_esmonitoringid
      AND sh_director.status_report_id = 810800010  -- Approve: Monitoring Director
)
SELECT       
	   ID,
       APPLICATIONID,
       AMENDMENTREQUESTID,
       ASSESSMENTROLE1,
       ASSESSMENTROLE2,
       REVIEWROLE,
       APPROVEROLE,
       PROCESSID,
       ASSESSMENTTEAM1_NAME,
       ASSESSMENTTEAM2_NAME,
       REVIEWTEAM1_NAME,
       APPROVETEAM1_NAME,
       --ASSESSMENTSTATUSID,
       ASSESSMENTROLEMOL,
       ASSESSMENTTEAMMOL_NAME,
       REVIEWROLE1,
       REVIEWROLE2,
       REVIEWTEAM2,
       MONITORINGROLE1,
       MONITORINGROLE2,
       MONITORINGTEAM1,
       MONITORINGTEAM2,
      CAST(NULL AS STRING) AS ASSESSMENT_SUBTYPE,
      CAST(NULL AS STRING) AS MIS_SOURCE_TABLE,
      CAST(NULL AS STRING) AS ASSESSMENT_ID,
      CAST(NULL AS STRING) AS ASSESSMENT_NO,
      CAST(NULL AS STRING) AS APPLICATION_ID,
      CAST(NULL AS STRING) AS MONITORING_ID,
      CAST(NULL AS STRING) AS SITE_VISIT_PARENT_ID,
      CAST(NULL AS STRING) AS COMPANY_ID,
      CAST(NULL AS STRING) AS APPLICATION_NO_NAME,
      CAST(NULL AS STRING) AS MONITORING_REF_NAME,
      CAST(NULL AS STRING) AS PAYMENT_REF_NAME,
     CAST(NULL AS STRING) AS OWNER_NAME,
     CAST(NULL AS STRING) AS SP_NAME,
     CAST(NULL AS TIMESTAMP) AS SITE_VISIT_DATE,
     CAST(NULL AS STRING) AS SITE_VISIT_TYPE,
     CAST(NULL AS STRING) AS VIRTUALLY_VERIFIED,
     CAST(NULL AS STRING) AS ON_HOLD,
     CAST(NULL AS STRING) AS WORKFLOW_STATUS,
     CAST(NULL AS STRING) AS STATE,
     CAST(NULL AS TIMESTAMP) AS SUBMIT_REQUEST_ON,
     CAST(NULL AS TIMESTAMP) AS SUBMIT_RESULTS_ON,
     CAST(NULL AS TIMESTAMP) AS CLOSE_REQUEST_ON,
     CAST(NULL AS STRING) AS SUBMIT_REQUEST_BY,
     CAST(NULL AS STRING) AS SUBMIT_RESULTS_BY, 
     CAST(NULL AS STRING) AS CLOSE_REQUEST_BY,
     CAST(NULL AS DECIMAL) AS MONITORING_REVENUE_T1,
     CAST(NULL AS DECIMAL) AS MONITORING_REVENUE_T,
     CAST(NULL AS DECIMAL) AS MONITORING_PROFIT_T1,
     CAST(NULL AS DECIMAL) AS MONITORING_PROFIT_T,
     CAST(NULL AS DECIMAL) AS MONITORING_REWARD_PERCENTAGE,
     CAST(NULL AS DECIMAL) AS MONITORING_REWARD_SCORE,
     CAST(NULL AS INTEGER) AS MONITORING_TOTAL_EMPLOYEES,
     CAST(NULL AS INTEGER) AS MONITORING_BAHRAINIS_COUNT,
     CAST(NULL AS INTEGER) AS MONITORING_NON_BAHRAINIS_COUNT,
     CAST(NULL AS INTEGER) AS MONITORING_DISABLED_BAHRAINIS,
     CAST(NULL AS BOOLEAN) AS MONITORING_ELIGIBLE,
     CAST(NULL AS BOOLEAN) AS MONITORING_AUDITED_FINANCIAL,
     --CAST(NULL AS TIMESTAMP) AS CREATED_ON,
     CAST(SOURCE_SYSTEM_NAME AS STRING) AS SOURCE_SYSTEM_NAME,
     CAST(IS_DELETED AS BOOLEAN) AS IS_DELETED,
     CAST(CURRENT_DATE AS DATE) AS REPORT_DATE,
     DBT_UPDATED_AT,
     CREATEDON,
     UPDATEDON
FROM  assessment_base_os2 
	
UNION ALL

SELECT
       CAST(NULL AS BIGINT) AS ID,
       CAST(NULL AS BIGINT) AS APPLICATIONID,
       CAST(NULL AS BIGINT) AS AMENDMENTREQUESTID,
       CAST(NULL AS INTEGER) AS ASSESSMENTROLE1,
       CAST(NULL AS INTEGER) AS ASSESSMENTROLE2,
       CAST(NULL AS INTEGER) AS REVIEWROLE,
       CAST(NULL AS INTEGER) AS APPROVEROLE,
       CAST(NULL AS INTEGER) AS PROCESSID,
       CAST(NULL AS STRING)  AS ASSESSMENTTEAM1_NAME,
       CAST(NULL AS STRING) AS ASSESSMENTTEAM2_NAME,
       CAST(NULL AS STRING) AS REVIEWTEAM1_NAME,
       CAST(NULL AS STRING) AS APPROVETEAM1_NAME,
       --CAST(NULL AS STRING) AS ASSESSMENTSTATUSID,
       CAST(NULL AS INTEGER) AS ASSESSMENTROLEMOL,
       CAST(NULL AS STRING) AS ASSESSMENTTEAMMOL_NAME,
       CAST(NULL AS INTEGER) AS REVIEWROLE1,
       CAST(NULL AS INTEGER) AS REVIEWROLE2,
       CAST(NULL AS BIGINT) AS REVIEWTEAM2,
       CAST(NULL AS INTEGER) AS MONITORINGROLE1,
       CAST(NULL AS INTEGER) AS MONITORINGROLE2,
       CAST(NULL AS BIGINT) AS MONITORINGTEAM1,
       CAST(NULL AS BIGINT) AS MONITORINGTEAM2,
	   ASSESSMENT_SUBTYPE,--MIS
       MIS_SOURCE_TABLE,
       ASSESSMENT_ID,
       ASSESSMENT_NO,
       APPLICATION_ID,
      MONITORING_ID,
      SITE_VISIT_PARENT_ID,
      COMPANY_ID,
      APPLICATION_NO_NAME,
      MONITORING_REF_NAME,
      PAYMENT_REF_NAME,
     OWNER_NAME,
     SP_NAME,
     SITE_VISIT_DATE,
     SITE_VISIT_TYPE,
     VIRTUALLY_VERIFIED,
     ON_HOLD,
     WORKFLOW_STATUS,
     STATE,
     SUBMIT_REQUEST_ON,
     SUBMIT_RESULTS_ON,
     CLOSE_REQUEST_ON,
     SUBMIT_REQUEST_BY,
     SUBMIT_RESULTS_BY, 
     CLOSE_REQUEST_BY,
     MONITORING_REVENUE_T1,
     MONITORING_REVENUE_T,
     MONITORING_PROFIT_T1,
     MONITORING_PROFIT_T,
     MONITORING_REWARD_PERCENTAGE,
     MONITORING_REWARD_SCORE,
     MONITORING_TOTAL_EMPLOYEES,
     MONITORING_BAHRAINIS_COUNT,
     MONITORING_NON_BAHRAINIS_COUNT,
     MONITORING_DISABLED_BAHRAINIS,
     MONITORING_ELIGIBLE,
     MONITORING_AUDITED_FINANCIAL,
     --CREATED_ON,
     SOURCE_SYSTEM_NAME,
     IS_DELETED,
     REPORT_DATE,
     DBT_UPDATED_AT,
     CAST(CREATED_ON AS TIMESTAMP) AS CREATEDON,
     CAST(NULL AS TIMESTAMP) AS UPDATEDON
FROM  assessment_base_mis
),

silver_layer AS (
SELECT
    `id`,
    `applicationid`,
    `amendmentrequestid`,
    `assessmentrole1`,
    `assessmentrole2`,
    `reviewrole`,
    `approverole`,
    `processid`,
    `assessmentteam1_name`,
    `assessmentteam2_name`,
    `reviewteam1_name`,
    `approveteam1_name`,
    `assessmentrolemol`,
    `assessmentteammol_name`,
    `reviewrole1`,
    `reviewrole2`,
    `reviewteam2`,
    `monitoringrole1`,
    `monitoringrole2`,
    `monitoringteam1`,
    `monitoringteam2`,
    `assessment_subtype`,
    `mis_source_table`,
    `assessment_id`,
    `assessment_no`,
    `application_id`,
    `monitoring_id`,
    `site_visit_parent_id`,
    `company_id`,
    `application_no_name`,
    `monitoring_ref_name`,
    `payment_ref_name`,
    `owner_name`,
    `sp_name`,
    `site_visit_date`,
    `site_visit_type`,
    `virtually_verified`,
    `on_hold`,
    `workflow_status`,
    `state`,
    `submit_request_on`,
    `submit_results_on`,
    `close_request_on`,
    `submit_request_by`,
    `submit_results_by`,
    `close_request_by`,
    `monitoring_revenue_t1`,
    `monitoring_revenue_t`,
    `monitoring_profit_t1`,
    `monitoring_profit_t`,
    `monitoring_reward_percentage`,
    `monitoring_reward_score`,
    `monitoring_total_employees`,
    `monitoring_bahrainis_count`,
    `monitoring_non_bahrainis_count`,
    `monitoring_disabled_bahrainis`,
    `monitoring_eligible`,
    `monitoring_audited_financial`,
    `source_system_name`,
    `is_deleted`,
    `report_date`,
    `dbt_updated_at`,
    `createdon`,
    `updatedon`
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`assessment_base`
),

bronze_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'id'),
        (2, 'applicationid'),
        (3, 'amendmentrequestid'),
        (4, 'assessmentrole1'),
        (5, 'assessmentrole2'),
        (6, 'reviewrole'),
        (7, 'approverole'),
        (8, 'processid'),
        (9, 'assessmentteam1_name'),
        (10, 'assessmentteam2_name'),
        (11, 'reviewteam1_name'),
        (12, 'approveteam1_name'),
        (13, 'assessmentrolemol'),
        (14, 'assessmentteammol_name'),
        (15, 'reviewrole1'),
        (16, 'reviewrole2'),
        (17, 'reviewteam2'),
        (18, 'monitoringrole1'),
        (19, 'monitoringrole2'),
        (20, 'monitoringteam1'),
        (21, 'monitoringteam2'),
        (22, 'assessment_subtype'),
        (23, 'mis_source_table'),
        (24, 'assessment_id'),
        (25, 'assessment_no'),
        (26, 'application_id'),
        (27, 'monitoring_id'),
        (28, 'site_visit_parent_id'),
        (29, 'company_id'),
        (30, 'application_no_name'),
        (31, 'monitoring_ref_name'),
        (32, 'payment_ref_name'),
        (33, 'owner_name'),
        (34, 'sp_name'),
        (35, 'site_visit_date'),
        (36, 'site_visit_type'),
        (37, 'virtually_verified'),
        (38, 'on_hold'),
        (39, 'workflow_status'),
        (40, 'state'),
        (41, 'submit_request_on'),
        (42, 'submit_results_on'),
        (43, 'close_request_on'),
        (44, 'submit_request_by'),
        (45, 'submit_results_by'),
        (46, 'close_request_by'),
        (47, 'monitoring_revenue_t1'),
        (48, 'monitoring_revenue_t'),
        (49, 'monitoring_profit_t1'),
        (50, 'monitoring_profit_t'),
        (51, 'monitoring_reward_percentage'),
        (52, 'monitoring_reward_score'),
        (53, 'monitoring_total_employees'),
        (54, 'monitoring_bahrainis_count'),
        (55, 'monitoring_non_bahrainis_count'),
        (56, 'monitoring_disabled_bahrainis'),
        (57, 'monitoring_eligible'),
        (58, 'monitoring_audited_financial'),
        (59, 'source_system_name'),
        (60, 'is_deleted'),
        (61, 'report_date'),
        (62, 'dbt_updated_at'),
        (63, 'createdon'),
        (64, 'updatedon')
    ) AS t(column_position, column_name)
),

silver_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'id'),
        (2, 'applicationid'),
        (3, 'amendmentrequestid'),
        (4, 'assessmentrole1'),
        (5, 'assessmentrole2'),
        (6, 'reviewrole'),
        (7, 'approverole'),
        (8, 'processid'),
        (9, 'assessmentteam1_name'),
        (10, 'assessmentteam2_name'),
        (11, 'reviewteam1_name'),
        (12, 'approveteam1_name'),
        (13, 'assessmentrolemol'),
        (14, 'assessmentteammol_name'),
        (15, 'reviewrole1'),
        (16, 'reviewrole2'),
        (17, 'reviewteam2'),
        (18, 'monitoringrole1'),
        (19, 'monitoringrole2'),
        (20, 'monitoringteam1'),
        (21, 'monitoringteam2'),
        (22, 'assessment_subtype'),
        (23, 'mis_source_table'),
        (24, 'assessment_id'),
        (25, 'assessment_no'),
        (26, 'application_id'),
        (27, 'monitoring_id'),
        (28, 'site_visit_parent_id'),
        (29, 'company_id'),
        (30, 'application_no_name'),
        (31, 'monitoring_ref_name'),
        (32, 'payment_ref_name'),
        (33, 'owner_name'),
        (34, 'sp_name'),
        (35, 'site_visit_date'),
        (36, 'site_visit_type'),
        (37, 'virtually_verified'),
        (38, 'on_hold'),
        (39, 'workflow_status'),
        (40, 'state'),
        (41, 'submit_request_on'),
        (42, 'submit_results_on'),
        (43, 'close_request_on'),
        (44, 'submit_request_by'),
        (45, 'submit_results_by'),
        (46, 'close_request_by'),
        (47, 'monitoring_revenue_t1'),
        (48, 'monitoring_revenue_t'),
        (49, 'monitoring_profit_t1'),
        (50, 'monitoring_profit_t'),
        (51, 'monitoring_reward_percentage'),
        (52, 'monitoring_reward_score'),
        (53, 'monitoring_total_employees'),
        (54, 'monitoring_bahrainis_count'),
        (55, 'monitoring_non_bahrainis_count'),
        (56, 'monitoring_disabled_bahrainis'),
        (57, 'monitoring_eligible'),
        (58, 'monitoring_audited_financial'),
        (59, 'source_system_name'),
        (60, 'is_deleted'),
        (61, 'report_date'),
        (62, 'dbt_updated_at'),
        (63, 'createdon'),
        (64, 'updatedon')
    ) AS t(column_position, column_name)
),

bronze_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`assessmentrole1` AS STRING) AS `assessmentrole1`,
        CAST(`assessmentrole2` AS STRING) AS `assessmentrole2`,
        CAST(`reviewrole` AS STRING) AS `reviewrole`,
        CAST(`approverole` AS STRING) AS `approverole`,
        CAST(`processid` AS STRING) AS `processid`,
        CAST(`assessmentteam1_name` AS STRING) AS `assessmentteam1_name`,
        CAST(`assessmentteam2_name` AS STRING) AS `assessmentteam2_name`,
        CAST(`reviewteam1_name` AS STRING) AS `reviewteam1_name`,
        CAST(`approveteam1_name` AS STRING) AS `approveteam1_name`,
        CAST(`assessmentrolemol` AS STRING) AS `assessmentrolemol`,
        CAST(`assessmentteammol_name` AS STRING) AS `assessmentteammol_name`,
        CAST(`reviewrole1` AS STRING) AS `reviewrole1`,
        CAST(`reviewrole2` AS STRING) AS `reviewrole2`,
        CAST(`reviewteam2` AS STRING) AS `reviewteam2`,
        CAST(`monitoringrole1` AS STRING) AS `monitoringrole1`,
        CAST(`monitoringrole2` AS STRING) AS `monitoringrole2`,
        CAST(`monitoringteam1` AS STRING) AS `monitoringteam1`,
        CAST(`monitoringteam2` AS STRING) AS `monitoringteam2`,
        CAST(`assessment_subtype` AS STRING) AS `assessment_subtype`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`assessment_id` AS STRING) AS `assessment_id`,
        CAST(`assessment_no` AS STRING) AS `assessment_no`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`monitoring_id` AS STRING) AS `monitoring_id`,
        CAST(`site_visit_parent_id` AS STRING) AS `site_visit_parent_id`,
        CAST(`company_id` AS STRING) AS `company_id`,
        CAST(`application_no_name` AS STRING) AS `application_no_name`,
        CAST(`monitoring_ref_name` AS STRING) AS `monitoring_ref_name`,
        CAST(`payment_ref_name` AS STRING) AS `payment_ref_name`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`sp_name` AS STRING) AS `sp_name`,
        CAST(`site_visit_date` AS STRING) AS `site_visit_date`,
        CAST(`site_visit_type` AS STRING) AS `site_visit_type`,
        CAST(`virtually_verified` AS STRING) AS `virtually_verified`,
        CAST(`on_hold` AS STRING) AS `on_hold`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`submit_request_on` AS STRING) AS `submit_request_on`,
        CAST(`submit_results_on` AS STRING) AS `submit_results_on`,
        CAST(`close_request_on` AS STRING) AS `close_request_on`,
        CAST(`submit_request_by` AS STRING) AS `submit_request_by`,
        CAST(`submit_results_by` AS STRING) AS `submit_results_by`,
        CAST(`close_request_by` AS STRING) AS `close_request_by`,
        CAST(`monitoring_revenue_t1` AS STRING) AS `monitoring_revenue_t1`,
        CAST(`monitoring_revenue_t` AS STRING) AS `monitoring_revenue_t`,
        CAST(`monitoring_profit_t1` AS STRING) AS `monitoring_profit_t1`,
        CAST(`monitoring_profit_t` AS STRING) AS `monitoring_profit_t`,
        CAST(`monitoring_reward_percentage` AS STRING) AS `monitoring_reward_percentage`,
        CAST(`monitoring_reward_score` AS STRING) AS `monitoring_reward_score`,
        CAST(`monitoring_total_employees` AS STRING) AS `monitoring_total_employees`,
        CAST(`monitoring_bahrainis_count` AS STRING) AS `monitoring_bahrainis_count`,
        CAST(`monitoring_non_bahrainis_count` AS STRING) AS `monitoring_non_bahrainis_count`,
        CAST(`monitoring_disabled_bahrainis` AS STRING) AS `monitoring_disabled_bahrainis`,
        CAST(`monitoring_eligible` AS STRING) AS `monitoring_eligible`,
        CAST(`monitoring_audited_financial` AS STRING) AS `monitoring_audited_financial`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`id` AS STRING) AS `id`,
        CAST(`applicationid` AS STRING) AS `applicationid`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`assessmentrole1` AS STRING) AS `assessmentrole1`,
        CAST(`assessmentrole2` AS STRING) AS `assessmentrole2`,
        CAST(`reviewrole` AS STRING) AS `reviewrole`,
        CAST(`approverole` AS STRING) AS `approverole`,
        CAST(`processid` AS STRING) AS `processid`,
        CAST(`assessmentteam1_name` AS STRING) AS `assessmentteam1_name`,
        CAST(`assessmentteam2_name` AS STRING) AS `assessmentteam2_name`,
        CAST(`reviewteam1_name` AS STRING) AS `reviewteam1_name`,
        CAST(`approveteam1_name` AS STRING) AS `approveteam1_name`,
        CAST(`assessmentrolemol` AS STRING) AS `assessmentrolemol`,
        CAST(`assessmentteammol_name` AS STRING) AS `assessmentteammol_name`,
        CAST(`reviewrole1` AS STRING) AS `reviewrole1`,
        CAST(`reviewrole2` AS STRING) AS `reviewrole2`,
        CAST(`reviewteam2` AS STRING) AS `reviewteam2`,
        CAST(`monitoringrole1` AS STRING) AS `monitoringrole1`,
        CAST(`monitoringrole2` AS STRING) AS `monitoringrole2`,
        CAST(`monitoringteam1` AS STRING) AS `monitoringteam1`,
        CAST(`monitoringteam2` AS STRING) AS `monitoringteam2`,
        CAST(`assessment_subtype` AS STRING) AS `assessment_subtype`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`assessment_id` AS STRING) AS `assessment_id`,
        CAST(`assessment_no` AS STRING) AS `assessment_no`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`monitoring_id` AS STRING) AS `monitoring_id`,
        CAST(`site_visit_parent_id` AS STRING) AS `site_visit_parent_id`,
        CAST(`company_id` AS STRING) AS `company_id`,
        CAST(`application_no_name` AS STRING) AS `application_no_name`,
        CAST(`monitoring_ref_name` AS STRING) AS `monitoring_ref_name`,
        CAST(`payment_ref_name` AS STRING) AS `payment_ref_name`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`sp_name` AS STRING) AS `sp_name`,
        CAST(`site_visit_date` AS STRING) AS `site_visit_date`,
        CAST(`site_visit_type` AS STRING) AS `site_visit_type`,
        CAST(`virtually_verified` AS STRING) AS `virtually_verified`,
        CAST(`on_hold` AS STRING) AS `on_hold`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`submit_request_on` AS STRING) AS `submit_request_on`,
        CAST(`submit_results_on` AS STRING) AS `submit_results_on`,
        CAST(`close_request_on` AS STRING) AS `close_request_on`,
        CAST(`submit_request_by` AS STRING) AS `submit_request_by`,
        CAST(`submit_results_by` AS STRING) AS `submit_results_by`,
        CAST(`close_request_by` AS STRING) AS `close_request_by`,
        CAST(`monitoring_revenue_t1` AS STRING) AS `monitoring_revenue_t1`,
        CAST(`monitoring_revenue_t` AS STRING) AS `monitoring_revenue_t`,
        CAST(`monitoring_profit_t1` AS STRING) AS `monitoring_profit_t1`,
        CAST(`monitoring_profit_t` AS STRING) AS `monitoring_profit_t`,
        CAST(`monitoring_reward_percentage` AS STRING) AS `monitoring_reward_percentage`,
        CAST(`monitoring_reward_score` AS STRING) AS `monitoring_reward_score`,
        CAST(`monitoring_total_employees` AS STRING) AS `monitoring_total_employees`,
        CAST(`monitoring_bahrainis_count` AS STRING) AS `monitoring_bahrainis_count`,
        CAST(`monitoring_non_bahrainis_count` AS STRING) AS `monitoring_non_bahrainis_count`,
        CAST(`monitoring_disabled_bahrainis` AS STRING) AS `monitoring_disabled_bahrainis`,
        CAST(`monitoring_eligible` AS STRING) AS `monitoring_eligible`,
        CAST(`monitoring_audited_financial` AS STRING) AS `monitoring_audited_financial`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
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
        'assessment_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'assessment_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'assessment_base' AS table_name,
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
        'assessment_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'assessment_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
