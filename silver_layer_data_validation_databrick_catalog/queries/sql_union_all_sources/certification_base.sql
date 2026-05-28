-- Compare bronze-layer query output with silver-layer table output for certification_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to VARCHAR.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\union of os2_os1_mis\certification_base_os2_os1_mis_union_bronze_layer.sql
-- Silver source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\updated_silver_layer_scripts\silver_layer_query\certification_base_silver_layer.sql

WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for certification_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: certification_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH certification_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR) AS option_key,
        max(sm.value) AS option_value
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".STRINGMAP sm
    INNER JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS VARCHAR)
),
option_set_map AS (
    SELECT map_agg(option_key, option_value) AS option_values
    FROM option_set_values
)
/*
============================================================================
silver_certification_mis.sql
============================================================================
Per-source intermediate Silver model for the Certification domain â€” MIS only.

Sources:
  â˜… mis_certificate          â€” anchor: certificate entity itself
    mis_certificateexprie    â€” joined: certificate-expiration / approval data
                                (used in training context as cert_approval link)

Reference SPs:
  - RPT-061_certificat                    (full certificate definitions)
  - RPT-058_Individual_Applications       (cert lookup from individual app)
  - RPT-051_TWS_Training_Enrollments      (cert + cert expiration as approval)

Approach:
  - Anchor on mis_certificate
  - LEFT JOIN to mis_certificateexprie via tmkn_certificate (FK from approval
    side back to certificate)
  - All option-set decodes from RPT-061 are preserved
  - No status history at this layer (Certification is reference data, not workflow-driven)

Note on the join: in RPT-051, the relationship is
  trn.tws_certificate_approval = mis_certificateexprie.mis_certificateexprieId
That pattern is reversed here (we anchor on cert, then look up its expiration
record). The cardinality may be 1:N â€” review with team if duplicate cert rows
appear in the output.
============================================================================
*/

SELECT
    'mis_certificate' AS mis_source_table,

    -- Identifiers
    CAST(cert.mis_certificateid AS VARCHAR)              AS certificate_id,
    cert.tmkn_id                                         AS certificate_external_id,
    cert.mis_name                                        AS certificate_name,

    -- Hierarchical classification (denormalised at source)
    cert.mis_category                               AS category,
    cert.mis_broad                                  AS broad,
    cert.mis_detailed                                AS detailed,
    cert.mis_narrow                                 AS narrow,

    -- Awarding body
    --cert.mis_awardingbodyname                            AS awarding_body,
    CAST(NULL AS VARCHAR) AS awarding_body,

    -- Option-set decoded fields (from RPT-061)
        

     CASE WHEN cert.mis_employmentrequired IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_employmentrequired') || '|' || CAST(cert.mis_employmentrequired AS VARCHAR)) END AS employment_required, 
     CASE WHEN cert.mis_bs IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_bs') || '|' || CAST(cert.mis_bs AS VARCHAR)) END AS basic_skill, 
     CASE WHEN cert.mis_certificatestatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_certificatestatus') || '|' || CAST(cert.mis_certificatestatus AS VARCHAR)) END AS certificate_status, 
     CASE WHEN cert.mis_onjobtraining IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_onjobtraining') || '|' || CAST(cert.mis_onjobtraining AS VARCHAR)) END AS on_job_training, 
     CASE WHEN cert.mis_tpcs IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_tpcs') || '|' || CAST(cert.mis_tpcs AS VARCHAR)) END AS tpcs, 
     CASE WHEN cert.mis_tws IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_tws') || '|' || CAST(cert.mis_tws AS VARCHAR)) END AS tws, 
     CASE WHEN cert.tmkn_targetedlearners_employee IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_employee') || '|' || CAST(cert.tmkn_targetedlearners_employee AS VARCHAR)) END AS targeted_learners_employee, 
     CASE WHEN cert.tmkn_targetedlearners_entrepreneur IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_entrepreneur') || '|' || CAST(cert.tmkn_targetedlearners_entrepreneur AS VARCHAR)) END AS targeted_learners_entrepreneur, 
     CASE WHEN cert.tmkn_targetedlearners_jobseeker IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_jobseeker') || '|' || CAST(cert.tmkn_targetedlearners_jobseeker AS VARCHAR)) END AS targeted_learners_jobseeker, 
     CASE WHEN cert.tmkn_targetedlearners_student IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_student') || '|' || CAST(cert.tmkn_targetedlearners_student AS VARCHAR)) END AS targeted_learners_student, 
     CASE WHEN cert.tmkn_studytype_selfstudy IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_selfstudy') || '|' || CAST(cert.tmkn_studytype_selfstudy AS VARCHAR)) END AS study_type_self_study, 
     CASE WHEN cert.tmkn_studytype_online IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_online') || '|' || CAST(cert.tmkn_studytype_online AS VARCHAR)) END AS study_type_online, 
     CASE WHEN cert.tmkn_studytype_blendedlearning IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_blendedlearning') || '|' || CAST(cert.tmkn_studytype_blendedlearning AS VARCHAR)) END AS study_type_blended_learning, 
     CASE WHEN cert.tmkn_studytype_localtrainingprovider IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_localtrainingprovider') || '|' || CAST(cert.tmkn_studytype_localtrainingprovider AS VARCHAR)) END AS study_type_local_training_provider, 
     CASE WHEN cert.tmkn_studytype_inhouse IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_inhouse') || '|' || CAST(cert.tmkn_studytype_inhouse AS VARCHAR)) END AS study_type_in_house, 
     CASE WHEN cert.nfc_deactivateflag IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('nfc_deactivateflag') || '|' || CAST(cert.nfc_deactivateflag AS VARCHAR)) END AS deactivate_flag, 
     CASE WHEN cert.mis_payment_structure IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_payment_structure') || '|' || CAST(cert.mis_payment_structure AS VARCHAR)) END AS payment_structure, 
     CASE WHEN cert.mis_cap_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_cap_type') || '|' || CAST(cert.mis_cap_type AS VARCHAR)) END AS cap_type, 
     CASE WHEN cert.mis_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_type') || '|' || CAST(cert.mis_type AS VARCHAR)) END AS old_type, 
     CASE WHEN cert.mis_level IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_level') || '|' || CAST(cert.mis_level AS VARCHAR)) END AS level, 
     CASE WHEN cert.mis_levelqcf IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_levelqcf') || '|' || CAST(cert.mis_levelqcf AS VARCHAR)) END AS level_qcf, 
     CASE WHEN cert.tmkn_certificatetype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_certificatetype') || '|' || CAST(cert.tmkn_certificatetype AS VARCHAR)) END AS certificate_type, 
     CASE WHEN cert.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('statuscode') || '|' || CAST(cert.statuscode AS VARCHAR)) END AS status_reason, 
     CASE WHEN cert.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('statecode') || '|' || CAST(cert.statecode AS VARCHAR)) END AS state, 

    -- Numeric attributes
    cert.mis_cap                                         AS cap,
    cert.mis_averagecontacthours                         AS average_contact_hours,
    cert.nfc_noofapplicants                              AS no_of_applicants,
    cert.nfc_tamkeensupport                              AS tamkeen_support_pct,
    cert.mis_price                                       AS price,
    cert.mis_priceperhourperperson                       AS price_per_hour_per_person,
    cert.mis_duration                                    AS duration,
    cert.mis_noofhours                                   AS no_of_hours,
    cert.exchangerate                                    AS exchange_rate,

    -- Aggregated counts (these are pre-computed in source â€” keep as-is in Silver)
    cert.nfc_totalnoofindividualapplications             AS total_individual_applications_approved,
    cert.nfc_totalnoofindividualapplications_state       AS total_individual_applications_approved_state,
    cert.nfc_totalnooftwsenrollments                     AS total_tws_enrollments_approved,
    cert.nfc_totalnooftwsenrollments_state               AS total_tws_enrollments_approved_state,
    cert.nfc_totalnoofapplicants                         AS total_no_of_applicants,
    cert.nfc_totalnooftwsenrollments_date                AS total_tws_enrollments_last_updated_on,
    cert.nfc_totalnoofindividualapplications_date        AS total_individual_applications_last_updated_on,

    -- Free-text descriptive fields
    cert.mis_overview                                    AS overview,
    cert.mis_tamkeeneligibilitycriteria                  AS tamkeen_eligibility_criteria,
    cert.tmkn_analystnote                                AS analyst_note,
    cert.mis_crt                                         AS crt,
    cert.mis_certificatewebsite                          AS certificate_website,
    cert.mis_levels                                      AS nvq_or_other_levels,
    cert.mis_availabilityofassessment                    AS availability_of_assessment,

    -- Joined: certificate expiration / approval data
    crtexp.tmkn_hours                                    AS expiration_training_hours,
    CAST(crtexp.mis_certificateexprieid AS VARCHAR)      AS certificate_expiration_id,

    -- Owner / audit
    cert.ownerid                                    AS owner_name,
    cert.createdby                                   AS created_by,
    cert.modifiedby                                 AS modified_by,
    cert.createdon                                       AS created_on,
    cert.modifiedon                                      AS modified_on,

    -- Standard trailing audit columns
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".MIS_CERTIFICATEBASE cert
LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".MIS_CERTIFICATEEXPRIEBASE crtexp
       ON crtexp.mis_certificateexprieid = cert.mis_certificateid
   -- NOTE: This join direction may need verification with the team.
   -- In RPT-051, the relationship is keyed via the training enrollment
   -- (trn.tws_certificate_approval = crtexp.mis_certificateexprieId).
   -- If a certificate has multiple expiration records, this join may
   -- produce duplicate cert rows â€” review and adjust with QUALIFY ROW_NUMBER
   -- if needed.
),
certification_base_os2_source AS (
-- Standalone Trino SQL converted from dbt model.
/*
=================================================================================================

Name        : CERTIFICATION_BASE
Description : This model extracts certification records from the NEO2 bronze
              layer and loads them into the Silver layer. It processes only
              the latest records using incremental logic based on CREATEDON
              and UPDATEDON timestamps.

Source Tables :
    - neo2.OSUSR_VW9_CERTIFICATION

Target Table :
    - certification_base_os2

Load Type    : Incremental (MERGE)
Materialized : Table
Format       : PARQUET
Tags         : neo2, daily

Logic :
    - Load only new or updated records using CREATEDON / UPDATEDON
    - Maintain soft delete using post_hook (records missing in source marked deleted)
    - Add audit columns (DBT_UPDATED_AT, SOURCE_SYSTEM_NAME)

Revision History:
--------------------------------------------------------------
Version | Date       | Author | Description
--------------------------------------------------------------
1.0     | 2026-03-25 | Siva    | Initial version

=================================================================================================
*/
with source_cte as (
SELECT
    cert.id,
    cert.trainingprogramid,
    cert.traininghours,
    cert.trainingprogramtypeid,
    cert.isallowpapers,
    cert.createdon,
    cert.updatedon,
    FALSE AS is_deleted,
    'NEO2' AS source_system_name,
    CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS dbt_updated_at,
    ROW_NUMBER() OVER (PARTITION BY cert.id ORDER BY cert.updatedon DESC NULLS LAST, cert.createdon DESC NULLS LAST) AS rnk
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_VW9_CERTIFICATION cert
)
select cert.id,
    cert.trainingprogramid,
    cert.traininghours,
    cert.trainingprogramtypeid,
    cert.isallowpapers,
    TRY_CAST(NULLIF(CAST(cert.createdon AS VARCHAR), '') AS TIMESTAMP) as createdon,
    TRY_CAST(NULLIF(CAST(cert.updatedon AS VARCHAR), '') AS TIMESTAMP) as updatedon,
    cert.is_deleted,
    UPPER(NULLIF(TRIM(CAST(cert.source_system_name AS VARCHAR)), '')) as source_system_name,
    cert.dbt_updated_at from source_cte cert
WHERE rnk=1
)
SELECT

    -- OS2 columns
    CAST(certificate_id AS VARCHAR) AS certificate_id,
    CAST(NULL AS BIGINT)            AS trainingprogramid,
    CAST(NULL AS BIGINT)            AS traininghours,
    CAST(NULL AS VARCHAR)           AS trainingprogramtypeid,
    CAST(NULL AS BOOLEAN)           AS isallowpapers,
    CAST(NULL AS TIMESTAMP(6))      AS createdon,
    CAST(NULL AS TIMESTAMP(6))      AS updatedon,

    -- MIS columns
    mis_source_table,
    certificate_external_id,
    certificate_name,
    category,
    broad,
    detailed,
    narrow,
    awarding_body,
    employment_required,
    basic_skill,
    certificate_status,
    on_job_training,
    tpcs,
    tws,
    targeted_learners_employee,
    targeted_learners_entrepreneur,
    targeted_learners_jobseeker,
    targeted_learners_student,
    study_type_self_study,
    study_type_online,
    study_type_blended_learning,
    study_type_local_training_provider,
    study_type_in_house,
    deactivate_flag,
    payment_structure,
    cap_type,
    old_type,
    level,
    level_qcf,
    certificate_type,
    status_reason,
    state,
    cap,
    average_contact_hours,
    no_of_applicants,
    tamkeen_support_pct,
    price,
    price_per_hour_per_person,
    duration,
    no_of_hours,
    exchange_rate,
    total_individual_applications_approved,
    total_individual_applications_approved_state,
    total_tws_enrollments_approved,
    total_tws_enrollments_approved_state,
    total_no_of_applicants,
    total_tws_enrollments_last_updated_on,
    total_individual_applications_last_updated_on,
    overview,
    tamkeen_eligibility_criteria,
    analyst_note,
    crt,
    certificate_website,
    nvq_or_other_levels,
    availability_of_assessment,
    expiration_training_hours,
    certificate_expiration_id,
    owner_name,
    created_by,
    modified_by,
    created_on,
    modified_on,

    -- Common columns
    UPPER(TRIM(CAST(source_system_name AS VARCHAR))) AS source_system_name,
    is_deleted,
    TRY_CAST(NULLIF(CAST(report_date AS VARCHAR), '') AS TIMESTAMP) AS report_date,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS VARCHAR), '') AS TIMESTAMP) AS dbt_updated_at

from certification_base_mis_source

UNION ALL

SELECT

    -- OS2 columns
    CAST(id AS VARCHAR)             AS certificate_id,
    trainingprogramid,
    traininghours,
    trainingprogramtypeid,
    isallowpapers,
    createdon,
    updatedon,

    -- MIS columns
    CAST(NULL AS VARCHAR)           AS mis_source_table,
    CAST(NULL AS VARCHAR)           AS certificate_external_id,
    CAST(NULL AS VARCHAR)           AS certificate_name,
    CAST(NULL AS VARCHAR)           AS category,
    CAST(NULL AS VARCHAR)           AS broad,
    CAST(NULL AS VARCHAR)           AS detailed,
    CAST(NULL AS VARCHAR)           AS narrow,
    CAST(NULL AS VARCHAR)           AS awarding_body,
    CAST(NULL AS VARCHAR)           AS employment_required,
    CAST(NULL AS VARCHAR)           AS basic_skill,
    CAST(NULL AS VARCHAR)           AS certificate_status,
    CAST(NULL AS VARCHAR)           AS on_job_training,
    CAST(NULL AS VARCHAR)           AS tpcs,
    CAST(NULL AS VARCHAR)           AS tws,
    CAST(NULL AS VARCHAR)           AS targeted_learners_employee,
    CAST(NULL AS VARCHAR)           AS targeted_learners_entrepreneur,
    CAST(NULL AS VARCHAR)           AS targeted_learners_jobseeker,
    CAST(NULL AS VARCHAR)           AS targeted_learners_student,
    CAST(NULL AS VARCHAR)           AS study_type_self_study,
    CAST(NULL AS VARCHAR)           AS study_type_online,
    CAST(NULL AS VARCHAR)           AS study_type_blended_learning,
    CAST(NULL AS VARCHAR)           AS study_type_local_training_provider,
    CAST(NULL AS VARCHAR)           AS study_type_in_house,
    CAST(NULL AS VARCHAR)           AS deactivate_flag,
    CAST(NULL AS VARCHAR)           AS payment_structure,
    CAST(NULL AS VARCHAR)           AS cap_type,
    CAST(NULL AS VARCHAR)           AS old_type,
    CAST(NULL AS VARCHAR)           AS level,
    CAST(NULL AS VARCHAR)           AS level_qcf,
    CAST(NULL AS VARCHAR)           AS certificate_type,
    CAST(NULL AS VARCHAR)           AS status_reason,
    CAST(NULL AS VARCHAR)           AS state,
    CAST(NULL AS DECIMAL(23,10))    AS cap,
    CAST(NULL AS BIGINT)            AS average_contact_hours,
    CAST(NULL AS BIGINT)            AS no_of_applicants,
    CAST(NULL AS BIGINT)            AS tamkeen_support_pct,
    CAST(NULL AS DECIMAL(19,4))     AS price,
    CAST(NULL AS DECIMAL(19,4))     AS price_per_hour_per_person,
    CAST(NULL AS VARCHAR)           AS duration,
    CAST(NULL AS VARCHAR)           AS no_of_hours,
    CAST(NULL AS DECIMAL(23,10))    AS exchange_rate,
    CAST(NULL AS BIGINT)            AS total_individual_applications_approved,
    CAST(NULL AS BIGINT)            AS total_individual_applications_approved_state,
    CAST(NULL AS BIGINT)            AS total_tws_enrollments_approved,
    CAST(NULL AS BIGINT)            AS total_tws_enrollments_approved_state,
    CAST(NULL AS BIGINT)            AS total_no_of_applicants,
    CAST(NULL AS TIMESTAMP(6))      AS total_tws_enrollments_last_updated_on,
    CAST(NULL AS TIMESTAMP(6))      AS total_individual_applications_last_updated_on,
    CAST(NULL AS VARCHAR)           AS overview,
    CAST(NULL AS VARCHAR)           AS tamkeen_eligibility_criteria,
    CAST(NULL AS VARCHAR)           AS analyst_note,
    CAST(NULL AS VARCHAR)           AS crt,
    CAST(NULL AS VARCHAR)           AS certificate_website,
    CAST(NULL AS VARCHAR)           AS nvq_or_other_levels,
    CAST(NULL AS VARCHAR)           AS availability_of_assessment,
    CAST(NULL AS BIGINT)            AS expiration_training_hours,
    CAST(NULL AS VARCHAR)           AS certificate_expiration_id,
    CAST(NULL AS VARCHAR)           AS owner_name,
    CAST(NULL AS VARCHAR)           AS created_by,
    CAST(NULL AS VARCHAR)           AS modified_by,
    CAST(NULL AS TIMESTAMP(6))      AS created_on,
    CAST(NULL AS TIMESTAMP(6))      AS modified_on,

    -- Common columns
    UPPER(TRIM(CAST(source_system_name AS VARCHAR))) AS source_system_name,
    is_deleted,
    CAST(NULL AS TIMESTAMP(6))      AS report_date,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS VARCHAR), '') AS TIMESTAMP) AS dbt_updated_at

from certification_base_os2_source
),

silver_layer AS (
SELECT
    certificate_id,
    trainingprogramid,
    traininghours,
    trainingprogramtypeid,
    isallowpapers,
    createdon,
    updatedon,
    mis_source_table,
    certificate_external_id,
    certificate_name,
    category,
    broad,
    detailed,
    narrow,
    awarding_body,
    employment_required,
    basic_skill,
    certificate_status,
    on_job_training,
    tpcs,
    tws,
    targeted_learners_employee,
    targeted_learners_entrepreneur,
    targeted_learners_jobseeker,
    targeted_learners_student,
    study_type_self_study,
    study_type_online,
    study_type_blended_learning,
    study_type_local_training_provider,
    study_type_in_house,
    deactivate_flag,
    payment_structure,
    cap_type,
    old_type,
    level,
    level_qcf,
    certificate_type,
    status_reason,
    state,
    cap,
    average_contact_hours,
    no_of_applicants,
    tamkeen_support_pct,
    price,
    price_per_hour_per_person,
    duration,
    no_of_hours,
    exchange_rate,
    total_individual_applications_approved,
    total_individual_applications_approved_state,
    total_tws_enrollments_approved,
    total_tws_enrollments_approved_state,
    total_no_of_applicants,
    total_tws_enrollments_last_updated_on,
    total_individual_applications_last_updated_on,
    overview,
    tamkeen_eligibility_criteria,
    analyst_note,
    crt,
    certificate_website,
    nvq_or_other_levels,
    availability_of_assessment,
    expiration_training_hours,
    certificate_expiration_id,
    owner_name,
    created_by,
    modified_by,
    created_on,
    modified_on,
    source_system_name,
    is_deleted,
    report_date,
    dbt_updated_at
FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".certification_base
),

bronze_columns(column_position, column_name) AS (
    VALUES
        (1, 'certificate_id'),
        (2, 'trainingprogramid'),
        (3, 'traininghours'),
        (4, 'trainingprogramtypeid'),
        (5, 'isallowpapers'),
        (6, 'createdon'),
        (7, 'updatedon'),
        (8, 'mis_source_table'),
        (9, 'certificate_external_id'),
        (10, 'certificate_name'),
        (11, 'category'),
        (12, 'broad'),
        (13, 'detailed'),
        (14, 'narrow'),
        (15, 'awarding_body'),
        (16, 'employment_required'),
        (17, 'basic_skill'),
        (18, 'certificate_status'),
        (19, 'on_job_training'),
        (20, 'tpcs'),
        (21, 'tws'),
        (22, 'targeted_learners_employee'),
        (23, 'targeted_learners_entrepreneur'),
        (24, 'targeted_learners_jobseeker'),
        (25, 'targeted_learners_student'),
        (26, 'study_type_self_study'),
        (27, 'study_type_online'),
        (28, 'study_type_blended_learning'),
        (29, 'study_type_local_training_provider'),
        (30, 'study_type_in_house'),
        (31, 'deactivate_flag'),
        (32, 'payment_structure'),
        (33, 'cap_type'),
        (34, 'old_type'),
        (35, 'level'),
        (36, 'level_qcf'),
        (37, 'certificate_type'),
        (38, 'status_reason'),
        (39, 'state'),
        (40, 'cap'),
        (41, 'average_contact_hours'),
        (42, 'no_of_applicants'),
        (43, 'tamkeen_support_pct'),
        (44, 'price'),
        (45, 'price_per_hour_per_person'),
        (46, 'duration'),
        (47, 'no_of_hours'),
        (48, 'exchange_rate'),
        (49, 'total_individual_applications_approved'),
        (50, 'total_individual_applications_approved_state'),
        (51, 'total_tws_enrollments_approved'),
        (52, 'total_tws_enrollments_approved_state'),
        (53, 'total_no_of_applicants'),
        (54, 'total_tws_enrollments_last_updated_on'),
        (55, 'total_individual_applications_last_updated_on'),
        (56, 'overview'),
        (57, 'tamkeen_eligibility_criteria'),
        (58, 'analyst_note'),
        (59, 'crt'),
        (60, 'certificate_website'),
        (61, 'nvq_or_other_levels'),
        (62, 'availability_of_assessment'),
        (63, 'expiration_training_hours'),
        (64, 'certificate_expiration_id'),
        (65, 'owner_name'),
        (66, 'created_by'),
        (67, 'modified_by'),
        (68, 'created_on'),
        (69, 'modified_on'),
        (70, 'source_system_name'),
        (71, 'is_deleted'),
        (72, 'report_date'),
        (73, 'dbt_updated_at')
),

silver_columns(column_position, column_name) AS (
    VALUES
        (1, 'certificate_id'),
        (2, 'trainingprogramid'),
        (3, 'traininghours'),
        (4, 'trainingprogramtypeid'),
        (5, 'isallowpapers'),
        (6, 'createdon'),
        (7, 'updatedon'),
        (8, 'mis_source_table'),
        (9, 'certificate_external_id'),
        (10, 'certificate_name'),
        (11, 'category'),
        (12, 'broad'),
        (13, 'detailed'),
        (14, 'narrow'),
        (15, 'awarding_body'),
        (16, 'employment_required'),
        (17, 'basic_skill'),
        (18, 'certificate_status'),
        (19, 'on_job_training'),
        (20, 'tpcs'),
        (21, 'tws'),
        (22, 'targeted_learners_employee'),
        (23, 'targeted_learners_entrepreneur'),
        (24, 'targeted_learners_jobseeker'),
        (25, 'targeted_learners_student'),
        (26, 'study_type_self_study'),
        (27, 'study_type_online'),
        (28, 'study_type_blended_learning'),
        (29, 'study_type_local_training_provider'),
        (30, 'study_type_in_house'),
        (31, 'deactivate_flag'),
        (32, 'payment_structure'),
        (33, 'cap_type'),
        (34, 'old_type'),
        (35, 'level'),
        (36, 'level_qcf'),
        (37, 'certificate_type'),
        (38, 'status_reason'),
        (39, 'state'),
        (40, 'cap'),
        (41, 'average_contact_hours'),
        (42, 'no_of_applicants'),
        (43, 'tamkeen_support_pct'),
        (44, 'price'),
        (45, 'price_per_hour_per_person'),
        (46, 'duration'),
        (47, 'no_of_hours'),
        (48, 'exchange_rate'),
        (49, 'total_individual_applications_approved'),
        (50, 'total_individual_applications_approved_state'),
        (51, 'total_tws_enrollments_approved'),
        (52, 'total_tws_enrollments_approved_state'),
        (53, 'total_no_of_applicants'),
        (54, 'total_tws_enrollments_last_updated_on'),
        (55, 'total_individual_applications_last_updated_on'),
        (56, 'overview'),
        (57, 'tamkeen_eligibility_criteria'),
        (58, 'analyst_note'),
        (59, 'crt'),
        (60, 'certificate_website'),
        (61, 'nvq_or_other_levels'),
        (62, 'availability_of_assessment'),
        (63, 'expiration_training_hours'),
        (64, 'certificate_expiration_id'),
        (65, 'owner_name'),
        (66, 'created_by'),
        (67, 'modified_by'),
        (68, 'created_on'),
        (69, 'modified_on'),
        (70, 'source_system_name'),
        (71, 'is_deleted'),
        (72, 'report_date'),
        (73, 'dbt_updated_at')
),

bronze_normalized AS (
    SELECT
        CAST("certificate_id" AS VARCHAR) AS "certificate_id",
        CAST("trainingprogramid" AS VARCHAR) AS "trainingprogramid",
        CAST("traininghours" AS VARCHAR) AS "traininghours",
        CAST("trainingprogramtypeid" AS VARCHAR) AS "trainingprogramtypeid",
        CAST("isallowpapers" AS VARCHAR) AS "isallowpapers",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("updatedon" AS VARCHAR) AS "updatedon",
        CAST("mis_source_table" AS VARCHAR) AS "mis_source_table",
        CAST("certificate_external_id" AS VARCHAR) AS "certificate_external_id",
        CAST("certificate_name" AS VARCHAR) AS "certificate_name",
        CAST("category" AS VARCHAR) AS "category",
        CAST("broad" AS VARCHAR) AS "broad",
        CAST("detailed" AS VARCHAR) AS "detailed",
        CAST("narrow" AS VARCHAR) AS "narrow",
        CAST("awarding_body" AS VARCHAR) AS "awarding_body",
        CAST("employment_required" AS VARCHAR) AS "employment_required",
        CAST("basic_skill" AS VARCHAR) AS "basic_skill",
        CAST("certificate_status" AS VARCHAR) AS "certificate_status",
        CAST("on_job_training" AS VARCHAR) AS "on_job_training",
        CAST("tpcs" AS VARCHAR) AS "tpcs",
        CAST("tws" AS VARCHAR) AS "tws",
        CAST("targeted_learners_employee" AS VARCHAR) AS "targeted_learners_employee",
        CAST("targeted_learners_entrepreneur" AS VARCHAR) AS "targeted_learners_entrepreneur",
        CAST("targeted_learners_jobseeker" AS VARCHAR) AS "targeted_learners_jobseeker",
        CAST("targeted_learners_student" AS VARCHAR) AS "targeted_learners_student",
        CAST("study_type_self_study" AS VARCHAR) AS "study_type_self_study",
        CAST("study_type_online" AS VARCHAR) AS "study_type_online",
        CAST("study_type_blended_learning" AS VARCHAR) AS "study_type_blended_learning",
        CAST("study_type_local_training_provider" AS VARCHAR) AS "study_type_local_training_provider",
        CAST("study_type_in_house" AS VARCHAR) AS "study_type_in_house",
        CAST("deactivate_flag" AS VARCHAR) AS "deactivate_flag",
        CAST("payment_structure" AS VARCHAR) AS "payment_structure",
        CAST("cap_type" AS VARCHAR) AS "cap_type",
        CAST("old_type" AS VARCHAR) AS "old_type",
        CAST("level" AS VARCHAR) AS "level",
        CAST("level_qcf" AS VARCHAR) AS "level_qcf",
        CAST("certificate_type" AS VARCHAR) AS "certificate_type",
        CAST("status_reason" AS VARCHAR) AS "status_reason",
        CAST("state" AS VARCHAR) AS "state",
        CAST("cap" AS VARCHAR) AS "cap",
        CAST("average_contact_hours" AS VARCHAR) AS "average_contact_hours",
        CAST("no_of_applicants" AS VARCHAR) AS "no_of_applicants",
        CAST("tamkeen_support_pct" AS VARCHAR) AS "tamkeen_support_pct",
        CAST("price" AS VARCHAR) AS "price",
        CAST("price_per_hour_per_person" AS VARCHAR) AS "price_per_hour_per_person",
        CAST("duration" AS VARCHAR) AS "duration",
        CAST("no_of_hours" AS VARCHAR) AS "no_of_hours",
        CAST("exchange_rate" AS VARCHAR) AS "exchange_rate",
        CAST("total_individual_applications_approved" AS VARCHAR) AS "total_individual_applications_approved",
        CAST("total_individual_applications_approved_state" AS VARCHAR) AS "total_individual_applications_approved_state",
        CAST("total_tws_enrollments_approved" AS VARCHAR) AS "total_tws_enrollments_approved",
        CAST("total_tws_enrollments_approved_state" AS VARCHAR) AS "total_tws_enrollments_approved_state",
        CAST("total_no_of_applicants" AS VARCHAR) AS "total_no_of_applicants",
        CAST("total_tws_enrollments_last_updated_on" AS VARCHAR) AS "total_tws_enrollments_last_updated_on",
        CAST("total_individual_applications_last_updated_on" AS VARCHAR) AS "total_individual_applications_last_updated_on",
        CAST("overview" AS VARCHAR) AS "overview",
        CAST("tamkeen_eligibility_criteria" AS VARCHAR) AS "tamkeen_eligibility_criteria",
        CAST("analyst_note" AS VARCHAR) AS "analyst_note",
        CAST("crt" AS VARCHAR) AS "crt",
        CAST("certificate_website" AS VARCHAR) AS "certificate_website",
        CAST("nvq_or_other_levels" AS VARCHAR) AS "nvq_or_other_levels",
        CAST("availability_of_assessment" AS VARCHAR) AS "availability_of_assessment",
        CAST("expiration_training_hours" AS VARCHAR) AS "expiration_training_hours",
        CAST("certificate_expiration_id" AS VARCHAR) AS "certificate_expiration_id",
        CAST("owner_name" AS VARCHAR) AS "owner_name",
        CAST("created_by" AS VARCHAR) AS "created_by",
        CAST("modified_by" AS VARCHAR) AS "modified_by",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("modified_on" AS VARCHAR) AS "modified_on",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("report_date" AS VARCHAR) AS "report_date",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST("certificate_id" AS VARCHAR) AS "certificate_id",
        CAST("trainingprogramid" AS VARCHAR) AS "trainingprogramid",
        CAST("traininghours" AS VARCHAR) AS "traininghours",
        CAST("trainingprogramtypeid" AS VARCHAR) AS "trainingprogramtypeid",
        CAST("isallowpapers" AS VARCHAR) AS "isallowpapers",
        CAST("createdon" AS VARCHAR) AS "createdon",
        CAST("updatedon" AS VARCHAR) AS "updatedon",
        CAST("mis_source_table" AS VARCHAR) AS "mis_source_table",
        CAST("certificate_external_id" AS VARCHAR) AS "certificate_external_id",
        CAST("certificate_name" AS VARCHAR) AS "certificate_name",
        CAST("category" AS VARCHAR) AS "category",
        CAST("broad" AS VARCHAR) AS "broad",
        CAST("detailed" AS VARCHAR) AS "detailed",
        CAST("narrow" AS VARCHAR) AS "narrow",
        CAST("awarding_body" AS VARCHAR) AS "awarding_body",
        CAST("employment_required" AS VARCHAR) AS "employment_required",
        CAST("basic_skill" AS VARCHAR) AS "basic_skill",
        CAST("certificate_status" AS VARCHAR) AS "certificate_status",
        CAST("on_job_training" AS VARCHAR) AS "on_job_training",
        CAST("tpcs" AS VARCHAR) AS "tpcs",
        CAST("tws" AS VARCHAR) AS "tws",
        CAST("targeted_learners_employee" AS VARCHAR) AS "targeted_learners_employee",
        CAST("targeted_learners_entrepreneur" AS VARCHAR) AS "targeted_learners_entrepreneur",
        CAST("targeted_learners_jobseeker" AS VARCHAR) AS "targeted_learners_jobseeker",
        CAST("targeted_learners_student" AS VARCHAR) AS "targeted_learners_student",
        CAST("study_type_self_study" AS VARCHAR) AS "study_type_self_study",
        CAST("study_type_online" AS VARCHAR) AS "study_type_online",
        CAST("study_type_blended_learning" AS VARCHAR) AS "study_type_blended_learning",
        CAST("study_type_local_training_provider" AS VARCHAR) AS "study_type_local_training_provider",
        CAST("study_type_in_house" AS VARCHAR) AS "study_type_in_house",
        CAST("deactivate_flag" AS VARCHAR) AS "deactivate_flag",
        CAST("payment_structure" AS VARCHAR) AS "payment_structure",
        CAST("cap_type" AS VARCHAR) AS "cap_type",
        CAST("old_type" AS VARCHAR) AS "old_type",
        CAST("level" AS VARCHAR) AS "level",
        CAST("level_qcf" AS VARCHAR) AS "level_qcf",
        CAST("certificate_type" AS VARCHAR) AS "certificate_type",
        CAST("status_reason" AS VARCHAR) AS "status_reason",
        CAST("state" AS VARCHAR) AS "state",
        CAST("cap" AS VARCHAR) AS "cap",
        CAST("average_contact_hours" AS VARCHAR) AS "average_contact_hours",
        CAST("no_of_applicants" AS VARCHAR) AS "no_of_applicants",
        CAST("tamkeen_support_pct" AS VARCHAR) AS "tamkeen_support_pct",
        CAST("price" AS VARCHAR) AS "price",
        CAST("price_per_hour_per_person" AS VARCHAR) AS "price_per_hour_per_person",
        CAST("duration" AS VARCHAR) AS "duration",
        CAST("no_of_hours" AS VARCHAR) AS "no_of_hours",
        CAST("exchange_rate" AS VARCHAR) AS "exchange_rate",
        CAST("total_individual_applications_approved" AS VARCHAR) AS "total_individual_applications_approved",
        CAST("total_individual_applications_approved_state" AS VARCHAR) AS "total_individual_applications_approved_state",
        CAST("total_tws_enrollments_approved" AS VARCHAR) AS "total_tws_enrollments_approved",
        CAST("total_tws_enrollments_approved_state" AS VARCHAR) AS "total_tws_enrollments_approved_state",
        CAST("total_no_of_applicants" AS VARCHAR) AS "total_no_of_applicants",
        CAST("total_tws_enrollments_last_updated_on" AS VARCHAR) AS "total_tws_enrollments_last_updated_on",
        CAST("total_individual_applications_last_updated_on" AS VARCHAR) AS "total_individual_applications_last_updated_on",
        CAST("overview" AS VARCHAR) AS "overview",
        CAST("tamkeen_eligibility_criteria" AS VARCHAR) AS "tamkeen_eligibility_criteria",
        CAST("analyst_note" AS VARCHAR) AS "analyst_note",
        CAST("crt" AS VARCHAR) AS "crt",
        CAST("certificate_website" AS VARCHAR) AS "certificate_website",
        CAST("nvq_or_other_levels" AS VARCHAR) AS "nvq_or_other_levels",
        CAST("availability_of_assessment" AS VARCHAR) AS "availability_of_assessment",
        CAST("expiration_training_hours" AS VARCHAR) AS "expiration_training_hours",
        CAST("certificate_expiration_id" AS VARCHAR) AS "certificate_expiration_id",
        CAST("owner_name" AS VARCHAR) AS "owner_name",
        CAST("created_by" AS VARCHAR) AS "created_by",
        CAST("modified_by" AS VARCHAR) AS "modified_by",
        CAST("created_on" AS VARCHAR) AS "created_on",
        CAST("modified_on" AS VARCHAR) AS "modified_on",
        CAST("source_system_name" AS VARCHAR) AS "source_system_name",
        CAST("is_deleted" AS VARCHAR) AS "is_deleted",
        CAST("report_date" AS VARCHAR) AS "report_date",
        CAST("dbt_updated_at" AS VARCHAR) AS "dbt_updated_at"
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
        'certification_base' AS table_name,
        'record_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_layer) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_layer) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_layer) = (SELECT COUNT(*) FROM silver_layer) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'certification_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'certification_base' AS table_name,
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
        'certification_base' AS table_name,
        'mismatching_rows_bronze_minus_silver' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_minus_silver) AS BIGINT) AS bronze_layer_count,
        CAST(0 AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_minus_silver) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'certification_base' AS table_name,
        'mismatching_rows_silver_minus_bronze' AS validation_point,
        CAST(0 AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_minus_bronze) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM silver_minus_bronze) = 0 THEN 'PASS' ELSE 'FAIL' END AS validation_status
)

SELECT *
FROM validation_results
ORDER BY validation_point;
