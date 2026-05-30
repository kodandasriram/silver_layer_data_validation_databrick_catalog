WITH
bronze_layer AS (
-- Bronze-layer UNION ALL for certification_base across OS2, OS1, and MIS.
-- Output column order follows the dbt model: certification_base_union all.sql.
-- Source CTEs preserve the standalone source joins/functionality; the dbt union mapping supplies typed NULLs.

WITH certification_base_mis_source AS (
WITH option_set_values AS (
    SELECT
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING) AS option_key,
        max(sm.value) AS option_value
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.STRINGMAP sm
    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.ENTITYLOGICALVIEW elv
        ON sm.objecttypecode = elv.objecttypecode
    WHERE sm.attributevalue IS NOT NULL
      AND sm.value IS NOT NULL
    GROUP BY
        lower(elv.name) || '|' || lower(sm.attributename) || '|' || CAST(sm.attributevalue AS STRING)
),
option_set_map AS (
    SELECT map_from_entries(collect_list(named_struct('key', option_key, 'value', option_value))) AS option_values
    FROM option_set_values
)
/*
============================================================================
silver_certification_mis.sql
============================================================================
Per-source intermediate Silver model for the Certification domain ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â MIS only.

Sources:
  ÃƒÂ¢Ã‹Å“Ã¢â‚¬Â¦ mis_certificate          ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â anchor: certificate entity itself
    mis_certificateexprie    ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â joined: certificate-expiration / approval data
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
record). The cardinality may be 1:N ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â review with team if duplicate cert rows
appear in the output.
============================================================================
*/

SELECT
    'mis_certificate' AS mis_source_table,

    -- Identifiers
    CAST(cert.mis_certificateid AS STRING)              AS certificate_id,
    cert.tmkn_id                                         AS certificate_external_id,
    cert.mis_name                                        AS certificate_name,

    -- Hierarchical classification (denormalised at source)
    cert.mis_category                               AS category,
    cert.mis_broad                                  AS broad,
    cert.mis_detailed                                AS detailed,
    cert.mis_narrow                                 AS narrow,

    -- Awarding body
    --cert.mis_awardingbodyname                            AS awarding_body,
    CAST(NULL AS STRING) AS awarding_body,

    -- Option-set decoded fields (from RPT-061)
        

     CASE WHEN cert.mis_employmentrequired IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_employmentrequired') || '|' || CAST(cert.mis_employmentrequired AS STRING)) END AS employment_required, 
     CASE WHEN cert.mis_bs IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_bs') || '|' || CAST(cert.mis_bs AS STRING)) END AS basic_skill, 
     CASE WHEN cert.mis_certificatestatus IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_certificatestatus') || '|' || CAST(cert.mis_certificatestatus AS STRING)) END AS certificate_status, 
     CASE WHEN cert.mis_onjobtraining IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_onjobtraining') || '|' || CAST(cert.mis_onjobtraining AS STRING)) END AS on_job_training, 
     CASE WHEN cert.mis_tpcs IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_tpcs') || '|' || CAST(cert.mis_tpcs AS STRING)) END AS tpcs, 
     CASE WHEN cert.mis_tws IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_tws') || '|' || CAST(cert.mis_tws AS STRING)) END AS tws, 
     CASE WHEN cert.tmkn_targetedlearners_employee IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_employee') || '|' || CAST(cert.tmkn_targetedlearners_employee AS STRING)) END AS targeted_learners_employee, 
     CASE WHEN cert.tmkn_targetedlearners_entrepreneur IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_entrepreneur') || '|' || CAST(cert.tmkn_targetedlearners_entrepreneur AS STRING)) END AS targeted_learners_entrepreneur, 
     CASE WHEN cert.tmkn_targetedlearners_jobseeker IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_jobseeker') || '|' || CAST(cert.tmkn_targetedlearners_jobseeker AS STRING)) END AS targeted_learners_jobseeker, 
     CASE WHEN cert.tmkn_targetedlearners_student IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_targetedlearners_student') || '|' || CAST(cert.tmkn_targetedlearners_student AS STRING)) END AS targeted_learners_student, 
     CASE WHEN cert.tmkn_studytype_selfstudy IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_selfstudy') || '|' || CAST(cert.tmkn_studytype_selfstudy AS STRING)) END AS study_type_self_study, 
     CASE WHEN cert.tmkn_studytype_online IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_online') || '|' || CAST(cert.tmkn_studytype_online AS STRING)) END AS study_type_online, 
     CASE WHEN cert.tmkn_studytype_blendedlearning IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_blendedlearning') || '|' || CAST(cert.tmkn_studytype_blendedlearning AS STRING)) END AS study_type_blended_learning, 
     CASE WHEN cert.tmkn_studytype_localtrainingprovider IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_localtrainingprovider') || '|' || CAST(cert.tmkn_studytype_localtrainingprovider AS STRING)) END AS study_type_local_training_provider, 
     CASE WHEN cert.tmkn_studytype_inhouse IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_studytype_inhouse') || '|' || CAST(cert.tmkn_studytype_inhouse AS STRING)) END AS study_type_in_house, 
     CASE WHEN cert.nfc_deactivateflag IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('nfc_deactivateflag') || '|' || CAST(cert.nfc_deactivateflag AS STRING)) END AS deactivate_flag, 
     CASE WHEN cert.mis_payment_structure IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_payment_structure') || '|' || CAST(cert.mis_payment_structure AS STRING)) END AS payment_structure, 
     CASE WHEN cert.mis_cap_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_cap_type') || '|' || CAST(cert.mis_cap_type AS STRING)) END AS cap_type, 
     CASE WHEN cert.mis_type IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_type') || '|' || CAST(cert.mis_type AS STRING)) END AS old_type, 
     CASE WHEN cert.mis_level IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_level') || '|' || CAST(cert.mis_level AS STRING)) END AS level, 
     CASE WHEN cert.mis_levelqcf IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('mis_levelqcf') || '|' || CAST(cert.mis_levelqcf AS STRING)) END AS level_qcf, 
     CASE WHEN cert.tmkn_certificatetype IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('tmkn_certificatetype') || '|' || CAST(cert.tmkn_certificatetype AS STRING)) END AS certificate_type, 
     CASE WHEN cert.statuscode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('statuscode') || '|' || CAST(cert.statuscode AS STRING)) END AS status_reason, 
     CASE WHEN cert.statecode IS NULL THEN NULL ELSE element_at((SELECT option_values FROM option_set_map), lower('mis_certificate') || '|' || lower('statecode') || '|' || CAST(cert.statecode AS STRING)) END AS state, 

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

    -- Aggregated counts (these are pre-computed in source ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â keep as-is in Silver)
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
    CAST(crtexp.mis_certificateexprieid AS STRING)      AS certificate_expiration_id,

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
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_CERTIFICATEBASE cert
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.MIS_CERTIFICATEEXPRIEBASE crtexp
       ON crtexp.mis_certificateexprieid = cert.mis_certificateid
   -- NOTE: This join direction may need verification with the team.
   -- In RPT-051, the relationship is keyed via the training enrollment
   -- (trn.tws_certificate_approval = crtexp.mis_certificateexprieId).
   -- If a certificate has multiple expiration records, this join may
   -- produce duplicate cert rows ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â review and adjust with QUALIFY ROW_NUMBER
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
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at,
    ROW_NUMBER() OVER (PARTITION BY cert.id ORDER BY cert.updatedon DESC NULLS LAST, cert.createdon DESC NULLS LAST) AS rnk
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.OSUSR_VW9_CERTIFICATION cert
)
select cert.id,
    cert.trainingprogramid,
    cert.traininghours,
    cert.trainingprogramtypeid,
    cert.isallowpapers,
    TRY_CAST(NULLIF(CAST(cert.createdon AS STRING), '') AS TIMESTAMP) as createdon,
    TRY_CAST(NULLIF(CAST(cert.updatedon AS STRING), '') AS TIMESTAMP) as updatedon,
    cert.is_deleted,
    UPPER(NULLIF(TRIM(CAST(cert.source_system_name AS STRING)), '')) as source_system_name,
    cert.dbt_updated_at from source_cte cert
WHERE rnk=1
)
SELECT

    -- OS2 columns
    CAST(certificate_id AS STRING) AS certificate_id,
    CAST(NULL AS BIGINT)            AS trainingprogramid,
    CAST(NULL AS BIGINT)            AS traininghours,
    CAST(NULL AS STRING)           AS trainingprogramtypeid,
    CAST(NULL AS BOOLEAN)           AS isallowpapers,
    CAST(NULL AS TIMESTAMP)      AS createdon,
    CAST(NULL AS TIMESTAMP)      AS updatedon,

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
    UPPER(TRIM(CAST(source_system_name AS STRING))) AS source_system_name,
    is_deleted,
    TRY_CAST(NULLIF(CAST(report_date AS STRING), '') AS TIMESTAMP) AS report_date,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS STRING), '') AS TIMESTAMP) AS dbt_updated_at

from certification_base_mis_source

UNION ALL

SELECT

    -- OS2 columns
    CAST(id AS STRING)             AS certificate_id,
    trainingprogramid,
    traininghours,
    trainingprogramtypeid,
    isallowpapers,
    createdon,
    updatedon,

    -- MIS columns
    CAST(NULL AS STRING)           AS mis_source_table,
    CAST(NULL AS STRING)           AS certificate_external_id,
    CAST(NULL AS STRING)           AS certificate_name,
    CAST(NULL AS STRING)           AS category,
    CAST(NULL AS STRING)           AS broad,
    CAST(NULL AS STRING)           AS detailed,
    CAST(NULL AS STRING)           AS narrow,
    CAST(NULL AS STRING)           AS awarding_body,
    CAST(NULL AS STRING)           AS employment_required,
    CAST(NULL AS STRING)           AS basic_skill,
    CAST(NULL AS STRING)           AS certificate_status,
    CAST(NULL AS STRING)           AS on_job_training,
    CAST(NULL AS STRING)           AS tpcs,
    CAST(NULL AS STRING)           AS tws,
    CAST(NULL AS STRING)           AS targeted_learners_employee,
    CAST(NULL AS STRING)           AS targeted_learners_entrepreneur,
    CAST(NULL AS STRING)           AS targeted_learners_jobseeker,
    CAST(NULL AS STRING)           AS targeted_learners_student,
    CAST(NULL AS STRING)           AS study_type_self_study,
    CAST(NULL AS STRING)           AS study_type_online,
    CAST(NULL AS STRING)           AS study_type_blended_learning,
    CAST(NULL AS STRING)           AS study_type_local_training_provider,
    CAST(NULL AS STRING)           AS study_type_in_house,
    CAST(NULL AS STRING)           AS deactivate_flag,
    CAST(NULL AS STRING)           AS payment_structure,
    CAST(NULL AS STRING)           AS cap_type,
    CAST(NULL AS STRING)           AS old_type,
    CAST(NULL AS STRING)           AS level,
    CAST(NULL AS STRING)           AS level_qcf,
    CAST(NULL AS STRING)           AS certificate_type,
    CAST(NULL AS STRING)           AS status_reason,
    CAST(NULL AS STRING)           AS state,
    CAST(NULL AS DECIMAL(23,10))    AS cap,
    CAST(NULL AS BIGINT)            AS average_contact_hours,
    CAST(NULL AS BIGINT)            AS no_of_applicants,
    CAST(NULL AS BIGINT)            AS tamkeen_support_pct,
    CAST(NULL AS DECIMAL(19,4))     AS price,
    CAST(NULL AS DECIMAL(19,4))     AS price_per_hour_per_person,
    CAST(NULL AS STRING)           AS duration,
    CAST(NULL AS STRING)           AS no_of_hours,
    CAST(NULL AS DECIMAL(23,10))    AS exchange_rate,
    CAST(NULL AS BIGINT)            AS total_individual_applications_approved,
    CAST(NULL AS BIGINT)            AS total_individual_applications_approved_state,
    CAST(NULL AS BIGINT)            AS total_tws_enrollments_approved,
    CAST(NULL AS BIGINT)            AS total_tws_enrollments_approved_state,
    CAST(NULL AS BIGINT)            AS total_no_of_applicants,
    CAST(NULL AS TIMESTAMP)      AS total_tws_enrollments_last_updated_on,
    CAST(NULL AS TIMESTAMP)      AS total_individual_applications_last_updated_on,
    CAST(NULL AS STRING)           AS overview,
    CAST(NULL AS STRING)           AS tamkeen_eligibility_criteria,
    CAST(NULL AS STRING)           AS analyst_note,
    CAST(NULL AS STRING)           AS crt,
    CAST(NULL AS STRING)           AS certificate_website,
    CAST(NULL AS STRING)           AS nvq_or_other_levels,
    CAST(NULL AS STRING)           AS availability_of_assessment,
    CAST(NULL AS BIGINT)            AS expiration_training_hours,
    CAST(NULL AS STRING)           AS certificate_expiration_id,
    CAST(NULL AS STRING)           AS owner_name,
    CAST(NULL AS STRING)           AS created_by,
    CAST(NULL AS STRING)           AS modified_by,
    CAST(NULL AS TIMESTAMP)      AS created_on,
    CAST(NULL AS TIMESTAMP)      AS modified_on,

    -- Common columns
    UPPER(TRIM(CAST(source_system_name AS STRING))) AS source_system_name,
    is_deleted,
    CAST(NULL AS TIMESTAMP)      AS report_date,
    TRY_CAST(NULLIF(CAST(dbt_updated_at AS STRING), '') AS TIMESTAMP) AS dbt_updated_at

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
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.certification_base
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
        CAST(`certificate_id` AS STRING) AS `certificate_id`,
        CAST(`trainingprogramid` AS STRING) AS `trainingprogramid`,
        CAST(`traininghours` AS STRING) AS `traininghours`,
        CAST(`trainingprogramtypeid` AS STRING) AS `trainingprogramtypeid`,
        CAST(`isallowpapers` AS STRING) AS `isallowpapers`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`certificate_external_id` AS STRING) AS `certificate_external_id`,
        CAST(`certificate_name` AS STRING) AS `certificate_name`,
        CAST(`category` AS STRING) AS `category`,
        CAST(`broad` AS STRING) AS `broad`,
        CAST(`detailed` AS STRING) AS `detailed`,
        CAST(`narrow` AS STRING) AS `narrow`,
        CAST(`awarding_body` AS STRING) AS `awarding_body`,
        CAST(`employment_required` AS STRING) AS `employment_required`,
        CAST(`basic_skill` AS STRING) AS `basic_skill`,
        CAST(`certificate_status` AS STRING) AS `certificate_status`,
        CAST(`on_job_training` AS STRING) AS `on_job_training`,
        CAST(`tpcs` AS STRING) AS `tpcs`,
        CAST(`tws` AS STRING) AS `tws`,
        CAST(`targeted_learners_employee` AS STRING) AS `targeted_learners_employee`,
        CAST(`targeted_learners_entrepreneur` AS STRING) AS `targeted_learners_entrepreneur`,
        CAST(`targeted_learners_jobseeker` AS STRING) AS `targeted_learners_jobseeker`,
        CAST(`targeted_learners_student` AS STRING) AS `targeted_learners_student`,
        CAST(`study_type_self_study` AS STRING) AS `study_type_self_study`,
        CAST(`study_type_online` AS STRING) AS `study_type_online`,
        CAST(`study_type_blended_learning` AS STRING) AS `study_type_blended_learning`,
        CAST(`study_type_local_training_provider` AS STRING) AS `study_type_local_training_provider`,
        CAST(`study_type_in_house` AS STRING) AS `study_type_in_house`,
        CAST(`deactivate_flag` AS STRING) AS `deactivate_flag`,
        CAST(`payment_structure` AS STRING) AS `payment_structure`,
        CAST(`cap_type` AS STRING) AS `cap_type`,
        CAST(`old_type` AS STRING) AS `old_type`,
        CAST(`level` AS STRING) AS `level`,
        CAST(`level_qcf` AS STRING) AS `level_qcf`,
        CAST(`certificate_type` AS STRING) AS `certificate_type`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`cap` AS STRING) AS `cap`,
        CAST(`average_contact_hours` AS STRING) AS `average_contact_hours`,
        CAST(`no_of_applicants` AS STRING) AS `no_of_applicants`,
        CAST(`tamkeen_support_pct` AS STRING) AS `tamkeen_support_pct`,
        CAST(`price` AS STRING) AS `price`,
        CAST(`price_per_hour_per_person` AS STRING) AS `price_per_hour_per_person`,
        CAST(`duration` AS STRING) AS `duration`,
        CAST(`no_of_hours` AS STRING) AS `no_of_hours`,
        CAST(`exchange_rate` AS STRING) AS `exchange_rate`,
        CAST(`total_individual_applications_approved` AS STRING) AS `total_individual_applications_approved`,
        CAST(`total_individual_applications_approved_state` AS STRING) AS `total_individual_applications_approved_state`,
        CAST(`total_tws_enrollments_approved` AS STRING) AS `total_tws_enrollments_approved`,
        CAST(`total_tws_enrollments_approved_state` AS STRING) AS `total_tws_enrollments_approved_state`,
        CAST(`total_no_of_applicants` AS STRING) AS `total_no_of_applicants`,
        CAST(`total_tws_enrollments_last_updated_on` AS STRING) AS `total_tws_enrollments_last_updated_on`,
        CAST(`total_individual_applications_last_updated_on` AS STRING) AS `total_individual_applications_last_updated_on`,
        CAST(`overview` AS STRING) AS `overview`,
        CAST(`tamkeen_eligibility_criteria` AS STRING) AS `tamkeen_eligibility_criteria`,
        CAST(`analyst_note` AS STRING) AS `analyst_note`,
        CAST(`crt` AS STRING) AS `crt`,
        CAST(`certificate_website` AS STRING) AS `certificate_website`,
        CAST(`nvq_or_other_levels` AS STRING) AS `nvq_or_other_levels`,
        CAST(`availability_of_assessment` AS STRING) AS `availability_of_assessment`,
        CAST(`expiration_training_hours` AS STRING) AS `expiration_training_hours`,
        CAST(`certificate_expiration_id` AS STRING) AS `certificate_expiration_id`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`certificate_id` AS STRING) AS `certificate_id`,
        CAST(`trainingprogramid` AS STRING) AS `trainingprogramid`,
        CAST(`traininghours` AS STRING) AS `traininghours`,
        CAST(`trainingprogramtypeid` AS STRING) AS `trainingprogramtypeid`,
        CAST(`isallowpapers` AS STRING) AS `isallowpapers`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`mis_source_table` AS STRING) AS `mis_source_table`,
        CAST(`certificate_external_id` AS STRING) AS `certificate_external_id`,
        CAST(`certificate_name` AS STRING) AS `certificate_name`,
        CAST(`category` AS STRING) AS `category`,
        CAST(`broad` AS STRING) AS `broad`,
        CAST(`detailed` AS STRING) AS `detailed`,
        CAST(`narrow` AS STRING) AS `narrow`,
        CAST(`awarding_body` AS STRING) AS `awarding_body`,
        CAST(`employment_required` AS STRING) AS `employment_required`,
        CAST(`basic_skill` AS STRING) AS `basic_skill`,
        CAST(`certificate_status` AS STRING) AS `certificate_status`,
        CAST(`on_job_training` AS STRING) AS `on_job_training`,
        CAST(`tpcs` AS STRING) AS `tpcs`,
        CAST(`tws` AS STRING) AS `tws`,
        CAST(`targeted_learners_employee` AS STRING) AS `targeted_learners_employee`,
        CAST(`targeted_learners_entrepreneur` AS STRING) AS `targeted_learners_entrepreneur`,
        CAST(`targeted_learners_jobseeker` AS STRING) AS `targeted_learners_jobseeker`,
        CAST(`targeted_learners_student` AS STRING) AS `targeted_learners_student`,
        CAST(`study_type_self_study` AS STRING) AS `study_type_self_study`,
        CAST(`study_type_online` AS STRING) AS `study_type_online`,
        CAST(`study_type_blended_learning` AS STRING) AS `study_type_blended_learning`,
        CAST(`study_type_local_training_provider` AS STRING) AS `study_type_local_training_provider`,
        CAST(`study_type_in_house` AS STRING) AS `study_type_in_house`,
        CAST(`deactivate_flag` AS STRING) AS `deactivate_flag`,
        CAST(`payment_structure` AS STRING) AS `payment_structure`,
        CAST(`cap_type` AS STRING) AS `cap_type`,
        CAST(`old_type` AS STRING) AS `old_type`,
        CAST(`level` AS STRING) AS `level`,
        CAST(`level_qcf` AS STRING) AS `level_qcf`,
        CAST(`certificate_type` AS STRING) AS `certificate_type`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`cap` AS STRING) AS `cap`,
        CAST(`average_contact_hours` AS STRING) AS `average_contact_hours`,
        CAST(`no_of_applicants` AS STRING) AS `no_of_applicants`,
        CAST(`tamkeen_support_pct` AS STRING) AS `tamkeen_support_pct`,
        CAST(`price` AS STRING) AS `price`,
        CAST(`price_per_hour_per_person` AS STRING) AS `price_per_hour_per_person`,
        CAST(`duration` AS STRING) AS `duration`,
        CAST(`no_of_hours` AS STRING) AS `no_of_hours`,
        CAST(`exchange_rate` AS STRING) AS `exchange_rate`,
        CAST(`total_individual_applications_approved` AS STRING) AS `total_individual_applications_approved`,
        CAST(`total_individual_applications_approved_state` AS STRING) AS `total_individual_applications_approved_state`,
        CAST(`total_tws_enrollments_approved` AS STRING) AS `total_tws_enrollments_approved`,
        CAST(`total_tws_enrollments_approved_state` AS STRING) AS `total_tws_enrollments_approved_state`,
        CAST(`total_no_of_applicants` AS STRING) AS `total_no_of_applicants`,
        CAST(`total_tws_enrollments_last_updated_on` AS STRING) AS `total_tws_enrollments_last_updated_on`,
        CAST(`total_individual_applications_last_updated_on` AS STRING) AS `total_individual_applications_last_updated_on`,
        CAST(`overview` AS STRING) AS `overview`,
        CAST(`tamkeen_eligibility_criteria` AS STRING) AS `tamkeen_eligibility_criteria`,
        CAST(`analyst_note` AS STRING) AS `analyst_note`,
        CAST(`crt` AS STRING) AS `crt`,
        CAST(`certificate_website` AS STRING) AS `certificate_website`,
        CAST(`nvq_or_other_levels` AS STRING) AS `nvq_or_other_levels`,
        CAST(`availability_of_assessment` AS STRING) AS `availability_of_assessment`,
        CAST(`expiration_training_hours` AS STRING) AS `expiration_training_hours`,
        CAST(`certificate_expiration_id` AS STRING) AS `certificate_expiration_id`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`modified_by` AS STRING) AS `modified_by`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`modified_on` AS STRING) AS `modified_on`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
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
