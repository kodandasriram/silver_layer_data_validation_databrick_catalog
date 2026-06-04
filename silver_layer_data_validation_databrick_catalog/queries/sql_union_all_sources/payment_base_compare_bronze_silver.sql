-- Compare bronze-layer query output with silver-layer table output for payment_base.
-- Validations included:
--   1. Record counts for bronze_layer and silver_layer.
--   2. Column counts for bronze_layer and silver_layer.
--   3. Column name/order match flag.
--   4. Mismatching row counts in each direction after casting all compared columns to STRING.
--
-- Bronze source: C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\Requirements\Silver Layer\Union All sources\Silver_layer_03-June-2026\converted db script to databricks\Databricks_union of all sources\payment_base.sql
-- Silver source: converted db script to databricks\silver_layer scripts\payment_base_silver_layer.sql

WITH
bronze_layer AS (
/*
Generated Databricks union layer for payment_base.
Column order and typed NULL placeholders follow dbt model: payment_base.sql.
Source transformations are embedded whole from the converted Databricks OS1/OS2/MIS scripts.
dbt macros expanded to Databricks TRY_CAST / string cleanup expressions.
*/

/*
 =================================================================================================
 
Name        : PAYMENT_BASE
Description : This model extracts and transforms enterprise training-related
              attributes from the OS2 source system Bronze Layer and loads them
              into the TRAINING_ENTERPRISE target table as part of the Silver
              Layer data pipeline.
 
              The model enriches training application support records with
              application, assessment, customer, training program, provider,
              certification, and payment-related details. It standardizes
              training dates, financial metrics, share percentages, and
              workflow statuses for enterprise training analytics and reporting.
 
Source Tables : payment_base_os1
                payment_base_os2
                payment_base_mis
 
Target Table : PAYMENT_BASE
Load Type    : Full Load
Materialized : table
Format       : PARQUET
Tags         : daily
 
Revision History:
--------------------------------------------------------------
 
Version | Date       | Author     | Description
--------------------------------------------------------------
1.0     | 2026-05-13 | Vignesh  | Initial version
 
================================================================================================= 
*/



WITH
    payment_base_os1 AS (
/*
============================================================================
silver_payment_os1.sql
============================================================================
Per-source intermediate Silver model for the Payment domain â€” OS1 only.

Source: OSUSR_PX1_PAYMENT (anchor)
Reference SPs:
  - RPT-180_neoTamkeen_Applications_Payment   (thin slice â€” merged in)
  - RPT-192_neoTamkeen_Payments               (canonical wide payment view â€”
                                                 itself a 2-branch UNION ALL)
  - RPT-187, RPT-197 (status history)         â†’ handled in silver_workflow_os1

Structure decision:

  RPT-192 has TWO branches, distinguished by program type:

  Branch 1 â€” REGULAR PAYMENTS:
    Filter: payment_reference NOT LIKE 'TP%' OR program_id = 4 (Train & Place)
    Total Amount source: pay.TOTALAMOUNT
    Item-level columns: from pay row directly
    Training payment type: NULL

  Branch 2 â€” TRAIN-AND-PLACE TRAINING PAYMENTS:
    Filter: payment_reference LIKE 'TP%' AND program_id <> 4
    Total Amount source: ASD.TAMKEENSHARE (joined via APPLICATIONSUPPORTDETAILS)
    Item-level columns: from APPLICATIONEMPLOYEETRAINING (AET)
    Training payment type: from AET.TRAININGPAYMENTTYPEID

  Both branches share the same payment row but pull amounts/items from
  different sources. They are mutually exclusive (the WHERE clauses partition
  the population), so UNION ALL is correct â€” no row is in both branches.

  The payment_subtype column identifies which branch:
    - REGULAR_PAYMENT
    - TRAIN_AND_PLACE_TRAINING_PAYMENT

Note on the #Qust temp table in RPT-192: this CTE filters application
answers to a specific SubQuestion (TM_SQ8) and joins it into the SELECT.
But the joined column `CurSt` is never selected in the final output â€”
it appears to be dead code. Omitted here.

Lookups joined inline (both branches):
  - OSUSR_PX1_PAYMENTREQUESTSTATUS        â†’ workflow status label
  - OSUSR_PX1_PAYEE                       â†’ payee label
  - OSUSR_PX1_PAYMENTTYPE                 â†’ payment type ('Fawateer' etc.)
  - OSUSR_PX1_PAYMENTTOVENDORTYPE         â†’ payment-to-vendor type (share)
  - OSUSR_PX1_PROGRAM, OSUSR_PX1_PROGRAMTYPE â†’ program context
  - OSUSR_PX1_APPLICATIONSTATUS           â†’ app workflow status
  - OSUSR_PX1_ERPBATCH21, OSUSR_PX1_ERPINVOICE21 â†’ ERP traceability
  - OSUSR_PX1_PAYMENTREQUESTTYPE21        â†’ payment request type
  - OSUSR_PX1_SUPPORT                     â†’ support category
  - OSUSR_PX1_TRAININGPROVIDER, OSUSR_D1O_VENDOR â†’ counterparties
  - ossys_User (Ã—7)                       â†’ various people roles

Cross-domain note: APPLICATIONID, IBANID, VENDORID, TRAININGPROVIDERID,
and the 7 user FKs are preserved for downstream re-joining.
============================================================================
*/


-- ============================================================================
-- BRANCH 1: Regular payments (most payment requests)
-- Filter mirrors RPT-192 branch 1: PAYMENTREFERENCENUMBER NOT LIKE 'TP%' OR PROGRAMID = 4
-- ============================================================================
SELECT
    'REGULAR_PAYMENT' AS payment_subtype,
    'OSUSR_PX1_PAYMENT' AS os1_source_table,

    -- Identifiers
    pay.ID                                                                AS payment_id,
    pay.PAYMENTREFERENCENUMBER                                            AS payment_no,
    COALESCE(pay.PAYMENTREFERENCENUMBER, '') || '-' ||
    COALESCE(App.IDENTIFIER, '')                                          AS payment_no_with_app_ref,

    -- Foreign keys preserved for cross-domain re-joining
    pay.APPLICATIONID                                                     AS application_id,
    pay.IBANID                                                            AS iban_id,
    pay.PAYEEID                                                           AS payee_id,
    pay.PAYMENTTYPEID                                                     AS payment_type_id,
    pay.PAYMENTREQUESTSTATUSID                                            AS payment_status_id,
    pay.PAYMENTTOVENDORTYPEID                                             AS payment_to_vendor_type_id,
    pay.ERPBATCHID                                                        AS erp_batch_id,
    pay.ERPINVOICEID                                                      AS erp_invoice_id,
    pay.PAYMENTREQUESTTYPE                                                AS payment_request_type_id,
    pay.SUPPORTCATEGORY                                                   AS support_category_id,
    pay.TRAININGPROVIDERID                                                AS training_provider_id,
    pay.VENDORID                                                          AS vendor_id,
    pay.APPROVEDBY                                                        AS approved_by_user_id,
    pay.PAYMENTAUDITORID                                                  AS payment_auditor_user_id,
    pay.SENTBACKTOCUSTOMERBY                                              AS sent_back_by_user_id,
    pay.CORRECTIONTEAMMEMBERID                                            AS correction_team_user_id,
    pay.APPROVERID                                                        AS approver_user_id,
    pay.CUSTOMERID                                                        AS customer_user_id,
    pay.MONITORINGAPPROVERID                                              AS monitoring_approver_user_id,

    -- Decoded labels
    PayReqSta.LABEL                                                       AS workflow_status,
    PayVenTyp.LABEL                                                       AS payment_share,
    payee.LABEL                                                           AS payee,
    PayTyp.LABEL                                                          AS is_fawateer,
    PayReqTyp.LABEL                                                       AS payment_request_type,
    --Sup.LABEL                                                             AS support_category,
    Program.PROGRAMNAME                                                   AS program_name,
    PrgTyp.LABEL                                                          AS program_type_name,
    ast.LABEL                                                             AS app_status,
    App.IDENTIFIER                                                        AS app_ref,

    -- Amounts (regular branch â€” directly from pay row)
    pay.TOTALAMOUNT                                                       AS total_amount,
    Pay.TOTALITEMNETCOST                                                  AS total_item_cost,
    pay.TOTALVATAUMOUNT                                                   AS total_vat_amount,
    pay.TOTALITEMSTAMKEENSHARE                                            AS total_items_tamkeen_share,
    pay.TOTALITEMSAPPLICANTSHAREWVAT                                      AS total_items_applicant_share_with_vat,

    -- Dates (with sentinel-1900 handling)
    CASE WHEN pay.CREATEDON               = DATE '1900-01-01' THEN NULL ELSE pay.CREATEDON               END  AS created_on,
    CASE WHEN pay.SUBMITTEDTOCUSTOMERON   = DATE '1900-01-01' THEN NULL ELSE pay.SUBMITTEDTOCUSTOMERON   END  AS submitted_to_customer_on,
    CASE WHEN pay.SUBMITTEDON             = DATE '1900-01-01' THEN NULL ELSE pay.SUBMITTEDON             END  AS submitted_on,
    CASE WHEN pay.APPROVEDON              = DATE '1900-01-01' THEN NULL ELSE pay.APPROVEDON              END  AS approved_on,
    CASE WHEN pay.SENTBACKTOCUSTOMERON    = DATE '1900-01-01' THEN NULL ELSE pay.SENTBACKTOCUSTOMERON    END  AS sent_back_to_customer_on,
    CASE WHEN pay.UPDATEDON               = DATE '1900-01-01' THEN NULL ELSE pay.UPDATEDON               END  AS updated_on,
    CASE WHEN pay.DUEDATE                 = DATE '1900-01-01' THEN NULL ELSE pay.DUEDATE                 END  AS due_date,

    -- ERP traceability
    ErpBatch.NAME                                                         AS erp_batch,
    ErpInv.REFERENCE                                                      AS erp_invoice,

    -- Counterparties (denormalised at source)
    TP.CRNO                                                               AS training_provider_cr,
    TP.NAME                                                               AS training_provider_name,
    Vndr.VENDORCR                                                         AS vendor_cr,
    Vndr.NAME                                                             AS vendor_name,

    -- People
    Usr_AprvBy.NAME                                                       AS user_accountant,
    Usr_Adt.NAME                                                          AS user_payment_auditor,
    Usr_SB.NAME                                                           AS sent_back_to_customer_by,
    Usr_Cor.NAME                                                          AS user_correction,
    Usr_Aprv.NAME                                                         AS approved_by,
    Usr_Cust.NAME                                                         AS customer_name,

    -- Flags
    --CASE WHEN pay.CLAIMEDBYVENDOR  = 1 THEN TRUE ELSE FALSE END           AS claimed_by_vendor,
   -- CASE WHEN pay.ISNEEDATTENTION  = 1 THEN TRUE ELSE FALSE END           AS is_need_attention,
   COALESCE(pay.CLAIMEDBYVENDOR, FALSE) AS claimed_by_vendor,
   COALESCE(pay.ISNEEDATTENTION, FALSE) AS is_need_attention,

    -- Misc
    pay.SUBMISSIONCOUNT                                                   AS submission_count,

    -- Train-and-Place specific (NULL for regular branch)
    CAST(NULL AS STRING)                                                 AS training_payment_type,
	usrext.userid AS userid,

    -- Standard trailing audit columns
    'NEO1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENT` pay
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTREQUESTSTATUS`  PayReqSta  ON PayReqSta.ID  = pay.PAYMENTREQUESTSTATUSID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYEE`                 payee      ON payee.ID      = pay.PAYEEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTTYPE`           PayTyp     ON PayTyp.ID     = pay.PAYMENTTYPEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_APPLICATION`           App        ON App.ID        = pay.APPLICATIONID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PROGRAM`               Program    ON Program.ID    = App.PROGRAMID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PROGRAMTYPE`           PrgTyp     ON PrgTyp.ID     = Program.PROGRAMTYPEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_APPLICATIONSTATUS`     ast        ON ast.ID        = App.APPLICATIONSTATUSID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTTOVENDORTYPE`   PayVenTyp  ON PayVenTyp.ID  = pay.PAYMENTTOVENDORTYPEID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      Usr_AprvBy ON Usr_AprvBy.ID = pay.APPROVEDBY
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      Usr_Adt    ON Usr_Adt.ID    = pay.PAYMENTAUDITORID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      Usr_SB     ON Usr_SB.ID     = pay.SENTBACKTOCUSTOMERBY
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      Usr_Cor    ON Usr_Cor.ID    = pay.CORRECTIONTEAMMEMBERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      Usr_Aprv   ON Usr_Aprv.ID   = pay.APPROVERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                      Usr_Cust   ON Usr_Cust.ID   = pay.CUSTOMERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_ERPBATCH`            ErpBatch   ON ErpBatch.ID   = pay.ERPBATCHID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_ERPINVOICE`          ErpInv     ON ErpInv.ID     = pay.ERPINVOICEID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTREQUESTTYPE`  PayReqTyp  ON PayReqTyp.ID  = pay.PAYMENTREQUESTTYPE
/*LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_SUPPORT`               Sup        ON Sup.ID        = pay.SUPPORTCATEGORY*/
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_TRAININGPROVIDER`      TP         ON TP.ID         = pay.TRAININGPROVIDERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_D1O_VENDOR`                Vndr       ON Vndr.ID       = pay.VENDORID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USEREXTENSION` usrext
    ON usrext.USERID = App.USERID

WHERE pay.PAYMENTREFERENCENUMBER NOT LIKE 'TP%'
   OR App.PROGRAMID = 4   -- Train & Place program


UNION ALL


-- ============================================================================
-- BRANCH 2: Train-and-Place training-employee payments
-- Filter mirrors RPT-192 branch 2: PAYMENTREFERENCENUMBER LIKE 'TP%' AND PROGRAMID <> 4
-- Amounts pulled from APPLICATIONSUPPORTDETAILS + APPLICATIONEMPLOYEETRAINING
-- ============================================================================
SELECT
    'TRAIN_AND_PLACE_TRAINING_PAYMENT' AS payment_subtype,
    'OSUSR_PX1_PAYMENT' AS os1_source_table,

    -- Identifiers
    pay.ID                                                                AS payment_id,
    pay.PAYMENTREFERENCENUMBER                                            AS payment_no,
    COALESCE(pay.PAYMENTREFERENCENUMBER, '') || '-' ||
    COALESCE(App.IDENTIFIER, '')                                          AS payment_no_with_app_ref,

    -- Foreign keys
    pay.APPLICATIONID                                                     AS application_id,
    pay.IBANID                                                            AS iban_id,
    pay.PAYEEID                                                           AS payee_id,
    pay.PAYMENTTYPEID                                                     AS payment_type_id,
    pay.PAYMENTREQUESTSTATUSID                                            AS payment_status_id,
    pay.PAYMENTTOVENDORTYPEID                                             AS payment_to_vendor_type_id,
    pay.ERPBATCHID                                                        AS erp_batch_id,
    pay.ERPINVOICEID                                                      AS erp_invoice_id,
    pay.PAYMENTREQUESTTYPE                                                AS payment_request_type_id,
    pay.SUPPORTCATEGORY                                                   AS support_category_id,
    pay.TRAININGPROVIDERID                                                AS training_provider_id,
    pay.VENDORID                                                          AS vendor_id,
    pay.APPROVEDBY                                                        AS approved_by_user_id,
    pay.PAYMENTAUDITORID                                                  AS payment_auditor_user_id,
    pay.SENTBACKTOCUSTOMERBY                                              AS sent_back_by_user_id,
    pay.CORRECTIONTEAMMEMBERID                                            AS correction_team_user_id,
    pay.APPROVERID                                                        AS approver_user_id,
    pay.CUSTOMERID                                                        AS customer_user_id,
    pay.MONITORINGAPPROVERID                                              AS monitoring_approver_user_id,

    -- Decoded labels
    PayReqSta.LABEL                                                       AS workflow_status,
    PayVenTyp.LABEL                                                       AS payment_share,
    payee.LABEL                                                           AS payee,
    PayTyp.LABEL                                                          AS is_fawateer,
    PayReqTyp.LABEL                                                       AS payment_request_type,
    --Sup.LABEL                                                             AS support_category,
    Program.PROGRAMNAME                                                   AS program_name,
    PrgTyp.LABEL                                                          AS program_type_name,
    ast.LABEL                                                             AS app_status,
    App.IDENTIFIER                                                        AS app_ref,

    -- Amounts (Train-and-Place branch â€” pulled from training/support details)
    ASD.TAMKEENSHARE                                                      AS total_amount,
    AET.COSTOFTRAINING                                                    AS total_item_cost,
    AET.VAT                                                               AS total_vat_amount,
    ASD.TAMKEENSHARE                                                      AS total_items_tamkeen_share,
    (AET.COSTOFTRAINING - ASD.TAMKEENSHARE)                               AS total_items_applicant_share_with_vat,

    -- Dates (sentinel handled)
    CASE WHEN pay.CREATEDON             = DATE '1900-01-01' THEN NULL ELSE pay.CREATEDON             END  AS created_on,
    CASE WHEN pay.SUBMITTEDTOCUSTOMERON = DATE '1900-01-01' THEN NULL ELSE pay.SUBMITTEDTOCUSTOMERON END  AS submitted_to_customer_on,
    CASE WHEN pay.SUBMITTEDON           = DATE '1900-01-01' THEN NULL ELSE pay.SUBMITTEDON           END  AS submitted_on,
    CASE WHEN pay.APPROVEDON            = DATE '1900-01-01' THEN NULL ELSE pay.APPROVEDON            END  AS approved_on,
    CASE WHEN pay.SENTBACKTOCUSTOMERON  = DATE '1900-01-01' THEN NULL ELSE pay.SENTBACKTOCUSTOMERON  END  AS sent_back_to_customer_on,
    CASE WHEN pay.UPDATEDON             = DATE '1900-01-01' THEN NULL ELSE pay.UPDATEDON             END  AS updated_on,
    CASE WHEN pay.DUEDATE               = DATE '1900-01-01' THEN NULL ELSE pay.DUEDATE               END  AS due_date,

    -- ERP traceability
    ErpBatch.NAME                                                         AS erp_batch,
    ErpInv.REFERENCE                                                      AS erp_invoice,

    -- Counterparties
    TP.CRNO                                                               AS training_provider_cr,
    TP.NAME                                                               AS training_provider_name,
    Vndr.VENDORCR                                                         AS vendor_cr,
    Vndr.NAME                                                             AS vendor_name,

    -- People
    Usr_AprvBy.NAME                                                       AS user_accountant,
    Usr_Adt.NAME                                                          AS user_payment_auditor,
    Usr_SB.NAME                                                           AS sent_back_to_customer_by,
    Usr_Cor.NAME                                                          AS user_correction,
    Usr_Aprv.NAME                                                         AS approved_by,
    Usr_Cust.NAME                                                         AS customer_name,

    -- Flags
   -- CASE WHEN pay.CLAIMEDBYVENDOR  = 1 THEN TRUE ELSE FALSE END           AS claimed_by_vendor,
    --CASE WHEN pay.ISNEEDATTENTION  = 1 THEN TRUE ELSE FALSE END           AS is_need_attention,
    COALESCE(pay.CLAIMEDBYVENDOR, FALSE)  AS claimed_by_vendor,
    COALESCE(pay.ISNEEDATTENTION, FALSE)  AS is_need_attention,
    -- Misc
    pay.SUBMISSIONCOUNT                                                   AS submission_count,

    -- Train-and-Place specific (populated for this branch)
    TrnPayTyp.LABEL                                                       AS training_payment_type,
	usrext.userid AS userid,

    -- Standard trailing audit columns
    'OS1' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENT` pay
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTREQUESTSTATUS`            PayReqSta  ON PayReqSta.ID  = pay.PAYMENTREQUESTSTATUSID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYEE`                           payee      ON payee.ID      = pay.PAYEEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTTYPE`                     PayTyp     ON PayTyp.ID     = pay.PAYMENTTYPEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5W2_APPLICATIONEMPLOYEETRAINING`     AET        ON AET.PAYMENTID = pay.ID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_5W2_APPLICATIONEMPLOYEE`             AE         ON AE.ID         = AET.APPLICATIONEMPLOYEEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_APPLICATION`                     App        ON App.ID        = AE.APPLICATIONID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_APPLICATIONSUPPORTDETAILS`       ASD        ON ASD.APPLICATIONID = App.ID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PROGRAM`                         Program    ON Program.ID    = App.PROGRAMID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PROGRAMTYPE`                     PrgTyp     ON PrgTyp.ID     = Program.PROGRAMTYPEID
INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_APPLICATIONSTATUS`               ast        ON ast.ID        = App.APPLICATIONSTATUSID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTTOVENDORTYPE`             PayVenTyp  ON PayVenTyp.ID  = pay.PAYMENTTOVENDORTYPEID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                                Usr_AprvBy ON Usr_AprvBy.ID = pay.APPROVEDBY
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                                Usr_Adt    ON Usr_Adt.ID    = pay.PAYMENTAUDITORID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                                Usr_SB     ON Usr_SB.ID     = pay.SENTBACKTOCUSTOMERBY
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                                Usr_Cor    ON Usr_Cor.ID    = pay.CORRECTIONTEAMMEMBERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                                Usr_Aprv   ON Usr_Aprv.ID   = pay.APPROVERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER`                                Usr_Cust   ON Usr_Cust.ID   = pay.CUSTOMERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_ERPBATCH`                      ErpBatch   ON ErpBatch.ID   = pay.ERPBATCHID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_ERPINVOICE`                    ErpInv     ON ErpInv.ID     = pay.ERPINVOICEID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_PAYMENTREQUESTTYPE`            PayReqTyp  ON PayReqTyp.ID  = pay.PAYMENTREQUESTTYPE
/*LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_SUPPORT`                         Sup        ON Sup.ID        = pay.SUPPORTCATEGORY*/
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_TRAININGPROVIDER`                TP         ON TP.ID         = pay.TRAININGPROVIDERID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_D1O_VENDOR`                          Vndr       ON Vndr.ID       = pay.VENDORID
LEFT JOIN  `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_PX1_TRAININGPAYMENTTYPE4`             TrnPayTyp  ON TrnPayTyp.ID  = AET.TRAININGPAYMENTTYPEID
LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MKZ_USEREXTENSION` usrext
    ON usrext.USERID = App.USERID

WHERE pay.PAYMENTREFERENCENUMBER LIKE 'TP%'
  AND App.PROGRAMID <> 4
),
    payment_base_os2 AS (
/* =================================================================================================

Name        : payment_base_os2
Description : This model extracts and transforms payment request-related attributes
              from the NEO2 (NTP) source system Bronze Layer and loads into the
              PAYMENT_REQUEST target table as part of the Silver Layer data pipeline.
              It supports incremental loading with merge strategy and implements
              soft delete handling using a post-hook.

Source Tables : neo2.OSUSR_WZ3_PAYMENTREQUEST
                neo2.OSUSR_wz3_PaymentSupport
                neo2.OSUSR_MYA_MOIC_CRDETAILS
                neo2.OSUSR_NTP_APPLICATION
                neo2.OSUSR_3QQ_PROGRAMVERSION
                neo2.OSUSR_3QQ_PROGRAM
                neo2.OSUSR_WZ3_PAYMENTREQUESTSTATUS
                neo2.OSUSR_398_PAYMENTREQUESTTYPES
                neo2.OSUSR_398_PaymentRequestTypeParent
                neo2.OSUSR_398_PAYEETYPE
                neo2.OSUSR_TLV_IBAN
                neo2.OSUSR_tlv_Bank
                neo2.OSUSR_TLV_IBANSTATUS
                neo2.OSUSR_ZMZ_CUSTOMERPROFILE
                neo2.OSUSR_ZMZ_CUSTOMER
                neo2.OSUSR_ZMZ_INDIVIDUAL
                neo2.OSUSR_ZMZ_COMPANY
                neo2.OSUSR_398_SUPPORTAREA
                neo2.OSUSR_2DA_APPLICATIONSUPPORT
                neo2.OSUSR_2da_ExternalProvider
                neo2.OSUSR_398_COUNTRY
                neo2.OSUSR_wz3_PaymentTraining
                neo2.OSUSR_vw9_Training
                neo2.OSUSR_vw9_TrainingPaymentType
                neo2.OSUSR_wz3_PaymentWage
                neo2.OSUSR_2DA_EMPLOYEE

Target Table : PAYMENT_REQUEST
Load Type    : Incremental Load (Merge + Soft Delete)
Materialized : incremental
Format       : PARQUET
Tags         : neo2, daily

Revision History:
--------------------------------------------------------------
Version | Date       | Author  | Description
--------------------------------------------------------------
1.0     | 2026-05-11 |    Abitha     | Initial version

================================================================================================= */

-- ============================================================================
-- CTE 1: Ranked MOIC CR Details â€” latest record per CR number
-- ============================================================================
WITH CTE_MOIC AS (
    SELECT
        COMNERCIALNAMEEN,
        APPLICATIONID,
        STATUS,
        CRNUMBER,
		ADDRESSTOWN,
        ROW_NUMBER() OVER (
            PARTITION BY CRNUMBER
            ORDER BY UPDATEDON DESC
        ) AS RNK
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_MYA_MOIC_CRDETAILS`
),

MOIC AS (
    SELECT *
    FROM CTE_MOIC
    WHERE RNK = 1
),


-- ============================================================================
-- CTE 2: Payment Request base
-- ============================================================================
CTE_PAYMENT_REQUEST AS (
    SELECT
        ID,
        PROCESSREFERENCE,
        CREATEDON,
        SUBMITTEDON,
        FawateerReference,
        TAMKEENSHAREVALUE,
        CustomerShareValue,
        TOTALCOSTVALUE,
        APPLICATIONID,
        PAYMENTSTATUSID,
        PAYMENTREQUESTTYPEID,
        PAYEETYPEID,
        IBANID2,
        SupportAreaId,
        CREATEDBY,
        UPDATEDON
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTREQUEST`
),


-- ============================================================================
-- CTE 3: Payment Support base
-- ============================================================================
CTE_PAYMENT_SUPPORT AS (
    SELECT
        ID,
        ApplicationSupportId,
        TKShareTotal,
        CUSTOMERSHARETOTAL,
        PaymentResquestId
    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTSUPPORT`
),


-- ============================================================================
-- CTE 4: Final assembled payment request rows
-- ============================================================================
CTE_PAYMENT_REQUEST_FINAL AS (
    SELECT
        CAST(CURRENT_DATE AS DATE)                                                  AS extract_date,
        A.ID                                                                        AS application_id,
        PR.ID                                                                       AS payment_request_id,
        PAYSUP.ID                                                                   AS payment_support_id,
        PAYSUP.ApplicationSupportId                                                 AS application_support_id,
        PR.PROCESSREFERENCE                                                         AS payment_request_reference,
        A.REFERENCENUMBER                                                           AS application_reference,
        P.NAME                                                                      AS program_name,
        PRS.LABEL                                                                   AS payment_request_status,
        CASE
            WHEN PR.CREATEDON = CAST('1900-01-01 00:00:00' AS TIMESTAMP) THEN NULL
            ELSE PR.CREATEDON
        END                                                                         AS createdon,
        CASE
            WHEN PR.SUBMITTEDON = CAST('1900-01-01 00:00:00' AS TIMESTAMP) THEN NULL
            ELSE PR.SUBMITTEDON
        END                                                                         AS submitted_on_payment_request,
        CASE
            WHEN PRS.LABEL = 'Paid' THEN PR.UPDATEDON
            ELSE NULL
        END                                                                         AS paid_on,
        PRT.LABEL                                                                   AS payment_type,
        I.IBANNUMBER                                                                AS iban,
        IBST.LABEL                                                                  AS iban_status,
        PT.LABEL                                                                    AS payee_type,
        C.NAMEEN                                                                    AS payee,
        BANK.BankName                                                               AS payee_bank_name,
        UPPER(TRIM(INDNAME.NAMEEN))                                                 AS individual_name,
        CASE
            WHEN PT.CODE = 'CST'              THEN IND.CPRNUMBER
            WHEN CUSINDAPP.CPRNUMBER IS NOT NULL THEN CUSINDAPP.CPRNUMBER
            ELSE NULL
        END                                                                         AS individual_cpr,
        CASE
            WHEN PROGVER.COMMERCIALNAME_EN IN (
                    'On-the-Job Training Program',
                    'On-the-Job Training Program "Lawyers Track"'
                ) THEN OJTCMP.CODE
            WHEN CMP.CODE IS NULL THEN MOIC.CRNUMBER
            ELSE CMP.CODE
        END                                                                         AS enterprise_cr_license,
        MOIC.STATUS                                                                 AS enterprise_cr_status,
        CASE
            WHEN PROGVER.COMMERCIALNAME_EN IN (
                    'On-the-Job Training Program',
                    'On-the-Job Training Program "Lawyers Track"'
                ) THEN OJTCUS.NAMEEN
            WHEN A.CUSTOMERTYPEID = 'IND'     THEN NULL
            WHEN UPPER(TRIM(C.NAMEEN)) IS NULL THEN MOIC.COMNERCIALNAMEEN
            ELSE UPPER(TRIM(C.NAMEEN))
        END                                                                         AS enterprise_commercial_name_en,
        CASE
            WHEN APPSUP.PROVIDERID > 0         THEN 'Bahrain'
            WHEN APPSUP.EXTERNALPROVIDERID > 0 THEN COUNTRYVENDOR.COUNTRYNAME
            ELSE NULL
        END                                                                         AS training_provider_location,
        TRAPAY.LABEL                                                                AS training_payment_type,
        SUPPAREA.LABEL                                                              AS support_type,
        PR.TOTALCOSTVALUE                                                           AS total_cost_value,
        PR.TAMKEENSHAREVALUE                                                        AS tamkeen_share_value,
        PR.CustomerShareValue                                                       AS customer_share_value,
        PR.TAMKEENSHAREVALUE                                                        AS total_amount_tamkeen_share,
        PAYSUP.TKShareTotal                                                         AS tamkeen_share_support,
        PAYSUP.CUSTOMERSHARETOTAL                                                   AS customer_share_support,
        PR.FawateerReference                                                        AS fawateer_reference,
        PAYWAGE.SIODeductions                                                       AS sio_deductions,
        PAYWAGE.AttendanceDeductions                                                AS attendance_deductions,
        PAYWAGE.OtherDeductions                                                     AS other_deductions,
        PAYWAGE.SIODeductions
            + PAYWAGE.AttendanceDeductions
            + PAYWAGE.OtherDeductions                                               AS total_deductions,
        PRTP.LABEL                                                                  AS process_type,
        CASE
            WHEN PR.CREATEDBY LIKE '%MIS Migration User%' THEN 'MIS'
            WHEN LENGTH(CAST(PR.CREATEDBY AS STRING)) IN (4, 5)  THEN 'NeoT 1.0'
            ELSE 'NeoT 2.0'
        END                                                                         AS origin_system,
		 CASE WHEN C.CUSTOMERTYPEID = 'CMP' THEN UPPER(LTRIM(RTRIM(C.NAMEEN))) ELSE NULL END AS commercial_name_en,
         CASE WHEN C.CUSTOMERTYPEID = 'CMP' THEN UPPER(LTRIM(RTRIM(C.NAMEAR))) ELSE NULL END AS commercial_name_ar,
		 act.closed AS closed,
		 MOIC.addresstown,
		 A.CREATEDON AS application_createdon,
		 C.CUSTOMERTYPEID AS customertypeid,
		 CMP.REGISTRATIONDATE AS registrationdate,
		 APST.LABEL AS application_status,
		 ass.amendmentrequestid amendmentrequestid,
		 R.NAME role_name,
		 actdef.LABEL                                                         AS activity_label,
		 actdef.ID AS activity_definition_id,
		 CASE WHEN U.ID IS NOT NULL THEN U.NAME ELSE 'n/a' END AS owner,
		 EMP.totalmonthsexperience AS total_months_experience,
		 
        PR.CREATEDBY                                                                AS created_by,
        PR.UPDATEDON                                                                AS updatedon,
        FALSE                                                                       AS is_deleted,
        UPPER(NULLIF(TRIM('NEO2'), ''))                                        AS source_system_name,
        CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)                    AS dbt_updated_at,
        ROW_NUMBER() OVER (

    PARTITION BY APPSUP.ID

    ORDER BY APPSUP.UPDATEDON DESC, APPSUP.CREATEDON DESC

  ) AS rnk

    FROM CTE_PAYMENT_REQUEST PR
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_NTP_APPLICATION4`                A          ON A.ID                    = PR.APPLICATIONID
    LEFT JOIN CTE_PAYMENT_SUPPORT                                           PAYSUP     ON PAYSUP.PaymentResquestId = PR.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION`             PV         ON PV.ID                   = A.PROGRAMVERSIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAM`                    P          ON P.ID                    = PV.PROGRAMID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTREQUESTSTATUS`       PRS        ON PRS.CODE                = PR.PAYMENTSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_PAYMENTREQUESTTYPES`        PRT        ON PRT.CODE                = PR.PAYMENTREQUESTTYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_PAYMENTREQUESTTYPEPARENT`   PRTP       ON PRTP.CODE               = PRT.PaymentRequestTypeParentId
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_PAYEETYPE`                  PT         ON PT.CODE                 = PR.PAYEETYPEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_IBAN`                       I          ON I.ID                    = PR.IBANID2
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_BANK`                       BANK       ON BANK.ID                 = I.BankId
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_TLV_IBANSTATUS`                 IBST       ON IBST.CODE               = I.IBANSTATUSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMERPROFILE`            CP         ON CP.ID                   = I.CUSTOMERPROFILEID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER`                   C          ON C.ID                    = CP.CUSTOMERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL`                 IND        ON IND.ID                  = CP.CUSTOMERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY`                    CMP        ON CMP.ID                  = CP.CUSTOMERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_SUPPORTAREA`                SUPPAREA   ON SUPPAREA.CODE            = PR.SupportAreaId
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_APPLICATIONSUPPORT`         APPSUP     ON APPSUP.ID               = PAYSUP.ApplicationSupportId
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EXTERNALPROVIDER`           PROVOVERSEAS ON PROVOVERSEAS.ID        = APPSUP.EXTERNALPROVIDERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_COUNTRY`                    COUNTRYVENDOR ON COUNTRYVENDOR.ID      = PROVOVERSEAS.COUNTRYID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_3QQ_PROGRAMVERSION`             PROGVER    ON PROGVER.ID              = A.PROGRAMVERSIONID
    LEFT JOIN MOIC                                                                     ON MOIC.APPLICATIONID      = A.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTTRAINING`            PAYTRAIN   ON PAYTRAIN.PaymentSupportId = PAYSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_TRAINING`                   TRA        ON TRA.ApplicationSupportId = APPSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_VW9_TRAININGPAYMENTTYPE`        TRAPAY     ON TRAPAY.CODE             = TRA.TrainingPaymentTypeId
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_WZ3_PAYMENTWAGE`                PAYWAGE    ON PAYWAGE.PaymentSupportId = PAYSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2DA_EMPLOYEE`                   EMP        ON EMP.APPLICATIONSUPPORTID = APPSUP.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER`                   OJTCUS     ON OJTCUS.ID               = EMP.EMPLOYERID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_COMPANY`                    OJTCMP     ON OJTCMP.ID               = OJTCUS.ID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_CUSTOMER`                   INDNAME    ON INDNAME.ID              = APPSUP.INDIVIDUALID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_ZMZ_INDIVIDUAL`                 CUSINDAPP  ON CUSINDAPP.ID            = APPSUP.INDIVIDUALID
	LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_1AT_ASSESSMENT` ass
    ON ass.ID = APPSUP.APPLICATIONID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_PROCESS` pro
    ON pro.TOP_PROCESS_ID = ass.PROCESSID
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY` act
    ON act.Process_Id = pro.Id
    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_398_APPLICATIONSTATUS` APST
        ON A.APPLICATIONSTATUSID = APST.CODE
	LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY_DEFINITION` actdef
    ON act.Activity_Def_Id = actdef.Id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_HMY_ACTIVITYEXTENDED` act_ext
    ON act_ext.ID = act.Id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSUSR_2FH_APPLICATIONASSESSMENTACTIONS` actions
    ON actions.KEY = act_ext.SELECTEDACTIONKEY

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_BPM_ACTIVITY_DEF_ROLE` ADR
    ON actdef.Id = ADR.Activity_Def_Id

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_ROLE` R
    ON ADR.Role_Id = R.ID

    LEFT JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`OSSYS_USER` U
    ON act.User_Id = U.ID

)


SELECT
    extract_date,
    application_id,
    payment_request_id,
    payment_support_id,
    application_support_id,
    payment_request_reference,
    application_reference,
    program_name,
    payment_request_status,
    createdon,
    submitted_on_payment_request,
    paid_on,
    payment_type,
    iban,
    iban_status,
    payee_type,
    payee,
    payee_bank_name,
    individual_name,
    individual_cpr,
    enterprise_cr_license,
    enterprise_cr_status,
    enterprise_commercial_name_en,
    training_provider_location,
    training_payment_type,
    support_type,
    total_cost_value,
    tamkeen_share_value,
    customer_share_value,
    total_amount_tamkeen_share,
    tamkeen_share_support,
    customer_share_support,
    fawateer_reference,
    sio_deductions,
    attendance_deductions,
    other_deductions,
    total_deductions,
    process_type,
    origin_system,
    commercial_name_en,
    commercial_name_ar,
    closed,
    addresstown,
    application_createdon,
    customertypeid,
    registrationdate,
    application_status,
    amendmentrequestid,
    role_name,
    activity_label,
    activity_definition_id,
    owner,
    total_months_experience,
    created_by,
    updatedon,
    is_deleted,
    source_system_name,
    dbt_updated_at
FROM CTE_PAYMENT_REQUEST_FINAL app
WHERE rnk = 1
),
    payment_base_mis AS (
/*
============================================================================
silver_payment_mis.sql
============================================================================
Per-source intermediate Silver model for the Payment domain â€” MIS only.

The Payment domain captures payment-request records across all program types
in MIS. Each program type has its own payment-request table â€” these are
PARALLEL entities (each program type writes payments to its own table
independently, with its own schema). They are UNIONed (not joined).

Sources (9 payment-request tables):
  â˜… mis_paymentreq                          â†’ Individual application payments
  â˜… tws_wagepaymentrequest                  â†’ TWS wage payment requests
  â˜… tws_trainingenrollmentpaymentrequest    â†’ TWS training enrollment payments
  â˜… tmkn_espaymentrequest                   â†’ ES payment requests
  â˜… tmkn_aubpayment                         â†’ AUB bank payment batches
  â˜… tmkn_invoiceforgp                       â†’ GP invoice payments
  â˜… tmkn_benefitpayment                     â†’ Benefit payments (online batches)
  â˜… tmkn_businesscontinuitysupportpayment   â†’ BC Support payments
  â˜… tmkn_onlinepb                           â†’ Online payment batches

Reference SPs:
  - RPT-059 (mis_paymentreq), RPT-060 (mis_paymentreq SH)
  - RPT-049 (tws_wagepaymentrequest), RPT-046, RPT-048
  - RPT-052 (tws_trainingenrollmentpaymentrequest)
  - RPT-034 (tmkn_espaymentrequest)
  - RPT-036 (tmkn_benefitpayment, tmkn_onlinepb)
  - BCApplications (tmkn_businesscontinuitysupportpayment)

Cross-domain note: RPT-049 / RPT-046 etc. join payment tables to
tws_enterpriseapplication, tmkn_company, tmkn_pid for denormalised display.
Those joins are NOT performed here â€” payments stay focused on the payment
entity itself, and FK columns (parent_application_id, company_id) are
preserved for downstream re-joining at the unified Silver layer.

Canonical column shape: every UNION branch produces the same column shape.
Fields that don't exist in a given branch are NULL-padded with explicit
casts. The payment_subtype + mis_source_table pair identifies the row's
origin.

WHERE clause from RPT-059 (filtering by specific product/PID GUIDs) is
omitted â€” Silver should preserve all payment requests; report-specific
filters belong in Gold/AGG.
============================================================================
*/


-- ============================================================================
-- BRANCH 1: Individual Application Payment Request (mis_paymentreq)
-- ============================================================================
SELECT
    'INDIVIDUAL_PAYMENT_REQUEST' AS payment_subtype,
    'mis_paymentreq' AS mis_source_table,

    -- Identifiers
    CAST(p.mis_paymentreqid AS STRING)                  AS payment_id,
    p.mis_name                                           AS payment_name,

    -- Foreign keys
    CAST(p.mis_individualapplication AS STRING)         AS parent_application_id,
    p.mis_individualapplication                      AS parent_application_name,
    CAST(NULL AS STRING)                                AS enterprise_application_id,
    CAST(NULL AS STRING)                                AS company_id,
    CAST(p.tmkn_aubpayment AS STRING)                   AS aub_payment_id,
    p.tmkn_aubpayment                                AS aub_payment_name,
    CAST(p.tmkn_gpinvoice AS STRING)                    AS gp_invoice_id,
    p.tmkn_gpinvoice                                AS gp_invoice_name,

    -- Dates
    p.createdon                                          AS created_on,
    p.mis_submittedforapprovalon                         AS submitted_for_approval_on,
    p.mis_approvedon                                     AS approved_on,
    p.mis_deliveredon                                    AS validated_on,
    p.mis_submittedtofinanceon                           AS submitted_to_finance_on,
    p.tmkn_dueDate                                       AS due_date,
    p.tmkn_approvedbyfctime                              AS approved_by_fc_on,
    p.tmkn_checkedbyaccountant                           AS checked_by_accountant_on,

    -- Amounts
    p.mis_totalamount                                    AS total_amount,
    p.mis_receiptamount                                  AS receipt_amount,
    CAST(NULL AS DECIMAL(18, 2))                         AS verified_amount,

    -- Status / workflow (decoded)
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

)            AS state,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

)           AS status_reason,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('mis_paymentprocessed')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.mis_paymentprocessed AS STRING)

) AS payment_processed,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('mis_sentbackbefore')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.mis_sentbackbefore AS STRING)

)   AS sent_back_before,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('mis_paymentstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.mis_paymentstatus AS STRING)

)    AS payment_status,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('mis_paymenttype')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.mis_paymenttype AS STRING)

)      AS payment_type,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('mis_payableto')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.mis_payableto AS STRING)

)        AS payable_to,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('tmkn_violationstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_violationstatus AS STRING)

) AS violation_status,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('mis_hascommitedtogp')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.mis_hascommitedtogp AS STRING)

)  AS has_committed_to_gp,
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('mis_paymentreq')

      AND LOWER(sm.attributename) = LOWER('tmkn_financeapproval')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_financeapproval AS STRING)

) AS finance_approval,
    CAST(NULL AS STRING)                                AS workflow_status,
    CAST(NULL AS STRING)                                AS is_flagged,

    -- People
    p.tmkn_accountant                                AS accountant_name,
    p.tmkn_verifyinguser                            AS verifying_user,
    p.tmkn_fcuser                                    AS fc_user,
    p.ownerid                                        AS owner_name,

    -- Migration
    CAST(NULL AS BOOLEAN)                                AS is_migrated,
	CAST(NULL AS STRING) AS name,

    -- Standard trailing
    'MIS' AS source_system_name,
    FALSE AS is_deleted,
    CURRENT_DATE AS report_date,
    CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP) AS dbt_updated_at

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`MIS_PAYMENTREQBASE` p

UNION ALL


-- ============================================================================
-- BRANCH 2: TWS Wage Payment Request (tws_wagepaymentrequest)
-- ============================================================================
SELECT
    'TWS_WAGE_PAYMENT_REQUEST', 'tws_wagepaymentrequest',

    CAST(p.tws_wagepaymentrequestid AS STRING),
    p.tws_name,

    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    --CAST(p.tws_enterprise_application AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(p.tmkn_aubpayment AS STRING),
    p.tmkn_aubpayment,
    CAST(p.tmkn_gpinvoice AS STRING),
    p.tmkn_gpinvoice,

    p.createdon,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),

    p.tws_totalamount,
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_wagepaymentrequest')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_wagepaymentrequest')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_wagepaymentrequest')

      AND LOWER(sm.attributename) = LOWER('tws_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tws_workflowstatus AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_wagepaymentrequest')

      AND LOWER(sm.attributename) = LOWER('tws_is_flagged')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tws_is_flagged AS STRING)

),

    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    p.ownerid,

    p.tmkn_ismigrated,
	CAST(NULL AS STRING),

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TWS_WAGEPAYMENTREQUESTBASE` p

UNION ALL


-- ============================================================================
-- BRANCH 3: TWS Training Enrollment Payment Request
-- ============================================================================
SELECT
    'TWS_TRAINING_PAYMENT_REQUEST', 'tws_trainingenrollmentpaymentrequest',

    CAST(p.tws_trainingenrollmentpaymentrequestid AS STRING),
    p.tws_name,

    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    --CAST(p.tws_enterprise_application AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(p.tmkn_aubpayment AS STRING),
    CAST(NULL AS STRING),
    CAST(p.tmkn_gpinvoice AS STRING),
    CAST(NULL AS STRING),

    p.createdon,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),

    p.tws_totalamount,
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_trainingenrollmentpaymentrequest')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_trainingenrollmentpaymentrequest')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tws_trainingenrollmentpaymentrequest')

      AND LOWER(sm.attributename) = LOWER('tws_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tws_workflowstatus AS STRING)

),
    CAST(NULL AS STRING),

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    p.ownerid,

    p.tmkn_ismigrated,
	CAST(NULL AS STRING),

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TWS_TRAININGENROLLMENTPAYMENTREQUESTBASE` p

UNION ALL


-- ============================================================================
-- BRANCH 4: ES Payment Request (tmkn_espaymentrequest)
-- ============================================================================
SELECT
    'ES_PAYMENT_REQUEST', 'tmkn_espaymentrequest',

    CAST(p.tmkn_espaymentrequestid AS STRING),
    p.tmkn_name,

    --CAST(p.tmkn_application AS STRING),
    CAST(NULL AS STRING),
    --p.tmkn_application,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    --CAST(p.tmkn_maincompany AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),

    p.createdon,
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),

    --p.tmkn_totalamount,
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_espaymentrequest')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_espaymentrequest')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_espaymentrequest')

      AND LOWER(sm.attributename) = LOWER('tmkn_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_workflowstatus AS STRING)

),
    CAST(NULL AS STRING),

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    p.ownerid,

    CAST(NULL AS BOOLEAN),
	WF.name AS name,  

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_ESPAYMENTREQUESTBASE` p
LEFT JOIN (

    SELECT s.value AS name, s.attributevalue AS id

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` s

    INNER JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` e

      ON s.objecttypecode = e.objecttypecode

    WHERE e.name = 'TMKN_ESPAYMENTREQUESTBASE'

      AND s.attributename = 'TMKN_WORKFLOWSTATUS'

) WF
    ON WF.ID = p.tmkn_workflowstatus


UNION ALL


-- ============================================================================
-- BRANCH 5: AUB Bank Payment Batch (tmkn_aubpayment)
-- ============================================================================
SELECT
    'AUB_BANK_PAYMENT', 'tmkn_aubpayment',

    CAST(p.tmkn_aubpaymentid AS STRING),
    p.tmkn_name,

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),

    p.createdon,
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),

    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_aubpayment')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_aubpayment')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_aubpayment')

      AND LOWER(sm.attributename) = LOWER('tmkn_paymentstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_paymentstatus AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS BOOLEAN),
	CAST(NULL AS STRING),

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_AUBPAYMENTBASE` p

UNION ALL


-- ============================================================================
-- BRANCH 6: GP Invoice Payment (tmkn_invoiceforgp)
-- ============================================================================
SELECT
    'GP_INVOICE_PAYMENT', 'tmkn_invoiceforgp',

    CAST(p.tmkn_invoiceforgpid AS STRING),
    p.tmkn_name,

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),

    p.createdon,
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),

    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_invoiceforgp')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_invoiceforgp')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_invoiceforgp')

      AND LOWER(sm.attributename) = LOWER('tmkn_paymentstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_paymentstatus AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS BOOLEAN),
	CAST(NULL AS STRING),

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_INVOICEFORGPBASE` p

UNION ALL


-- ============================================================================
-- BRANCH 7: Benefit Payment (tmkn_benefitpayment)
-- ============================================================================
SELECT
    'BENEFIT_PAYMENT', 'tmkn_benefitpayment',

    CAST(p.tmkn_benefitpaymentid AS STRING),
    p.tmkn_name,

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),

    p.createdon,
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),

    CAST(NULL AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_benefitpayment')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_benefitpayment')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING),

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS BOOLEAN),
	CAST(NULL AS STRING),

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_BENEFITPAYMENTBASE` p

/*-- UNION ALL


 -- ============================================================================
 -- BRANCH 8: BC Support Payment (tmkn_businesscontinuitysupportpayment)
 -- ============================================================================
 SELECT
     'BC_SUPPORT_PAYMENT', 'tmkn_businesscontinuitysupportpayment',

     CAST(p.tmkn_businesscontinuitysupportpaymentid AS STRING),
     p.tmkn_name,

     CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
     CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),

     p.createdon,
     CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
     CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),

     p.tmkn_totalamount,
     CAST(NULL AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),

     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_businesscontinuitysupportpayment')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_businesscontinuitysupportpayment')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
     CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
     CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
     (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_businesscontinuitysupportpayment')

      AND LOWER(sm.attributename) = LOWER('tmkn_workflowstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_workflowstatus AS STRING)

),
     CAST(NULL AS STRING),

     CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
     p.owneridname,

     CAST(NULL AS BOOLEAN),
	 CAST(NULL AS STRING),

     'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

 FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`tmkn_businesscontinuitysupportpayment` p
*/

UNION ALL


-- ============================================================================
-- BRANCH 9: Online Payment Batch (tmkn_onlinepb)
-- ============================================================================
SELECT
    'ONLINE_PAYMENT_BATCH', 'tmkn_onlinepb',

    CAST(p.tmkn_onlinepbid AS STRING),
    p.tmkn_name,

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),

    p.createdon,
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),

    --p.tmkn_totalamount,
    CAST(NULL AS DECIMAL(18, 2)),
    CAST(NULL AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),

    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_onlinepb')

      AND LOWER(sm.attributename) = LOWER('statecode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statecode AS STRING)

),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_onlinepb')

      AND LOWER(sm.attributename) = LOWER('statuscode')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.statuscode AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    (

    SELECT MAX(sm.value)

    FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`stringmap` sm

    JOIN `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`entitylogicalview` ev

      ON sm.objecttypecode = ev.objecttypecode

    WHERE LOWER(ev.name) = LOWER('tmkn_onlinepb')

      AND LOWER(sm.attributename) = LOWER('tmkn_paymentstatus')

      AND CAST(sm.attributevalue AS STRING) = CAST(p.tmkn_paymentstatus AS STRING)

),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING),

    CAST(NULL AS BOOLEAN),
	CAST(NULL AS STRING),

    'MIS', FALSE, CURRENT_DATE, CAST(to_utc_timestamp(current_timestamp(), current_timezone()) AS TIMESTAMP)

FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-bronze`.`TMKN_ONLINEPBBASE` p
)
SELECT

    -- =========================
    -- COMMON COLUMNS
    -- =========================
	CAST(os1.payment_id AS STRING)                               AS payment_id,
    os1.payment_subtype                                           AS payment_subtype,
    os1.os1_source_table                                          AS source_table,   
    os1.payment_no                                                AS payment_no,
    CAST(os1.application_id AS STRING)                           AS application_id,
    os1.iban_id                                                   AS iban_id,
    os1.payee_id                                                  AS payee_id,
    os1.payment_type_id                                           AS payment_type_id,
    os1.payment_status_id                                         AS payment_status_id,
    os1.payment_to_vendor_type_id                                 AS payment_to_vendor_type_id,
    os1.erp_batch_id                                              AS erp_batch_id,
    os1.erp_invoice_id                                            AS erp_invoice_id,
    os1.payment_request_type_id                                   AS payment_request_type_id,
    os1.support_category_id                                       AS support_category_id,
    os1.training_provider_id                                      AS training_provider_id,
    os1.vendor_id                                                 AS vendor_id,
    os1.approved_by_user_id                                       AS approved_by_user_id,
    os1.payment_auditor_user_id                                   AS payment_auditor_user_id,
    os1.sent_back_by_user_id                                      AS sent_back_by_user_id,
    os1.correction_team_user_id                                   AS correction_team_user_id,
    os1.approver_user_id                                          AS approver_user_id,
    os1.customer_user_id                                          AS customer_user_id,
    os1.monitoring_approver_user_id                               AS monitoring_approver_user_id,
    os1.workflow_status                                           AS workflow_status,
    os1.payment_share                                             AS payment_share,
    os1.payee                                                     AS payee,
    os1.is_fawateer                                               AS is_fawateer,
    os1.payment_request_type                                      AS payment_request_type,
    os1.program_name                                              AS program_name,
    --os1.support_category                                          AS support_category,
    os1.program_type_name                                         AS program_type_name,
    os1.app_status                                                AS app_status,
    os1.app_ref                                                   AS app_ref,
    os1.total_amount                                              AS total_amount,
    os1.total_item_cost                                           AS total_item_cost,
    os1.total_vat_amount                                          AS total_vat_amount,
    os1.total_items_tamkeen_share                                 AS total_items_tamkeen_share,
    os1.total_items_applicant_share_with_vat                      AS total_items_applicant_share_with_vat,
    os1.created_on                                                AS created_on,
    os1.submitted_to_customer_on                                  AS submitted_to_customer_on,
    os1.submitted_on                                              AS submitted_on,
    os1.approved_on                                               AS approved_on,
    os1.sent_back_to_customer_on                                  AS sent_back_to_customer_on,
    os1.updated_on                                                AS updated_on,
    os1.due_date                                                  AS due_date,
    os1.erp_batch                                                 AS erp_batch,
    os1.erp_invoice                                               AS erp_invoice,
    os1.training_provider_cr                                      AS training_provider_cr,
    os1.training_provider_name                                    AS training_provider_name,
    os1.vendor_cr                                                 AS vendor_cr,
    os1.vendor_name                                               AS vendor_name,
    os1.user_accountant                                           AS user_accountant,
    os1.user_payment_auditor                                      AS user_payment_auditor,
    os1.sent_back_to_customer_by                                  AS sent_back_to_customer_by,
    os1.user_correction                                           AS user_correction,
    os1.approved_by                                               AS approved_by,
    os1.customer_name                                             AS customer_name,
    os1.claimed_by_vendor                                         AS claimed_by_vendor,
    os1.is_need_attention                                         AS is_need_attention,
    os1.submission_count                                          AS submission_count,
    os1.training_payment_type                                     AS training_payment_type,

    CAST(NULL AS TIMESTAMP)                     AS extract_date,
    CAST(NULL AS STRING)                                         AS payment_request_reference,
    CAST(os1.payment_no_with_app_ref AS STRING)                  AS application_reference,
    CAST(NULL AS STRING)                                         AS payment_request_status,
    CAST(NULL AS TIMESTAMP)                                    AS created_on_payment_request,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_on_payment_request,
    CAST(NULL AS TIMESTAMP)                                    AS paid_on,
    CAST(NULL AS STRING)                                         AS payment_type,
    CAST(NULL AS STRING)                                         AS iban,
    CAST(NULL AS STRING)                                         AS iban_status,
    CAST(NULL AS STRING)                                         AS payee_type,
    CAST(NULL AS DECIMAL(37,3))                                   AS total_cost_value,
    CAST(NULL AS DECIMAL(37,3))                                   AS tamkeen_share_value,
    CAST(NULL AS DECIMAL(37,3))                                   AS customer_share_value,
    CAST(NULL AS STRING)                                         AS fawateer_reference,
    CAST(NULL AS STRING)                                         AS origin_system,
    CAST(NULL AS STRING)                                         AS created_by,
    CAST(NULL AS TIMESTAMP)                                    AS updatedon,
    CAST(NULL AS TIMESTAMP)                                    AS createdon,

    CAST(NULL AS STRING)                                         AS payment_name,
    CAST(NULL AS STRING)                                         AS parent_application_id,
    CAST(NULL AS STRING)                                         AS parent_application_name,
    CAST(NULL AS STRING)                                         AS enterprise_application_id,
    CAST(NULL AS STRING)                                         AS company_id,
    CAST(NULL AS STRING)                                         AS aub_payment_id,
    CAST(NULL AS STRING)                                         AS aub_payment_name,
    CAST(NULL AS STRING)                                         AS gp_invoice_id,
    CAST(NULL AS STRING)                                         AS gp_invoice_name,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_for_approval_on,
    CAST(NULL AS TIMESTAMP)                                    AS validated_on,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_to_finance_on,
    CAST(NULL AS TIMESTAMP)                                    AS approved_by_fc_on,
    CAST(NULL AS TIMESTAMP)                                    AS checked_by_accountant_on,
    CAST(NULL AS DECIMAL(20,4))                                   AS receipt_amount,
    CAST(NULL AS DECIMAL(18,2))                                   AS verified_amount,
    CAST(NULL AS STRING)                                         AS state,
    CAST(NULL AS STRING)                                         AS status_reason,
    CAST(NULL AS STRING)                                         AS payment_processed,
    CAST(NULL AS STRING)                                         AS sent_back_before,
    CAST(NULL AS STRING)                                         AS payment_status,
    CAST(NULL AS STRING)                                         AS payable_to,
    CAST(NULL AS STRING)                                         AS violation_status,
    CAST(NULL AS STRING)                                         AS has_committed_to_gp,
    CAST(NULL AS STRING)                                         AS finance_approval,
    CAST(NULL AS STRING)                                         AS is_flagged,
    CAST(NULL AS STRING)                                         AS accountant_name,
    CAST(NULL AS STRING)                                         AS verifying_user,
    CAST(NULL AS STRING)                                         AS fc_user,
    CAST(NULL AS STRING)                                         AS owner_name,
    CAST(NULL AS BOOLEAN)                                         AS is_migrated,
    CAST(NULL AS BIGINT)         AS payment_support_id,
    CAST(NULL AS BIGINT)         AS application_support_id,
    CAST(NULL AS STRING)        AS individual_name,
    CAST(NULL AS STRING)        AS individual_cpr,
    CAST(NULL AS STRING)        AS enterprise_cr_license,
    CAST(NULL AS STRING)        AS enterprise_cr_status,
    CAST(NULL AS STRING)        AS enterprise_commercial_name_en,
    CAST(NULL AS STRING)        AS training_provider_location,
    CAST(NULL AS STRING)        AS support_type,
    CAST(NULL AS DECIMAL(37,3))  AS total_amount_tamkeen_share,
    CAST(NULL AS DECIMAL(37,8))  AS tamkeen_share_support,
    CAST(NULL AS DECIMAL(37,8))  AS customer_share_support,
    CAST(NULL AS DECIMAL(37,8))  AS sio_deductions,
    CAST(NULL AS DECIMAL(37,8))  AS attendance_deductions,
    CAST(NULL AS DECIMAL(37,8))  AS other_deductions,
    CAST(NULL AS DECIMAL(38,8))  AS total_deductions,
    CAST(NULL AS STRING)        AS process_type,
	CAST(NULL AS STRING)         AS commercial_name_ar,
    CAST(NULL AS STRING)         AS commercial_name_en,
    CAST(NULL AS TIMESTAMP)    AS closed,
    CAST(NULL AS STRING)         AS addresstown,
    CAST(NULL AS TIMESTAMP)    AS application_createdon,
    CAST(NULL AS STRING)         AS customertypeid,
    CAST(NULL AS TIMESTAMP)    AS registrationdate,
    CAST(NULL AS STRING)         AS application_status,
    CAST(NULL AS BIGINT)          AS amendmentrequestid,
    CAST(NULL AS STRING)         AS role_name,
    CAST(NULL AS STRING)         AS activity_label,
    CAST(NULL AS INTEGER)         AS activity_definition_id,
    CAST(NULL AS STRING)         AS owner,
    CAST(NULL AS BIGINT)          AS payment_request_id,
    CAST(NULL AS INTEGER)         AS total_months_experience,
    os1.userid        AS userid,
    CAST(NULL AS STRING)         AS name,

    os1.source_system_name                                        AS source_system_name,
    os1.is_deleted                                                AS is_deleted,
    os1.report_date                                               AS report_date,
    os1.dbt_updated_at

FROM payment_base_os1 os1


UNION ALL

SELECT
    CAST(os2.payment_request_id AS STRING)                       AS payment_id,
    CAST(NULL AS STRING)                                         AS payment_subtype,
    CAST(NULL AS STRING)                                         AS source_table,
    CAST(NULL AS STRING)                                         AS payment_no,
    CAST(os2.application_id AS STRING)                           AS application_id,
    CAST(NULL AS BIGINT)                                          AS iban_id,
    CAST(NULL AS INTEGER)                                         AS payee_id,
    CAST(NULL AS INTEGER)                                         AS payment_type_id,
    CAST(NULL AS INTEGER)                                         AS payment_status_id,
    CAST(NULL AS INTEGER)                                         AS payment_to_vendor_type_id,
    CAST(NULL AS BIGINT)                                          AS erp_batch_id,
    CAST(NULL AS BIGINT)                                          AS erp_invoice_id,
    CAST(NULL AS INTEGER)                                         AS payment_request_type_id,
    CAST(NULL AS INTEGER)                                         AS support_category_id,
    CAST(NULL AS BIGINT)                                          AS training_provider_id,
    CAST(NULL AS BIGINT)                                          AS vendor_id,
    CAST(NULL AS INTEGER)                                         AS approved_by_user_id,
    CAST(NULL AS INTEGER)                                         AS payment_auditor_user_id,
    CAST(NULL AS INTEGER)                                         AS sent_back_by_user_id,
    CAST(NULL AS INTEGER)                                         AS correction_team_user_id,
    CAST(NULL AS INTEGER)                                         AS approver_user_id,
    CAST(NULL AS INTEGER)                                         AS customer_user_id,
    CAST(NULL AS INTEGER)                                         AS monitoring_approver_user_id,
    CAST(NULL AS STRING)                                         AS workflow_status,
    CAST(NULL AS STRING)                                         AS payment_share,
    os2.payee                                                     AS payee,
    CAST(NULL AS STRING)                                         AS is_fawateer,
    CAST(NULL AS STRING)                                         AS payment_request_type,
    os2.program_name                                              AS program_name,
    --CAST(NULL AS STRING)                                         AS support_category,
    CAST(NULL AS STRING)                                         AS program_type_name,
    CAST(NULL AS STRING)                                         AS app_status,
    CAST(NULL AS STRING)                                         AS app_ref,
    CAST(NULL AS DECIMAL(38,8))                                   AS total_amount,
    CAST(NULL AS DECIMAL(37,8))                                   AS total_item_cost,
    CAST(NULL AS DECIMAL(37,8))                                   AS total_vat_amount,
    CAST(NULL AS DECIMAL(38,8))                                   AS total_items_tamkeen_share,
    CAST(NULL AS DECIMAL(38,8))                                   AS total_items_applicant_share_with_vat,
    CAST(NULL AS TIMESTAMP)                                    AS created_on,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_to_customer_on,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_on,
    CAST(NULL AS TIMESTAMP)                                    AS approved_on,
    CAST(NULL AS TIMESTAMP)                                    AS sent_back_to_customer_on,
    os2.updatedon                                                 AS updated_on,
    CAST(NULL AS TIMESTAMP)                                    AS due_date,
    CAST(NULL AS STRING)                                         AS erp_batch,
    CAST(NULL AS STRING)                                         AS erp_invoice,
    CAST(NULL AS STRING)                                         AS training_provider_cr,
    CAST(NULL AS STRING)                                         AS training_provider_name,
    CAST(NULL AS STRING)                                         AS vendor_cr,
    CAST(NULL AS STRING)                                         AS vendor_name,
    CAST(NULL AS STRING)                                         AS user_accountant,
    CAST(NULL AS STRING)                                         AS user_payment_auditor,
    CAST(NULL AS STRING)                                         AS sent_back_to_customer_by,
    CAST(NULL AS STRING)                                         AS user_correction,
    CAST(NULL AS STRING)                                         AS approved_by,
    CAST(NULL AS STRING)                                         AS customer_name,
    CAST(NULL AS BOOLEAN)                                         AS claimed_by_vendor,
    CAST(NULL AS BOOLEAN)                                         AS is_need_attention,
    CAST(NULL AS INTEGER)                                         AS submission_count,
    CAST(NULL AS STRING)                                         AS training_payment_type,

    os2.extract_date                                              AS extract_date,
    os2.payment_request_reference                                 AS payment_request_reference,
    os2.application_reference                                     AS application_reference,
    os2.payment_request_status                                    AS payment_request_status,
    os2.createdon                                                AS created_on_payment_request,
    os2.submitted_on_payment_request                              AS submitted_on_payment_request,
    os2.paid_on                                                   AS paid_on,
    os2.payment_type                                              AS payment_type,
    os2.iban                                                      AS iban,
    os2.iban_status                                               AS iban_status,
    os2.payee_type                                                AS payee_type,
    os2.total_cost_value                                          AS total_cost_value,
    os2.tamkeen_share_value                                       AS tamkeen_share_value,
    os2.customer_share_value                                      AS customer_share_value,
    os2.fawateer_reference                                        AS fawateer_reference,
    os2.origin_system                                             AS origin_system,
    os2.created_by                                                AS created_by,
    os2.updatedon                                                 AS updatedon,
    os2.createdon                                                 AS createdon,

    CAST(NULL AS STRING)                                         AS payment_name,
    CAST(NULL AS STRING)                                         AS parent_application_id,
    CAST(NULL AS STRING)                                         AS parent_application_name,
    CAST(NULL AS STRING)                                         AS enterprise_application_id,
    CAST(NULL AS STRING)                                         AS company_id,
    CAST(NULL AS STRING)                                         AS aub_payment_id,
    CAST(NULL AS STRING)                                         AS aub_payment_name,
    CAST(NULL AS STRING)                                         AS gp_invoice_id,
    CAST(NULL AS STRING)                                         AS gp_invoice_name,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_for_approval_on,
    CAST(NULL AS TIMESTAMP)                                    AS validated_on,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_to_finance_on,
    CAST(NULL AS TIMESTAMP)                                    AS approved_by_fc_on,
    CAST(NULL AS TIMESTAMP)                                    AS checked_by_accountant_on,
    CAST(NULL AS DECIMAL(20,4))                                   AS receipt_amount,
    CAST(NULL AS DECIMAL(18,2))                                   AS verified_amount,
    CAST(NULL AS STRING)                                         AS state,
    CAST(NULL AS STRING)                                         AS status_reason,
    CAST(NULL AS STRING)                                         AS payment_processed,
    CAST(NULL AS STRING)                                         AS sent_back_before,
    CAST(NULL AS STRING)                                         AS payment_status,
    CAST(NULL AS STRING)                                         AS payable_to,
    CAST(NULL AS STRING)                                         AS violation_status,
    CAST(NULL AS STRING)                                         AS has_committed_to_gp,
    CAST(NULL AS STRING)                                         AS finance_approval,
    CAST(NULL AS STRING)                                         AS is_flagged,
    CAST(NULL AS STRING)                                         AS accountant_name,
    CAST(NULL AS STRING)                                         AS verifying_user,
    CAST(NULL AS STRING)                                         AS fc_user,
    CAST(NULL AS STRING)                                         AS owner_name,
    CAST(NULL AS BOOLEAN)                                         AS is_migrated,
    os2.payment_support_id,
    os2.application_support_id,
    os2.individual_name,
    os2.individual_cpr,
    os2.enterprise_cr_license,
    os2.enterprise_cr_status,
    os2.enterprise_commercial_name_en,
    os2.training_provider_location,
    os2.support_type,
    os2.total_amount_tamkeen_share,
    os2.tamkeen_share_support,
    os2.customer_share_support,
    os2.sio_deductions,
    os2.attendance_deductions,
    os2.other_deductions,
    os2.total_deductions,
    os2.process_type,
	os2.commercial_name_ar         AS commercial_name_ar,
    os2.commercial_name_en         AS commercial_name_en,
    os2.closed    AS closed,
    os2.addresstown         AS addresstown,
    os2.application_createdon    AS application_createdon,
    os2.customertypeid         AS customertypeid,
    os2.registrationdate    AS registrationdate,
    os2.application_status        AS application_status,
    os2.amendmentrequestid          AS amendmentrequestid,
    os2.role_name         AS role_name,
    os2.activity_label         AS activity_label,
    os2.activity_definition_id         AS activity_definition_id,
    os2.owner        AS owner,
    os2.payment_request_id          AS payment_request_id,
    os2.total_months_experience         AS total_months_experience,
    CAST(NULL AS INTEGER)         AS userid,
    CAST(NULL AS STRING)         AS name,

    os2.source_system_name                                        AS source_system_name,
    os2.is_deleted                                                AS is_deleted,
    CAST(CURRENT_DATE AS DATE) AS report_date,
    os2.dbt_updated_at

FROM  payment_base_os2 os2


UNION ALL

SELECT
    CAST(mis.payment_id AS STRING)                                AS payment_id,
    mis.payment_subtype                                           AS payment_subtype,
    mis.mis_source_table                                          AS source_table,
    CAST(NULL AS STRING)                                         AS payment_no,
    CAST(mis.parent_application_id AS STRING)                     AS application_id,
    CAST(NULL AS BIGINT)                                          AS iban_id,
    CAST(NULL AS INTEGER)                                         AS payee_id,
    CAST(NULL AS INTEGER)                                         AS payment_type_id,
    CAST(NULL AS INTEGER)                                         AS payment_status_id,
    CAST(NULL AS INTEGER)                                         AS payment_to_vendor_type_id,
    CAST(NULL AS BIGINT)                                          AS erp_batch_id,
    CAST(NULL AS BIGINT)                                          AS erp_invoice_id,
    CAST(NULL AS INTEGER)                                         AS payment_request_type_id,
    CAST(NULL AS INTEGER)                                         AS support_category_id,
    CAST(NULL AS BIGINT)                                          AS training_provider_id,
    CAST(NULL AS BIGINT)                                          AS vendor_id,
    CAST(NULL AS INTEGER)                                         AS approved_by_user_id,
    CAST(NULL AS INTEGER)                                         AS payment_auditor_user_id,
    CAST(NULL AS INTEGER)                                         AS sent_back_by_user_id,
    CAST(NULL AS INTEGER)                                         AS correction_team_user_id,
    CAST(NULL AS INTEGER)                                         AS approver_user_id,
    CAST(NULL AS INTEGER)                                         AS customer_user_id,
    CAST(NULL AS INTEGER)                                         AS monitoring_approver_user_id,

    mis.workflow_status                                           AS workflow_status,

    CAST(NULL AS STRING)                                         AS payment_share,
    CAST(NULL AS STRING)                                         AS payee,
    CAST(NULL AS STRING)                                         AS is_fawateer,
    CAST(NULL AS STRING)                                         AS payment_request_type,
    CAST(NULL AS STRING)                                         AS program_name,
    --CAST(NULL AS STRING)                                         AS support_category,
    CAST(NULL AS STRING)                                         AS program_type_name,
    CAST(NULL AS STRING)                                         AS app_status,
    CAST(NULL AS STRING)                                         AS app_ref,

    mis.total_amount                                              AS total_amount,

    CAST(NULL AS DECIMAL(37,8))                                   AS total_item_cost,
    CAST(NULL AS DECIMAL(37,8))                                   AS total_vat_amount,
    CAST(NULL AS DECIMAL(38,8))                                   AS total_items_tamkeen_share,
    CAST(NULL AS DECIMAL(38,8))                                   AS total_items_applicant_share_with_vat,

    mis.created_on                                                AS created_on,

    CAST(NULL AS TIMESTAMP)                                    AS submitted_to_customer_on,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_on,

    mis.approved_on                                               AS approved_on,

    CAST(NULL AS TIMESTAMP)                                    AS sent_back_to_customer_on,
    CAST(NULL AS TIMESTAMP)                                    AS updated_on,

    mis.due_date                                                  AS due_date,

    CAST(NULL AS STRING)                                         AS erp_batch,
    CAST(NULL AS STRING)                                         AS erp_invoice,
    CAST(NULL AS STRING)                                         AS training_provider_cr,
    CAST(NULL AS STRING)                                         AS training_provider_name,
    CAST(NULL AS STRING)                                         AS vendor_cr,
    CAST(NULL AS STRING)                                         AS vendor_name,

    mis.accountant_name                                           AS user_accountant,

    CAST(NULL AS STRING)                                         AS user_payment_auditor,
    CAST(NULL AS STRING)                                         AS sent_back_to_customer_by,
    CAST(NULL AS STRING)                                         AS user_correction,
    CAST(NULL AS STRING)                                         AS approved_by,
    CAST(NULL AS STRING)                                         AS customer_name,

    CAST(NULL AS BOOLEAN)                                         AS claimed_by_vendor,
    CAST(NULL AS BOOLEAN)                                         AS is_need_attention,
    CAST(NULL AS INTEGER)                                         AS submission_count,
    CAST(NULL AS STRING)                                         AS training_payment_type,

    CAST(NULL AS TIMESTAMP)                     AS extract_date,
    CAST(NULL AS STRING)                                         AS payment_request_reference,
    CAST(NULL AS STRING)                                         AS application_reference,
    CAST(NULL AS STRING)                                         AS payment_request_status,
    CAST(NULL AS TIMESTAMP)                                    AS created_on_payment_request,
    CAST(NULL AS TIMESTAMP)                                    AS submitted_on_payment_request,
    CAST(NULL AS TIMESTAMP)                                    AS paid_on,

    mis.payment_type                                              AS payment_type,

    CAST(NULL AS STRING)                                         AS iban,
    CAST(NULL AS STRING)                                         AS iban_status,
    CAST(NULL AS STRING)                                         AS payee_type,

    CAST(NULL AS DECIMAL(37,3))                                   AS total_cost_value,
    CAST(NULL AS DECIMAL(37,3))                                   AS tamkeen_share_value,
    CAST(NULL AS DECIMAL(37,3))                                   AS customer_share_value,

    CAST(NULL AS STRING)                                         AS fawateer_reference,
    CAST(NULL AS STRING)                                         AS origin_system,
    CAST(NULL AS STRING)                                         AS created_by,

    CAST(NULL AS TIMESTAMP)                                    AS updatedon,
    CAST(NULL AS TIMESTAMP)                                    AS createdon,

    mis.payment_name                                              AS payment_name,
    mis.parent_application_id                                     AS parent_application_id,
    mis.parent_application_name                                   AS parent_application_name,
    mis.enterprise_application_id                                 AS enterprise_application_id,
    mis.company_id                                                AS company_id,
    mis.aub_payment_id                                            AS aub_payment_id,
    mis.aub_payment_name                                          AS aub_payment_name,
    mis.gp_invoice_id                                             AS gp_invoice_id,
    mis.gp_invoice_name                                           AS gp_invoice_name,
    mis.submitted_for_approval_on                                 AS submitted_for_approval_on,
    mis.validated_on                                              AS validated_on,
    mis.submitted_to_finance_on                                   AS submitted_to_finance_on,
    mis.approved_by_fc_on                                         AS approved_by_fc_on,
    mis.checked_by_accountant_on                                  AS checked_by_accountant_on,
    mis.receipt_amount                                            AS receipt_amount,
    mis.verified_amount                                           AS verified_amount,
    mis.state                                                     AS state,
    mis.status_reason                                             AS status_reason,
    mis.payment_processed                                         AS payment_processed,
    mis.sent_back_before                                          AS sent_back_before,
    mis.payment_status                                            AS payment_status,
    mis.payable_to                                                AS payable_to,
    mis.violation_status                                          AS violation_status,
    mis.has_committed_to_gp                                       AS has_committed_to_gp,
    mis.finance_approval                                          AS finance_approval,
    mis.is_flagged                                                AS is_flagged,
    mis.accountant_name                                           AS accountant_name,
    mis.verifying_user                                            AS verifying_user,
    mis.fc_user                                                   AS fc_user,
    mis.owner_name                                                AS owner_name,
    mis.is_migrated                                               AS is_migrated,
    CAST(NULL AS BIGINT)         AS payment_support_id,
    CAST(NULL AS BIGINT)         AS application_support_id,
    CAST(NULL AS STRING)        AS individual_name,
    CAST(NULL AS STRING)        AS individual_cpr,
    CAST(NULL AS STRING)        AS enterprise_cr_license,
    CAST(NULL AS STRING)        AS enterprise_cr_status,
    CAST(NULL AS STRING)        AS enterprise_commercial_name_en,
    CAST(NULL AS STRING)        AS training_provider_location,
    CAST(NULL AS STRING)        AS support_type,
    CAST(NULL AS DECIMAL(37,3))  AS total_amount_tamkeen_share,
    CAST(NULL AS DECIMAL(37,8))  AS tamkeen_share_support,
    CAST(NULL AS DECIMAL(37,8))  AS customer_share_support,
    CAST(NULL AS DECIMAL(37,8))  AS sio_deductions,
    CAST(NULL AS DECIMAL(37,8))  AS attendance_deductions,
    CAST(NULL AS DECIMAL(37,8))  AS other_deductions,
    CAST(NULL AS DECIMAL(38,8))  AS total_deductions,
    CAST(NULL AS STRING)        AS process_type,
	CAST(NULL AS STRING)         AS commercial_name_ar,
    CAST(NULL AS STRING)         AS commercial_name_en,
    CAST(NULL AS TIMESTAMP)    AS closed,
    CAST(NULL AS STRING)         AS addresstown,
    CAST(NULL AS TIMESTAMP)    AS application_createdon,
    CAST(NULL AS STRING)         AS customertypeid,
    CAST(NULL AS TIMESTAMP)    AS registrationdate,
    CAST(NULL AS STRING)         AS application_status,
    CAST(NULL AS BIGINT)          AS amendmentrequestid,
    CAST(NULL AS STRING)         AS role_name,
    CAST(NULL AS STRING)         AS activity_label,
    CAST(NULL AS INTEGER)         AS activity_definition_id,
    CAST(NULL AS STRING)         AS owner,
    CAST(NULL AS BIGINT)          AS payment_request_id,
    CAST(NULL AS INTEGER)         AS total_months_experience,
    CAST(NULL AS INTEGER)         AS userid,
    mis.name         AS name,

    mis.source_system_name                                        AS source_system_name,
    mis.is_deleted                                                AS is_deleted,
    CAST(CURRENT_DATE AS DATE)  AS report_date,
    mis.dbt_updated_at

FROM payment_base_mis mis
),

silver_layer AS (
SELECT
    `payment_id`,
    `payment_subtype`,
    `source_table`,
    `payment_no`,
    `application_id`,
    `iban_id`,
    `payee_id`,
    `payment_type_id`,
    `payment_status_id`,
    `payment_to_vendor_type_id`,
    `erp_batch_id`,
    `erp_invoice_id`,
    `payment_request_type_id`,
    `support_category_id`,
    `training_provider_id`,
    `vendor_id`,
    `approved_by_user_id`,
    `payment_auditor_user_id`,
    `sent_back_by_user_id`,
    `correction_team_user_id`,
    `approver_user_id`,
    `customer_user_id`,
    `monitoring_approver_user_id`,
    `workflow_status`,
    `payment_share`,
    `payee`,
    `is_fawateer`,
    `payment_request_type`,
    `program_name`,
    `program_type_name`,
    `app_status`,
    `app_ref`,
    `total_amount`,
    `total_item_cost`,
    `total_vat_amount`,
    `total_items_tamkeen_share`,
    `total_items_applicant_share_with_vat`,
    `created_on`,
    `submitted_to_customer_on`,
    `submitted_on`,
    `approved_on`,
    `sent_back_to_customer_on`,
    `updated_on`,
    `due_date`,
    `erp_batch`,
    `erp_invoice`,
    `training_provider_cr`,
    `training_provider_name`,
    `vendor_cr`,
    `vendor_name`,
    `user_accountant`,
    `user_payment_auditor`,
    `sent_back_to_customer_by`,
    `user_correction`,
    `approved_by`,
    `customer_name`,
    `claimed_by_vendor`,
    `is_need_attention`,
    `submission_count`,
    `training_payment_type`,
    `extract_date`,
    `payment_request_reference`,
    `application_reference`,
    `payment_request_status`,
    `created_on_payment_request`,
    `submitted_on_payment_request`,
    `paid_on`,
    `payment_type`,
    `iban`,
    `iban_status`,
    `payee_type`,
    `total_cost_value`,
    `tamkeen_share_value`,
    `customer_share_value`,
    `fawateer_reference`,
    `origin_system`,
    `created_by`,
    `updatedon`,
    `createdon`,
    `payment_name`,
    `parent_application_id`,
    `parent_application_name`,
    `enterprise_application_id`,
    `company_id`,
    `aub_payment_id`,
    `aub_payment_name`,
    `gp_invoice_id`,
    `gp_invoice_name`,
    `submitted_for_approval_on`,
    `validated_on`,
    `submitted_to_finance_on`,
    `approved_by_fc_on`,
    `checked_by_accountant_on`,
    `receipt_amount`,
    `verified_amount`,
    `state`,
    `status_reason`,
    `payment_processed`,
    `sent_back_before`,
    `payment_status`,
    `payable_to`,
    `violation_status`,
    `has_committed_to_gp`,
    `finance_approval`,
    `is_flagged`,
    `accountant_name`,
    `verifying_user`,
    `fc_user`,
    `owner_name`,
    `is_migrated`,
    `payment_support_id`,
    `application_support_id`,
    `individual_name`,
    `individual_cpr`,
    `enterprise_cr_license`,
    `enterprise_cr_status`,
    `enterprise_commercial_name_en`,
    `training_provider_location`,
    `support_type`,
    `total_amount_tamkeen_share`,
    `tamkeen_share_support`,
    `customer_share_support`,
    `sio_deductions`,
    `attendance_deductions`,
    `other_deductions`,
    `total_deductions`,
    `process_type`,
    `commercial_name_ar`,
    `commercial_name_en`,
    `closed`,
    `addresstown`,
    `application_createdon`,
    `customertypeid`,
    `registrationdate`,
    `application_status`,
    `amendmentrequestid`,
    `role_name`,
    `activity_label`,
    `activity_definition_id`,
    `owner`,
    `payment_request_id`,
    `total_months_experience`,
    `userid`,
    `name`,
    `source_system_name`,
    `is_deleted`,
    `report_date`,
    `dbt_updated_at`
FROM `tmkn-dwh-iceberg-dev-fc`.`tmkn-aws-dwh-dev-iceberg-silver`.`payment_base`
),

bronze_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'payment_id'),
        (2, 'payment_subtype'),
        (3, 'source_table'),
        (4, 'payment_no'),
        (5, 'application_id'),
        (6, 'iban_id'),
        (7, 'payee_id'),
        (8, 'payment_type_id'),
        (9, 'payment_status_id'),
        (10, 'payment_to_vendor_type_id'),
        (11, 'erp_batch_id'),
        (12, 'erp_invoice_id'),
        (13, 'payment_request_type_id'),
        (14, 'support_category_id'),
        (15, 'training_provider_id'),
        (16, 'vendor_id'),
        (17, 'approved_by_user_id'),
        (18, 'payment_auditor_user_id'),
        (19, 'sent_back_by_user_id'),
        (20, 'correction_team_user_id'),
        (21, 'approver_user_id'),
        (22, 'customer_user_id'),
        (23, 'monitoring_approver_user_id'),
        (24, 'workflow_status'),
        (25, 'payment_share'),
        (26, 'payee'),
        (27, 'is_fawateer'),
        (28, 'payment_request_type'),
        (29, 'program_name'),
        (30, 'program_type_name'),
        (31, 'app_status'),
        (32, 'app_ref'),
        (33, 'total_amount'),
        (34, 'total_item_cost'),
        (35, 'total_vat_amount'),
        (36, 'total_items_tamkeen_share'),
        (37, 'total_items_applicant_share_with_vat'),
        (38, 'created_on'),
        (39, 'submitted_to_customer_on'),
        (40, 'submitted_on'),
        (41, 'approved_on'),
        (42, 'sent_back_to_customer_on'),
        (43, 'updated_on'),
        (44, 'due_date'),
        (45, 'erp_batch'),
        (46, 'erp_invoice'),
        (47, 'training_provider_cr'),
        (48, 'training_provider_name'),
        (49, 'vendor_cr'),
        (50, 'vendor_name'),
        (51, 'user_accountant'),
        (52, 'user_payment_auditor'),
        (53, 'sent_back_to_customer_by'),
        (54, 'user_correction'),
        (55, 'approved_by'),
        (56, 'customer_name'),
        (57, 'claimed_by_vendor'),
        (58, 'is_need_attention'),
        (59, 'submission_count'),
        (60, 'training_payment_type'),
        (61, 'extract_date'),
        (62, 'payment_request_reference'),
        (63, 'application_reference'),
        (64, 'payment_request_status'),
        (65, 'created_on_payment_request'),
        (66, 'submitted_on_payment_request'),
        (67, 'paid_on'),
        (68, 'payment_type'),
        (69, 'iban'),
        (70, 'iban_status'),
        (71, 'payee_type'),
        (72, 'total_cost_value'),
        (73, 'tamkeen_share_value'),
        (74, 'customer_share_value'),
        (75, 'fawateer_reference'),
        (76, 'origin_system'),
        (77, 'created_by'),
        (78, 'updatedon'),
        (79, 'createdon'),
        (80, 'payment_name'),
        (81, 'parent_application_id'),
        (82, 'parent_application_name'),
        (83, 'enterprise_application_id'),
        (84, 'company_id'),
        (85, 'aub_payment_id'),
        (86, 'aub_payment_name'),
        (87, 'gp_invoice_id'),
        (88, 'gp_invoice_name'),
        (89, 'submitted_for_approval_on'),
        (90, 'validated_on'),
        (91, 'submitted_to_finance_on'),
        (92, 'approved_by_fc_on'),
        (93, 'checked_by_accountant_on'),
        (94, 'receipt_amount'),
        (95, 'verified_amount'),
        (96, 'state'),
        (97, 'status_reason'),
        (98, 'payment_processed'),
        (99, 'sent_back_before'),
        (100, 'payment_status'),
        (101, 'payable_to'),
        (102, 'violation_status'),
        (103, 'has_committed_to_gp'),
        (104, 'finance_approval'),
        (105, 'is_flagged'),
        (106, 'accountant_name'),
        (107, 'verifying_user'),
        (108, 'fc_user'),
        (109, 'owner_name'),
        (110, 'is_migrated'),
        (111, 'payment_support_id'),
        (112, 'application_support_id'),
        (113, 'individual_name'),
        (114, 'individual_cpr'),
        (115, 'enterprise_cr_license'),
        (116, 'enterprise_cr_status'),
        (117, 'enterprise_commercial_name_en'),
        (118, 'training_provider_location'),
        (119, 'support_type'),
        (120, 'total_amount_tamkeen_share'),
        (121, 'tamkeen_share_support'),
        (122, 'customer_share_support'),
        (123, 'sio_deductions'),
        (124, 'attendance_deductions'),
        (125, 'other_deductions'),
        (126, 'total_deductions'),
        (127, 'process_type'),
        (128, 'commercial_name_ar'),
        (129, 'commercial_name_en'),
        (130, 'closed'),
        (131, 'addresstown'),
        (132, 'application_createdon'),
        (133, 'customertypeid'),
        (134, 'registrationdate'),
        (135, 'application_status'),
        (136, 'amendmentrequestid'),
        (137, 'role_name'),
        (138, 'activity_label'),
        (139, 'activity_definition_id'),
        (140, 'owner'),
        (141, 'payment_request_id'),
        (142, 'total_months_experience'),
        (143, 'userid'),
        (144, 'name'),
        (145, 'source_system_name'),
        (146, 'is_deleted'),
        (147, 'report_date'),
        (148, 'dbt_updated_at')
    ) AS t(column_position, column_name)
),

silver_columns AS (
    SELECT *
    FROM (VALUES
        (1, 'payment_id'),
        (2, 'payment_subtype'),
        (3, 'source_table'),
        (4, 'payment_no'),
        (5, 'application_id'),
        (6, 'iban_id'),
        (7, 'payee_id'),
        (8, 'payment_type_id'),
        (9, 'payment_status_id'),
        (10, 'payment_to_vendor_type_id'),
        (11, 'erp_batch_id'),
        (12, 'erp_invoice_id'),
        (13, 'payment_request_type_id'),
        (14, 'support_category_id'),
        (15, 'training_provider_id'),
        (16, 'vendor_id'),
        (17, 'approved_by_user_id'),
        (18, 'payment_auditor_user_id'),
        (19, 'sent_back_by_user_id'),
        (20, 'correction_team_user_id'),
        (21, 'approver_user_id'),
        (22, 'customer_user_id'),
        (23, 'monitoring_approver_user_id'),
        (24, 'workflow_status'),
        (25, 'payment_share'),
        (26, 'payee'),
        (27, 'is_fawateer'),
        (28, 'payment_request_type'),
        (29, 'program_name'),
        (30, 'program_type_name'),
        (31, 'app_status'),
        (32, 'app_ref'),
        (33, 'total_amount'),
        (34, 'total_item_cost'),
        (35, 'total_vat_amount'),
        (36, 'total_items_tamkeen_share'),
        (37, 'total_items_applicant_share_with_vat'),
        (38, 'created_on'),
        (39, 'submitted_to_customer_on'),
        (40, 'submitted_on'),
        (41, 'approved_on'),
        (42, 'sent_back_to_customer_on'),
        (43, 'updated_on'),
        (44, 'due_date'),
        (45, 'erp_batch'),
        (46, 'erp_invoice'),
        (47, 'training_provider_cr'),
        (48, 'training_provider_name'),
        (49, 'vendor_cr'),
        (50, 'vendor_name'),
        (51, 'user_accountant'),
        (52, 'user_payment_auditor'),
        (53, 'sent_back_to_customer_by'),
        (54, 'user_correction'),
        (55, 'approved_by'),
        (56, 'customer_name'),
        (57, 'claimed_by_vendor'),
        (58, 'is_need_attention'),
        (59, 'submission_count'),
        (60, 'training_payment_type'),
        (61, 'extract_date'),
        (62, 'payment_request_reference'),
        (63, 'application_reference'),
        (64, 'payment_request_status'),
        (65, 'created_on_payment_request'),
        (66, 'submitted_on_payment_request'),
        (67, 'paid_on'),
        (68, 'payment_type'),
        (69, 'iban'),
        (70, 'iban_status'),
        (71, 'payee_type'),
        (72, 'total_cost_value'),
        (73, 'tamkeen_share_value'),
        (74, 'customer_share_value'),
        (75, 'fawateer_reference'),
        (76, 'origin_system'),
        (77, 'created_by'),
        (78, 'updatedon'),
        (79, 'createdon'),
        (80, 'payment_name'),
        (81, 'parent_application_id'),
        (82, 'parent_application_name'),
        (83, 'enterprise_application_id'),
        (84, 'company_id'),
        (85, 'aub_payment_id'),
        (86, 'aub_payment_name'),
        (87, 'gp_invoice_id'),
        (88, 'gp_invoice_name'),
        (89, 'submitted_for_approval_on'),
        (90, 'validated_on'),
        (91, 'submitted_to_finance_on'),
        (92, 'approved_by_fc_on'),
        (93, 'checked_by_accountant_on'),
        (94, 'receipt_amount'),
        (95, 'verified_amount'),
        (96, 'state'),
        (97, 'status_reason'),
        (98, 'payment_processed'),
        (99, 'sent_back_before'),
        (100, 'payment_status'),
        (101, 'payable_to'),
        (102, 'violation_status'),
        (103, 'has_committed_to_gp'),
        (104, 'finance_approval'),
        (105, 'is_flagged'),
        (106, 'accountant_name'),
        (107, 'verifying_user'),
        (108, 'fc_user'),
        (109, 'owner_name'),
        (110, 'is_migrated'),
        (111, 'payment_support_id'),
        (112, 'application_support_id'),
        (113, 'individual_name'),
        (114, 'individual_cpr'),
        (115, 'enterprise_cr_license'),
        (116, 'enterprise_cr_status'),
        (117, 'enterprise_commercial_name_en'),
        (118, 'training_provider_location'),
        (119, 'support_type'),
        (120, 'total_amount_tamkeen_share'),
        (121, 'tamkeen_share_support'),
        (122, 'customer_share_support'),
        (123, 'sio_deductions'),
        (124, 'attendance_deductions'),
        (125, 'other_deductions'),
        (126, 'total_deductions'),
        (127, 'process_type'),
        (128, 'commercial_name_ar'),
        (129, 'commercial_name_en'),
        (130, 'closed'),
        (131, 'addresstown'),
        (132, 'application_createdon'),
        (133, 'customertypeid'),
        (134, 'registrationdate'),
        (135, 'application_status'),
        (136, 'amendmentrequestid'),
        (137, 'role_name'),
        (138, 'activity_label'),
        (139, 'activity_definition_id'),
        (140, 'owner'),
        (141, 'payment_request_id'),
        (142, 'total_months_experience'),
        (143, 'userid'),
        (144, 'name'),
        (145, 'source_system_name'),
        (146, 'is_deleted'),
        (147, 'report_date'),
        (148, 'dbt_updated_at')
    ) AS t(column_position, column_name)
),

bronze_normalized AS (
    SELECT
        CAST(`payment_id` AS STRING) AS `payment_id`,
        CAST(`payment_subtype` AS STRING) AS `payment_subtype`,
        CAST(`source_table` AS STRING) AS `source_table`,
        CAST(`payment_no` AS STRING) AS `payment_no`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`iban_id` AS STRING) AS `iban_id`,
        CAST(`payee_id` AS STRING) AS `payee_id`,
        CAST(`payment_type_id` AS STRING) AS `payment_type_id`,
        CAST(`payment_status_id` AS STRING) AS `payment_status_id`,
        CAST(`payment_to_vendor_type_id` AS STRING) AS `payment_to_vendor_type_id`,
        CAST(`erp_batch_id` AS STRING) AS `erp_batch_id`,
        CAST(`erp_invoice_id` AS STRING) AS `erp_invoice_id`,
        CAST(`payment_request_type_id` AS STRING) AS `payment_request_type_id`,
        CAST(`support_category_id` AS STRING) AS `support_category_id`,
        CAST(`training_provider_id` AS STRING) AS `training_provider_id`,
        CAST(`vendor_id` AS STRING) AS `vendor_id`,
        CAST(`approved_by_user_id` AS STRING) AS `approved_by_user_id`,
        CAST(`payment_auditor_user_id` AS STRING) AS `payment_auditor_user_id`,
        CAST(`sent_back_by_user_id` AS STRING) AS `sent_back_by_user_id`,
        CAST(`correction_team_user_id` AS STRING) AS `correction_team_user_id`,
        CAST(`approver_user_id` AS STRING) AS `approver_user_id`,
        CAST(`customer_user_id` AS STRING) AS `customer_user_id`,
        CAST(`monitoring_approver_user_id` AS STRING) AS `monitoring_approver_user_id`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`payment_share` AS STRING) AS `payment_share`,
        CAST(`payee` AS STRING) AS `payee`,
        CAST(`is_fawateer` AS STRING) AS `is_fawateer`,
        CAST(`payment_request_type` AS STRING) AS `payment_request_type`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_type_name` AS STRING) AS `program_type_name`,
        CAST(`app_status` AS STRING) AS `app_status`,
        CAST(`app_ref` AS STRING) AS `app_ref`,
        CAST(`total_amount` AS STRING) AS `total_amount`,
        CAST(`total_item_cost` AS STRING) AS `total_item_cost`,
        CAST(`total_vat_amount` AS STRING) AS `total_vat_amount`,
        CAST(`total_items_tamkeen_share` AS STRING) AS `total_items_tamkeen_share`,
        CAST(`total_items_applicant_share_with_vat` AS STRING) AS `total_items_applicant_share_with_vat`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`submitted_to_customer_on` AS STRING) AS `submitted_to_customer_on`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`sent_back_to_customer_on` AS STRING) AS `sent_back_to_customer_on`,
        CAST(`updated_on` AS STRING) AS `updated_on`,
        CAST(`due_date` AS STRING) AS `due_date`,
        CAST(`erp_batch` AS STRING) AS `erp_batch`,
        CAST(`erp_invoice` AS STRING) AS `erp_invoice`,
        CAST(`training_provider_cr` AS STRING) AS `training_provider_cr`,
        CAST(`training_provider_name` AS STRING) AS `training_provider_name`,
        CAST(`vendor_cr` AS STRING) AS `vendor_cr`,
        CAST(`vendor_name` AS STRING) AS `vendor_name`,
        CAST(`user_accountant` AS STRING) AS `user_accountant`,
        CAST(`user_payment_auditor` AS STRING) AS `user_payment_auditor`,
        CAST(`sent_back_to_customer_by` AS STRING) AS `sent_back_to_customer_by`,
        CAST(`user_correction` AS STRING) AS `user_correction`,
        CAST(`approved_by` AS STRING) AS `approved_by`,
        CAST(`customer_name` AS STRING) AS `customer_name`,
        CAST(`claimed_by_vendor` AS STRING) AS `claimed_by_vendor`,
        CAST(`is_need_attention` AS STRING) AS `is_need_attention`,
        CAST(`submission_count` AS STRING) AS `submission_count`,
        CAST(`training_payment_type` AS STRING) AS `training_payment_type`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`payment_request_reference` AS STRING) AS `payment_request_reference`,
        CAST(`application_reference` AS STRING) AS `application_reference`,
        CAST(`payment_request_status` AS STRING) AS `payment_request_status`,
        CAST(`created_on_payment_request` AS STRING) AS `created_on_payment_request`,
        CAST(`submitted_on_payment_request` AS STRING) AS `submitted_on_payment_request`,
        CAST(`paid_on` AS STRING) AS `paid_on`,
        CAST(`payment_type` AS STRING) AS `payment_type`,
        CAST(`iban` AS STRING) AS `iban`,
        CAST(`iban_status` AS STRING) AS `iban_status`,
        CAST(`payee_type` AS STRING) AS `payee_type`,
        CAST(`total_cost_value` AS STRING) AS `total_cost_value`,
        CAST(`tamkeen_share_value` AS STRING) AS `tamkeen_share_value`,
        CAST(`customer_share_value` AS STRING) AS `customer_share_value`,
        CAST(`fawateer_reference` AS STRING) AS `fawateer_reference`,
        CAST(`origin_system` AS STRING) AS `origin_system`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`payment_name` AS STRING) AS `payment_name`,
        CAST(`parent_application_id` AS STRING) AS `parent_application_id`,
        CAST(`parent_application_name` AS STRING) AS `parent_application_name`,
        CAST(`enterprise_application_id` AS STRING) AS `enterprise_application_id`,
        CAST(`company_id` AS STRING) AS `company_id`,
        CAST(`aub_payment_id` AS STRING) AS `aub_payment_id`,
        CAST(`aub_payment_name` AS STRING) AS `aub_payment_name`,
        CAST(`gp_invoice_id` AS STRING) AS `gp_invoice_id`,
        CAST(`gp_invoice_name` AS STRING) AS `gp_invoice_name`,
        CAST(`submitted_for_approval_on` AS STRING) AS `submitted_for_approval_on`,
        CAST(`validated_on` AS STRING) AS `validated_on`,
        CAST(`submitted_to_finance_on` AS STRING) AS `submitted_to_finance_on`,
        CAST(`approved_by_fc_on` AS STRING) AS `approved_by_fc_on`,
        CAST(`checked_by_accountant_on` AS STRING) AS `checked_by_accountant_on`,
        CAST(`receipt_amount` AS STRING) AS `receipt_amount`,
        CAST(`verified_amount` AS STRING) AS `verified_amount`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`payment_processed` AS STRING) AS `payment_processed`,
        CAST(`sent_back_before` AS STRING) AS `sent_back_before`,
        CAST(`payment_status` AS STRING) AS `payment_status`,
        CAST(`payable_to` AS STRING) AS `payable_to`,
        CAST(`violation_status` AS STRING) AS `violation_status`,
        CAST(`has_committed_to_gp` AS STRING) AS `has_committed_to_gp`,
        CAST(`finance_approval` AS STRING) AS `finance_approval`,
        CAST(`is_flagged` AS STRING) AS `is_flagged`,
        CAST(`accountant_name` AS STRING) AS `accountant_name`,
        CAST(`verifying_user` AS STRING) AS `verifying_user`,
        CAST(`fc_user` AS STRING) AS `fc_user`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`is_migrated` AS STRING) AS `is_migrated`,
        CAST(`payment_support_id` AS STRING) AS `payment_support_id`,
        CAST(`application_support_id` AS STRING) AS `application_support_id`,
        CAST(`individual_name` AS STRING) AS `individual_name`,
        CAST(`individual_cpr` AS STRING) AS `individual_cpr`,
        CAST(`enterprise_cr_license` AS STRING) AS `enterprise_cr_license`,
        CAST(`enterprise_cr_status` AS STRING) AS `enterprise_cr_status`,
        CAST(`enterprise_commercial_name_en` AS STRING) AS `enterprise_commercial_name_en`,
        CAST(`training_provider_location` AS STRING) AS `training_provider_location`,
        CAST(`support_type` AS STRING) AS `support_type`,
        CAST(`total_amount_tamkeen_share` AS STRING) AS `total_amount_tamkeen_share`,
        CAST(`tamkeen_share_support` AS STRING) AS `tamkeen_share_support`,
        CAST(`customer_share_support` AS STRING) AS `customer_share_support`,
        CAST(`sio_deductions` AS STRING) AS `sio_deductions`,
        CAST(`attendance_deductions` AS STRING) AS `attendance_deductions`,
        CAST(`other_deductions` AS STRING) AS `other_deductions`,
        CAST(`total_deductions` AS STRING) AS `total_deductions`,
        CAST(`process_type` AS STRING) AS `process_type`,
        CAST(`commercial_name_ar` AS STRING) AS `commercial_name_ar`,
        CAST(`commercial_name_en` AS STRING) AS `commercial_name_en`,
        CAST(`closed` AS STRING) AS `closed`,
        CAST(`addresstown` AS STRING) AS `addresstown`,
        CAST(`application_createdon` AS STRING) AS `application_createdon`,
        CAST(`customertypeid` AS STRING) AS `customertypeid`,
        CAST(`registrationdate` AS STRING) AS `registrationdate`,
        CAST(`application_status` AS STRING) AS `application_status`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`role_name` AS STRING) AS `role_name`,
        CAST(`activity_label` AS STRING) AS `activity_label`,
        CAST(`activity_definition_id` AS STRING) AS `activity_definition_id`,
        CAST(`owner` AS STRING) AS `owner`,
        CAST(`payment_request_id` AS STRING) AS `payment_request_id`,
        CAST(`total_months_experience` AS STRING) AS `total_months_experience`,
        CAST(`userid` AS STRING) AS `userid`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM bronze_layer
),

silver_normalized AS (
    SELECT
        CAST(`payment_id` AS STRING) AS `payment_id`,
        CAST(`payment_subtype` AS STRING) AS `payment_subtype`,
        CAST(`source_table` AS STRING) AS `source_table`,
        CAST(`payment_no` AS STRING) AS `payment_no`,
        CAST(`application_id` AS STRING) AS `application_id`,
        CAST(`iban_id` AS STRING) AS `iban_id`,
        CAST(`payee_id` AS STRING) AS `payee_id`,
        CAST(`payment_type_id` AS STRING) AS `payment_type_id`,
        CAST(`payment_status_id` AS STRING) AS `payment_status_id`,
        CAST(`payment_to_vendor_type_id` AS STRING) AS `payment_to_vendor_type_id`,
        CAST(`erp_batch_id` AS STRING) AS `erp_batch_id`,
        CAST(`erp_invoice_id` AS STRING) AS `erp_invoice_id`,
        CAST(`payment_request_type_id` AS STRING) AS `payment_request_type_id`,
        CAST(`support_category_id` AS STRING) AS `support_category_id`,
        CAST(`training_provider_id` AS STRING) AS `training_provider_id`,
        CAST(`vendor_id` AS STRING) AS `vendor_id`,
        CAST(`approved_by_user_id` AS STRING) AS `approved_by_user_id`,
        CAST(`payment_auditor_user_id` AS STRING) AS `payment_auditor_user_id`,
        CAST(`sent_back_by_user_id` AS STRING) AS `sent_back_by_user_id`,
        CAST(`correction_team_user_id` AS STRING) AS `correction_team_user_id`,
        CAST(`approver_user_id` AS STRING) AS `approver_user_id`,
        CAST(`customer_user_id` AS STRING) AS `customer_user_id`,
        CAST(`monitoring_approver_user_id` AS STRING) AS `monitoring_approver_user_id`,
        CAST(`workflow_status` AS STRING) AS `workflow_status`,
        CAST(`payment_share` AS STRING) AS `payment_share`,
        CAST(`payee` AS STRING) AS `payee`,
        CAST(`is_fawateer` AS STRING) AS `is_fawateer`,
        CAST(`payment_request_type` AS STRING) AS `payment_request_type`,
        CAST(`program_name` AS STRING) AS `program_name`,
        CAST(`program_type_name` AS STRING) AS `program_type_name`,
        CAST(`app_status` AS STRING) AS `app_status`,
        CAST(`app_ref` AS STRING) AS `app_ref`,
        CAST(`total_amount` AS STRING) AS `total_amount`,
        CAST(`total_item_cost` AS STRING) AS `total_item_cost`,
        CAST(`total_vat_amount` AS STRING) AS `total_vat_amount`,
        CAST(`total_items_tamkeen_share` AS STRING) AS `total_items_tamkeen_share`,
        CAST(`total_items_applicant_share_with_vat` AS STRING) AS `total_items_applicant_share_with_vat`,
        CAST(`created_on` AS STRING) AS `created_on`,
        CAST(`submitted_to_customer_on` AS STRING) AS `submitted_to_customer_on`,
        CAST(`submitted_on` AS STRING) AS `submitted_on`,
        CAST(`approved_on` AS STRING) AS `approved_on`,
        CAST(`sent_back_to_customer_on` AS STRING) AS `sent_back_to_customer_on`,
        CAST(`updated_on` AS STRING) AS `updated_on`,
        CAST(`due_date` AS STRING) AS `due_date`,
        CAST(`erp_batch` AS STRING) AS `erp_batch`,
        CAST(`erp_invoice` AS STRING) AS `erp_invoice`,
        CAST(`training_provider_cr` AS STRING) AS `training_provider_cr`,
        CAST(`training_provider_name` AS STRING) AS `training_provider_name`,
        CAST(`vendor_cr` AS STRING) AS `vendor_cr`,
        CAST(`vendor_name` AS STRING) AS `vendor_name`,
        CAST(`user_accountant` AS STRING) AS `user_accountant`,
        CAST(`user_payment_auditor` AS STRING) AS `user_payment_auditor`,
        CAST(`sent_back_to_customer_by` AS STRING) AS `sent_back_to_customer_by`,
        CAST(`user_correction` AS STRING) AS `user_correction`,
        CAST(`approved_by` AS STRING) AS `approved_by`,
        CAST(`customer_name` AS STRING) AS `customer_name`,
        CAST(`claimed_by_vendor` AS STRING) AS `claimed_by_vendor`,
        CAST(`is_need_attention` AS STRING) AS `is_need_attention`,
        CAST(`submission_count` AS STRING) AS `submission_count`,
        CAST(`training_payment_type` AS STRING) AS `training_payment_type`,
        CAST(`extract_date` AS STRING) AS `extract_date`,
        CAST(`payment_request_reference` AS STRING) AS `payment_request_reference`,
        CAST(`application_reference` AS STRING) AS `application_reference`,
        CAST(`payment_request_status` AS STRING) AS `payment_request_status`,
        CAST(`created_on_payment_request` AS STRING) AS `created_on_payment_request`,
        CAST(`submitted_on_payment_request` AS STRING) AS `submitted_on_payment_request`,
        CAST(`paid_on` AS STRING) AS `paid_on`,
        CAST(`payment_type` AS STRING) AS `payment_type`,
        CAST(`iban` AS STRING) AS `iban`,
        CAST(`iban_status` AS STRING) AS `iban_status`,
        CAST(`payee_type` AS STRING) AS `payee_type`,
        CAST(`total_cost_value` AS STRING) AS `total_cost_value`,
        CAST(`tamkeen_share_value` AS STRING) AS `tamkeen_share_value`,
        CAST(`customer_share_value` AS STRING) AS `customer_share_value`,
        CAST(`fawateer_reference` AS STRING) AS `fawateer_reference`,
        CAST(`origin_system` AS STRING) AS `origin_system`,
        CAST(`created_by` AS STRING) AS `created_by`,
        CAST(`updatedon` AS STRING) AS `updatedon`,
        CAST(`createdon` AS STRING) AS `createdon`,
        CAST(`payment_name` AS STRING) AS `payment_name`,
        CAST(`parent_application_id` AS STRING) AS `parent_application_id`,
        CAST(`parent_application_name` AS STRING) AS `parent_application_name`,
        CAST(`enterprise_application_id` AS STRING) AS `enterprise_application_id`,
        CAST(`company_id` AS STRING) AS `company_id`,
        CAST(`aub_payment_id` AS STRING) AS `aub_payment_id`,
        CAST(`aub_payment_name` AS STRING) AS `aub_payment_name`,
        CAST(`gp_invoice_id` AS STRING) AS `gp_invoice_id`,
        CAST(`gp_invoice_name` AS STRING) AS `gp_invoice_name`,
        CAST(`submitted_for_approval_on` AS STRING) AS `submitted_for_approval_on`,
        CAST(`validated_on` AS STRING) AS `validated_on`,
        CAST(`submitted_to_finance_on` AS STRING) AS `submitted_to_finance_on`,
        CAST(`approved_by_fc_on` AS STRING) AS `approved_by_fc_on`,
        CAST(`checked_by_accountant_on` AS STRING) AS `checked_by_accountant_on`,
        CAST(`receipt_amount` AS STRING) AS `receipt_amount`,
        CAST(`verified_amount` AS STRING) AS `verified_amount`,
        CAST(`state` AS STRING) AS `state`,
        CAST(`status_reason` AS STRING) AS `status_reason`,
        CAST(`payment_processed` AS STRING) AS `payment_processed`,
        CAST(`sent_back_before` AS STRING) AS `sent_back_before`,
        CAST(`payment_status` AS STRING) AS `payment_status`,
        CAST(`payable_to` AS STRING) AS `payable_to`,
        CAST(`violation_status` AS STRING) AS `violation_status`,
        CAST(`has_committed_to_gp` AS STRING) AS `has_committed_to_gp`,
        CAST(`finance_approval` AS STRING) AS `finance_approval`,
        CAST(`is_flagged` AS STRING) AS `is_flagged`,
        CAST(`accountant_name` AS STRING) AS `accountant_name`,
        CAST(`verifying_user` AS STRING) AS `verifying_user`,
        CAST(`fc_user` AS STRING) AS `fc_user`,
        CAST(`owner_name` AS STRING) AS `owner_name`,
        CAST(`is_migrated` AS STRING) AS `is_migrated`,
        CAST(`payment_support_id` AS STRING) AS `payment_support_id`,
        CAST(`application_support_id` AS STRING) AS `application_support_id`,
        CAST(`individual_name` AS STRING) AS `individual_name`,
        CAST(`individual_cpr` AS STRING) AS `individual_cpr`,
        CAST(`enterprise_cr_license` AS STRING) AS `enterprise_cr_license`,
        CAST(`enterprise_cr_status` AS STRING) AS `enterprise_cr_status`,
        CAST(`enterprise_commercial_name_en` AS STRING) AS `enterprise_commercial_name_en`,
        CAST(`training_provider_location` AS STRING) AS `training_provider_location`,
        CAST(`support_type` AS STRING) AS `support_type`,
        CAST(`total_amount_tamkeen_share` AS STRING) AS `total_amount_tamkeen_share`,
        CAST(`tamkeen_share_support` AS STRING) AS `tamkeen_share_support`,
        CAST(`customer_share_support` AS STRING) AS `customer_share_support`,
        CAST(`sio_deductions` AS STRING) AS `sio_deductions`,
        CAST(`attendance_deductions` AS STRING) AS `attendance_deductions`,
        CAST(`other_deductions` AS STRING) AS `other_deductions`,
        CAST(`total_deductions` AS STRING) AS `total_deductions`,
        CAST(`process_type` AS STRING) AS `process_type`,
        CAST(`commercial_name_ar` AS STRING) AS `commercial_name_ar`,
        CAST(`commercial_name_en` AS STRING) AS `commercial_name_en`,
        CAST(`closed` AS STRING) AS `closed`,
        CAST(`addresstown` AS STRING) AS `addresstown`,
        CAST(`application_createdon` AS STRING) AS `application_createdon`,
        CAST(`customertypeid` AS STRING) AS `customertypeid`,
        CAST(`registrationdate` AS STRING) AS `registrationdate`,
        CAST(`application_status` AS STRING) AS `application_status`,
        CAST(`amendmentrequestid` AS STRING) AS `amendmentrequestid`,
        CAST(`role_name` AS STRING) AS `role_name`,
        CAST(`activity_label` AS STRING) AS `activity_label`,
        CAST(`activity_definition_id` AS STRING) AS `activity_definition_id`,
        CAST(`owner` AS STRING) AS `owner`,
        CAST(`payment_request_id` AS STRING) AS `payment_request_id`,
        CAST(`total_months_experience` AS STRING) AS `total_months_experience`,
        CAST(`userid` AS STRING) AS `userid`,
        CAST(`name` AS STRING) AS `name`,
        CAST(`source_system_name` AS STRING) AS `source_system_name`,
        CAST(`is_deleted` AS STRING) AS `is_deleted`,
        CAST(`report_date` AS STRING) AS `report_date`,
        CAST(`dbt_updated_at` AS STRING) AS `dbt_updated_at`
    FROM silver_layer
),

combined_normalized AS (
    SELECT
        'bronze' AS comparison_source,
        `payment_id`,
        `payment_subtype`,
        `source_table`,
        `payment_no`,
        `application_id`,
        `iban_id`,
        `payee_id`,
        `payment_type_id`,
        `payment_status_id`,
        `payment_to_vendor_type_id`,
        `erp_batch_id`,
        `erp_invoice_id`,
        `payment_request_type_id`,
        `support_category_id`,
        `training_provider_id`,
        `vendor_id`,
        `approved_by_user_id`,
        `payment_auditor_user_id`,
        `sent_back_by_user_id`,
        `correction_team_user_id`,
        `approver_user_id`,
        `customer_user_id`,
        `monitoring_approver_user_id`,
        `workflow_status`,
        `payment_share`,
        `payee`,
        `is_fawateer`,
        `payment_request_type`,
        `program_name`,
        `program_type_name`,
        `app_status`,
        `app_ref`,
        `total_amount`,
        `total_item_cost`,
        `total_vat_amount`,
        `total_items_tamkeen_share`,
        `total_items_applicant_share_with_vat`,
        `created_on`,
        `submitted_to_customer_on`,
        `submitted_on`,
        `approved_on`,
        `sent_back_to_customer_on`,
        `updated_on`,
        `due_date`,
        `erp_batch`,
        `erp_invoice`,
        `training_provider_cr`,
        `training_provider_name`,
        `vendor_cr`,
        `vendor_name`,
        `user_accountant`,
        `user_payment_auditor`,
        `sent_back_to_customer_by`,
        `user_correction`,
        `approved_by`,
        `customer_name`,
        `claimed_by_vendor`,
        `is_need_attention`,
        `submission_count`,
        `training_payment_type`,
        `extract_date`,
        `payment_request_reference`,
        `application_reference`,
        `payment_request_status`,
        `created_on_payment_request`,
        `submitted_on_payment_request`,
        `paid_on`,
        `payment_type`,
        `iban`,
        `iban_status`,
        `payee_type`,
        `total_cost_value`,
        `tamkeen_share_value`,
        `customer_share_value`,
        `fawateer_reference`,
        `origin_system`,
        `created_by`,
        `updatedon`,
        `createdon`,
        `payment_name`,
        `parent_application_id`,
        `parent_application_name`,
        `enterprise_application_id`,
        `company_id`,
        `aub_payment_id`,
        `aub_payment_name`,
        `gp_invoice_id`,
        `gp_invoice_name`,
        `submitted_for_approval_on`,
        `validated_on`,
        `submitted_to_finance_on`,
        `approved_by_fc_on`,
        `checked_by_accountant_on`,
        `receipt_amount`,
        `verified_amount`,
        `state`,
        `status_reason`,
        `payment_processed`,
        `sent_back_before`,
        `payment_status`,
        `payable_to`,
        `violation_status`,
        `has_committed_to_gp`,
        `finance_approval`,
        `is_flagged`,
        `accountant_name`,
        `verifying_user`,
        `fc_user`,
        `owner_name`,
        `is_migrated`,
        `payment_support_id`,
        `application_support_id`,
        `individual_name`,
        `individual_cpr`,
        `enterprise_cr_license`,
        `enterprise_cr_status`,
        `enterprise_commercial_name_en`,
        `training_provider_location`,
        `support_type`,
        `total_amount_tamkeen_share`,
        `tamkeen_share_support`,
        `customer_share_support`,
        `sio_deductions`,
        `attendance_deductions`,
        `other_deductions`,
        `total_deductions`,
        `process_type`,
        `commercial_name_ar`,
        `commercial_name_en`,
        `closed`,
        `addresstown`,
        `application_createdon`,
        `customertypeid`,
        `registrationdate`,
        `application_status`,
        `amendmentrequestid`,
        `role_name`,
        `activity_label`,
        `activity_definition_id`,
        `owner`,
        `payment_request_id`,
        `total_months_experience`,
        `userid`,
        `name`,
        `source_system_name`,
        `is_deleted`,
        `report_date`,
        `dbt_updated_at`
    FROM bronze_normalized
    UNION ALL
    SELECT
        'silver' AS comparison_source,
        `payment_id`,
        `payment_subtype`,
        `source_table`,
        `payment_no`,
        `application_id`,
        `iban_id`,
        `payee_id`,
        `payment_type_id`,
        `payment_status_id`,
        `payment_to_vendor_type_id`,
        `erp_batch_id`,
        `erp_invoice_id`,
        `payment_request_type_id`,
        `support_category_id`,
        `training_provider_id`,
        `vendor_id`,
        `approved_by_user_id`,
        `payment_auditor_user_id`,
        `sent_back_by_user_id`,
        `correction_team_user_id`,
        `approver_user_id`,
        `customer_user_id`,
        `monitoring_approver_user_id`,
        `workflow_status`,
        `payment_share`,
        `payee`,
        `is_fawateer`,
        `payment_request_type`,
        `program_name`,
        `program_type_name`,
        `app_status`,
        `app_ref`,
        `total_amount`,
        `total_item_cost`,
        `total_vat_amount`,
        `total_items_tamkeen_share`,
        `total_items_applicant_share_with_vat`,
        `created_on`,
        `submitted_to_customer_on`,
        `submitted_on`,
        `approved_on`,
        `sent_back_to_customer_on`,
        `updated_on`,
        `due_date`,
        `erp_batch`,
        `erp_invoice`,
        `training_provider_cr`,
        `training_provider_name`,
        `vendor_cr`,
        `vendor_name`,
        `user_accountant`,
        `user_payment_auditor`,
        `sent_back_to_customer_by`,
        `user_correction`,
        `approved_by`,
        `customer_name`,
        `claimed_by_vendor`,
        `is_need_attention`,
        `submission_count`,
        `training_payment_type`,
        `extract_date`,
        `payment_request_reference`,
        `application_reference`,
        `payment_request_status`,
        `created_on_payment_request`,
        `submitted_on_payment_request`,
        `paid_on`,
        `payment_type`,
        `iban`,
        `iban_status`,
        `payee_type`,
        `total_cost_value`,
        `tamkeen_share_value`,
        `customer_share_value`,
        `fawateer_reference`,
        `origin_system`,
        `created_by`,
        `updatedon`,
        `createdon`,
        `payment_name`,
        `parent_application_id`,
        `parent_application_name`,
        `enterprise_application_id`,
        `company_id`,
        `aub_payment_id`,
        `aub_payment_name`,
        `gp_invoice_id`,
        `gp_invoice_name`,
        `submitted_for_approval_on`,
        `validated_on`,
        `submitted_to_finance_on`,
        `approved_by_fc_on`,
        `checked_by_accountant_on`,
        `receipt_amount`,
        `verified_amount`,
        `state`,
        `status_reason`,
        `payment_processed`,
        `sent_back_before`,
        `payment_status`,
        `payable_to`,
        `violation_status`,
        `has_committed_to_gp`,
        `finance_approval`,
        `is_flagged`,
        `accountant_name`,
        `verifying_user`,
        `fc_user`,
        `owner_name`,
        `is_migrated`,
        `payment_support_id`,
        `application_support_id`,
        `individual_name`,
        `individual_cpr`,
        `enterprise_cr_license`,
        `enterprise_cr_status`,
        `enterprise_commercial_name_en`,
        `training_provider_location`,
        `support_type`,
        `total_amount_tamkeen_share`,
        `tamkeen_share_support`,
        `customer_share_support`,
        `sio_deductions`,
        `attendance_deductions`,
        `other_deductions`,
        `total_deductions`,
        `process_type`,
        `commercial_name_ar`,
        `commercial_name_en`,
        `closed`,
        `addresstown`,
        `application_createdon`,
        `customertypeid`,
        `registrationdate`,
        `application_status`,
        `amendmentrequestid`,
        `role_name`,
        `activity_label`,
        `activity_definition_id`,
        `owner`,
        `payment_request_id`,
        `total_months_experience`,
        `userid`,
        `name`,
        `source_system_name`,
        `is_deleted`,
        `report_date`,
        `dbt_updated_at`
    FROM silver_normalized
),

grouped_comparison AS (
    SELECT
        `payment_id`,
        `payment_subtype`,
        `source_table`,
        `payment_no`,
        `application_id`,
        `iban_id`,
        `payee_id`,
        `payment_type_id`,
        `payment_status_id`,
        `payment_to_vendor_type_id`,
        `erp_batch_id`,
        `erp_invoice_id`,
        `payment_request_type_id`,
        `support_category_id`,
        `training_provider_id`,
        `vendor_id`,
        `approved_by_user_id`,
        `payment_auditor_user_id`,
        `sent_back_by_user_id`,
        `correction_team_user_id`,
        `approver_user_id`,
        `customer_user_id`,
        `monitoring_approver_user_id`,
        `workflow_status`,
        `payment_share`,
        `payee`,
        `is_fawateer`,
        `payment_request_type`,
        `program_name`,
        `program_type_name`,
        `app_status`,
        `app_ref`,
        `total_amount`,
        `total_item_cost`,
        `total_vat_amount`,
        `total_items_tamkeen_share`,
        `total_items_applicant_share_with_vat`,
        `created_on`,
        `submitted_to_customer_on`,
        `submitted_on`,
        `approved_on`,
        `sent_back_to_customer_on`,
        `updated_on`,
        `due_date`,
        `erp_batch`,
        `erp_invoice`,
        `training_provider_cr`,
        `training_provider_name`,
        `vendor_cr`,
        `vendor_name`,
        `user_accountant`,
        `user_payment_auditor`,
        `sent_back_to_customer_by`,
        `user_correction`,
        `approved_by`,
        `customer_name`,
        `claimed_by_vendor`,
        `is_need_attention`,
        `submission_count`,
        `training_payment_type`,
        `extract_date`,
        `payment_request_reference`,
        `application_reference`,
        `payment_request_status`,
        `created_on_payment_request`,
        `submitted_on_payment_request`,
        `paid_on`,
        `payment_type`,
        `iban`,
        `iban_status`,
        `payee_type`,
        `total_cost_value`,
        `tamkeen_share_value`,
        `customer_share_value`,
        `fawateer_reference`,
        `origin_system`,
        `created_by`,
        `updatedon`,
        `createdon`,
        `payment_name`,
        `parent_application_id`,
        `parent_application_name`,
        `enterprise_application_id`,
        `company_id`,
        `aub_payment_id`,
        `aub_payment_name`,
        `gp_invoice_id`,
        `gp_invoice_name`,
        `submitted_for_approval_on`,
        `validated_on`,
        `submitted_to_finance_on`,
        `approved_by_fc_on`,
        `checked_by_accountant_on`,
        `receipt_amount`,
        `verified_amount`,
        `state`,
        `status_reason`,
        `payment_processed`,
        `sent_back_before`,
        `payment_status`,
        `payable_to`,
        `violation_status`,
        `has_committed_to_gp`,
        `finance_approval`,
        `is_flagged`,
        `accountant_name`,
        `verifying_user`,
        `fc_user`,
        `owner_name`,
        `is_migrated`,
        `payment_support_id`,
        `application_support_id`,
        `individual_name`,
        `individual_cpr`,
        `enterprise_cr_license`,
        `enterprise_cr_status`,
        `enterprise_commercial_name_en`,
        `training_provider_location`,
        `support_type`,
        `total_amount_tamkeen_share`,
        `tamkeen_share_support`,
        `customer_share_support`,
        `sio_deductions`,
        `attendance_deductions`,
        `other_deductions`,
        `total_deductions`,
        `process_type`,
        `commercial_name_ar`,
        `commercial_name_en`,
        `closed`,
        `addresstown`,
        `application_createdon`,
        `customertypeid`,
        `registrationdate`,
        `application_status`,
        `amendmentrequestid`,
        `role_name`,
        `activity_label`,
        `activity_definition_id`,
        `owner`,
        `payment_request_id`,
        `total_months_experience`,
        `userid`,
        `name`,
        `source_system_name`,
        `is_deleted`,
        `report_date`,
        `dbt_updated_at`,
        COUNT_IF(comparison_source = 'bronze') AS bronze_row_count,
        COUNT_IF(comparison_source = 'silver') AS silver_row_count
    FROM combined_normalized
    GROUP BY
        `payment_id`,
        `payment_subtype`,
        `source_table`,
        `payment_no`,
        `application_id`,
        `iban_id`,
        `payee_id`,
        `payment_type_id`,
        `payment_status_id`,
        `payment_to_vendor_type_id`,
        `erp_batch_id`,
        `erp_invoice_id`,
        `payment_request_type_id`,
        `support_category_id`,
        `training_provider_id`,
        `vendor_id`,
        `approved_by_user_id`,
        `payment_auditor_user_id`,
        `sent_back_by_user_id`,
        `correction_team_user_id`,
        `approver_user_id`,
        `customer_user_id`,
        `monitoring_approver_user_id`,
        `workflow_status`,
        `payment_share`,
        `payee`,
        `is_fawateer`,
        `payment_request_type`,
        `program_name`,
        `program_type_name`,
        `app_status`,
        `app_ref`,
        `total_amount`,
        `total_item_cost`,
        `total_vat_amount`,
        `total_items_tamkeen_share`,
        `total_items_applicant_share_with_vat`,
        `created_on`,
        `submitted_to_customer_on`,
        `submitted_on`,
        `approved_on`,
        `sent_back_to_customer_on`,
        `updated_on`,
        `due_date`,
        `erp_batch`,
        `erp_invoice`,
        `training_provider_cr`,
        `training_provider_name`,
        `vendor_cr`,
        `vendor_name`,
        `user_accountant`,
        `user_payment_auditor`,
        `sent_back_to_customer_by`,
        `user_correction`,
        `approved_by`,
        `customer_name`,
        `claimed_by_vendor`,
        `is_need_attention`,
        `submission_count`,
        `training_payment_type`,
        `extract_date`,
        `payment_request_reference`,
        `application_reference`,
        `payment_request_status`,
        `created_on_payment_request`,
        `submitted_on_payment_request`,
        `paid_on`,
        `payment_type`,
        `iban`,
        `iban_status`,
        `payee_type`,
        `total_cost_value`,
        `tamkeen_share_value`,
        `customer_share_value`,
        `fawateer_reference`,
        `origin_system`,
        `created_by`,
        `updatedon`,
        `createdon`,
        `payment_name`,
        `parent_application_id`,
        `parent_application_name`,
        `enterprise_application_id`,
        `company_id`,
        `aub_payment_id`,
        `aub_payment_name`,
        `gp_invoice_id`,
        `gp_invoice_name`,
        `submitted_for_approval_on`,
        `validated_on`,
        `submitted_to_finance_on`,
        `approved_by_fc_on`,
        `checked_by_accountant_on`,
        `receipt_amount`,
        `verified_amount`,
        `state`,
        `status_reason`,
        `payment_processed`,
        `sent_back_before`,
        `payment_status`,
        `payable_to`,
        `violation_status`,
        `has_committed_to_gp`,
        `finance_approval`,
        `is_flagged`,
        `accountant_name`,
        `verifying_user`,
        `fc_user`,
        `owner_name`,
        `is_migrated`,
        `payment_support_id`,
        `application_support_id`,
        `individual_name`,
        `individual_cpr`,
        `enterprise_cr_license`,
        `enterprise_cr_status`,
        `enterprise_commercial_name_en`,
        `training_provider_location`,
        `support_type`,
        `total_amount_tamkeen_share`,
        `tamkeen_share_support`,
        `customer_share_support`,
        `sio_deductions`,
        `attendance_deductions`,
        `other_deductions`,
        `total_deductions`,
        `process_type`,
        `commercial_name_ar`,
        `commercial_name_en`,
        `closed`,
        `addresstown`,
        `application_createdon`,
        `customertypeid`,
        `registrationdate`,
        `application_status`,
        `amendmentrequestid`,
        `role_name`,
        `activity_label`,
        `activity_definition_id`,
        `owner`,
        `payment_request_id`,
        `total_months_experience`,
        `userid`,
        `name`,
        `source_system_name`,
        `is_deleted`,
        `report_date`,
        `dbt_updated_at`
),

comparison_summary AS (
    SELECT
        COALESCE(SUM(bronze_row_count), 0) AS bronze_total_count,
        COALESCE(SUM(silver_row_count), 0) AS silver_total_count,
        COALESCE(SUM(GREATEST(COALESCE(bronze_row_count, 0) - COALESCE(silver_row_count, 0), 0)), 0) AS bronze_minus_silver_count,
        COALESCE(SUM(GREATEST(COALESCE(silver_row_count, 0) - COALESCE(bronze_row_count, 0), 0)), 0) AS silver_minus_bronze_count
    FROM grouped_comparison
),
summary_validation AS (
    SELECT
        'payment_base' AS table_name,
        validation_point,
        bronze_layer_count,
        silver_layer_count,
        validation_status
    FROM comparison_summary c
    LATERAL VIEW stack(
        3,
        'record_count',
        CAST(c.bronze_total_count AS BIGINT),
        CAST(c.silver_total_count AS BIGINT),
        CASE WHEN c.bronze_total_count = c.silver_total_count THEN 'PASS' ELSE 'FAIL' END,
        'mismatching_rows_bronze_minus_silver',
        CAST(c.bronze_minus_silver_count AS BIGINT),
        CAST(0 AS BIGINT),
        CASE WHEN c.bronze_minus_silver_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        'mismatching_rows_silver_minus_bronze',
        CAST(0 AS BIGINT),
        CAST(c.silver_minus_bronze_count AS BIGINT),
        CASE WHEN c.silver_minus_bronze_count = 0 THEN 'PASS' ELSE 'FAIL' END
    ) t AS validation_point, bronze_layer_count, silver_layer_count, validation_status
),
validation_results AS (
    SELECT
        table_name,
        validation_point,
        bronze_layer_count,
        silver_layer_count,
        validation_status
    FROM summary_validation

    UNION ALL

    SELECT
        'payment_base' AS table_name,
        'column_count' AS validation_point,
        CAST((SELECT COUNT(*) FROM bronze_columns) AS BIGINT) AS bronze_layer_count,
        CAST((SELECT COUNT(*) FROM silver_columns) AS BIGINT) AS silver_layer_count,
        CASE WHEN (SELECT COUNT(*) FROM bronze_columns) = (SELECT COUNT(*) FROM silver_columns) THEN 'PASS' ELSE 'FAIL' END AS validation_status

    UNION ALL

    SELECT
        'payment_base' AS table_name,
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
)
SELECT *
FROM validation_results
ORDER BY validation_point;



