SELECT

    CURRENT_TIMESTAMP + INTERVAL '3' HOUR                AS "EXTRACT_DATE",

    APP.REFERENCENUMBER                                  AS "APPLICATION_NO",

    APP.ID                                               AS "APPLICATION_ID",

    tik.REFNUMBER                                        AS "SECURITY_CHEQUE_NO_TICKETS",

    SC.SECURITYID                                        AS "SECURITY_CHEQUE_REF",

    secstat.LABEL                                        AS "WORKFLOW_STATUS_SECURITY_CHEQUE",

    ProgVer.COMMERCIALNAME_EN                            AS "PROGRAM_NAME",

    collect.LABEL                                        AS "COLLECTION_METHOD",

    SC.SECURITYAMOUNT                                    AS "CHEQUE_AMOUNT",

    SC.SECURITYNUMBER                                    AS "SECURITY_NUMBER",

    sectype.LABEL                                        AS "SECURITY_TYPE",

    SC.ID                                         AS "ID",
    SC.COLLECTIONMETHODID                         AS "COLLECTION_METHOD_ID",
    SC.CHEQUEREFNUMBER                            AS "CHEQUE_REF_NUMBER",
    SC.SECURITYTYPEID                             AS "SECURITY_TYPE_ID",
    SC.SECURITYID                                 AS "SECURITY_ID",
    SC.SECURITYDATE                               AS "SECURITY_DATE",
    SC.BANKID                                     AS "BANK_ID",
    SC.SUBMITTEDLOCATIONID                        AS "SUBMITTED_LOCATION_ID",
    SC.SECURITYAMOUNT                             AS "SECURITY_AMOUNT",
    SC.ISSUEDATE                                  AS "ISSUE_DATE",
    SC.SECURITYSTATUSID                           AS "SECURITY_STATUS_ID",
    SC.APPLICATIONID                              AS "SECURITY_APPLICATION_ID",
    SC.RELEASEDON                                 AS "RELEASED_ON",
    SC.SECURITYCHEQUESTATUS                       AS "SECURITY_CHEQUE_STATUS",
    SC.PORTALUSER                                 AS "PORTAL_USER",
    SC.SECURITYCHEQUEPROCESSINGSTEP               AS "SECURITY_CHEQUE_PROCESSING_STEP",
    SC.ECHEQUEPROCESSEXTERNALEVENTS               AS "ECHEQUE_PROCESS_EXTERNAL_EVENTS",
    SC.RELEASEAGENTUSERID                         AS "RELEASE_AGENT_USER_ID",
    SC.RELEASECOMMENTS                            AS "RELEASE_COMMENTS",
    SC.DELIVERYMETHODID                           AS "DELIVERY_METHOD_ID",
    SC.COLLECTORCPR                               AS "COLLECTOR_CPR",
    SC.COLLECTORNAME                              AS "COLLECTOR_NAME",
    SC.COLLECTORSRELATIONSHIPTOENTE               AS "COLLECTOR_RELATIONSHIP_TO_ENTE",

    CASE
        WHEN SC.BANKID = 0
        THEN 'Unclassified'
        ELSE UPPER(TRIM(bank.BankName))
    END                                                  AS "BANK_NAME",

    sublocation.LABEL                                    AS "SUBMITTED_LOCATION",

    CASE
        WHEN SC.RELEASEDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN 'No'
        ELSE 'Yes'
    END                                                  AS "REPLACED",

    CASE
        WHEN SC.REPLACEDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE SC.REPLACEDON + INTERVAL '3' HOUR
    END                                                  AS "REPLACED_ON",

    CASE
        WHEN SC.CREATEDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE SC.CREATEDON + INTERVAL '3' HOUR
    END                                                  AS "DATE_COLLECTED",

    usr.NAME                                             AS "OWNER",

    UPPER(TRIM(CUS.NAMEEN))                              AS "COMMERCIAL_NAME",

    CASE
        WHEN APP.ENDON = TIMESTAMP '1900-01-01 00:00:00'
        THEN NULL
        ELSE CAST(APP.ENDON AS DATE)
    END                                                  AS "CONTACT_END_DATE",

    CMP.CODE                                             AS "CR_LICENSE_NO",

    secstatdetailed.LABEL                                AS "WORKFLOW_STATUS_DETAILED_SECURITY_CHEQUE"

FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_TICKET tik

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_TICKETSTATUS tikstat
    ON tik.TICKETSTATUSID = tikstat.CODE

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_SECURITYCHEQUE SC
    ON tik.ENTITYIDENTIFIER = SC.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATION APP
    ON APP.ID = SC.APPLICATIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_3QQ_PROGRAMVERSION ProgVer
    ON ProgVer.ID = APP.PROGRAMVERSIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONCUSTOMER APPCUS
    ON APP.ID = APPCUS.APPLICATIONID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMERPROFILE CUSPROF
    ON CUSPROF.ID = APPCUS.CUSTOMERPROFILEID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_CUSTOMER CUS
    ON CUSPROF.CUSTOMERID = CUS.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ZMZ_COMPANY CMP
    ON CUS.ID = CMP.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_COLLECTIONMETHOD collect
    ON SC.CollectionMethodId = collect.ID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_SUBMITTEDLOCATION sublocation
    ON sublocation.ID = SC.SubmittedLocationId

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSSYS_USER usr
    ON usr.USERNAME = SC.CREATEDBY

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_TLV_BANK bank
    ON bank.ID = SC.BANKID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_SECURITYTYPE sectype
    ON sectype.ID = SC.SecurityTypeId

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_SECURITYSTATUS secstat
    ON secstat.ID = SC.SECURITYSTATUSID

LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_5HX_SECURITYCHEQUESTATUS secstatdetailed
    ON secstatdetailed.CODE = SC.SECURITYCHEQUESTATUS

WHERE tik.TICKETTYPEID = 'SCK'
