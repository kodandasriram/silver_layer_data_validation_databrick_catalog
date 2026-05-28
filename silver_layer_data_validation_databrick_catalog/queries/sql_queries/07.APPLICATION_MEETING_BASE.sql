WITH bronze_layer AS (
    SELECT
    id
	--,entityguid
	,applicationid
	,meetstatus.code as meetingstatus ---meetingstatusid --OSUSR_ntp_MeetingStatus (CODE)
	,date
	--,time --time fromat is not matching with the source
	,meetloc.label as meetinglocationtype ---meetinglocationtypeid --OSUSR_ntp_MeetingLocationType (CODE)
	,meettype.label as meetingtype --meetingtypeid --OSUSR_ntp_MeetingType (CODE)
	,mobilecountryprefix
	,mobilenumber
	,emailaddress
	,tamkeenattendes
	,customerattendees
	,notes
	,actionpoints
	,remarks
	,proc.label as processtype -- OSUSR_MM5_PROCESSTYPE4
	---,instancedocguid
	--,createdby
	--,createdon
	--,updatedby
	--,updatedon
	--,bronze_created_on
	--,bronze_updated_on  
    FROM 
    dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_NTP_APPLICATIONMEETING4  as appmeet 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ntp_MeetingStatus as meetstatus 
        ON meetstatus.code = appmeet.meetingstatusid 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ntp_MeetingLocationType as meetloc 
        ON meetloc.code = appmeet.meetinglocationtypeid 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_ntp_MeetingType as meettype 
        ON meettype.code = appmeet.meetingtypeid 
    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_MM5_PROCESSTYPE4 as proc 
        ON proc.code = appmeet.processtype 
),

silver_layer AS (
   SELECT 
   	id
	,applicationid
	,meetingstatus
	,meeting_date
	--,meeting_time
	,meetinglocationtype
	,meetingtype
	,mobilecountryprefix
	,mobilenumber
	,emailaddress
	,tamkeenattendes
	,customerattendees
	,notes
	,actionpoints
	,remarks
	,processtype
   FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".APPLICATION_MEETING_BASE
)

-- =========================================



-- ✅ NULL PRIMARY KEY CHECK (BRONZE)
SELECT 'NULL_PK_BRONZE', COUNT(*), NULL
FROM bronze_layer
WHERE id IS NULL

UNION ALL

-- ✅ NULL PRIMARY KEY CHECK (SILVER)
SELECT 'NULL_PK_SILVER', COUNT(*), NULL
FROM silver_layer
WHERE id IS NULL

union all

SELECT 'COUNT_VALIDATION', COUNT(*), (SELECT COUNT(*) FROM silver_layer)
FROM bronze_layer

UNION ALL

SELECT 'DUPLICATE_BRONZE', COUNT(*), NULL
FROM (
    SELECT id FROM bronze_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

SELECT 'DUPLICATE_SILVER', COUNT(*), NULL
FROM (
    SELECT id FROM silver_layer GROUP BY id HAVING COUNT(*) > 1
)

UNION ALL

-- ✅ COLUMN MISMATCH (FIXED)
SELECT 'COLUMN_MISMATCH_COUNT', COUNT(*), NULL
FROM (
    SELECT *
    FROM bronze_layer b
    JOIN silver_layer s ON b.id = s.id
    WHERE
        COALESCE(CAST(b.applicationid AS VARCHAR), '') <> COALESCE(CAST(s.applicationid AS VARCHAR), '')
     OR COALESCE(CAST(b.meetingstatus AS VARCHAR), '') <> COALESCE(CAST(s.meetingstatus AS VARCHAR), '')
     OR COALESCE(CAST(b.date AS VARCHAR), '') <> COALESCE(CAST(s.meeting_date AS VARCHAR), '')
     --OR COALESCE(CAST(b.time AS VARCHAR), '') <> COALESCE(CAST(s.meeting_time AS VARCHAR), '')
     OR COALESCE(CAST(b.meetinglocationtype AS VARCHAR), '') <> COALESCE(CAST(s.meetinglocationtype AS VARCHAR), '')
     OR COALESCE(CAST(b.meetingtype AS VARCHAR), '') <> COALESCE(CAST(s.meetingtype AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilecountryprefix AS VARCHAR), '') <> COALESCE(CAST(s.mobilecountryprefix AS VARCHAR), '')
     OR COALESCE(CAST(b.mobilenumber AS VARCHAR), '') <> COALESCE(CAST(s.mobilenumber AS VARCHAR), '')
     OR COALESCE(CAST(b.emailaddress AS VARCHAR), '') <> COALESCE(CAST(s.emailaddress AS VARCHAR), '')
     OR COALESCE(CAST(b.tamkeenattendes AS VARCHAR), '') <> COALESCE(CAST(s.tamkeenattendes AS VARCHAR), '')
     OR COALESCE(CAST(b.customerattendees AS VARCHAR), '') <> COALESCE(CAST(s.customerattendees AS VARCHAR), '')
     OR COALESCE(CAST(b.notes AS VARCHAR), '') <> COALESCE(CAST(s.notes AS VARCHAR), '')
     OR COALESCE(CAST(b.actionpoints AS VARCHAR), '') <> COALESCE(CAST(s.actionpoints AS VARCHAR), '')
     OR COALESCE(CAST(b.remarks AS VARCHAR), '') <> COALESCE(CAST(s.remarks AS VARCHAR), '')
     OR COALESCE(CAST(b.processtype AS VARCHAR), '') <> COALESCE(CAST(s.processtype AS VARCHAR), '')
) t

UNION ALL

-- ✅ BRONZE NOT IN SILVER
SELECT 'BRONZE_NOT_IN_SILVER', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(meetingstatus AS VARCHAR),
        CAST(date AS VARCHAR),
        --CAST(time AS VARCHAR),
        CAST(meetinglocationtype AS VARCHAR),
        CAST(meetingtype AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(tamkeenattendes AS VARCHAR),
        CAST(customerattendees AS VARCHAR),
        CAST(notes AS VARCHAR),
        CAST(actionpoints AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(processtype AS VARCHAR)
    FROM bronze_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(meetingstatus AS VARCHAR),
        CAST(meeting_date AS VARCHAR),
        --CAST(meeting_time AS VARCHAR),
        CAST(meetinglocationtype AS VARCHAR),
        CAST(meetingtype AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(tamkeenattendes AS VARCHAR),
        CAST(customerattendees AS VARCHAR),
        CAST(notes AS VARCHAR),
        CAST(actionpoints AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(processtype AS VARCHAR)
    FROM silver_layer
) t

UNION ALL

-- ✅ SILVER NOT IN BRONZE
SELECT 'SILVER_NOT_IN_BRONZE', COUNT(*), NULL
FROM (
    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(meetingstatus AS VARCHAR),
        CAST(meeting_date AS VARCHAR),
        --CAST(meeting_time AS VARCHAR),
        CAST(meetinglocationtype AS VARCHAR),
        CAST(meetingtype AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(tamkeenattendes AS VARCHAR),
        CAST(customerattendees AS VARCHAR),
        CAST(notes AS VARCHAR),
        CAST(actionpoints AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(processtype AS VARCHAR)
    FROM silver_layer

    EXCEPT

    SELECT 
        id,
        CAST(applicationid AS VARCHAR),
        CAST(meetingstatus AS VARCHAR),
        CAST(date AS VARCHAR),
        --CAST(time AS VARCHAR),
        CAST(meetinglocationtype AS VARCHAR),
        CAST(meetingtype AS VARCHAR),
        CAST(mobilecountryprefix AS VARCHAR),
        CAST(mobilenumber AS VARCHAR),
        CAST(emailaddress AS VARCHAR),
        CAST(tamkeenattendes AS VARCHAR),
        CAST(customerattendees AS VARCHAR),
        CAST(notes AS VARCHAR),
        CAST(actionpoints AS VARCHAR),
        CAST(remarks AS VARCHAR),
        CAST(processtype AS VARCHAR)
    FROM bronze_layer
) t;