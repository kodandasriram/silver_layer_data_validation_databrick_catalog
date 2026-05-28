WITH bronze_layer AS (
    select
    	ass.id,
		ass.applicationid,
		ass.amendmentrequestid,
		ass.assessmentrole1,
		ass.assessmentrole2,
		ass.reviewrole,
		ass.approverole,
		ass.processid,
		team1.name AS assessmentteam1_name,
		team2.name AS assessmentteam2_name,
		team3.name AS reviewteam1_name,
		team4.name AS approveteam1_name,
		assstatus.label AS assessmentstatusid,
		team5.name AS assessmentteammol_name,
		ass.reviewrole1,
		ass.reviewrole2,
		ass.reviewteam2,
		ass.monitoringrole1,
		ass.monitoringrole2,
		ass.monitoringteam1,
		ass.monitoringteam2
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENT ass

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team1
        ON team1.id = ass.assessmentteam1

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team2
        ON team2.id = ass.assessmentteam2

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team3
        ON team3.id = ass.reviewteam1

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team4
        ON team4.id = ass.approveteam1   -- ✅ FIXED

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_KUO_TEAM team5
        ON team5.id = ass.assessmentteammol 

    LEFT JOIN dev_iceberg."tmkn-aws-dwh-dev-iceberg-bronze".OSUSR_1AT_ASSESSMENTSTATUS assstatus
        ON assstatus.code = ass.assessmentstatusid
),

silver_layer AS (
    SELECT 
        id,
        applicationid,
        amendmentrequestid,
        assessmentrole1,
        assessmentrole2,
        reviewrole,
        approverole,
        processid,
        assessmentteam1_name,
        assessmentteam2_name,
        reviewteam1_name,
        approveteam1_name,
        assessmentstatusid,
        assessmentteammol_name,
        reviewrole1,
        reviewrole2,
        reviewteam2,
        monitoringrole1,
        monitoringrole2,
        monitoringteam1,
        monitoringteam2
    FROM dev_iceberg."tmkn-aws-dwh-dev-iceberg-silver".ASSESSMENT_BASE
)

SELECT *
FROM bronze_layer b
JOIN silver_layer s
    ON b.id = s.id
WHERE 
    b.applicationid IS DISTINCT FROM s.applicationid
 OR b.amendmentrequestid IS DISTINCT FROM s.amendmentrequestid
 OR b.assessmentrole1 IS DISTINCT FROM s.assessmentrole1
 OR b.assessmentrole2 IS DISTINCT FROM s.assessmentrole2
 OR b.reviewrole IS DISTINCT FROM s.reviewrole
 OR b.approverole IS DISTINCT FROM s.approverole
 OR b.processid IS DISTINCT FROM s.processid
 OR b.assessmentteam1_name IS DISTINCT FROM s.assessmentteam1_name
 OR b.assessmentteam2_name IS DISTINCT FROM s.assessmentteam2_name
 OR b.reviewteam1_name IS DISTINCT FROM s.reviewteam1_name
 OR b.approveteam1_name IS DISTINCT FROM s.approveteam1_name
 OR b.assessmentstatusid IS DISTINCT FROM s.assessmentstatusid
 OR b.assessmentteammol_name IS DISTINCT FROM s.assessmentteammol_name
 OR b.reviewrole1 IS DISTINCT FROM s.reviewrole1
 OR b.reviewrole2 IS DISTINCT FROM s.reviewrole2
 OR b.reviewteam2 IS DISTINCT FROM s.reviewteam2
 OR b.monitoringrole1 IS DISTINCT FROM s.monitoringrole1
 OR b.monitoringrole2 IS DISTINCT FROM s.monitoringrole2
 OR b.monitoringteam1 IS DISTINCT FROM s.monitoringteam1
 OR b.monitoringteam2 IS DISTINCT FROM s.monitoringteam2;