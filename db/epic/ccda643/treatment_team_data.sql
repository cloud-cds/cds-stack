
delete from analytics.dbo.CCDA643_CSNLookupTable;
insert into analytics.dbo.CCDA643_CSNLookupTable
("PAT_ID", "PAT_MRN_ID", "PAT_ENC_CSN_ID", "EXTERNAL_ID")
(SELECT DISTINCT patient.PAT_ID, patient.PAT_MRN_ID, pat_enc_hsp_1.PAT_ENC_CSN_ID, pat_enc_hsp_1.PAT_ENC_CSN_ID
  FROM CLARITY.dbo.PAT_ENC_HSP PAT_ENC_HSP_1
  INNER JOIN CLARITY.dbo.PATIENT patient ON PAT_ENC_HSP_1.PAT_ID = patIENT.PAT_ID
  INNER JOIN [CLARITY].[dbo].[CLARITY_ADT] Medicalxferin ON Medicalxferin.PAT_ENC_CSN_ID = PAT_ENC_HSP_1.PAT_ENC_CSN_ID
  LEFT JOIN [CLARITY].[dbo].[CLARITY_ADT] edxferout ON EDxferout.event_id = Medicalxferin.XFER_EVENT_ID
  LEFT JOIN CLARITY.dbo.CLARITY_ADT Medicalxferout ON Medicalxferin.EVENT_ID = Medicalxferout.LAST_IN_EVENT_ID
  LEFT JOIN CLARITY.dbo.CLARITY_ADT discharge ON Medicalxferin.DIS_EVENT_ID = discharge.EVENT_ID
  WHERE
  -- Age is greater than 15 at hospital admission
  datediff(year, patient.birth_date, PAT_ENC_HSP_1.HOSP_ADMSN_TIME) + CASE
      WHEN MONTH(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < month(patient.birth_date)
        THEN - 1
      WHEN month(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) = month(patient.birth_date)
        AND day(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < day(patient.birth_date)
        THEN - 1
      ELSE 0
      END >= 15
  -- EVENT TYPE FOR TRANSFER INTO MEDICAL UNIT IS ADMISSION, TRANSFER OR DISCHARGE
    AND Medicalxferin.EVENT_TYPE_C < 5
  -- ENSURE PATIENT WAS ADMITTED TO ONE OF THE HOWARD COUNTY UNITS YOU ARE STUDYING
    AND Medicalxferin.DEPARTMENT_ID like '1103%'
    -- ADT EVENT WAS NOT CANCELED
    AND Medicalxferin.EVENT_SUBTYPE_C IN (
      1
      ,3
      )
    -- Either they came right from the HCGH ED or were a direct admit to HCGH
    AND (
      (edxferout.EVENT_ID IS NULL and Medicalxferin.EVENT_TYPE_C = 1)
      OR (
        EDxferout.EVENT_SUBTYPE_C IN (
          1
          ,3
          )
        AND edxferout.DEPARTMENT_ID IN (
          '110300470'
          ,'110300460'
          )
        )
      )

    AND
    -- for patients that are still present in hospital
    --no patients that are still present in hospital
    HOSP_DISCH_TIME IS NOT NULL
    --admitted between the dates in your cohort
    AND HOSP_ADMSN_TIME >= '2017-10-01' AND HOSP_ADMSN_TIME < '2018-02-01'
);



delete from analytics.dbo.CCDA643_PatLookupTable;
insert into analytics.dbo.CCDA643_PatLookupTable
( "PAT_ID",  "PAT_MRN_ID",  "EXTERNAL_ID")

(SELECT DISTINCT patient.PAT_ID, patient.PAT_MRN_ID, IDENTITY_ID.IDENTITY_ID
  FROM CLARITY.dbo.PAT_ENC_HSP PAT_ENC_HSP_1
  INNER JOIN CLARITY.dbo.PATIENT patient ON PAT_ENC_HSP_1.PAT_ID = patient.PAT_ID
  INNER JOIN CLARITY.dbo.IDENTITY_ID on IDENTITY_ID.PAT_ID = PAT_ENC_HSP_1.PAT_ID
  INNER JOIN [CLARITY].[dbo].[CLARITY_ADT] Medicalxferin ON Medicalxferin.PAT_ENC_CSN_ID = PAT_ENC_HSP_1.PAT_ENC_CSN_ID
  LEFT JOIN [CLARITY].[dbo].[CLARITY_ADT] edxferout ON EDxferout.event_id = Medicalxferin.XFER_EVENT_ID
  LEFT JOIN CLARITY.dbo.CLARITY_ADT Medicalxferout ON Medicalxferin.EVENT_ID = Medicalxferout.LAST_IN_EVENT_ID
  LEFT JOIN CLARITY.dbo.CLARITY_ADT discharge ON Medicalxferin.DIS_EVENT_ID = discharge.EVENT_ID
  WHERE
      IDENTITY_ID.IDENTITY_ID like 'E%'
  -- Age is greater than 15 at hospital admission
  AND datediff(year, patient.birth_date, PAT_ENC_HSP_1.HOSP_ADMSN_TIME) + CASE
      WHEN MONTH(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < month(patient.birth_date)
        THEN - 1
      WHEN month(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) = month(patient.birth_date)
        AND day(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < day(patient.birth_date)
        THEN - 1
      ELSE 0
      END >= 15
  -- EVENT TYPE FOR TRANSFER INTO MEDICAL UNIT IS ADMISSION, TRANSFER OR DISCHARGE
    AND Medicalxferin.EVENT_TYPE_C < 5
  -- ENSURE PATIENT WAS ADMITTED TO ONE OF THE HOWARD COUNTY UNITS YOU ARE STUDYING
  AND Medicalxferin.DEPARTMENT_ID like '1103%'
    -- ADT EVENT WAS NOT CANCELED
    AND Medicalxferin.EVENT_SUBTYPE_C IN (
      1
      ,3
      )
    -- Either they came right from the HCGH ED or were a direct admit to HCGH
    AND (
      (edxferout.EVENT_ID IS NULL and Medicalxferin.EVENT_TYPE_C = 1)
      OR (
        EDxferout.EVENT_SUBTYPE_C IN (
          1
          ,3
          )
        AND edxferout.DEPARTMENT_ID IN (
          '110300470'
          ,'110300460'
          )
        )
      )

    AND
    --no patients that are still present in hospital
    HOSP_DISCH_TIME IS NOT NULL
    --admitted between the dates in your cohort
    AND HOSP_ADMSN_TIME >= '2017-10-01' AND HOSP_ADMSN_TIME < '2018-02-01'
) ;
GO

USE Analytics;
:OUT \\Client\F$\clarity\treatment_team.201710.rpt
select DISTINCT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
  ,C.PROV_NAME
  ,C.PROV_TYPE
  ,C.USER_ID
  ,C2.ADMIN_ROLE_C
  --,C.CLINICAL_TITLE
  ,T.TR_TEAM_BEG_DTTM
  ,T.TR_TEAM_END_DTTM
  ,T.CONTACT_DATE
  ,T.TR_TEAM_SPEC_C
  --,Z.*
from CLARITY..TREATMENT_TEAM T
INNER JOIN CLARITY..CLARITY_SER C ON T.TR_TEAM_ID=C.PROV_ID
INNER JOIN CLARITY..CLARITY_SER_2 C2 ON T.TR_TEAM_ID=C.PROV_ID
INNER JOIN CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = T.PAT_ENC_CSN_ID
group by PAT_ENC_HSP_1.EXTERNAL_ID, C.USER_ID, C.PROV_TYPE, C.PROV_NAME, T.TR_TEAM_SPEC_C, C2.ADMIN_ROLE_C, T.TR_TEAM_BEG_DTTM,T.TR_TEAM_END_DTTM, T.CONTACT_DATE;
GO


CREATE TABLE TREATMENT_TEAM_1017_0218 (
visit_id                text,
prov_name               text,
prov_type               text,
user_id                 text,
admin_role_c            text,
tr_team_beg_dttm        timestamp with    time zone,
tr_team_end_dttm        timestamp with    time zone,
contact_date            timestamp with    time zone,
tr_team_spec_c          text
);
\copy TREATMENT_TEAM_1017_0218 from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/treatment_team.201710.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
