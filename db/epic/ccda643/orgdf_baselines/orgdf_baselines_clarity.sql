
IF OBJECT_ID('analytics.dbo.CCDA643_CSN_orgdf', 'U') IS NOT NULL
drop table analytics.dbo.CCDA643_CSN_orgdf;
select pat_id "PAT_ID", pat_mrn_id "PAT_MRN_ID", csn "PAT_ENC_CSN_ID", csn "EXTERNAL_ID"
INTO
analytics.dbo.CCDA643_CSN_orgdf
FROM
(SELECT DISTINCT pat_enc_hsp_1.PAT_ENC_CSN_ID, patient.PAT_ID, patient.PAT_MRN_ID
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
    AND (Medicalxferin.DEPARTMENT_ID like '1101%'
    or Medicalxferin.DEPARTMENT_ID like '1102%'
    or Medicalxferin.DEPARTMENT_ID like '1103%')
    -- ADT EVENT WAS NOT CANCELED
    AND Medicalxferin.EVENT_SUBTYPE_C IN (
      1
      ,3
      )
    AND
    -- for patients that are still present in hospital
    --no patients that are still present in hospital
    HOSP_DISCH_TIME IS NOT NULL
    --admitted between the dates in your cohort
    AND HOSP_DISCH_TIME >= '2017-04-01'
) A (csn, pat_id, pat_mrn_id);

-- DO NOT RUN THIS CODE UNTIL YOU'VE ALTERED THE DATES IN THE COHORT, OTHERWISE IT WILL CREATE DUPLICATE RECORDS!!!
IF OBJECT_ID('analytics.dbo.CCDA643_Pat_orgdf', 'U') IS NOT NULL
drop table analytics.dbo.CCDA643_Pat_orgdf;
select pat_id "PAT_ID", pat_mrn_id "PAT_MRN_ID", IDENTITY_ID "EXTERNAL_ID"
INTO
analytics.dbo.CCDA643_Pat_orgdf
FROM
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
  AND (Medicalxferin.DEPARTMENT_ID like '1101%'
    or Medicalxferin.DEPARTMENT_ID like '1102%'
    or Medicalxferin.DEPARTMENT_ID like '1103%')
    -- ADT EVENT WAS NOT CANCELED
    AND Medicalxferin.EVENT_SUBTYPE_C IN (
      1
      ,3
      )
    AND
    --no patients that are still present in hospital
    HOSP_DISCH_TIME IS NOT NULL
    --admitted between the dates in your cohort
    AND HOSP_DISCH_TIME >= '2017-04-01'
) A (pat_id, pat_mrn_id, identity_id);
GO

:OUT \\Client\F$\clarity\orgdf_baselines_raw.rpt
SET NOCOUNT ON
SELECT csn.pat_enc_csn_id csn_id
  ,pat.EXTERNAL_ID pat_id
  ,COMP.BASE_NAME
  ,RES.RESULT_TIME
  ,RES.REFERENCE_UNIT
  ,RES.ORD_VALUE ResultValue
FROM Analytics.dbo.CCDA643_CSN_orgdf csn
inner join Analytics.dbo.CCDA643_Pat_orgdf pat on csn.pat_id = pat.pat_id
INNER JOIN dbo.ORDER_RESULTS res ON res.PAT_ENC_CSN_ID = csn.pat_enc_csn_id
INNER JOIN dbo.CLARITY_COMPONENT COMP ON res.COMPONENT_ID = COMP.COMPONENT_ID
WHERE res.RESULT_STATUS_C IN (
    3
    ,4,5
    )
  AND res.lab_status_c >= 3
  and COMP.BASE_NAME in ('BILITOT','CREATININE','PLT','INR');
GO
