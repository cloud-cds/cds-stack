HCGH_SPECIFIC = '''
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
'''

template = '''
IF OBJECT_ID('analytics.dbo.CCDA643_CSNLookupTable', 'U') IS NOT NULL
drop table analytics.dbo.CCDA643_CSNLookupTable;
select pat_id "PAT_ID", pat_mrn_id "PAT_MRN_ID", csn "PAT_ENC_CSN_ID", csn "EXTERNAL_ID"
INTO
analytics.dbo.CCDA643_CSNLookupTable
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
    AND Medicalxferin.DEPARTMENT_ID like '{hosp}%'
    -- ADT EVENT WAS NOT CANCELED
    AND Medicalxferin.EVENT_SUBTYPE_C IN (
      1
      ,3
      ){hcgh_specific}
    AND
    -- for patients that are still present in hospital
    --no patients that are still present in hospital
    HOSP_DISCH_TIME IS NOT NULL
    --admitted between the dates in your cohort
    AND HOSP_ADMSN_TIME >= '{start_date}' AND HOSP_ADMSN_TIME < '{end_date}'
) A (csn, pat_id, pat_mrn_id);

-- DO NOT RUN THIS CODE UNTIL YOU'VE ALTERED THE DATES IN THE COHORT, OTHERWISE IT WILL CREATE DUPLICATE RECORDS!!!
IF OBJECT_ID('analytics.dbo.CCDA643_PatLookupTable', 'U') IS NOT NULL
drop table analytics.dbo.CCDA643_PatLookupTable;
select pat_id "PAT_ID", pat_mrn_id "PAT_MRN_ID", IDENTITY_ID "EXTERNAL_ID"
INTO
analytics.dbo.CCDA643_PatLookupTable
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
  AND Medicalxferin.DEPARTMENT_ID like '{hosp}%'
    -- ADT EVENT WAS NOT CANCELED
    AND Medicalxferin.EVENT_SUBTYPE_C IN (
      1
      ,3
      ){hcgh_specific}
    AND
    --no patients that are still present in hospital
    HOSP_DISCH_TIME IS NOT NULL
    --admitted between the dates in your cohort
    AND HOSP_ADMSN_TIME >= '{start_date}' AND HOSP_ADMSN_TIME < '{end_date}'
) A (pat_id, pat_mrn_id, identity_id);
GO


USE Analytics;
:OUT \\\\Client\F$\clarity\\vent_info.{idx}.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
  ,procs.proc_id OrderProcId
  ,inst.INSTNTD_ORDER_ID
  ,inst.order_id as parent_order_id
  ,procs.chng_order_Proc_id
  ,coalesce (procs.display_name, procs.description)
  ,eap.proc_name
  ,proccat.proc_cat_name
  ,procs.ORDER_TIME
  ,PROCS.RESULT_TIME
  ,PARENTS.ORDER_TIME ParentOrderTime
  ,PROCS.PROC_START_TIME
  ,PROCS.PROC_ENDING_TIME
  ,PARENTS.proc_start_time ParentStarttime
  ,PARENTS.PROC_ENDING_TIME ParentEndingTime
  ,ordstat.NAME OrderStatus
  ,labstats.NAME LabStatus
  ,osq.order_id
  , osq.line
  , osq.ord_quest_id
  , osq.IS_ANSWR_BYPROC_YN 
  , osq.ord_quest_resp
  , cq.quest_name
  , tm.question
  , osq.ORD_QUEST_CMT comment
  , ZC_ORDER_PRIORITY.name "priority"
  , IP_FREQUENCY.freq_name
  , ZC_ORDER_CLASS.name
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID
left join clarity.dbo.ORD_SPEC_QUEST osq on osq.ORDER_ID = procs.ORDER_PROC_ID
left join clarity.dbo.CL_QQUEST cq on osq.ORD_QUEST_ID = cq.QUEST_ID 
left join clarity.dbo.CL_QQUEST_OVTM tm on tm.QUEST_ID = cq.QUEST_ID
inner join clarity.dbo.HV_ORDER_PROC
on procs.order_proc_id = HV_ORDER_PROC.order_proc_id
inner join clarity.dbo.ZC_ORDER_PRIORITY
on procs.ORDER_PRIORITY_C = ZC_ORDER_PRIORITY.ORDER_PRIORITY_C
inner join clarity.dbo.IP_FREQUENCY
on HV_ORDER_PROC.discr_freq_id = IP_FREQUENCY.freq_id
inner join clarity.dbo.ZC_ORDER_CLASS
on ZC_ORDER_CLASS.ORDER_CLASS_C = procs.ORDER_CLASS_C
where
 procs.proc_id in
        ('38825','55910','38817','38819','174947','38821','38823','100372','304267','150061', '104569')
or lower(eap.proc_name) like '%mechanical ventilation%'
GO

:OUT \\\\Client\F$\clarity\diag.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT CSN.EXTERNAL_ID CSN_ID
  ,edg.DX_ID
  ,DX_ED_YN
  ,dx.PRIMARY_DX_YN
  ,DX.line
  ,edg.DX_NAME diagName
  ,icd9.Code
  ,dx.Annotation
  ,dx.COMMENTS
  ,DX_CHRONIC_YN
  ,NULL --icdIndex."ICD-9 Code category"
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
INNER JOIN CLARITY.dbo.PAT_ENC_DX dx ON dx.PAT_ENC_CSN_ID = CSN.PAT_ENC_CSN_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON dx.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON dx.DX_ID = icd9.DX_ID
-- INNER JOIN Analytics.dbo.CCDA264_ICD9Codes icdIndex ON ISNUMERIC(icd9.Code) = 1
-- AND icd9.Code >= icdIndex."Low Range"
-- AND icd9.Code < icdIndex."High Cutoff";
GO


:OUT \\\\Client\F$\clarity\hist.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
  ,pat.EXTERNAL_ID PATIENTID
  ,CAST(COALESCE(Encounter.DEPARTMENT_ID, HospitalEncounter.DEPARTMENT_ID) AS VARCHAR(50)) DEPARTMENTID
  ,edg.DX_NAME diagName
  ,icd9.Code
  ,NULL --icdIndex."ICD-9 Code category"
  ,MedicalHistory.COMMENTS
  ,MedicalHistory.Med_Hx_Annotation Annotation
  ,MedicalHistory.Medical_Hx_Date
  ,coalesce(Encounter.APPT_TIME, HospitalEncounter.HOSP_ADMSN_TIME) ENC_Date
  --INTO CCDA276_MedicalHistory
FROM (
  SELECT PAT_ENC_CSN_ID
    ,PAT_ID
    ,DX_ID
    ,Med_Hx_Annotation
    ,Medical_Hx_Date
    ,Comments
    ,MIN(HX_LNK_ENC_CSN) MINCSN
  FROM CLARITY.dbo.MEDICAL_HX
  GROUP BY PAT_ENC_CSN_ID
    ,PAT_ID
    ,DX_ID
    ,Med_Hx_Annotation
    ,Medical_Hx_Date
    ,Comments
  ) MedicalHistory
LEFT OUTER JOIN clarity.dbo.PAT_ENC Encounter ON MedicalHistory.MINCSN = Encounter.PAT_ENC_CSN_ID
LEFT OUTER JOIN clarity.dbo.PAT_ENC_HSP HospitalEncounter ON MedicalHistory.MINCSN = HospitalEncounter.PAT_ENC_CSN_ID
LEFT OUTER JOIN analytics.dbo.CCDA643_CSNLookupTable csn ON MedicalHistory.MINCSN = csn.PAT_ENC_CSN_ID
INNER JOIN analytics.dbo.CCDA643_PatLookupTable pat ON pat.PAT_ID = MedicalHistory.PAT_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON MedicalHistory.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON MedicalHistory.DX_ID = icd9.DX_ID
-- INNER JOIN Analytics.dbo.CCDA264_ICD9Codes icdIndex ON ISNUMERIC(icd9.Code) = 1
--   AND icd9.Code >= icdIndex."Low Range"
--   AND icd9.Code < icdIndex."High Cutoff";
GO

USE Analytics;
:OUT \\\\Client\F$\clarity\ordermed.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
  ,coalesce (lower(med.display_name), lower(med.description))
  ,med.ORDER_INST
  ,medrt.NAME MedRoute
  ,med.HV_DISCRETE_DOSE Dose
  ,medUnit.NAME MedUnit
  ,MED.MIN_DISCRETE_DOSE
  ,MED.MAX_DISCRETE_DOSE
FROM CLARITY.dbo.ORDER_MED MED
INNER JOIN CLARITY.dbo.CLARITY_MEDICATION meds ON med.MEDICATION_ID = meds.MEDICATION_ID
left JOIN CLARITY.dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
left JOIN CLARITY.dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
-- LEFT JOIN Analytics.dbo.CCDA264_MedicationClasses cohortMedClass ON (
--     cohortMedClass.PharmaceuticalClass = pharmClass.NAME
--     AND thera.NAME = cohortMedClass.TherapeuticClass
--     )
--   OR (
--     pharmClass.Name IS NULL
--     AND thera.NAME = cohortMedClass.TherapeuticClass
--     )
--   OR (
--     thera.Name IS NULL
--     AND pharmClass.NAME = cohortMedClass.PharmaceuticalClass
--     )
-- LEFT JOIN Analytics.dbo.CCDA264_MedicationIDs indivMeds ON indivMeds.MedId = med.MEDICATION_ID
LEFT JOIN CLARITY.dbo.ZC_ADMIN_ROUTE medrt ON medrt.MED_ROUTE_C = MED.MED_ROUTE_C
LEFT JOIN CLARITY.dbo.ZC_MED_UNIT medunit ON medunit.DISP_QTYUNIT_C = MED.HV_DOSE_UNIT_C
INNER JOIN
Analytics.dbo.CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = med.PAT_ENC_CSN_ID
WHERE (
    med.IS_PENDING_ORD_YN = 'N'
    OR med.IS_PENDING_ORD_YN IS NULL
    )
  AND med.ORDER_STATUS_C NOT IN (
    1
    ,4
    ,7
    );
    -- AND (
    -- indivMeds.MedId IS NOT NULL
    -- OR cohortMedClass.TherapeuticClass IS NOT NULL
    -- OR cohortMedClass.PharmaceuticalClass IS NOT NULL
    -- );
GO

USE Analytics;
:OUT \\\\Client\F$\clarity\ordermed_home.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
  ,coalesce (lower(med.display_name), lower(med.description))
  ,med.ORDER_INST
  ,medrt.NAME MedRoute
  ,med.HV_DISCRETE_DOSE Dose
  ,medUnit.NAME MedUnit
  ,MED.MIN_DISCRETE_DOSE
  ,MED.MAX_DISCRETE_DOSE
FROM CLARITY.dbo.ORDER_MED MED
INNER JOIN CLARITY.dbo.CLARITY_MEDICATION meds ON med.MEDICATION_ID = meds.MEDICATION_ID
left JOIN CLARITY.dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
left JOIN CLARITY.dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
-- LEFT JOIN Analytics.dbo.CCDA264_MedicationClasses cohortMedClass ON (
--     cohortMedClass.PharmaceuticalClass = pharmClass.NAME
--     AND thera.NAME = cohortMedClass.TherapeuticClass
--     )
--   OR (
--     pharmClass.Name IS NULL
--     AND thera.NAME = cohortMedClass.TherapeuticClass
--     )
--   OR (
--     thera.Name IS NULL
--     AND pharmClass.NAME = cohortMedClass.PharmaceuticalClass
--     )
LEFT JOIN CLARITY.dbo.ZC_ADMIN_ROUTE medrt ON medrt.MED_ROUTE_C = MED.MED_ROUTE_C
LEFT JOIN CLARITY.dbo.ZC_MED_UNIT medunit ON medunit.DISP_QTYUNIT_C = MED.HV_DOSE_UNIT_C
INNER JOIN
Analytics.dbo.CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = med.PAT_ENC_CSN_ID
WHERE (
    med.IS_PENDING_ORD_YN = 'N'
    OR med.IS_PENDING_ORD_YN IS NULL
    )
  and med.ORDERING_MODE_C = 1
  AND med.ORDER_STATUS_C NOT IN (
    1
    ,4
    ,7
    );
    -- AND
    -- cohortMedClass.PharmaceuticalClass IS NOT NULL;
GO


USE Analytics;
:OUT \\\\Client\F$\clarity\orderproc_new.{idx}.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
  ,procs.proc_id OrderProcId
  ,inst.INSTNTD_ORDER_ID
  ,inst.order_id as parent_order_id
  ,procs.chng_order_Proc_id
  ,coalesce (procs.display_name, procs.description)
  ,eap.proc_name
  ,proccat.proc_cat_name
  ,freq.display_name FrequencyOfOrder
  ,procs.ORDER_TIME
  ,PROCS.RESULT_TIME
  ,PARENTS.ORDER_TIME ParentOrderTime
  ,PROCS.PROC_START_TIME
  ,PROCS.PROC_ENDING_TIME
  ,PARENTS.proc_start_time ParentStarttime
  ,PARENTS.PROC_ENDING_TIME ParentEndingTime
  ,ordstat.NAME OrderStatus
  ,labstats.NAME LabStatus
  ,osq.order_id
  , osq.line
  , osq.ord_quest_id
  , osq.IS_ANSWR_BYPROC_YN 
  , osq.ord_quest_resp
  , cq.quest_name
  , tm.question
  , osq.ORD_QUEST_CMT comment
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID
left join clarity.dbo.ORD_SPEC_QUEST osq on osq.ORDER_ID = procs.ORDER_PROC_ID
left join clarity.dbo.CL_QQUEST cq on osq.ORD_QUEST_ID = cq.QUEST_ID 
left join clarity.dbo.CL_QQUEST_OVTM tm on tm.QUEST_ID = cq.QUEST_ID
where
procs.proc_id in
(
'160318',
'2015293',
'67479', '38825', '38817', '38819', '38821', '38823', '55910', '100294', '100296', '100298', '100300', '304267',
'67413',
'67415',
'127058',
'70027',
'71988',
'71990',
'110189',
'131944',
'165547',
'3041752',
'22362','22364','22366','22368','22370','22372','22374','22376','66891','66895','66899','66903','66907','66911','66915','66919','66923','66927','66931','66935','66939','66943','66947','66951','66955','66959','66963','66967','291','293','295','297','301','303' -- dialysis
, '211374','177692','160318','210948','2015293', -- dialysis supplement
'519', '39121324','389874983', '389874983', '6783734684', -- code status (cpr, ...)
'38374', '114644', '127813', '143925', '150159', '151273', '165545' -- cardiac catheterization
)
or
procs.proc_code like 'CON%'
or
procs.proc_code like 'SUR%'
or
eap.proc_name = 'BIPAP'
or
lower(eap.proc_name) like '%cpr%' or
lower(procs.display_name) like '%cpr%' or
lower(eap.proc_name) like 'wall cpap - adult%' or
lower(eap.proc_name) like 'cpap continuous%' or
lower(eap.proc_name) like 'mechanical ventilation - adult cpap%' or
eap.proc_name = 'ULTRAFILTRATION' or
eap.proc_name in ('CONTINUOUS VENOVENOUS HEMODIALYSIS', 'HC INPATIENT HEMODIALYSIS', 'HEMODIALYSIS', 'HEMODIALYSIS INPATIENT', 'HEMODIALYSIS OUTPATIENT', 'HEMODIALYSIS INPATIENT - ACADEMIC')
or lower(eap.proc_name) like '%percuaneous coronary intervention%'
or proccat.proc_cat_name in ('IMG IR ORDERABLES', 'CV CARDIAC SERVICES ORDERABLES', 'GENERAL SURGICAL ORDERABLES', 'GI PROCEDURE ORDERABLES', 'CV CARDIAC CATH PERFORMABLES', 'CV IR PERFORMABLES', 'CV VASCULAR ORDERABLES')
;
GO

:OUT \\\\Client\F$\clarity\prob.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT pat.EXTERNAL_ID PAT_ID
  ,CSN.EXTERNAL_ID CSN_ID
  ,CAST(COALESCE(Encounter.DEPARTMENT_ID, HospitalEncounter.DEPARTMENT_ID) AS VARCHAR(50)) DEPARTMENTID
  ,COALESCE(ProblemList.NOTED_DATE, FirstHistory.HX_DATE_OF_ENTRY) FirstDocumented
  ,CASE ProblemList.PROBLEM_STATUS_C
    WHEN 2
      THEN ProblemList.RESOLVED_DATE
    END ResolvedDate
  ,CAST(CASE
      WHEN ProblemList.PROBLEM_STATUS_C IS NULL
        THEN 'Active'
      WHEN StatusCategory.PROBLEM_STATUS_C IS NULL
        THEN '*Unknown'
      ELSE StatusCategory.NAME
      END AS VARCHAR(300)) ProblemSTATUS
  ,CAST(CASE
      WHEN ProblemList.HOSPITAL_PL_YN = 'Y'
        THEN 1
      WHEN ProblemList.HOSPITAL_PL_YN = 'N'
        THEN 0
      ELSE NULL
      END AS INTEGER) HOSPITALDIAGNOSIS
  ,CAST(CASE
      WHEN ProblemList.IS_PRESENT_ON_ADM_C IS NULL
        THEN '*Unspecified'
      WHEN PoaCategory.DX_POA_C IS NULL
        THEN '*Unknown'
      ELSE PoaCategory.NAME
      END AS VARCHAR(300)) PRESENTONADMISSION
  ,CAST(CASE
      WHEN ProblemList.CHRONIC_YN = 'Y'
        THEN 1
      WHEN ProblemList.CHRONIC_YN = 'N'
        THEN 0
      ELSE NULL
      END AS INTEGER) CHRONIC
  ,edg.DX_NAME diagName
  ,icd9.Code
  ,NULL --icdIndex."ICD-9 Code category"
FROM CLARITY.DBO.PROBLEM_LIST ProblemList
LEFT OUTER JOIN CLARITY.DBO.PROBLEM_LIST_HX FirstHistory ON ProblemList.PROBLEM_LIST_ID = FirstHistory.PROBLEM_LIST_ID
  AND FirstHistory.LINE = 1
LEFT OUTER JOIN CLARITY.DBO.PAT_ENC Encounter ON FirstHistory.HX_PROBLEM_EPT_CSN = Encounter.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.DBO.PAT_ENC_HSP HospitalEncounter ON FirstHistory.HX_PROBLEM_EPT_CSN = HospitalEncounter.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.DBO.ZC_PROBLEM_STATUS StatusCategory ON ProblemList.PROBLEM_STATUS_C = StatusCategory.PROBLEM_STATUS_C
LEFT OUTER JOIN CLARITY.DBO.ZC_DX_POA PoaCategory ON ProblemList.IS_PRESENT_ON_ADM_C = PoaCategory.DX_POA_C
LEFT OUTER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON FirstHistory.HX_PROBLEM_EPT_CSN = csn.PAT_ENC_CSN_ID
INNER JOIN Analytics.dbo.CCDA643_PatLookupTable pat ON pat.PAT_ID = ProblemList.PAT_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON ProblemList.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON ProblemList.DX_ID = icd9.DX_ID
-- INNER JOIN Analytics.dbo.CCDA264_ICD9Codes icdIndex ON ISNUMERIC(icd9.Code) = 1
--  AND icd9.Code >= icdIndex."Low Range"
--  AND icd9.Code < icdIndex."High Cutoff"
WHERE ProblemList.DX_ID IS NOT NULL
  AND NULLIF(ProblemList.PAT_ID, '') IS NOT NULL
  AND NULLIF(3, ProblemList.PROBLEM_STATUS_C) IS NOT NULL;
GO


:OUT \\\\Client\F$\clarity\\final_dx.{idx}.rpt
SET NOCOUNT ON
select
  CSN.EXTERNAL_ID CSN_ID
  ,dx.line
  ,dx.dx_id
  ,icd9.code icd9
  ,icd10.code icd10
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
inner join HSP_ACCOUNT a on csn.pat_enc_csn_id = a.prim_enc_csn_id
inner join HSP_ACCT_DX_LIST dx on dx.HSP_ACCOUNT_ID = a.HSP_ACCOUNT_ID
left JOIN CLARITY.DBO.EDG_CURRENT_ICD10 icd10 ON dx.DX_ID = icd10.DX_ID
left JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON dx.DX_ID = icd9.DX_ID
where dx.dx_id is not null
GO
'''


# 1101 jhh
# 1102 bmc
# 1103 hcgh
hosp = '1103'
start_date = [2013, 9]
end_date = [2017, 5]

end_date[1] += 1
num_months = 13
total_num_months = 12*(end_date[0] - start_date[0]) + (end_date[1] - start_date[1])
import math
num_splits = math.ceil(total_num_months / float(num_months))

for i in range(num_splits):
    if i == 0:
        year = start_date[0]
        month = start_date[1]
    else:
        year = prev_year
        month = prev_month

    this_start_date = "{year}-{month}-01".format(year=year, month=str(month).zfill(2))
    idx = "{year}{month}".format(year=year, month=str(month).zfill(2))
    next_month = (month + num_months) % 12
    next_year = year + int(month + num_months) // int(12)

    if next_year > end_date[0]:
        next_year = end_date[0]
    if next_month > end_date[1]:
        next_month = end_date[1]
    this_end_date = "{year}-{month}-01".format(year=next_year, month=str(next_month).zfill(2))

    prev_year = next_year
    prev_month = next_month

    if hosp == '1103':
        hcgh_specific = HCGH_SPECIFIC
    else:
        hcgh_specific = ''

    print(template.format(start_date=this_start_date, end_date=this_end_date, idx=idx, hosp=hosp, hcgh_specific=hcgh_specific))

print(dict)
