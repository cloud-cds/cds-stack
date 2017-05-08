:OUT \\Client\H$\Downloads\clarity\adt.rpt
SET NOCOUNT ON
SELECT CLARITY_ADT.PAT_ENC_CSN_ID CSN_ID
  ,CASE
    WHEN EVENT_TYPE_C = 1
      THEN 'Admission'
    WHEN EVENT_TYPE_C = 2
      THEN 'Discharge'
    WHEN event_type_c = 3
      THEN 'Transfer In'
    WHEN event_type_c = 4
      THEN 'Transfer Out'
    ELSE NULL
    END EventType
  ,effective_time
  ,ptClass.NAME PatientClassAtEvent
  ,dep.DEPARTMENT_NAME
  ,room.ROOM_NAME

FROM CLARITY.dbo.CLARITY_ADT
LEFT JOIN CLARITY.DBO.ZC_PAT_CLASS ptClass ON ptClass.ADT_PAT_CLASS_C = clarity_adt.PAT_CLASS_C
LEFT JOIN CLARITY.dbo.CLARITY_DEP DEP ON DEP.DEPARTMENT_ID = clarity_adt.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.CLARITY_ROM room ON room.ROOM_CSN_ID = CLARITY_ADT.ROOM_CSN_ID
WHERE CLARITY_ADT.EVENT_TYPE_C < 5
  AND CLARITY_ADT.EVENT_SUBTYPE_C IN (
    1
    ,3
    )
  AND effective_time >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\demo.rpt
SET NOCOUNT ON
SELECT DISTINCT PAT_ENC_HSP.PAT_ENC_CSN_ID
  ,PAT_ENC_HSP.pat_id pat_id
  ,ADT_ARRIVAL_TIME
  ,ED_DEPARTURE_TIME
  ,HOSP_ADMSN_TIME
  ,HOSP_DISCH_TIME
  ,datediff(year, patient.birth_date, PAT_ENC_HSP.HOSP_ADMSN_TIME) + CASE
    WHEN MONTH(PAT_ENC_HSP.HOSP_ADMSN_TIME) < month(patient.birth_date)
      THEN - 1
    WHEN month(PAT_ENC_HSP.HOSP_ADMSN_TIME) = month(patient.birth_date)
      AND day(PAT_ENC_HSP.HOSP_ADMSN_TIME) < day(patient.birth_date)
      THEN - 1
    ELSE 0
    END AgeDuringVisit
  ,CASE
    WHEN SEX_C = 1
      THEN 'Female'
    WHEN sex_c = 2
      THEN 'Male'
    ELSE 'Other or Unknown'
    END Gender
  ,CASE
    WHEN PAT_ENC_HSP.ED_EPISODE_ID IS NOT NULL
      THEN 1
    ELSE 0
    END IsEDPatient
  ,depDisch.DEPARTMENT_NAME DischargeDepartment
  ,zc_disch_disp.NAME DischargeDisposition
--INTO CCDA276_Demographics
FROM CLARITY.dbo.PAT_ENC_HSP PAT_ENC_HSP
INNER JOIN CLARITY.dbo.PATIENT patient ON PAT_ENC_HSP.pat_id = patient.pat_id
INNER JOIN CLARITY.dbo.CLARITY_DEP depDisch ON PAT_ENC_HSP.DEPARTMENT_ID = depDisch.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.zc_disch_disp zc_disch_disp ON PAT_ENC_HSP.disch_disp_c = zc_disch_disp.disch_disp_c
WHERE ADT_ARRIVAL_TIME >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\diag.rpt
SET NOCOUNT ON
SELECT DISTINCT PAT_ENC_HSP.PAT_ENC_CSN_ID CSN_ID
  ,edg.DX_ID
  ,DX_ED_YN
  ,dx.PRIMARY_DX_YN
  ,DX.line
  ,edg.DX_NAME diagName
  ,icd9.Code
  ,dx.Annotation
  ,dx.COMMENTS
  ,DX_CHRONIC_YN
FROM CLARITY.dbo.PAT_ENC_HSP PAT_ENC_HSP
INNER JOIN CLARITY.dbo.PAT_ENC_DX dx ON dx.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON dx.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON dx.DX_ID = icd9.DX_ID
WHERE ADT_ARRIVAL_TIME >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\flt_lda.rpt
SET NOCOUNT ON
SELECT DISTINCT pat_enc_hsp.PAT_ENC_CSN_ID CSN_ID
  ,IP_FLO_GP_DATA.FLO_MEAS_NAME
  ,IP_FLO_GP_DATA.DISP_NAME
  ,IP_FLWSHT_MEAS.FLO_MEAS_ID
  ,rowtype.NAME ROW_TYPE
  ,intakeType.NAME INTAKE_TYPE
  ,outputType.NAME OUTPUT_TYPE
  ,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
  ,CASE
    WHEN IP_FLO_GP_DATA.val_type_c = 5
      THEN convert(FLOAT, IP_FLWSHT_MEAS.MEAS_VALUE) / 16.0
    ELSE NULL
    END ConvertedWeightValue
  ,meas_value Value
  ,IP_FLO_GP_DATA.UNITS
  ,flt.TEMPLATE_ID
  ,flt.TEMPLATE_NAME
  ,flt.DISPLAY_NAME TEMPLATE_DISP_NAME
  ,lda.DESCRIPTION ldaDescription
  ,lda.PROPERTIES_DISPLAY
  ,ldagrp.FLO_MEAS_ID LDAFLOMEASID
  ,LDAGRP.FLO_MEAS_NAME LDAFLOMEASNAME
  ,LDAGRP.DISP_NAME LDAGRPDISPNAME
FROM CLARITY.dbo.IP_FLWSHT_MEAS IP_FLWSHT_MEAS
INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.IP_FLWSHT_REC IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
INNER JOIN CLARITY.DBO.IP_FLOWSHEET_ROWS RWS ON RWS.LINE = IP_FLWSHT_MEAS.OCCURANCE
  AND IP_FLWSHT_REC.FSD_ID = RWS.INPATIENT_DATA_ID
INNER JOIN CLARITY.DBO.IP_LDA_NOADDSINGLE lda ON LDA.IP_LDA_ID = RWS.IP_LDA_ID
INNER JOIN CLARITY.DBO.IP_FLO_GP_DATA ldaGrp ON ldaGrp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON IP_FLWSHT_REC.INPATIENT_DATA_ID = PAT_ENC_HSP.INPATIENT_DATA_ID
LEFT JOIN CLARITY.dbo.IP_FLT_DATA flt ON flt.TEMPLATE_ID = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.DBO.ZC_ROW_TYP rowtype ON rowtype.ROW_TYP_C = ip_flo_gp_data.ROW_TYP_C
LEFT JOIN CLARITY.DBO.ZC_INTAKE_TYPE_P intakeType ON intakeType.INTAKE_TYPe_P_C = ip_flo_gp_data.INTAKE_TYP_C
LEFT JOIN CLARITY.DBO.ZC_OUTPUT_TYPE_P outputType ON outputType.OUTPUT_TYPE_P_C = ip_flo_gp_data.OUTPUT_TYP_C
WHERE ADT_ARRIVAL_TIME >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\flt_new.rpt
SET NOCOUNT ON
SELECT DISTINCT  PAT_ENC_HSP.PAT_ENC_CSN_ID CSN_ID
  ,IP_FLO_GP_DATA.FLO_MEAS_NAME
  ,IP_FLO_GP_DATA.DISP_NAME
  ,IP_FLWSHT_MEAS.FLO_MEAS_ID
  ,rowtype.NAME ROW_TYPE
  ,intakeType.NAME INTAKE_TYPE
  ,outputType.NAME OUTPUT_TYPE
  ,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
  ,CASE
    WHEN IP_FLO_GP_DATA.val_type_c = 5
      THEN convert(FLOAT, IP_FLWSHT_MEAS.MEAS_VALUE) / 16.0
    ELSE NULL
    END ConvertedWeightValue
  ,meas_value Value
  ,IP_FLO_GP_DATA.UNITS
  ,flt.TEMPLATE_ID
  ,flt.TEMPLATE_NAME
  ,flt.DISPLAY_NAME TEMPLATE_DISP_NAME
  --,lda.DESCRIPTION ldaDescription
  --,lda.PROPERTIES_DISPLAY
  --,ldagrp.FLO_MEAS_ID LDAFLOMEASID
  --,LDAGRP.FLO_MEAS_NAME LDAFLOMEASNAME
  --,LDAGRP.DISP_NAME LDAGRPDISPNAME
FROM CLARITY.dbo.IP_FLWSHT_MEAS IP_FLWSHT_MEAS
INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.IP_FLWSHT_REC IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
LEFT JOIN CLARITY.DBO.IP_FLOWSHEET_ROWS RWS ON RWS.LINE = IP_FLWSHT_MEAS.OCCURANCE
  AND IP_FLWSHT_MEAS.FSD_ID = RWS.INPATIENT_DATA_ID
--LEFT JOIN CLARITY.DBO.IP_LDA_NOADDSINGLE lda ON LDA.IP_LDA_ID = RWS.IP_LDA_ID
--LEFT JOIN CLARITY.DBO.IP_FLO_GP_DATA ldaGrp ON ldaGrp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON IP_FLWSHT_REC.INPATIENT_DATA_ID = PAT_ENC_HSP.INPATIENT_DATA_ID
LEFT JOIN CLARITY.dbo.IP_FLT_DATA flt ON flt.TEMPLATE_ID = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.DBO.ZC_ROW_TYP rowtype ON rowtype.ROW_TYP_C = ip_flo_gp_data.ROW_TYP_C
LEFT JOIN CLARITY.DBO.ZC_INTAKE_TYPE_P intakeType ON intakeType.INTAKE_TYPe_P_C = ip_flo_gp_data.INTAKE_TYP_C
LEFT JOIN CLARITY.DBO.ZC_OUTPUT_TYPE_P outputType ON outputType.OUTPUT_TYPE_P_C = ip_flo_gp_data.OUTPUT_TYP_C
WHERE
  IP_FLWSHT_MEAS.FLO_MEAS_ID in

(
'16000582',
'300220',
'16000450606',
'19685',
'400636',
'7096010',
'1600400636',
'11',
'7096010',
'301070',
'8123',
'8126',
'301260',
'301280',
'10980',
'6365',
'306270', '301360', '301250', '1120100022', '111301360','212301360', '450070' -- map
) AND ADT_ARRIVAL_TIME >= DATEADD(day,-7, GETDATE())
AND IP_FLWSHT_MEAS.RECORDED_TIME >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\flt.rpt
SET NOCOUNT ON
SELECT DISTINCT  PAT_ENC_HSP.PAT_ENC_CSN_ID CSN_ID
  ,IP_FLO_GP_DATA.FLO_MEAS_NAME
  ,IP_FLO_GP_DATA.DISP_NAME
  ,IP_FLWSHT_MEAS.FLO_MEAS_ID
  ,rowtype.NAME ROW_TYPE
  ,intakeType.NAME INTAKE_TYPE
  ,outputType.NAME OUTPUT_TYPE
  ,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
  ,CASE
    WHEN IP_FLO_GP_DATA.val_type_c = 5
      THEN convert(FLOAT, IP_FLWSHT_MEAS.MEAS_VALUE) / 16.0
    ELSE NULL
    END ConvertedWeightValue
  ,meas_value Value
  ,IP_FLO_GP_DATA.UNITS
  ,flt.TEMPLATE_ID
  ,flt.TEMPLATE_NAME
  ,flt.DISPLAY_NAME TEMPLATE_DISP_NAME
  --,lda.DESCRIPTION ldaDescription
  --,lda.PROPERTIES_DISPLAY
  --,ldagrp.FLO_MEAS_ID LDAFLOMEASID
  --,LDAGRP.FLO_MEAS_NAME LDAFLOMEASNAME
  --,LDAGRP.DISP_NAME LDAGRPDISPNAME
FROM CLARITY.dbo.IP_FLWSHT_MEAS IP_FLWSHT_MEAS
INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.IP_FLWSHT_REC IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
LEFT JOIN CLARITY.DBO.IP_FLOWSHEET_ROWS RWS ON RWS.LINE = IP_FLWSHT_MEAS.OCCURANCE
  AND IP_FLWSHT_MEAS.FSD_ID = RWS.INPATIENT_DATA_ID
--LEFT JOIN CLARITY.DBO.IP_LDA_NOADDSINGLE lda ON LDA.IP_LDA_ID = RWS.IP_LDA_ID
--LEFT JOIN CLARITY.DBO.IP_FLO_GP_DATA ldaGrp ON ldaGrp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON IP_FLWSHT_REC.INPATIENT_DATA_ID = PAT_ENC_HSP.INPATIENT_DATA_ID
LEFT JOIN CLARITY.dbo.IP_FLT_DATA flt ON flt.TEMPLATE_ID = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.DBO.ZC_ROW_TYP rowtype ON rowtype.ROW_TYP_C = ip_flo_gp_data.ROW_TYP_C
LEFT JOIN CLARITY.DBO.ZC_INTAKE_TYPE_P intakeType ON intakeType.INTAKE_TYPe_P_C = ip_flo_gp_data.INTAKE_TYP_C
LEFT JOIN CLARITY.DBO.ZC_OUTPUT_TYPE_P outputType ON outputType.OUTPUT_TYPE_P_C = ip_flo_gp_data.OUTPUT_TYP_C
WHERE IP_FLWSHT_MEAS.RECORDED_TIME >= DATEADD(day,-7, GETDATE());
GO

USE CLARITY;

:OUT \\Client\H$\Downloads\clarity\labs.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP.PAT_ENC_CSN_ID CSN_ID
  ,COMP.COMPONENT_ID
  ,COMP.BASE_NAME
  ,COMP.NAME
  ,COMP.EXTERNAL_NAME
  ,RES.RESULT_TIME
  ,RES.REFERENCE_UNIT
  ,RES.ORD_VALUE ResultValue
  ,RES.COMPONENT_COMMENT
  ,res.ORDER_PROC_ID OrderProcId
FROM CLARITY.dbo.PAT_ENC_HSP
INNER JOIN dbo.ORDER_RESULTS res ON res.PAT_ENC_CSN_ID = PAT_ENC_HSP.pat_enc_csn_id
INNER JOIN dbo.CLARITY_COMPONENT COMP ON res.COMPONENT_ID = COMP.COMPONENT_ID
WHERE res.RESULT_STATUS_C IN (
    3
    ,4,5
    )
  AND res.lab_status_c >= 3
  AND RES.RESULT_TIME >= DATEADD(day,-7, GETDATE());
GO

USE CLARITY;

:OUT \\Client\H$\Downloads\clarity\labs_new.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP.PAT_ENC_CSN_ID CSN_ID
  ,COMP.COMPONENT_ID
  ,COMP.BASE_NAME
  ,COMP.NAME
  ,COMP.EXTERNAL_NAME
  ,RES.RESULT_TIME
  ,RES.REFERENCE_UNIT
  ,RES.ORD_VALUE ResultValue
  ,RES.COMPONENT_COMMENT
  ,res.ORDER_PROC_ID OrderProcId
FROM CLARITY.dbo.PAT_ENC_HSP
INNER JOIN dbo.ORDER_RESULTS res ON res.PAT_ENC_CSN_ID = PAT_ENC_HSP.pat_enc_csn_id
INNER JOIN dbo.CLARITY_COMPONENT COMP ON res.COMPONENT_ID = COMP.COMPONENT_ID
WHERE res.RESULT_STATUS_C IN (
    3
    ,4,5
    )
  AND res.lab_status_c >= 3
and
(
  res.COMPONENT_ID in
  (
     '2000000024', '2000002279', '8100000556', '8200004363', '7100000062',
        '8100001373' -- bands
    )
  or
  comp.BASE_NAME in
  (
  'EKG',
  'ACAN',
  'ADENOCULT',
  'AERANACUL',
  'AERANACULST',
  'AFBCULT',
  'AFBCULTBLDBM',
  'AFBCULTSMEAR',
  'AFBRES',
  'BACTSTOOLCUL',
  'BLDBANKCULT',
  'BLOODCULT',
  'BLOODCULTFNG',
  'BLOODISOCUL',
  'BORDCULT',
  'BORDP',
  'BPERTINASO',
  'BSTREPCULT',
  'CAMPYCUL',
  'CDIFFCULT',
  'CFRESPCULT',
  'CHLAMCUL',
  'CHRMNOTC',
  'CHRMTCONLY',
  'CLOSTD',
  'CMVCULTRAP',
  'CMVCULTURE',
  'COMPVCUL',
  'CSFCULT',
  'CULTFLDSMR',
  'CULTLOWRES',
  'CULTRES',
  'CULTRPT',
  'CULTSITE',
  'DERMATCULT',
  'EARCULT',
  'ECOLIO157CUL',
  'ENTEROVIRCUL',
  'EPIBMFL',
  'EPICULT',
  'FLUIDCULT',
  'FUNGBLDCULT',
  'FUNGCUL',
  'FUNGCULOTH',
  'FUNGRPTSTTS',
  'FUNGSURV',
  'GRPBSTREP',
  'HIVCULTQL',
  'HIVCULTQN',
  'HPYLORICULT',
  'HSV1CULT',
  'HSV2CULT',
  'HSVCULT',
  'HSVVZRAPCUL',
  'INFLUABCULT',
  'INFLUCULT',
  'LABAERO',
  'LABAEROBM',
  'LABFUNG',
  'LABGCC',
  'LABGENI',
  'LABGRAM',
  'LABHSV',
  'LABLEGI',
  'MANCULTBLD',
  'MOLDRPTSTATS',
  'MRSASURVCULT',
  'MTBCXSMRNR',
  'MYCOPNEUCULT',
  'PARAFLURAPCX',
  'RAPIDCULT',
  'RESPCULT',
  'RFXURINE',
  'RSVCULTURE',
  'SITEMYCCUL',
  'SITEVZVCUL',
  'STOOLCULT',
  'STREPACULT',
  'STREPBCULRFX',
  'STREPBCULT',
  'SURVCULT',
  'THROATCULT',
  'TRICHCUL',
  'TUBECULTRES',
  'UCCOM',
  'UREAPLCX',
  'URINECUL',
  'URMC',
  'VIRUSCULTRAP',
  'VIRUSCULTURE',
  'VRESURVCULT',
  'YEASTCUL'
  )
)
AND RES.RESULT_TIME >= DATEADD(day,-7, GETDATE());
GO

/****** Script for SelectTopNRows command from SSMS  ******/
:OUT \\Client\H$\Downloads\clarity\lda.rpt
SET NOCOUNT ON
SELECT pat.pat_id PAT_ID
      ,[PLACEMENT_INSTANT]
     ,GP.FLO_MEAS_NAME
    ,GP.DISP_NAME
      ,[PROPERTIES_DISPLAY]
      ,[SITE]
      ,[REMOVAL_DTTM]
  FROM [CLARITY].[dbo].[IP_LDA_NOADDSINGLE] LDA
  LEFT JOIN [CLARITY].[dbo].[IP_FLO_GP_DATA] gp on gp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
  INNER JOIN
  pat_enc_hsp pat ON pat.PAT_ID = LDA.PAT_ID
  AND [PLACEMENT_INSTANT] >= DATEADD(day,-7, GETDATE());
GO

USE CLARITY;
:OUT \\Client\H$\Downloads\clarity\mar.rpt
SET NOCOUNT ON
SELECT
  pat_enc_hsp.pat_enc_csn_id CSN_ID,
  med.display_name, medindex.MEDICATION_ID, medindex.Thera_Class_c, medindex.pharm_class_c
  ,med.ORDER_INST
  ,mar.taken_time TimeActionTaken
  ,maract.Name ActionTaken
  ,mar.MAR_ORIG_DUE_TM
  ,mar.SCHEDULED_TIME
  ,medrt.NAME MedRoute
  ,mar.sig Dose
    ,medUnit.NAME MedUnit
  ,marsite.NAME AdminSite
  ,mar.INFUSION_RATE
  ,themedunit.NAME MAR_INF_RATE_UNIT
  ,mar.mar_duration,
  CASE WHEN mar.mar_duration_unit_c = 1 then 'minutes' when mar.mar_duration_unit_c = 2 then 'HOURS'
  WHEN MAR.MAR_DURATION_UNIT_C = 3 THEN 'days' END MAR_DURATION_UNIT
  --,mar.comments
  ,MED.MIN_DISCRETE_DOSE
  ,MED.MAX_DISCRETE_DOSE
FROM dbo.ORDER_MED MED
inner join pat_enc_hsp ON med.PAT_ENC_CSN_ID = pat_enc_hsp.pat_enc_csn_id
LEFT JOIN dbo.CLARITY_MEDICATION medIndex on medIndex.MEDICATION_ID = med.MEDICATION_ID
INNER JOIN dbo.MAR_ADMIN_INFO MAR on MED.ORDER_MED_ID = MAR.ORDER_MED_ID
LEFT JOIN dbo.ZC_MED_UNIT themedunit on MAR.MAR_INF_RATE_UNIT_C = themedunit.DISP_QTYUNIT_C
LEFT JOIN dbo.ZC_MAR_SITE marsite ON marsite.SITE_C = mar.SITE_C
LEFT JOIN dbo.ZC_EDIT_MAR_RSLT MARACT ON MAR.MAR_ACTION_C = MARACT.RESULT_C
INNER JOIN dbo.CLARITY_MEDICATION meds ON med.MEDICATION_ID = meds.MEDICATION_ID
LEFT  JOIN dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
LEFT JOIN dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
LEFT JOIN dbo.ZC_ADMIN_ROUTE medrt ON medrt.MED_ROUTE_C = MED.MED_ROUTE_C
LEFT JOIN dbo.ZC_MED_UNIT medunit ON medunit.DISP_QTYUNIT_C = mar.DOSE_UNIT_C
WHERE (
    med.IS_PENDING_ORD_YN = 'N'
    OR med.IS_PENDING_ORD_YN IS NULL
    )
  AND med.ORDER_STATUS_C NOT IN (
    1
    ,4
    ,7
    )
AND mar.taken_time >= DATEADD(day,-7, GETDATE())
ORDER BY mar.TAKEN_TIME;
GO

:OUT \\Client\H$\Downloads\clarity\hist.rpt
SET NOCOUNT ON
SELECT DISTINCT pat.pat_enc_csn_id CSN_ID
  ,pat.pat_id PATIENTID
  ,CAST(COALESCE(Encounter.DEPARTMENT_ID, HospitalEncounter.DEPARTMENT_ID) AS VARCHAR(50)) DEPARTMENTID
  ,edg.DX_NAME diagName
  ,icd9.Code
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
INNER JOIN pat_enc_hsp pat ON pat.PAT_ID = MedicalHistory.PAT_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON MedicalHistory.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON MedicalHistory.DX_ID = icd9.DX_ID
AND pat.ADT_ARRIVAL_TIME >= DATEADD(day,-7, GETDATE());
GO


:OUT \\Client\H$\Downloads\clarity\ordermed.rpt
SET NOCOUNT ON
SELECT DISTINCT pat_enc_hsp.pat_enc_csn_id CSN_ID
  ,med.display_name
  ,med.ORDER_INST
  ,medrt.NAME MedRoute
  ,med.HV_DISCRETE_DOSE Dose
  ,medUnit.NAME MedUnit
  ,MED.MIN_DISCRETE_DOSE
  ,MED.MAX_DISCRETE_DOSE
FROM CLARITY.dbo.ORDER_MED MED
INNER JOIN CLARITY.dbo.CLARITY_MEDICATION meds ON med.MEDICATION_ID = meds.MEDICATION_ID
INNER JOIN CLARITY.dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
INNER JOIN CLARITY.dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
LEFT JOIN CLARITY.dbo.ZC_ADMIN_ROUTE medrt ON medrt.MED_ROUTE_C = MED.MED_ROUTE_C
LEFT JOIN CLARITY.dbo.ZC_MED_UNIT medunit ON medunit.DISP_QTYUNIT_C = MED.HV_DOSE_UNIT_C
INNER JOIN
pat_enc_hsp ON pat_enc_hsp.pat_enc_csn_id = med.PAT_ENC_CSN_ID
WHERE (
    med.IS_PENDING_ORD_YN = 'N'
    OR med.IS_PENDING_ORD_YN IS NULL
    )
  AND med.ORDER_STATUS_C NOT IN (
    1
    ,4
    ,7
    ) AND med.ORDER_INST >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\ordermed_home.rpt
SET NOCOUNT ON
SELECT DISTINCT PAT_ENC_HSP.pat_enc_csn_id CSN_ID
  ,med.display_name
  ,med.ORDER_INST
  ,medrt.NAME MedRoute
  ,med.HV_DISCRETE_DOSE Dose
  ,medUnit.NAME MedUnit
  ,MED.MIN_DISCRETE_DOSE
  ,MED.MAX_DISCRETE_DOSE
FROM CLARITY.dbo.ORDER_MED MED
INNER JOIN CLARITY.dbo.CLARITY_MEDICATION meds ON med.MEDICATION_ID = meds.MEDICATION_ID
INNER JOIN CLARITY.dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
INNER JOIN CLARITY.dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
LEFT JOIN CLARITY.dbo.ZC_ADMIN_ROUTE medrt ON medrt.MED_ROUTE_C = MED.MED_ROUTE_C
LEFT JOIN CLARITY.dbo.ZC_MED_UNIT medunit ON medunit.DISP_QTYUNIT_C = MED.HV_DOSE_UNIT_C
INNER JOIN
clarity.dbo.pat_enc_hsp ON pat_enc_hsp.pat_enc_csn_id = med.PAT_ENC_CSN_ID
WHERE (
    med.IS_PENDING_ORD_YN = 'N'
    OR med.IS_PENDING_ORD_YN IS NULL
    )
  and med.ORDERING_MODE_C = 1
  AND med.ORDER_STATUS_C NOT IN (
    1
    ,4
    ,7
    ) AND med.ORDER_INST >= DATEADD(day,-7, GETDATE());
GO


:OUT \\Client\H$\Downloads\clarity\orderproc.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP.pat_enc_csn_id CSN_ID
  ,procs.PROC_ID OrderProcId
  ,procs.display_name
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
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN pat_enc_hsp ON pat_enc_hsp.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID
WHERE PROCS.RESULT_TIME >= DATEADD(day,-7, GETDATE());
GO


:OUT \\Client\H$\Downloads\clarity\orderproc_img.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP.pat_enc_csn_id CSN_ID
  ,procs.ORDER_PROC_ID
  ,procs.display_name
  ,eap.proc_name
  ,proccat.proc_cat_name
  ,procs.ORDER_TIME
  ,PROCS.RESULT_TIME
  ,PROCS.PROC_START_TIME
  ,PROCS.PROC_ENDING_TIME
  ,ordstat.NAME OrderStatus
  ,labstats.NAME LabStatus
  ,hnt.LINE
  ,hnt.NOTE_TEXT
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
LEFT JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN clarity..pat_enc_hsp ON pat_enc_hsp.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
inner join clarity..V_IMG_STUDY img on img.order_id = procs.ORDER_PROC_ID
inner join clarity..HNO_NOTE_TEXT hnt on hnt.NOTE_CSN_ID = img.RESULT_NOTE_CSN
where (img.proc_name like '%MRI%' or img.proc_name like '%CT%')
 AND PROCS.RESULT_TIME >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\orderproc_new.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP.pat_enc_csn_id CSN_ID
  ,procs.proc_id OrderProcId
  ,procs.display_name
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
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN clarity..pat_enc_hsp ON pat_enc_hsp.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID
left join clarity.dbo.ORD_SPEC_QUEST osq on osq.ORDER_ID = procs.ORDER_PROC_ID
left join clarity.dbo.CL_QQUEST cq on osq.ORD_QUEST_ID = cq.QUEST_ID 
left join clarity.dbo.CL_QQUEST_OVTM tm on tm.QUEST_ID = cq.QUEST_ID
where procs.proc_id in
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
)  AND PROCS.RESULT_TIME >= DATEADD(day,-7, GETDATE());
GO

:OUT \\Client\H$\Downloads\clarity\prob.rpt
SET NOCOUNT ON
SELECT DISTINCT pat.PAT_ID PAT_ID
  ,pat.pat_enc_csn_id CSN_ID
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
FROM CLARITY.DBO.PROBLEM_LIST ProblemList
LEFT OUTER JOIN CLARITY.DBO.PROBLEM_LIST_HX FirstHistory ON ProblemList.PROBLEM_LIST_ID = FirstHistory.PROBLEM_LIST_ID
  AND FirstHistory.LINE = 1
LEFT OUTER JOIN CLARITY.DBO.PAT_ENC Encounter ON FirstHistory.HX_PROBLEM_EPT_CSN = Encounter.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.DBO.PAT_ENC_HSP HospitalEncounter ON FirstHistory.HX_PROBLEM_EPT_CSN = HospitalEncounter.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.DBO.ZC_PROBLEM_STATUS StatusCategory ON ProblemList.PROBLEM_STATUS_C = StatusCategory.PROBLEM_STATUS_C
LEFT OUTER JOIN CLARITY.DBO.ZC_DX_POA PoaCategory ON ProblemList.IS_PRESENT_ON_ADM_C = PoaCategory.DX_POA_C
INNER JOIN pat_enc_hsp pat ON pat.PAT_ID = ProblemList.PAT_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON ProblemList.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON ProblemList.DX_ID = icd9.DX_ID
WHERE ProblemList.DX_ID IS NOT NULL
  AND NULLIF(ProblemList.PAT_ID, '') IS NOT NULL
  AND NULLIF(3, ProblemList.PROBLEM_STATUS_C) IS NOT NULL
   AND pat.ADT_ARRIVAL_TIME >= DATEADD(day,-7, GETDATE());
GO


USE CLARITY;
:OUT \\Client\H$\Downloads\clarity\note.rpt
SET NOCOUNT ON
SELECT DISTINCT PAT_ENC_HSP.pat_enc_csn_id CSN_ID
  ,info.NOTE_ID
  ,prov_type.NAME AuthorType
  ,TYPINDEX.NAME NoteType
  ,info.CREATE_INSTANT_DTTM
  ,txt.line
  ,TXT.NOTE_TEXT
  ,txt.CONTACT_DATE_REAL
  ,noteStat.NAME NoteStatus
  ,noteEncs.SPEC_NOTE_TIME_DTTM
  ,noteEncs.ENTRY_INSTANT_DTTM
FROM dbo.pat_enc_hsp
INNER JOIN dbo.HNO_INFO info ON info.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
INNER JOIN dbo.note_enc_info noteEncs ON noteEncs.NOTE_ID = info.NOTE_ID
INNER JOIN dbo.HNO_NOTE_TEXT txt ON txt.NOTE_CSN_ID = noteEncs.CONTACT_SERIAL_NUM
INNER JOIN dbo.ZC_NOTE_TYPE_IP TYPINDEX ON info.IP_NOTE_TYPE_C = TYPINDEX.TYPE_IP_C
INNER JOIN dbo.CLARITY_EMP emp ON emp."USER_ID" = noteEncs.AUTHOR_USER_ID
INNER JOIN dbo.CLARITY_SER ser ON ser.PROV_ID = emp.PROV_ID
INNER JOIN dbo.ZC_PROV_TYPE PROV_TYPE ON PROV_TYPE.PROV_TYPE_C = ser.PROVIDER_TYPE_C
LEFT JOIN dbo.ZC_NOTE_STATUS noteStat ON noteStat.NOTE_STATUS_C = noteEncs.NOTE_STATUS_C
WHERE info.DELETE_USER_ID IS NULL
  AND (
    info.UNSIGNED_YN IS NULL
    OR info.unsigned_yn = 'N'
    )
  AND (
    info.AMB_NOTE_YN IS NULL
    OR info.AMB_NOTE_YN = 'N'
    )
    and (noteEncs.NOTE_STATUS_C NOT IN (1,4,8) OR noteEncs.NOTE_STATUS_C IS NULL)
     AND info.CREATE_INSTANT_DTTM >= DATEADD(day,-7, GETDATE());
GO

-- need to turn sqlcmd mode on
:OUT \\Client\H$\Downloads\clarity\flowsheet_dict.rpt
SET NOCOUNT ON
select DISTINCT FLO_MEAS_ID, FLO_MEAS_NAME, DISP_NAME FROM CLARITY.dbo.IP_FLO_GP_DATA;
GO

:OUT \\Client\H$\Downloads\clarity\lab_dict.rpt
SET NOCOUNT ON
select DISTINCT component_id, name, base_name, external_name FROM dbo.CLARITY_COMPONENT
GO

:OUT \\Client\H$\Downloads\clarity\lab_proc.rpt
SET NOCOUNT ON
select DISTINCT proc_id, proc_name, proc_code FROM CLARITY..CLARITY_EAP
GO

:OUT \\Client\H$\Downloads\clarity\med_dict.rpt
SET NOCOUNT ON
select
MEDICATION_ID,
meds.name,
GENERIC_NAME,
STRENGTH,
form,
route,
meds.PHARM_CLASS_C,
pharmClass.name pharm_class_name,
pharmClass.title pharm_class_title,
meds.THERA_CLASS_C,
thera.name threa_class_name,
thera.title threa_class_title
 from clarity.dbo.CLARITY_MEDICATION meds
 LEFT  JOIN dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
 LEFT JOIN dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
GO