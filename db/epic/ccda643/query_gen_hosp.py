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


:OUT \\\\Client\F$\clarity\\adt.{idx}.rpt
SET NOCOUNT ON
SELECT CSN.EXTERNAL_ID CSN_ID
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
INNER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON CLARITY_ADT.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
LEFT JOIN CLARITY.DBO.ZC_PAT_CLASS ptClass ON ptClass.ADT_PAT_CLASS_C = clarity_adt.PAT_CLASS_C
LEFT JOIN CLARITY.dbo.CLARITY_DEP DEP ON DEP.DEPARTMENT_ID = clarity_adt.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.CLARITY_ROM room ON room.ROOM_CSN_ID = CLARITY_ADT.ROOM_CSN_ID
WHERE CLARITY_ADT.EVENT_TYPE_C < 5
  AND CLARITY_ADT.EVENT_SUBTYPE_C IN (
    1
    ,3
    );
GO

:OUT \\\\Client\F$\clarity\demo.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
  ,pat.EXTERNAL_ID pat_id
  ,ADT_ARRIVAL_TIME
  ,ED_DEPARTURE_TIME
  ,HOSP_ADMSN_TIME
  ,HOSP_DISCH_TIME
  ,datediff(year, patient.birth_date, PAT_ENC_HSP_1.HOSP_ADMSN_TIME) + CASE
    WHEN MONTH(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < month(patient.birth_date)
      THEN - 1
    WHEN month(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) = month(patient.birth_date)
      AND day(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < day(patient.birth_date)
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
    WHEN PAT_ENC_HSP_1.ED_EPISODE_ID IS NOT NULL
      THEN 1
    ELSE 0
    END IsEDPatient
  ,depDisch.DEPARTMENT_NAME DischargeDepartment
  ,zc_disch_disp.NAME DischargeDisposition
--INTO CCDA276_Demographics
FROM CLARITY.dbo.PAT_ENC_HSP PAT_ENC_HSP_1
INNER JOIN CLARITY.dbo.PATIENT patient ON PAT_ENC_HSP_1.pat_id = patient.pat_id
INNER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON PAT_ENC_HSP_1.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
INNER JOIN Analytics.dbo.CCDA643_PatLookupTable pat ON pat.pat_id = pat_enc_hsp_1.pat_id
INNER JOIN CLARITY.dbo.CLARITY_DEP depDisch ON PAT_ENC_HSP_1.DEPARTMENT_ID = depDisch.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.zc_disch_disp zc_disch_disp ON PAT_ENC_HSP_1.disch_disp_c = zc_disch_disp.disch_disp_c
ORDER BY csn.EXTERNAL_ID;
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


:OUT \\\\Client\F$\clarity\\flt_lda.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
  ,IP_FLO_GP_DATA.FLO_MEAS_NAME
  ,IP_FLO_GP_DATA.DISP_NAME
  ,IP_FLWSHT_MEAS.FLO_MEAS_ID
  ,rowtype.NAME ROW_TYPE
  ,intakeType.NAME INTAKE_TYPE
  ,outputType.NAME OUTPUT_TYPE
  ,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
  ,COALESCE(indices.NAME, 'Intake') FlowsheetVAlueType
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
INNER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON pat_enc_hsp.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValues fsvals ON fsvals.FLO_MEAS_ID = IP_flo_GP_DATA.FLO_MEAS_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValueIndex indices ON indices.ID = fsvals.FSValueType
LEFT JOIN Analytics.dbo.CCDA264_IntakeRowsTemplatesOnly intake ON intake.[ROW MEAS ID] = IP_flo_GP_DATA.FLO_MEAS_ID
  AND intake.[TEMPLATE ID] = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.dbo.IP_FLT_DATA flt ON flt.TEMPLATE_ID = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.DBO.ZC_ROW_TYP rowtype ON rowtype.ROW_TYP_C = ip_flo_gp_data.ROW_TYP_C
LEFT JOIN CLARITY.DBO.ZC_INTAKE_TYPE_P intakeType ON intakeType.INTAKE_TYPe_P_C = ip_flo_gp_data.INTAKE_TYP_C
LEFT JOIN CLARITY.DBO.ZC_OUTPUT_TYPE_P outputType ON outputType.OUTPUT_TYPE_P_C = ip_flo_gp_data.OUTPUT_TYP_C
WHERE
  (
    IP_FLWSHT_MEAS.FLT_ID = '3040010002'
    OR intake.[ROW MEAS ID] IS NOT NULL
    OR fsvals.FLO_MEAS_ID IS NOT NULL
    );
GO

:OUT \\\\Client\F$\clarity\\flt_new.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT  csn.EXTERNAL_ID CSN_ID
  ,IP_FLO_GP_DATA.FLO_MEAS_NAME
  ,IP_FLO_GP_DATA.DISP_NAME
  ,IP_FLWSHT_MEAS.FLO_MEAS_ID
  ,rowtype.NAME ROW_TYPE
  ,intakeType.NAME INTAKE_TYPE
  ,outputType.NAME OUTPUT_TYPE
  ,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
  ,COALESCE(indices.NAME, 'Intake') FlowsheetVAlueType
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
INNER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON pat_enc_hsp.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValues fsvals ON fsvals.FLO_MEAS_ID = IP_flo_GP_DATA.FLO_MEAS_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValueIndex indices ON indices.ID = fsvals.FSValueType
LEFT JOIN Analytics.dbo.CCDA264_IntakeRowsTemplatesOnly intake ON intake.[ROW MEAS ID] = IP_flo_GP_DATA.FLO_MEAS_ID
  AND intake.[TEMPLATE ID] = IP_FLWSHT_MEAS.FLT_ID
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
'306270', '301360', '301250', '1120100022', '111301360','212301360', '450070', -- map
'52', '55', '16304', '331268', '331317', '7071002', '7070009', '3040101336', '3040100475', '3040101332', -- fluids intake
'304123801','304123802','3043046001','30440104362','61' -- urine output
);
GO

:OUT \\\\Client\F$\clarity\\flt.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT  csn.EXTERNAL_ID CSN_ID
  ,IP_FLO_GP_DATA.FLO_MEAS_NAME
  ,IP_FLO_GP_DATA.DISP_NAME
  ,IP_FLWSHT_MEAS.FLO_MEAS_ID
  ,rowtype.NAME ROW_TYPE
  ,intakeType.NAME INTAKE_TYPE
  ,outputType.NAME OUTPUT_TYPE
  ,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
  ,COALESCE(indices.NAME, 'Intake') FlowsheetVAlueType
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
INNER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON pat_enc_hsp.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValues fsvals ON fsvals.FLO_MEAS_ID = IP_flo_GP_DATA.FLO_MEAS_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValueIndex indices ON indices.ID = fsvals.FSValueType
LEFT JOIN Analytics.dbo.CCDA264_IntakeRowsTemplatesOnly intake ON intake.[ROW MEAS ID] = IP_flo_GP_DATA.FLO_MEAS_ID
  AND intake.[TEMPLATE ID] = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.dbo.IP_FLT_DATA flt ON flt.TEMPLATE_ID = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.DBO.ZC_ROW_TYP rowtype ON rowtype.ROW_TYP_C = ip_flo_gp_data.ROW_TYP_C
LEFT JOIN CLARITY.DBO.ZC_INTAKE_TYPE_P intakeType ON intakeType.INTAKE_TYPe_P_C = ip_flo_gp_data.INTAKE_TYP_C
LEFT JOIN CLARITY.DBO.ZC_OUTPUT_TYPE_P outputType ON outputType.OUTPUT_TYPE_P_C = ip_flo_gp_data.OUTPUT_TYP_C
WHERE RWS.IP_LDA_ID IS NULL AND (FLT_ID = '3040010002'
  OR intake.[ROW MEAS ID] IS NOT NULL
  OR fsvals.FLO_MEAS_ID IS NOT NULL);
GO

USE CLARITY;

:OUT \\\\Client\F$\clarity\labs.{idx}.rpt
SET NOCOUNT ON
SELECT csn.EXTERNAL_ID CSN_ID
  ,COMP.COMPONENT_ID
  ,COMP.BASE_NAME
  ,COMP.NAME
  ,COMP.EXTERNAL_NAME
  ,RES.RESULT_TIME
  ,RES.REFERENCE_UNIT
  ,RES.ORD_VALUE ResultValue
  ,RES.COMPONENT_COMMENT
  ,res.ORDER_PROC_ID OrderProcId
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
INNER JOIN dbo.ORDER_RESULTS res ON res.PAT_ENC_CSN_ID = csn.pat_enc_csn_id
INNER JOIN dbo.CLARITY_COMPONENT COMP ON res.COMPONENT_ID = COMP.COMPONENT_ID
INNER JOIN Analytics.dbo.CCDA264_ComponentBaseNames basenames ON comp.BASE_NAME = basenames.BASE_NAME
WHERE res.RESULT_STATUS_C IN (
    3
    ,4,5
    )
  AND res.lab_status_c >= 3;
GO

USE CLARITY;

:OUT \\\\Client\F$\clarity\labs_new.{idx}.rpt
SET NOCOUNT ON
SELECT csn.EXTERNAL_ID CSN_ID
  ,COMP.COMPONENT_ID
  ,COMP.BASE_NAME
  ,COMP.NAME
  ,COMP.EXTERNAL_NAME
  ,RES.RESULT_TIME
  ,RES.REFERENCE_UNIT
  ,RES.ORD_VALUE ResultValue
  ,RES.COMPONENT_COMMENT
  ,res.ORDER_PROC_ID OrderProcId
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
INNER JOIN dbo.ORDER_RESULTS res ON res.PAT_ENC_CSN_ID = csn.pat_enc_csn_id
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
  'EKG',
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
  'YEASTCUL',
  'INFLUANAT',
  'INFLUBNAT',
  'RSVNAT'
  )
);
GO

/****** Script for SelectTopNRows command from SSMS  ******/
:OUT \\\\Client\F$\clarity\lda.{idx}.rpt
SET NOCOUNT ON
SELECT pat.EXTERNAL_ID PAT_ID
      ,[PLACEMENT_INSTANT]
     ,GP.FLO_MEAS_NAME
    ,GP.DISP_NAME
      ,[PROPERTIES_DISPLAY]
      ,[SITE]
      ,[REMOVAL_DTTM]
  FROM [CLARITY].[dbo].[IP_LDA_NOADDSINGLE] LDA
  LEFT JOIN [CLARITY].[dbo].[IP_FLO_GP_DATA] gp on gp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
  INNER JOIN
  Analytics.dbo.CCDA643_PatLookupTable pat ON pat.PAT_ID = LDA.PAT_ID;
GO

USE CLARITY;
:OUT \\\\Client\F$\clarity\mar.{idx}.rpt
SET NOCOUNT ON
SELECT
  csn.EXTERNAL_ID CSN_ID,
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
inner join Analytics.dbo.CCDA643_CSNLookupTable csn ON med.PAT_ENC_CSN_ID = csn.pat_enc_csn_id
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
  AND mar.TAKEN_TIME <= CURRENT_TIMESTAMP
ORDER BY mar.TAKEN_TIME;
GO


:OUT \\\\Client\F$\clarity\hist.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT coalesce(csn.EXTERNAL_ID, Encounter.PAT_ENC_CSN_ID) CSN_ID 
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
WHERE not csn.EXTERNAL_ID is null or not Encounter.PAT_ENC_CSN_ID is null;
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
:OUT \\\\Client\F$\clarity\orderproc.{idx}.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
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
  ,procs2.SPECIMN_TAKEN_TIME
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CCDA264_OrderProcCategories AllowedCats ON proccat.PROC_CAT_NAME = AllowedCats.CategoryName
LEFT JOIN CCDA264_OrderProcCodes codes ON codes.ProcCode = eap.PROC_CODE
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID
LEFT JOIN CLARITY..ORDER_PROC_2 procs2 on procs.ORDER_PROC_ID = procs2.ORDER_PROC_ID;
GO


USE Analytics;
:OUT \\\\Client\F$\clarity\orderproc_img.{idx}.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
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
INNER JOIN CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
LEFT JOIN CLARITY..zc_lab_status labstats on labstats.LAB_STATUS_C = procs.lab_status_c
inner join clarity..V_IMG_STUDY img on img.order_id = procs.ORDER_PROC_ID
inner join clarity..HNO_NOTE_TEXT hnt on hnt.NOTE_CSN_ID = img.RESULT_NOTE_CSN
where img.proc_name like '%MRI%' or img.proc_name like '%CT%';
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

USE CLARITY;
:OUT \\\\Client\F$\clarity\code_rrt_events.{idx}.rpt
SET NOCOUNT ON
SELECT PAT_ENC_HSP_1.EXTERNAL_ID csn_id 
,a.event_id
,a.EVENT_TYPE event_type
,a.event_display_name
,a.event_time
,a.EVENT_RECORD_TIME event_record_time
,a.event_user_id
,a.event_cmt
,d.DEPARTMENT_NAME department_name
,g.FLO_MEAS_ID flo_meas_id
,h.DISP_NAME disp_name
,g.MEAS_COMMENT meas_comment 
,g.MEAS_VALUE meas_value 
from ED_IEV_EVENT_INFO a with (nolock)
inner join ED_IEV_PAT_INFO b with (nolock) on a.event_id = b.EVENT_ID
inner join PATIENT c with (nolock) on b.pat_id = c.pat_id 
inner join Analytics.dbo.CCDA643_CSNLookupTable pat_enc_hsp_1 on pat_enc_hsp_1.pat_enc_csn_id = b.pat_csn 
inner join CLARITY_DEP d with (nolock) on a.EVENT_DEPT_ID = d.DEPARTMENT_ID
inner join IP_DATA_STORE e with (nolock) on b.PAT_CSN = e.EPT_CSN
inner join IP_FLWSHT_REC f with (nolock) on e.inpatient_data_id = f.inpatient_data_id
inner join IP_FLWSHT_MEAS g with (nolock) on f.fsd_id = g.FSD_ID
inner join IP_FLO_GP_DATA h with (nolock) on h.FLO_MEAS_ID = g.FLO_MEAS_ID 
where event_type in (
'34370',
'34371',
'34380',
'34381'
)
and h.FLO_MEAS_ID in (
'1600075003'
,'1600075020'
,'1600075021'
,'1600075023'
,'1600075055'
)
order by PAT_ENC_HSP_1.EXTERNAL_ID, a.event_id,a.line
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



USE CLARITY;
:OUT \\\\Client\F$\clarity\\note.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
  ,info.NOTE_ID
  ,authType.AuthorType
  ,notetypes.NoteType
  ,info.CREATE_INSTANT_DTTM
  ,txt.line
  ,TXT.NOTE_TEXT
  ,txt.CONTACT_DATE_REAL
  ,noteStat.NAME NoteStatus
  ,noteEncs.SPEC_NOTE_TIME_DTTM
  ,noteEncs.ENTRY_INSTANT_DTTM
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
INNER JOIN dbo.HNO_INFO info ON info.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
INNER JOIN dbo.note_enc_info noteEncs ON noteEncs.NOTE_ID = info.NOTE_ID
INNER JOIN dbo.HNO_NOTE_TEXT txt ON txt.NOTE_CSN_ID = noteEncs.CONTACT_SERIAL_NUM
INNER JOIN dbo.ZC_NOTE_TYPE_IP TYPINDEX ON info.IP_NOTE_TYPE_C = TYPINDEX.TYPE_IP_C
INNER JOIN Analytics.dbo.CCDA264_NoteType notetypes ON notetypes.NOTETYPE = TYPINDEX.NAME
INNER JOIN dbo.CLARITY_EMP emp ON emp."USER_ID" = noteEncs.AUTHOR_USER_ID
INNER JOIN dbo.CLARITY_SER ser ON ser.PROV_ID = emp.PROV_ID
INNER JOIN dbo.ZC_PROV_TYPE PROV_TYPE ON PROV_TYPE.PROV_TYPE_C = ser.PROVIDER_TYPE_C
INNER JOIN Analytics.dbo.CCDA264_NoteAuthorType authType ON authType.AuthorType = prov_type.NAME
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
    and (noteEncs.NOTE_STATUS_C NOT IN (1,4,8) OR noteEncs.NOTE_STATUS_C IS NULL);
GO

:OUT \\\\Client\F$\clarity\\chief_complaint.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT CSN.EXTERNAL_ID CSN_ID
  ,LINE
  ,CONTACT_DATE
  ,ENC_REASON_ID
  ,ENC_REASON_NAME
  ,DISPLAY_TEXT
  ,COMMENTS
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
inner join clarity.dbo.PAT_ENC_RSN_VISIT rv on csn.PAT_ENC_CSN_ID = rv.PAT_ENC_CSN_ID
inner join CL_RSN_FOR_VISIT v2 on rv.ENC_REASON_ID = v2.REASON_VISIT_ID ;
GO

:OUT \\\\Client\F$\clarity\\ed_events.{idx}.rpt
SET NOCOUNT ON
SELECT DISTINCT CSN.EXTERNAL_ID CSN_ID
  ,ein.EVENT_ID
  ,LINE
  ,EVENT_DISPLAY_NAME
  ,EVENT_TIME
  ,EVENT_RECORD_TIME
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
inner join Clarity.dbo.ED_IEV_PAT_INFO pin on csn.PAT_ENC_CSN_ID = pin.PAT_ENC_CSN_ID
inner join Clarity.dbo.ED_IEV_EVENT_INFO ein on ein.EVENT_ID = pin.EVENT_ID
where EVENT_RECORD_TIME is not null;
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

dict = '''
use clarity;
-- need to turn sqlcmd mode on
:OUT \\\\Client\F$\clarity\\flowsheet_dict.rpt
SET NOCOUNT ON
select DISTINCT FLO_MEAS_ID, FLO_MEAS_NAME, DISP_NAME FROM CLARITY.dbo.IP_FLO_GP_DATA;
GO

:OUT \\\\Client\F$\clarity\lab_dict.rpt
SET NOCOUNT ON
select DISTINCT component_id, name, base_name, external_name FROM dbo.CLARITY_COMPONENT
GO

:OUT \\\\Client\F$\clarity\lab_proc.rpt
SET NOCOUNT ON
select DISTINCT proc_id, proc_name, proc_code FROM CLARITY..CLARITY_EAP
GO

:OUT \\\\Client\F$\clarity\med_dict.rpt
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
'''

# 1101 jhh
# 1102 bmc
# 1103 hcgh
hosp = '1103'
start_date = [2014, 2]
end_date = [2018, 3]

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
