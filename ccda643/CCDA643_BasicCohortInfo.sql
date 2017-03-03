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
INNER JOIN Analytics.dbo.CCDA264_CSNLookupTable csn ON PAT_ENC_HSP_1.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
INNER JOIN Analytics.dbo.CCDA264_PatLookupTable pat ON pat.pat_id = pat_enc_hsp_1.pat_id
INNER JOIN CLARITY.dbo.CLARITY_DEP depDisch ON PAT_ENC_HSP_1.DEPARTMENT_ID = depDisch.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.zc_disch_disp zc_disch_disp ON PAT_ENC_HSP_1.disch_disp_c = zc_disch_disp.disch_disp_c
ORDER BY csn.EXTERNAL_ID
