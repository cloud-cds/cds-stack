SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
	,pat.EXTERNAL_ID PATIENTID
	,CAST(COALESCE(Encounter.DEPARTMENT_ID, HospitalEncounter.DEPARTMENT_ID) AS VARCHAR(50)) DEPARTMENTID
	,edg.DX_NAME diagName
	,icd9.Code
	,icdIndex."ICD-9 Code category"
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
INNER JOIN Analytics.dbo.CCDA264_ICD9Codes icdIndex ON ISNUMERIC(icd9.Code) = 1
	AND icd9.Code >= icdIndex."Low Range"
	AND icd9.Code < icdIndex."High Cutoff"
