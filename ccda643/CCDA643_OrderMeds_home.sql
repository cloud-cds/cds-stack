USE Analytics;
SELECT DISTINCT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
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
LEFT JOIN Analytics.dbo.CCDA264_MedicationClasses cohortMedClass ON (
		cohortMedClass.PharmaceuticalClass = pharmClass.NAME
		AND thera.NAME = cohortMedClass.TherapeuticClass
		)
	OR (
		pharmClass.Name IS NULL
		AND thera.NAME = cohortMedClass.TherapeuticClass
		)
	OR (
		thera.Name IS NULL
		AND pharmClass.NAME = cohortMedClass.PharmaceuticalClass
		)
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
		)
		AND
		 cohortMedClass.PharmaceuticalClass IS NOT NULL