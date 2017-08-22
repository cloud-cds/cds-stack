USE CLARITY;
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
ORDER BY mar.TAKEN_TIME