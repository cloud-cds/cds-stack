USE CLARITY;

SELECT csn.EXTERNAL_ID CSN_ID
	,COMP.BASE_NAME
	,COMP.NAME
	,COMP.EXTERNAL_NAME
	,RES.RESULT_TIME
	,RES.REFERENCE_UNIT
	,RES.ORD_VALUE ResultValue
	,RES.COMPONENT_COMMENT
	,PROCIDS.EXTERNAL_ID OrderProcId
FROM Analytics.[WIN\MTOERPE1].CCDA264_CSNLookupTable csn
INNER JOIN dbo.ORDER_RESULTS res ON res.PAT_ENC_CSN_ID = csn.pat_enc_csn_id
INNER JOIN dbo.CLARITY_COMPONENT COMP ON res.COMPONENT_ID = COMP.COMPONENT_ID
INNER JOIN Analytics.dbo.CCDA264_ComponentBaseNames basenames ON comp.BASE_NAME = basenames.BASE_NAME
LEFT JOIN Analytics.[WIN\MTOERPE1].CCDA264_OrderProcIds procids on procids.ORDER_PROC_ID = res.ORDER_PROC_ID
WHERE res.RESULT_STATUS_C IN (
		3
		,4,5
		)
	AND res.lab_status_c >= 3
