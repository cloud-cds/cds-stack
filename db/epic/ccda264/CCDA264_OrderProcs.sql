USE Analytics;

SELECT PAT_ENC_HSP_1.EXTERNAL_ID CSN_ID
	,procids.EXTERNAL_ID OrderProcId
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
FROM CLARITY..ORDER_PROC procs
INNER JOIN ccda264_OrderProcIds procids on procs.ORDER_PROC_ID = procids.ORDER_PROC_ID
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN [WIN\mtoerpe1].CCDA264_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CCDA264_OrderProcCategories AllowedCats ON proccat.PROC_CAT_NAME = AllowedCats.CategoryName
LEFT JOIN CCDA264_OrderProcCodes codes ON codes.ProcCode = eap.PROC_CODE
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c 
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID 
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID 
--INNER JOIN CLARITY..ORDER_PROC_3 parents3 ON parents.ORDER_PROC_ID = parents3.ORDER_ID
--WHERE (
--		procs.IS_PENDING_ORD_YN = 'N'
--		OR procs.IS_PENDING_ORD_YN IS NULL
--		)
--	AND PROCS.FUTURE_OR_STAND IS NULL
--	AND procs.INSTANTIATED_TIME IS NOT NULL
--	AND (
--		AllowedCats.CategoryName IS NOT NULL
--		OR codes.ProcCode IS NOT NULL
--		)
