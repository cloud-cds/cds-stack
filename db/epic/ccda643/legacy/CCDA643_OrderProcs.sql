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
	,labstats.NAME LabStatus
FROM CLARITY..ORDER_PROC procs
INNER JOIN ccda264_OrderProcIds procids on procs.ORDER_PROC_ID = procids.ORDER_PROC_ID
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