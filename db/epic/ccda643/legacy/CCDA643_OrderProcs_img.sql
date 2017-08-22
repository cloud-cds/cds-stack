USE Analytics;

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
	,hnt.LINE
	,hnt.NOTE_TEXT
FROM CLARITY..ORDER_PROC procs
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
LEFT JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN CCDA643_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
inner join clarity..V_IMG_STUDY img on img.order_id = procs.ORDER_PROC_ID
inner join clarity..HNO_NOTE_TEXT hnt on hnt.NOTE_CSN_ID = img.RESULT_NOTE_CSN
where img.proc_name like '%MRI%' or img.proc_name like '%CT%';