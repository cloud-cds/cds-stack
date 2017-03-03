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
	,osq.order_id
, osq.line
, osq.ord_quest_id
, osq.IS_ANSWR_BYPROC_YN 
, osq.ord_quest_resp
, cq.quest_name
, tm.question
FROM CLARITY..ORDER_PROC procs
INNER JOIN ccda264_OrderProcIds procids on procs.ORDER_PROC_ID = procids.ORDER_PROC_ID
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
INNER JOIN CCDA264_CSNLookupTable pat_enc_hsp_1 ON pat_enc_hsp_1.pat_enc_csn_id = procs.PAT_ENC_CSN_ID
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
left join clarity.dbo.ORD_SPEC_QUEST osq on osq.ORDER_ID = procs.ORDER_PROC_ID
left join clarity.dbo.CL_QQUEST cq on osq.ORD_QUEST_ID = cq.QUEST_ID 
left join clarity.dbo.CL_QQUEST_OVTM tm on tm.QUEST_ID = cq.QUEST_ID
where procs.proc_id in 
(
'293',
'160318',
'2015293',
'38825',
'67413',
'67415',
'67479',
'127058',
'70027',
'71988',
'71990',
'110189',
'131944',
'165547',
'3041752'
)