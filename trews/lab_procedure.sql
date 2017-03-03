-- lab_procedure.sql
USE Analytics;

SELECT  IDENTITY_ID.IDENTITY_ID pat_id
    ,PAT_ENC_HSP.PAT_ENC_CSN_ID
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
INNER JOIN CLARITY..CLARITY_EAP eap ON procs.proc_id = eap.PROC_ID
LEFT JOIN CLARITY..IP_FREQUENCY freq on freq.FREQ_ID = eap.DFLT_INTER_ID
INNER JOIN clarity..EDP_PROC_CAT_INFO proccat ON eap.proc_cat_id = proccat.PROC_CAT_ID
-----------------------
-- get pat_id
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON procs.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
INNER JOIN CLARITY.dbo.IDENTITY_ID on IDENTITY_ID.PAT_ID = PAT_ENC_HSP.PAT_ID
-----------------------
LEFT JOIN CLARITY..zc_order_status ordstat on ordstat.ORDER_STATUS_C = procs.order_status_c
INNER JOIN CLARITY..ORDER_INSTANTIATED inst ON inst.INSTNTD_ORDER_ID = PROCS.ORDER_PROC_ID
INNER JOIN CLARITY..ORDER_PROC parents on inst.ORDER_ID = parents.ORDER_PROC_ID

-- limit timestamp to be 72 hours until now
AND PROCS.RESULT_TIME >= DATEADD(hh, -72, GETDATE())

-- only query HCGH patients
and PAT_ENC_HSP.DEPARTMENT_ID like '1103%'