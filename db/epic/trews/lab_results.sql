USE CLARITY;

SELECT IDENTITY_ID.IDENTITY_ID pat_id
    ,PAT_ENC_HSP.PAT_ENC_CSN_ID
    ,COMP.COMPONENT_ID
    ,COMP.BASE_NAME
    ,COMP.NAME
    ,COMP.EXTERNAL_NAME
    ,RES.RESULT_TIME
    ,RES.REFERENCE_UNIT
    ,RES.ORD_VALUE ResultValue
    ,RES.COMPONENT_COMMENT
FROM dbo.ORDER_RESULTS res
INNER JOIN dbo.CLARITY_COMPONENT COMP ON res.COMPONENT_ID = COMP.COMPONENT_ID
INNER JOIN Analytics.dbo.CCDA264_ComponentBaseNames basenames ON comp.BASE_NAME = basenames.BASE_NAME
LEFT JOIN Analytics.dbo.CCDA264_OrderProcIds procids on procids.ORDER_PROC_ID = res.ORDER_PROC_ID
-----------------------
-- get pat_id
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON res.PAT_ENC_CSN_ID = PAT_ENC_HSP.pat_enc_csn_id
INNER JOIN IDENTITY_ID on IDENTITY_ID.PAT_ID = PAT_ENC_HSP.PAT_ID
-----------------------
WHERE res.RESULT_STATUS_C IN (
        3
        ,4,5
        )
    AND res.lab_status_c >= 3
and     IDENTITY_ID.line = 1
-- limit timestamp to be 72 hours until now
AND RES.RESULT_TIME >= DATEADD(hh, -72, GETDATE())

-- only query HCGH patients
and PAT_ENC_HSP.DEPARTMENT_ID like '1103%'
