-- med_orders.sql

USE Analytics;
SELECT IDENTITY_ID.IDENTITY_ID pat_id
    ,PAT_ENC_HSP.PAT_ENC_CSN_ID
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
LEFT JOIN CLARITY.dbo.ZC_ADMIN_ROUTE medrt ON medrt.MED_ROUTE_C = MED.MED_ROUTE_C
LEFT JOIN CLARITY.dbo.ZC_MED_UNIT medunit ON medunit.DISP_QTYUNIT_C = MED.HV_DOSE_UNIT_C
-----------------------
-- get pat_id
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON  pat_enc_hsp.pat_enc_csn_id = med.PAT_ENC_CSN_ID
INNER JOIN CLARITY.dbo.IDENTITY_ID on IDENTITY_ID.PAT_ID = PAT_ENC_HSP.PAT_ID
-----------------------
WHERE (
        med.IS_PENDING_ORD_YN = 'N'
        OR med.IS_PENDING_ORD_YN IS NULL
        )
    AND med.ORDER_STATUS_C NOT IN (
        1
        ,4
        ,7
        )
    AND IDENTITY_ID.line = 1
-- limit timestamp to be 72 hours until now
AND med.ORDER_INST >= DATEADD(hh, -72, GETDATE())

-- only query HCGH patients
and PAT_ENC_HSP.DEPARTMENT_ID like '1103%'