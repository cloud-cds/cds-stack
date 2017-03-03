SELECT DISTINCT  csn.EXTERNAL_ID CSN_ID
	,IP_FLO_GP_DATA.FLO_MEAS_NAME
	,IP_FLO_GP_DATA.DISP_NAME
	,IP_FLWSHT_MEAS.FLO_MEAS_ID
	,rowtype.NAME ROW_TYPE
	,intakeType.NAME INTAKE_TYPE
	,outputType.NAME OUTPUT_TYPE
	,IP_FLWSHT_MEAS.RECORDED_TIME TimeTaken
	,COALESCE(indices.NAME, 'Intake') FlowsheetVAlueType
	,CASE 
		WHEN IP_FLO_GP_DATA.val_type_c = 5
			THEN convert(FLOAT, IP_FLWSHT_MEAS.MEAS_VALUE) / 16.0
		ELSE NULL
		END ConvertedWeightValue
	,meas_value Value
	,IP_FLO_GP_DATA.UNITS
	,flt.TEMPLATE_ID
	,flt.TEMPLATE_NAME
	,flt.DISPLAY_NAME TEMPLATE_DISP_NAME
	--,lda.DESCRIPTION ldaDescription
	--,lda.PROPERTIES_DISPLAY
	--,ldagrp.FLO_MEAS_ID LDAFLOMEASID
	--,LDAGRP.FLO_MEAS_NAME LDAFLOMEASNAME
	--,LDAGRP.DISP_NAME LDAGRPDISPNAME
FROM CLARITY.dbo.IP_FLWSHT_MEAS IP_FLWSHT_MEAS
INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.IP_FLWSHT_REC IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
LEFT JOIN CLARITY.DBO.IP_FLOWSHEET_ROWS RWS ON RWS.LINE = IP_FLWSHT_MEAS.OCCURANCE
	AND IP_FLWSHT_MEAS.FSD_ID = RWS.INPATIENT_DATA_ID
--LEFT JOIN CLARITY.DBO.IP_LDA_NOADDSINGLE lda ON LDA.IP_LDA_ID = RWS.IP_LDA_ID
--LEFT JOIN CLARITY.DBO.IP_FLO_GP_DATA ldaGrp ON ldaGrp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
INNER JOIN CLARITY.dbo.PAT_ENC_HSP ON IP_FLWSHT_REC.INPATIENT_DATA_ID = PAT_ENC_HSP.INPATIENT_DATA_ID
INNER JOIN Analytics.dbo.CCDA264_CSNLookupTable csn ON pat_enc_hsp.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValues fsvals ON fsvals.FLO_MEAS_ID = IP_flo_GP_DATA.FLO_MEAS_ID
LEFT JOIN Analytics.dbo.CCDA264_FlowsheetValueIndex indices ON indices.ID = fsvals.FSValueType
LEFT JOIN Analytics.dbo.CCDA264_IntakeRowsTemplatesOnly intake ON intake.[ROW MEAS ID] = IP_flo_GP_DATA.FLO_MEAS_ID
	AND intake.[TEMPLATE ID] = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.dbo.IP_FLT_DATA flt ON flt.TEMPLATE_ID = IP_FLWSHT_MEAS.FLT_ID
LEFT JOIN CLARITY.DBO.ZC_ROW_TYP rowtype ON rowtype.ROW_TYP_C = ip_flo_gp_data.ROW_TYP_C
LEFT JOIN CLARITY.DBO.ZC_INTAKE_TYPE_P intakeType ON intakeType.INTAKE_TYPe_P_C = ip_flo_gp_data.INTAKE_TYP_C
LEFT JOIN CLARITY.DBO.ZC_OUTPUT_TYPE_P outputType ON outputType.OUTPUT_TYPE_P_C = ip_flo_gp_data.OUTPUT_TYP_C
WHERE 
	IP_FLWSHT_MEAS.FLO_MEAS_ID in 

(
'16000582',
'300220',
'16000450606',
'19685',
'400636',
'7096010',
'1600400636',
'11',
'7096010',
'301070',
'8123',
'8126',
'301260',
'301280',
'10980',
'6365'
)
