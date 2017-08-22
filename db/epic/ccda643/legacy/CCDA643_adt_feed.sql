SELECT CSN.EXTERNAL_ID CSN_ID
	,CASE
		WHEN EVENT_TYPE_C = 1
			THEN 'Admission'
		WHEN EVENT_TYPE_C = 2
			THEN 'Discharge'
		WHEN event_type_c = 3
			THEN 'Transfer In'
		WHEN event_type_c = 4
			THEN 'Transfer Out'
		ELSE NULL
		END EventType
	,effective_time
	,ptClass.NAME PatientClassAtEvent
	,dep.DEPARTMENT_NAME
	,room.ROOM_NAME

FROM CLARITY.dbo.CLARITY_ADT
INNER JOIN Analytics.dbo.CCDA643_CSNLookupTable csn ON CLARITY_ADT.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
LEFT JOIN CLARITY.DBO.ZC_PAT_CLASS ptClass ON ptClass.ADT_PAT_CLASS_C = clarity_adt.PAT_CLASS_C
LEFT JOIN CLARITY.dbo.CLARITY_DEP DEP ON DEP.DEPARTMENT_ID = clarity_adt.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.CLARITY_ROM room ON room.ROOM_CSN_ID = CLARITY_ADT.ROOM_CSN_ID
WHERE CLARITY_ADT.EVENT_TYPE_C < 5
	AND CLARITY_ADT.EVENT_SUBTYPE_C IN (
		1
		,3
		)
