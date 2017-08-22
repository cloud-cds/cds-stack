USE CLARITY;
SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
	,info.NOTE_ID
	,authType.AuthorType
	,notetypes.NoteType 
	,info.CREATE_INSTANT_DTTM
	,txt.line
	,TXT.NOTE_TEXT
	,txt.CONTACT_DATE_REAL
	,noteStat.NAME NoteStatus
	,noteEncs.SPEC_NOTE_TIME_DTTM
	,noteEncs.ENTRY_INSTANT_DTTM
FROM Analytics.[WIN\MTOERPE1].CCDA264_CSNLookupTable csn
INNER JOIN dbo.HNO_INFO info ON info.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
INNER JOIN dbo.note_enc_info noteEncs ON noteEncs.NOTE_ID = info.NOTE_ID
INNER JOIN dbo.HNO_NOTE_TEXT txt ON txt.NOTE_CSN_ID = noteEncs.CONTACT_SERIAL_NUM
INNER JOIN dbo.ZC_NOTE_TYPE_IP TYPINDEX ON info.IP_NOTE_TYPE_C = TYPINDEX.TYPE_IP_C
INNER JOIN Analytics.dbo.CCDA264_NoteType notetypes ON notetypes.NOTETYPE = TYPINDEX.NAME
INNER JOIN dbo.CLARITY_EMP emp ON emp."USER_ID" = noteEncs.AUTHOR_USER_ID
INNER JOIN dbo.CLARITY_SER ser ON ser.PROV_ID = emp.PROV_ID
INNER JOIN dbo.ZC_PROV_TYPE PROV_TYPE ON PROV_TYPE.PROV_TYPE_C = ser.PROVIDER_TYPE_C
INNER JOIN Analytics.dbo.CCDA264_NoteAuthorType authType ON authType.AuthorType = prov_type.NAME
LEFT JOIN dbo.ZC_NOTE_STATUS noteStat ON noteStat.NOTE_STATUS_C = noteEncs.NOTE_STATUS_C
WHERE info.DELETE_USER_ID IS NULL
	AND (
		info.UNSIGNED_YN IS NULL
		OR info.unsigned_yn = 'N'
		)
	AND (
		info.AMB_NOTE_YN IS NULL
		OR info.AMB_NOTE_YN = 'N'
		) 
		and (noteEncs.NOTE_STATUS_C NOT IN (1,4,8) OR noteEncs.NOTE_STATUS_C IS NULL)
