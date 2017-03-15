SELECT DISTINCT CSN.EXTERNAL_ID CSN_ID
	,edg.DX_ID
	,DX_ED_YN
	,dx.PRIMARY_DX_YN
	,DX.line
	,edg.DX_NAME diagName
	,icd9.Code
	,dx.Annotation
	,dx.COMMENTS
	,DX_CHRONIC_YN
	,icdIndex."ICD-9 Code category"
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
INNER JOIN CLARITY.dbo.PAT_ENC_DX dx ON dx.PAT_ENC_CSN_ID = CSN.PAT_ENC_CSN_ID
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON dx.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON dx.DX_ID = icd9.DX_ID
INNER JOIN Analytics.dbo.CCDA264_ICD9Codes icdIndex ON ISNUMERIC(icd9.Code) = 1
	AND icd9.Code >= icdIndex."Low Range"
	AND icd9.Code < icdIndex."High Cutoff"


