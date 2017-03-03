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
	
FROM Analytics.dbo.CCDA264_CSNLookupTable csn
INNER JOIN CLARITY.dbo.PAT_ENC_DX dx ON dx.PAT_ENC_CSN_ID = CSN.PAT_ENC_CSN_ID --and DX_ED_YN = 'Y'
INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON dx.DX_ID = edg.DX_ID
INNER JOIN CLARITY.DBO.EDG_CURRENT_ICD9 icd9 ON dx.DX_ID = icd9.DX_ID
INNER JOIN Analytics.dbo.CCDA264_ICD9Codes icdIndex ON ISNUMERIC(icd9.Code) = 1
	AND icd9.Code >= icdIndex."Low Range"
	AND icd9.Code < icdIndex."High Cutoff"

	--SELECT DISTINCT
	--	basediag.PAT_ENC_CSN_ID
	--	,basediag.DX_ID 
	--	,basediag.[ICD9Codes]
	--	,basediag.diagName
	--	,CASE 
	--		WHEN PrimaryDIAG.PAT_ENC_CSN_ID IS NOT NULL
	--			THEN basediag.[Primary_DX_YN]
	--		WHEN basediag.line = minline
	--			THEN 'Y'
	--		ELSE 'N'
	--		END PrimaryDX_YN
	--	,basediag.[line] 
	--	INTO dbo.PrimaryDiagnoses
	--FROM [dbo].diags basediag
	--LEFT JOIN [dbo].diags PrimaryDiag ON basediag.PAT_ENC_CSN_ID = PrimaryDiag.PAT_ENC_CSN_ID
	--	AND PrimaryDiag.Primary_DX_YN = 'Y'
	--LEFT JOIN (
	--	SELECT PAT_ENC_CSN_ID
	--		,MIN(line)
	--	FROM dbo.diags
	--	GROUP BY PAT_ENC_CSN_ID
	--	) minLine(CSN, minline) ON basediag.PAT_ENC_CSN_ID = minLine.CSN
	--	WHERE CASE 
	--		WHEN PrimaryDIAG.PAT_ENC_CSN_ID IS NOT NULL
	--			THEN basediag.[Primary_DX_YN]
	--		WHEN basediag.line = minline
	--			THEN 'Y'
	--		ELSE 'N'
	--		END = 'Y'	
	--SELECT PAT_ENC_CSN_ID, p.DX_ID, cur.Code INTO 
	--dbo.PrimaryDiagnosesDiscrete FROM dbo.PrimaryDiagnoses p INNER JOIN JHEPICRPTDBSQL.CLARITY.dbo.EDG_CURRENT_ICD9 cur
	--ON p.DX_ID = cur.DX_ID
