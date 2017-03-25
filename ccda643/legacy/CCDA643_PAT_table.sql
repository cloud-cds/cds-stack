-- DO NOT RUN THIS CODE UNTIL YOU'VE ALTERED THE DATES IN THE COHORT, OTHERWISE IT WILL CREATE DUPLICATE RECORDS!!!
select pat_id "PAT_ID", pat_mrn_id "PAT_MRN_ID", NEWID() "EXTERNAL_ID"
INTO
analytics.dbo.CCDA643_PatLookupTable
FROM
(SELECT DISTINCT patient.PAT_ID, patient.PAT_MRN_ID
	FROM CLARITY.dbo.PAT_ENC_HSP PAT_ENC_HSP_1
	INNER JOIN CLARITY.dbo.PATIENT patient ON PAT_ENC_HSP_1.PAT_ID = patIENT.PAT_ID
	INNER JOIN [CLARITY].[dbo].[CLARITY_ADT] Medicalxferin ON Medicalxferin.PAT_ENC_CSN_ID = PAT_ENC_HSP_1.PAT_ENC_CSN_ID
	LEFT JOIN [CLARITY].[dbo].[CLARITY_ADT] edxferout ON EDxferout.event_id = Medicalxferin.XFER_EVENT_ID
	LEFT JOIN CLARITY.dbo.CLARITY_ADT Medicalxferout ON Medicalxferin.EVENT_ID = Medicalxferout.LAST_IN_EVENT_ID
	LEFT JOIN CLARITY.dbo.CLARITY_ADT discharge ON Medicalxferin.DIS_EVENT_ID = discharge.EVENT_ID
	WHERE
	--Age is greater than 15 at hospital admission
	datediff(year, patient.birth_date, PAT_ENC_HSP_1.HOSP_ADMSN_TIME) + CASE
			WHEN MONTH(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < month(patient.birth_date)
				THEN - 1
			WHEN month(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) = month(patient.birth_date)
				AND day(PAT_ENC_HSP_1.HOSP_ADMSN_TIME) < day(patient.birth_date)
				THEN - 1
			ELSE 0
			END >= 15
	-- EVENT TYPE FOR TRANSFER INTO MEDICAL UNIT IS ADMISSION, TRANSFER OR DISCHARGE
		AND Medicalxferin.EVENT_TYPE_C < 5
	-- ENSURE PATIENT WAS ADMITTED TO ONE OF THE HOWARD COUNTY UNITS YOU ARE STUDYING
		AND Medicalxferin.DEPARTMENT_ID IN (
			'110300814'
		,'110300855'
		,'110300270'
		,'110300140'
		,'110300280'
		,'110300180'
		,'110300110'
		,'110300120'
		,'110300130'
		,'110300170'
			)
		-- ADT EVENT WAS NOT CANCELED
		AND Medicalxferin.EVENT_SUBTYPE_C IN (
			1
			,3
			)
		-- Either they came right from the HCGH ED or were a direct admit to HCGH
		AND (
			(edxferout.EVENT_ID IS NULL and Medicalxferin.EVENT_TYPE_C = 1)
			OR (
				EDxferout.EVENT_SUBTYPE_C IN (
					1
					,3
					)
				AND edxferout.DEPARTMENT_ID IN (
					'110300470'
					,'110300460'
					)
				)
			) AND
		--no patients that are still present in hospital
		HOSP_DISCH_TIME IS NOT NULL
		--admitted between the dates in your cohort
		AND HOSP_ADMSN_TIME BETWEEN '2013-06-01' AND  '2017-01-01'
) A (pat_id, pat_mrn_id)

