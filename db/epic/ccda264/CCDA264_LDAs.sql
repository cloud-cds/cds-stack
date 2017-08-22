/****** Script for SelectTopNRows command from SSMS  ******/
SELECT pat.EXTERNAL_ID PAT_ID 
      ,[PLACEMENT_INSTANT]
     ,GP.FLO_MEAS_NAME
	  ,GP.DISP_NAME
      ,[PROPERTIES_DISPLAY]
      ,[SITE]
      ,[REMOVAL_DTTM]
	  into CCDA276_LDAs
  FROM [CLARITY].[dbo].[IP_LDA_NOADDSINGLE] LDA 
  LEFT JOIN [CLARITY].[dbo].[IP_FLO_GP_DATA] gp on gp.FLO_MEAS_ID = LDA.FLO_MEAS_ID
  INNER JOIN 
  CCDA276_PatLookupTable pat ON pat.PAT_ID = LDA.PAT_ID