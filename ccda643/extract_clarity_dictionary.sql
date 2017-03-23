-- need to turn sqlcmd mode on
:OUT \\Client\H$\Downloads\clarity\flowsheet_dict.rpt
SET NOCOUNT ON
select DISTINCT FLO_MEAS_ID, FLO_MEAS_NAME FROM CLARITY.dbo.IP_FLO_GP_DATA;
GO

:OUT \\Client\H$\Downloads\clarity\lab_dict.rpt
SET NOCOUNT ON
select DISTINCT component_id, name, external_name FROM dbo.CLARITY_COMPONENT
GO

:OUT \\Client\H$\Downloads\clarity\lab_proc.rpt
SET NOCOUNT ON
select DISTINCT proc_id, proc_name, proc_code FROM CLARITY..CLARITY_EAP
GO