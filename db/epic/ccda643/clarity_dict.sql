-- need to turn sqlcmd mode on
:OUT \\Client\F$\clarity-all\flowsheet_dict.rpt
SET NOCOUNT ON
select DISTINCT FLO_MEAS_ID, FLO_MEAS_NAME, DISP_NAME FROM CLARITY.dbo.IP_FLO_GP_DATA;
GO

:OUT \\Client\F$\clarity-all\lab_dict.rpt
SET NOCOUNT ON
select DISTINCT component_id, name, base_name, external_name FROM dbo.CLARITY_COMPONENT
GO

:OUT \\Client\F$\clarity-all\lab_proc.rpt
SET NOCOUNT ON
select DISTINCT proc_id, proc_name, proc_code FROM CLARITY..CLARITY_EAP
GO

:OUT \\Client\F$\clarity-all\med_dict.rpt
SET NOCOUNT ON
select
MEDICATION_ID,
meds.name,
GENERIC_NAME,
STRENGTH,
form,
route,
meds.PHARM_CLASS_C,
pharmClass.name pharm_class_name,
pharmClass.title pharm_class_title,
meds.THERA_CLASS_C,
thera.name threa_class_name,
thera.title threa_class_title
 from clarity.dbo.CLARITY_MEDICATION meds
 LEFT  JOIN dbo.ZC_PHARM_CLASS pharmClass ON pharmClass.PHARM_CLASS_C = meds.PHARM_CLASS_C
 LEFT JOIN dbo.ZC_THERA_CLASS thera ON thera.THERA_CLASS_C = meds.THERA_CLASS_C
GO