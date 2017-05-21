template = '''
USE CLARITY;
:OUT \\\\Client\F$\clarity\\note-{start_date}.rpt
SET NOCOUNT ON
SELECT DISTINCT csn.EXTERNAL_ID CSN_ID
  ,info.NOTE_ID
  ,prov_type.NAME AuthorType
  ,TYPINDEX.NAME NoteType
  ,info.CREATE_INSTANT_DTTM
  ,txt.line
  ,TXT.NOTE_TEXT
  ,txt.CONTACT_DATE_REAL
  ,noteStat.NAME NoteStatus
  ,noteEncs.SPEC_NOTE_TIME_DTTM
  ,noteEncs.ENTRY_INSTANT_DTTM
FROM Analytics.dbo.CCDA643_CSNLookupTable csn
INNER JOIN dbo.HNO_INFO info ON info.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
INNER JOIN dbo.note_enc_info noteEncs ON noteEncs.NOTE_ID = info.NOTE_ID
INNER JOIN dbo.HNO_NOTE_TEXT txt ON txt.NOTE_CSN_ID = noteEncs.CONTACT_SERIAL_NUM
INNER JOIN dbo.ZC_NOTE_TYPE_IP TYPINDEX ON info.IP_NOTE_TYPE_C = TYPINDEX.TYPE_IP_C
INNER JOIN dbo.CLARITY_EMP emp ON emp."USER_ID" = noteEncs.AUTHOR_USER_ID
INNER JOIN dbo.CLARITY_SER ser ON ser.PROV_ID = emp.PROV_ID
INNER JOIN dbo.ZC_PROV_TYPE PROV_TYPE ON PROV_TYPE.PROV_TYPE_C = ser.PROVIDER_TYPE_C
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
    AND info.CREATE_INSTANT_DTTM >= '{start_date}'
    AND info.CREATE_INSTANT_DTTM < '{end_date}';
GO
'''

start_date = (2014, 1)
end_date = (2017, 12)
for year in range(start_date[0], end_date[0]+1):
  for month in range(1,13,6):
    if year == start_date[0] and month < start_date[1]:
      continue
    if year == end_date[0] and month >= end_date[1]:
      break
    this_start_date = "{year}-{month}-01".format(year=year, month=str(month).zfill(2))
    if month == 7:
      this_end_date = "{year}-{month}-01".format(year=year+1, month=str(1).zfill(2))
    else:
      this_end_date = "{year}-{month}-01".format(year=year, month=str(month + 6).zfill(2))
    print(template.format(start_date=this_start_date, end_date=this_end_date))

