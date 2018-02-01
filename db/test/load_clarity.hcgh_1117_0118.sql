CREATE SCHEMA IF NOT EXISTS hcgh_1117_0118;

drop table if exists hcgh_1117_0118."Demographics";
create table hcgh_1117_0118."Demographics"
(
 "CSN_ID"                text,
 "pat_id"                text,
 "ADT_ARRIVAL_TIME"      timestamp with    time zone,
 "ED_DEPARTURE_TIME"     timestamp with    time zone,
 "HOSP_ADMSN_TIME"       timestamp with    time zone,
 "HOSP_DISCH_TIME"       timestamp with    time zone,
 "AgeDuringVisit"        text,
 "Gender"                text not  null,
 "IsEDPatient"           integer not  null,
 "DischargeDepartment"   text,
 "DischargeDisposition"  text
 );
\copy hcgh_1117_0118."Demographics" from '/home/ubuntu/zad/mnt/clarity_1ydemo.clarity_1y' with csv delimiter as E'\t' NULL 'NULL';

drop table if exists hcgh_1117_0118."ADT_Feed";
create table hcgh_1117_0118."ADT_Feed"
(
"CSN_ID"    text,
"EventType"  text,
"effective_time"    text,
"PatientClassAtEvent"  text,
"DEPARTMENT_NAME"   text,
"ROOM_NAME"    text
);
\copy hcgh_1117_0118."ADT_Feed" from '/home/ubuntu/zad/mnt/clarity_1yadt.clarity_1y' with csv delimiter as E'\t' NULL 'NULL';

drop table if exists hcgh_1117_0118."Diagnoses";
drop index if exists diag_idx_code;
drop index if exists diag_idx_name;
create table hcgh_1117_0118."Diagnoses"
(
 "CSN_ID"         text
 ,"DX_ID"         numeric
 ,"DX_ED_YN"      text
 ,"PRIMARY_DX_YN" text
 ,"line"          integer
 ,"diagName"      text
 ,"Code"          text
 ,"Annotation"    text
 ,"COMMENTS"      text
 ,"DX_CHRONIC_YN" text
 ,"ICD-9 Code category" text
);
\copy hcgh_1117_0118."Diagnoses" from '/home/ubuntu/zad/mnt/clarity_1ydiag.clarity_1y' with csv delimiter as E'\t' NULL 'NULL';
create index diag_idx_code on hcgh_1117_0118."Diagnoses" ("Code");
create index diag_idx_name on hcgh_1117_0118."Diagnoses" ("diagName");

drop index if exists flt_lda_idx_name;
drop index if exists flt_lda_idx_id;
drop table if exists hcgh_1117_0118."FlowsheetValue-LDA";
create table hcgh_1117_0118."FlowsheetValue-LDA"
(
 "CSN_ID"               text      ,
 "FLO_MEAS_NAME"        text      ,
 "DISP_NAME"            text      ,
 "FLO_MEAS_ID"          text      ,
 "ROW_TYPE"             text      ,
 "INTAKE_TYPE"          text      ,
 "OUTPUT_TYPE"          text      ,
 "TimeTaken"            timestamp without time zone ,
 "FlowsheetVAlueType"   text      ,
 "ConvertedWeightValue" real      ,
 "Value"                text      ,
 "UNITS"                text      ,
 "TEMPLATE_ID"          text      ,
 "TEMPLATE_NAME"        text      ,
 "TEMPLATE_DISP_NAME"   text      ,
 "ldaDescription"       text      ,
 "PROPERTIES_DISPLAY"   text      ,
 "LDAFLOMEASID"         text      ,
 "LDAFLOMEASNAME"       text      ,
 "LDAGRPDISPNAME"       text
);
\copy hcgh_1117_0118."FlowsheetValue-LDA" from '/home/ubuntu/zad/mnt/clarity_1yflt_lda.clarity_1y' with csv delimiter as E'\t' NULL 'NULL';
create index flt_lda_idx_name on hcgh_1117_0118."FlowsheetValue-LDA" ("FLO_MEAS_NAME");
create index flt_lda_idx_id on hcgh_1117_0118."FlowsheetValue-LDA" ("FLO_MEAS_ID");

drop index if exists flt_idx_name;
drop index if exists flt_idx_id;
drop table if exists hcgh_1117_0118."FlowsheetValue";
create table hcgh_1117_0118."FlowsheetValue"
(
 "CSN_ID"               text      ,
 "FLO_MEAS_NAME"        text      ,
 "DISP_NAME"            text      ,
 "FLO_MEAS_ID"          text      ,
 "ROW_TYPE"             text      ,
 "INTAKE_TYPE"          text      ,
 "OUTPUT_TYPE"          text      ,
 "TimeTaken"            timestamp without time zone,
 "FlowsheetVAlueType"   text      ,
 "ConvertedWeightValue" real      ,
 "Value"                text      ,
 "UNITS"                text      ,
 "TEMPLATE_ID"          text      ,
 "TEMPLATE_NAME"        text      ,
 "TEMPLATE_DISP_NAME"   text
);
\copy hcgh_1117_0118."FlowsheetValue" from '/home/ubuntu/zad/mnt/clarity_1yflt.clarity_1y' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';
create index flt_idx_name on hcgh_1117_0118."FlowsheetValue" ("FLO_MEAS_NAME");
create index flt_idx_id on hcgh_1117_0118."FlowsheetValue" ("FLO_MEAS_ID");

drop index if exists flt_643_idx_name;
drop index if exists flt_643_idx_id;
drop table if exists hcgh_1117_0118."FlowsheetValue_643";
create table hcgh_1117_0118."FlowsheetValue_643"
(
 "CSN_ID"               text      ,
 "FLO_MEAS_NAME"        text      ,
 "DISP_NAME"            text      ,
 "FLO_MEAS_ID"          text      ,
 "ROW_TYPE"             text      ,
 "INTAKE_TYPE"          text      ,
 "OUTPUT_TYPE"          text      ,
 "TimeTaken"            timestamp without time zone,
 "FlowsheetVAlueType"   text      ,
 "ConvertedWeightValue" real      ,
 "Value"                text      ,
 "UNITS"                text      ,
 "TEMPLATE_ID"          text      ,
 "TEMPLATE_NAME"        text      ,
 "TEMPLATE_DISP_NAME"   text
);
\copy hcgh_1117_0118."FlowsheetValue_643" from '/home/ubuntu/zad/mnt/clarity_1yflt_new.clarity_1y' with csv delimiter as E'\t' NULL 'NULL';
create index flt_643_idx_name on hcgh_1117_0118."FlowsheetValue_643" ("FLO_MEAS_NAME");
create index flt_643_idx_id on hcgh_1117_0118."FlowsheetValue_643" ("FLO_MEAS_ID");

-- start to use clarity_1y format (i.e., delimiter is tab) because there are double quote in the data which makes the csv format hard to import to postgresql
-- remember to remove the last two lines in clarity_1y files before importing to the database

drop index if exists labs_idx_name;
drop index if exists labs_idx_id;
drop table if exists hcgh_1117_0118."Labs";
create table hcgh_1117_0118."Labs"
(
 "CSN_ID"            text      ,
 "COMPONENT_ID" text,
 "BASE_NAME"         text,
 "NAME"              text,
 "EXTERNAL_NAME"     text,
 "RESULT_TIME"       timestamp without time zone,
 "REFERENCE_UNIT"    text,
 "ResultValue"       text      ,
 "COMPONENT_COMMENT" text,
 "ORDER_PROC_ID"     text
);
\copy hcgh_1117_0118."Labs" from '/home/ubuntu/zad/mnt/clarity_1ylabs.clarity_1y' with NULL 'NULL' csv delimiter as E'\t';
create index labs_idx_name on hcgh_1117_0118."Labs" ("BASE_NAME");
create index labs_idx_id on hcgh_1117_0118."Labs" ("COMPONENT_ID");

drop index if exists labs_643_idx_name;
drop index if exists labs_643_idx_id;
drop table if exists hcgh_1117_0118."Labs_643";
create table hcgh_1117_0118."Labs_643"
(
 "CSN_ID"            text      ,
 "COMPONENT_ID" text,
 "BASE_NAME"         text,
 "NAME"              text,
 "EXTERNAL_NAME"     text,
 "RESULT_TIME"       timestamp without time zone,
 "REFERENCE_UNIT"    text,
 "ResultValue"       text      ,
 "COMPONENT_COMMENT" text,
 "ORDER_PROC_ID"     text
);
\copy hcgh_1117_0118."Labs_643" from '/home/ubuntu/zad/mnt/clarity_1ylabs_new.clarity_1y' with NULL 'NULL' csv delimiter as E'\t';
create index labs_643_idx_name on hcgh_1117_0118."Labs_643" ("BASE_NAME");
create index labs_643_idx_id on hcgh_1117_0118."Labs_643" ("COMPONENT_ID");

drop index if exists ldas_idx_name;
drop table if exists hcgh_1117_0118."LDAs";
create table hcgh_1117_0118."LDAs"
(
 "PAT_ID"              text,
 "PLACEMENT_INSTANT"   timestamp with    time zone,
 "FLO_MEAS_NAME"       text,
 "DISP_NAME"           text,
 "PROPERTIES_DISPLAY"  text,
 "SITE"                text,
 "REMOVAL_DTTM"        timestamp with    time zone
);
\copy hcgh_1117_0118."LDAs" from '/home/ubuntu/zad/mnt/clarity_1ylda.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ldas_idx_name on hcgh_1117_0118."LDAs" ("FLO_MEAS_NAME");

drop index if exists mar_idx_name;
drop index if exists mar_idx_id;
drop table if exists hcgh_1117_0118."MedicationAdministration";
create table hcgh_1117_0118."MedicationAdministration"
(
 "CSN_ID"             text,
 "display_name"       text,
 "MEDICATION_ID"      text,
 "Thera_Class_c"      text,
 "pharm_class_c"      text,
 "ORDER_INST"         timestamp without time zone,
 "TimeActionTaken"    timestamp without time zone,
 "ActionTaken"        text,
 "MAR_ORIG_DUE_TM"    timestamp without time zone,
 "SCHEDULED_TIME"     timestamp without time zone,
 "MedRoute"           text,
 "Dose"               text,
 "MedUnit"            text,
 "AdminSite"          text,
 "INFUSION_RATE"      text,
 "MAR_INF_RATE_UNIT"  text,
 "mar_duration"       text,
 "MAR_DURATION_UNIT"  text,
 "MIN_DISCRETE_DOSE"  text,
 "MAX_DISCRETE_DOSE"  text
);
\copy hcgh_1117_0118."MedicationAdministration" from '/home/ubuntu/zad/mnt/clarity_1ymar.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index mar_idx_name on hcgh_1117_0118."MedicationAdministration" ("display_name");
create index mar_idx_id on hcgh_1117_0118."MedicationAdministration" ("MEDICATION_ID");

drop index if exists hist_idx;
drop table if exists hcgh_1117_0118."MedicalHistory";
create table hcgh_1117_0118."MedicalHistory"
(
 "CSN_ID"               text,
 "PATIENTID"            text,
 "DEPARTMENTID"         text,
 "diagName"             text,
 "Code"                 text,
 "ICD-9 Code category"  text,
 "COMMENTS"             text,
 "Annotation"           text,
 "Medical_Hx_Date"      text,
 "ENC_Date"             timestamp with time zone
);
\copy hcgh_1117_0118."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity_1yhist.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index hist_idx on hcgh_1117_0118."MedicalHistory" ("Code");

drop table if exists hcgh_1117_0118."Notes";
create table hcgh_1117_0118."Notes"
(
 "CSN_ID"               text,
 "NOTE_ID"              text,
 "AuthorType"           text,
 "NoteType"             text,
 "CREATE_INSTANT_DTTM"  timestamp without time zone,
 "line"                 integer,
 "NOTE_TEXT"            text,
 "CONTACT_DATE_REAL"    real,
 "NoteStatus"           text,
 "SPEC_NOTE_TIME_DTTM"  timestamp without time zone ,
 "ENTRY_ISTANT_DTTM"    timestamp without time zone
);
\copy hcgh_1117_0118."Notes" from '/home/ubuntu/zad/mnt/clarity_1ynote.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists hcgh_1117_0118."OrderMed";
create table hcgh_1117_0118."OrderMed"
(
 "CSN_ID"            text                       ,
 "display_name"      text     ,
 "ORDER_INST"        timestamp without time zone,
 "MedRoute"          text     ,
 "Dose"              text     ,
 "MedUnit"           text     ,
 "MIN_DISCRETE_DOSE" real                       ,
 "MAX_DISCRETE_DOSE" real
);
\copy hcgh_1117_0118."OrderMed" from '/home/ubuntu/zad/mnt/clarity_1yordermed.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists hcgh_1117_0118."OrderMedHome";
create table hcgh_1117_0118."OrderMedHome"
(
 "CSN_ID"            text                       ,
 "display_name"      text     ,
 "ORDER_INST"        timestamp without time zone,
 "MedRoute"          text     ,
 "Dose"              text     ,
 "MedUnit"           text     ,
 "MIN_DISCRETE_DOSE" real                       ,
 "MAX_DISCRETE_DOSE" real
);
\copy hcgh_1117_0118."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity_1yordermed_home.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop index if exists op_idx_name;
drop index if exists op_idx_id;
drop index if exists op_idx_cat;
drop table if exists hcgh_1117_0118."OrderProcs";
create table hcgh_1117_0118."OrderProcs"
(
 "CSN_ID"          text                        ,
 "OrderProcId"     text                        ,
 "display_name"    text      ,
 "proc_name"       text      ,
 "proc_cat_name"   text      ,
 "FrequencyOfOrder" text      ,
 "ORDER_TIME"      timestamp without time zone ,
 "RESULT_TIME"     timestamp without time zone ,
 "ParentOrderTime"  timestamp without time zone ,
 "PROC_START_TIME"  timestamp without time zone ,
 "PROC_ENDING_TIME" timestamp without time zone ,
 "ParentStartTime"  timestamp without time zone ,
 "ParentEndingTime" timestamp without time zone ,
 "OrderStatus"     text,
 "LabStatus"     text,
 "SPECIMN_TAKEN_TIME"  timestamp without time zone
);
\copy hcgh_1117_0118."OrderProcs" from '/home/ubuntu/zad/mnt/clarity_1yorderproc.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_idx_name on hcgh_1117_0118."OrderProcs" ("display_name");
create index op_idx_id on hcgh_1117_0118."OrderProcs" ("OrderProcId");
create index op_idx_cat on hcgh_1117_0118."OrderProcs" ("proc_cat_name");

drop index if exists op_img_idx_name;
drop index if exists op_img_idx_id;
drop index if exists op_img_idx_cat;
drop table if exists hcgh_1117_0118."OrderProcsImage";
create table hcgh_1117_0118."OrderProcsImage"
(
 "CSN_ID"            text                       ,
 "OrderProcId"       text                       ,
 "display_name"      text     ,
 "proc_name"         text     ,
 "proc_cat_name"     text     ,
 "ORDER_TIME"        timestamp without time zone,
 "RESULT_TIME"       timestamp without time zone,
 "PROC_START_TIME"   timestamp without time zone,
 "PROC_ENDING_TIME"  timestamp without time zone,
 "OrderStatus"       text     ,
 "LabStatus"     text,
 "LINE"              integer                    ,
 "NOTE_TEXT"         text
);
\copy hcgh_1117_0118."OrderProcsImage" from '/home/ubuntu/zad/mnt/clarity_1yorderproc_img.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_img_idx_name on hcgh_1117_0118."OrderProcsImage" ("display_name");
create index op_img_idx_id on hcgh_1117_0118."OrderProcsImage" ("OrderProcId");
create index op_img_idx_cat on hcgh_1117_0118."OrderProcsImage" ("proc_cat_name");

drop index if exists op_643_idx_name;
drop index if exists op_643_idx_id;
drop index if exists op_643_idx_cat;
drop table if exists hcgh_1117_0118."OrderProcs_643";
create table hcgh_1117_0118."OrderProcs_643"
(
 "CSN_ID"          text                        ,
 "OrderProcId"     text                        ,
 "INSTNTD_ORDER_ID" text,
 "parent_order_id" text,
 "chng_order_Proc_id" text,
 "display_name"    text      ,
 "proc_name"       text      ,
 "proc_cat_name"   text      ,
 "FrequencyOfOrder" text      ,
 "ORDER_TIME"      timestamp without time zone ,
 "RESULT_TIME"     timestamp without time zone ,
 "ParentOrderTime"  timestamp without time zone ,
 "PROC_START_TIME"  timestamp without time zone ,
 "PROC_ENDING_TIME" timestamp without time zone ,
 "ParentStartTime"  timestamp without time zone ,
 "ParentEndingTime" timestamp without time zone ,
 "OrderStatus"     text,
 "LabStatus"     text,
 order_id     text,
 line       text,
 ord_quest_id       text,
 IS_ANSWR_BYPROC_YN         boolean,
 ord_quest_resp     text,
 quest_name         text,
 question       text,
 comment        text
);
\copy hcgh_1117_0118."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity_1yorderproc_new.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_643_idx_name on hcgh_1117_0118."OrderProcs_643" ("display_name");
create index op_643_idx_id on hcgh_1117_0118."OrderProcs_643" ("OrderProcId");
create index op_643_idx_cat on hcgh_1117_0118."OrderProcs_643" ("proc_cat_name");

drop index if exists prob_idx_name;
drop index if exists prob_idx_code;
drop table if exists hcgh_1117_0118."ProblemList";
create table hcgh_1117_0118."ProblemList"
(
 pat_id              text                       ,
 csn_id              text                       ,
 departmentid           text     ,
 firstdocumented     timestamp without time zone,
 resolveddate        timestamp without time zone,
 problemstatus          text     ,
 hospitaldiagnosis   integer                    ,
 presentonadmission     text     ,
 chronic             integer                    ,
 diagname               text     ,
 code                   text     ,
 codecategory           text
);
\copy hcgh_1117_0118."ProblemList" from '/home/ubuntu/zad/mnt/clarity_1yprob.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index prob_idx_name on hcgh_1117_0118."ProblemList" ("diagname");
create index prob_idx_code on hcgh_1117_0118."ProblemList" ("code");


drop index if exists cc_idx_id;
drop index if exists cc_idx_name;
drop index if exists cc_idx_display;
drop table if exists hcgh_1117_0118."ChiefComplaint";
create table hcgh_1117_0118."ChiefComplaint"
(
 csn_id              text
 ,LINE               int
 ,CONTACT_DATE       date
 ,ENC_REASON_ID      int
 ,ENC_REASON_NAME    text
 ,DISPLAY_TEXT       text
 ,COMMENTS           text
);
\copy hcgh_1117_0118."ChiefComplaint" from '/home/ubuntu/zad/mnt/clarity_1ychief_complaint.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index cc_idx_id on hcgh_1117_0118."ChiefComplaint" ("enc_reason_id");
create index cc_idx_name on hcgh_1117_0118."ChiefComplaint" ("enc_reason_name");
create index cc_idx_display on hcgh_1117_0118."ChiefComplaint" ("display_text");


drop index if exists ede_idx_id;
drop index if exists ede_idx_event_id;
drop index if exists ede_idx_event_disp_name;
drop table if exists hcgh_1117_0118."EdEvents";
create table hcgh_1117_0118."EdEvents"
(
 CSN_ID                text
 ,EVENT_ID             bigint
 ,LINE                 int
 ,EVENT_DISPLAY_NAME   text
 ,EVENT_TIME           timestamp without time zone
 ,EVENT_RECORD_TIME    timestamp without time zone
);
\copy hcgh_1117_0118."EdEvents" from '/home/ubuntu/zad/mnt/clarity_1yed_events.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ede_idx_id on hcgh_1117_0118."EdEvents" ("csn_id");
create index ede_idx_event_id on hcgh_1117_0118."EdEvents" ("event_id");
create index ede_idx_event_disp_name on hcgh_1117_0118."EdEvents" ("event_display_name");

drop index if exists fd_idx_id;
drop index if exists fd_idx_icd9;
drop index if exists fd_idx_icd10;
create table hcgh_1117_0118."FinalDiagnosis"
(
  CSN_ID    text,
  line      int,
  dx_id     int,
  icd9      text,
  icd10     text
);
\copy hcgh_1117_0118."FinalDiagnosis" from '/home/ubuntu/zad/mnt/clarity_1yfinal_dx.clarity_1y' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index fd_idx_id on hcgh_1117_0118."FinalDiagnosis"(csn_id);
create index fd_idx_icd9 on hcgh_1117_0118."FinalDiagnosis"(icd9);
create index fd_idx_icd10 on hcgh_1117_0118."FinalDiagnosis"(icd10);

drop table if exists hcgh_1117_0118.flowsheet_dict;
create table hcgh_1117_0118.flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text,
 DISP_NAME text
 );
\copy hcgh_1117_0118.flowsheet_dict from '/home/ubuntu/zad/mnt/clarity_1yflowsheet_dict.clarity_1y' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists hcgh_1117_0118.lab_dict;
create table hcgh_1117_0118.lab_dict
(
 component_id text,
 name text,
 base_name text,
 external_name text
 );
\copy hcgh_1117_0118.lab_dict from '/home/ubuntu/zad/mnt/clarity_1ylab_dict.clarity_1y' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists hcgh_1117_0118.lab_proc_dict;
create table hcgh_1117_0118.lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy hcgh_1117_0118.lab_proc_dict from '/home/ubuntu/zad/mnt/clarity_1ylab_proc.clarity_1y' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists hcgh_1117_0118.med_dict;
create table hcgh_1117_0118.med_dict
(
 MEDICATION_ID text,
 name text,
 GENERIC_NAME text,
 STRENGTH text,
 form text,
 route text,
 PHARM_CLASS_C int,
 pharm_class_name text,
 pharm_class_title text,
 THERA_CLASS_C int,
 threa_class_name text,
 threa_class_title text
 );
\copy hcgh_1117_0118.med_dict from '/home/ubuntu/zad/mnt/clarity_1ymed_dict.clarity_1y' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';