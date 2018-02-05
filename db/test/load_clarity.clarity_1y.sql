CREATE SCHEMA IF NOT EXISTS clarity_1y;

drop table if exists clarity_1y."tmp_vent";
create table clarity_1y."tmp_vent"
(
 "CSN_ID"          text                        ,
 "OrderProcId"     text                        ,
 "INSTNTD_ORDER_ID" text,
 "parent_order_id" text,
 "chng_order_Proc_id" text,
 "display_name"    text      ,
 "proc_name"       text      ,
 "proc_cat_name"   text      ,
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
 IS_ANSWR_BYPROC_YN boolean,
 ord_quest_resp     text,
 quest_name         text,
 question       text,
 comment        text,
 "PRIORITY"     text,
 "FrequencyOfOrder" text      ,
 "ORDER_CLASS_NAME" text
);
\copy clarity_1y."tmp_vent" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/vent.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
-- create index op_643_idx_name on clarity_1y."OrderProcs_643" ("display_name");
-- create index op_643_idx_id on clarity_1y."OrderProcs_643" ("OrderProcId");
-- create index op_643_idx_cat on clarity_1y."OrderProcs_643" ("proc_cat_name");







drop table if exists clarity_1y."Demographics";
create table clarity_1y."Demographics"
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
\copy clarity_1y."Demographics" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/demo.rpt' with csv delimiter as E'\t' NULL 'NULL';

drop table if exists clarity_1y."ADT_Feed";
create table clarity_1y."ADT_Feed"
(
"CSN_ID"    text,
"EventType"  text,
"effective_time"    text,
"PatientClassAtEvent"  text,
"DEPARTMENT_NAME"   text,
"ROOM_NAME"    text
);
\copy clarity_1y."ADT_Feed" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/adt.rpt' with csv delimiter as E'\t' NULL 'NULL';

drop table if exists clarity_1y."Diagnoses";
drop index if exists diag_idx_code;
drop index if exists diag_idx_name;
create table clarity_1y."Diagnoses"
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
\copy clarity_1y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/diag.rpt' with csv delimiter as E'\t' NULL 'NULL';
create index diag_idx_code on clarity_1y."Diagnoses" ("Code");
create index diag_idx_name on clarity_1y."Diagnoses" ("diagName");

drop index if exists flt_lda_idx_name;
drop index if exists flt_lda_idx_id;
drop table if exists clarity_1y."FlowsheetValue-LDA";
create table clarity_1y."FlowsheetValue-LDA"
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
\copy clarity_1y."FlowsheetValue-LDA" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/flt_lda.rpt' with csv delimiter as E'\t' NULL 'NULL';
create index flt_lda_idx_name on clarity_1y."FlowsheetValue-LDA" ("FLO_MEAS_NAME");
create index flt_lda_idx_id on clarity_1y."FlowsheetValue-LDA" ("FLO_MEAS_ID");

drop index if exists flt_idx_name;
drop index if exists flt_idx_id;
drop table if exists clarity_1y."FlowsheetValue";
create table clarity_1y."FlowsheetValue"
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
\copy clarity_1y."FlowsheetValue" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/flt.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';
create index flt_idx_name on clarity_1y."FlowsheetValue" ("FLO_MEAS_NAME");
create index flt_idx_id on clarity_1y."FlowsheetValue" ("FLO_MEAS_ID");

drop index if exists flt_643_idx_name;
drop index if exists flt_643_idx_id;
drop table if exists clarity_1y."FlowsheetValue_643";
create table clarity_1y."FlowsheetValue_643"
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
\copy clarity_1y."FlowsheetValue_643" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/flt_new.rpt' with csv delimiter as E'\t' NULL 'NULL';
create index flt_643_idx_name on clarity_1y."FlowsheetValue_643" ("FLO_MEAS_NAME");
create index flt_643_idx_id on clarity_1y."FlowsheetValue_643" ("FLO_MEAS_ID");

-- start to use rpt format (i.e., delimiter is tab) because there are double quote in the data which makes the csv format hard to import to postgresql
-- remember to remove the last two lines in rpt files before importing to the database

drop index if exists labs_idx_name;
drop index if exists labs_idx_id;
drop table if exists clarity_1y."Labs";
create table clarity_1y."Labs"
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
\copy clarity_1y."Labs" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/labs.rpt' with NULL 'NULL' csv delimiter as E'\t';
create index labs_idx_name on clarity_1y."Labs" ("BASE_NAME");
create index labs_idx_id on clarity_1y."Labs" ("COMPONENT_ID");

drop index if exists labs_643_idx_name;
drop index if exists labs_643_idx_id;
drop table if exists clarity_1y."Labs_643";
create table clarity_1y."Labs_643"
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
\copy clarity_1y."Labs_643" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/labs_new.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b';
create index labs_643_idx_name on clarity_1y."Labs_643" ("BASE_NAME");
create index labs_643_idx_id on clarity_1y."Labs_643" ("COMPONENT_ID");

drop index if exists ldas_idx_name;
drop table if exists clarity_1y."LDAs";
create table clarity_1y."LDAs"
(
 "PAT_ID"              text,
 "PLACEMENT_INSTANT"   timestamp with    time zone,
 "FLO_MEAS_NAME"       text,
 "DISP_NAME"           text,
 "PROPERTIES_DISPLAY"  text,
 "SITE"                text,
 "REMOVAL_DTTM"        timestamp with    time zone
);
\copy clarity_1y."LDAs" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/lda.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ldas_idx_name on clarity_1y."LDAs" ("FLO_MEAS_NAME");

drop index if exists mar_idx_name;
drop index if exists mar_idx_id;
drop table if exists clarity_1y."MedicationAdministration";
create table clarity_1y."MedicationAdministration"
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
\copy clarity_1y."MedicationAdministration" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/mar.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index mar_idx_name on clarity_1y."MedicationAdministration" ("display_name");
create index mar_idx_id on clarity_1y."MedicationAdministration" ("MEDICATION_ID");

drop index if exists hist_idx;
drop table if exists clarity_1y."MedicalHistory";
create table clarity_1y."MedicalHistory"
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
\copy clarity_1y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/hist.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index hist_idx on clarity_1y."MedicalHistory" ("Code");

drop table if exists clarity_1y."Notes";
create table clarity_1y."Notes"
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
\copy clarity_1y."Notes" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/note.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists clarity_1y."OrderMed";
create table clarity_1y."OrderMed"
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
\copy clarity_1y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/ordermed.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists clarity_1y."OrderMedHome";
create table clarity_1y."OrderMedHome"
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
\copy clarity_1y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/ordermed_home.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop index if exists op_idx_name;
drop index if exists op_idx_id;
drop index if exists op_idx_cat;
drop table if exists clarity_1y."OrderProcs";
create table clarity_1y."OrderProcs"
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
\copy clarity_1y."OrderProcs" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/orderproc.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_idx_name on clarity_1y."OrderProcs" ("display_name");
create index op_idx_id on clarity_1y."OrderProcs" ("OrderProcId");
create index op_idx_cat on clarity_1y."OrderProcs" ("proc_cat_name");

drop index if exists op_img_idx_name;
drop index if exists op_img_idx_id;
drop index if exists op_img_idx_cat;
drop table if exists clarity_1y."OrderProcsImage";
create table clarity_1y."OrderProcsImage"
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
\copy clarity_1y."OrderProcsImage" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/orderproc_img.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_img_idx_name on clarity_1y."OrderProcsImage" ("display_name");
create index op_img_idx_id on clarity_1y."OrderProcsImage" ("OrderProcId");
create index op_img_idx_cat on clarity_1y."OrderProcsImage" ("proc_cat_name");

drop index if exists op_643_idx_name;
drop index if exists op_643_idx_id;
drop index if exists op_643_idx_cat;
drop table if exists clarity_1y."OrderProcs_643";
create table clarity_1y."OrderProcs_643"
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
 IS_ANSWR_BYPROC_YN boolean,
 ord_quest_resp     text,
 quest_name         text,
 question       text,
 comment        text
);
\copy clarity_1y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/orderproc_new.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_643_idx_name on clarity_1y."OrderProcs_643" ("display_name");
create index op_643_idx_id on clarity_1y."OrderProcs_643" ("OrderProcId");
create index op_643_idx_cat on clarity_1y."OrderProcs_643" ("proc_cat_name");

drop index if exists prob_idx_name;
drop index if exists prob_idx_code;
drop table if exists clarity_1y."ProblemList";
create table clarity_1y."ProblemList"
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
\copy clarity_1y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/prob.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index prob_idx_name on clarity_1y."ProblemList" ("diagname");
create index prob_idx_code on clarity_1y."ProblemList" ("code");


drop index if exists cc_idx_id;
drop index if exists cc_idx_name;
drop index if exists cc_idx_display;
drop table if exists clarity_1y."ChiefComplaint";
create table clarity_1y."ChiefComplaint"
(
 csn_id              text
 ,LINE               int
 ,CONTACT_DATE       date
 ,ENC_REASON_ID      int
 ,ENC_REASON_NAME    text
 ,DISPLAY_TEXT       text
 ,COMMENTS           text
);
\copy clarity_1y."ChiefComplaint" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/chief_complaint.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index cc_idx_id on clarity_1y."ChiefComplaint" ("enc_reason_id");
create index cc_idx_name on clarity_1y."ChiefComplaint" ("enc_reason_name");
create index cc_idx_display on clarity_1y."ChiefComplaint" ("display_text");


drop index if exists ede_idx_id;
drop index if exists ede_idx_event_id;
drop index if exists ede_idx_event_disp_name;
drop table if exists clarity_1y."EdEvents";
create table clarity_1y."EdEvents"
(
 CSN_ID                text
 ,EVENT_ID             bigint
 ,LINE                 int
 ,EVENT_DISPLAY_NAME   text
 ,EVENT_TIME           timestamp without time zone
 ,EVENT_RECORD_TIME    timestamp without time zone
);
\copy clarity_1y."EdEvents" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/ed_events.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ede_idx_id on clarity_1y."EdEvents" ("csn_id");
create index ede_idx_event_id on clarity_1y."EdEvents" ("event_id");
create index ede_idx_event_disp_name on clarity_1y."EdEvents" ("event_display_name");

drop table if exists clarity_1y."FinalDiagnosis";
drop index if exists fd_idx_id;
drop index if exists fd_idx_icd9;
drop index if exists fd_idx_icd10;
create table clarity_1y."FinalDiagnosis"
(
  CSN_ID    text,
  line      int,
  dx_id     int,
  icd9      text,
  icd10     text
);
\copy clarity_1y."FinalDiagnosis" from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/final_dx.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index fd_idx_id on clarity_1y."FinalDiagnosis"(csn_id);
create index fd_idx_icd9 on clarity_1y."FinalDiagnosis"(icd9);
create index fd_idx_icd10 on clarity_1y."FinalDiagnosis"(icd10);

drop table if exists clarity_1y.flowsheet_dict;
create table clarity_1y.flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text,
 DISP_NAME text
 );
\copy clarity_1y.flowsheet_dict from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/flowsheet_dict.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists clarity_1y.lab_dict;
create table clarity_1y.lab_dict
(
 component_id text,
 name text,
 base_name text,
 external_name text
 );
\copy clarity_1y.lab_dict from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/lab_dict.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists clarity_1y.lab_proc_dict;
create table clarity_1y.lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy clarity_1y.lab_proc_dict from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/lab_proc.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists clarity_1y.med_dict;
create table clarity_1y.med_dict
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
\copy clarity_1y.med_dict from '/home/ubuntu/zad/mnt/clarity-1y/01-30-2018/med_dict.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';