CREATE SCHEMA IF NOT EXISTS clarity_4y;

drop table if exists clarity_4y."Demographics";
create table clarity_4y."Demographics"
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
\copy clarity_4y."Demographics" from '/home/ubuntu/zad/mnt/clarity-4y/demo.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Demographics" from '/home/ubuntu/zad/mnt/clarity-4y/demo.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Demographics" from '/home/ubuntu/zad/mnt/clarity-4y/demo.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Demographics" from '/home/ubuntu/zad/mnt/clarity-4y/demo.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes


drop table if exists clarity_4y."ADT_Feed";
create table clarity_4y."ADT_Feed"
(
"CSN_ID"    text,
"EventType"  text,
"effective_time"    text,
"PatientClassAtEvent"  text,
"DEPARTMENT_NAME"   text,
"ROOM_NAME"    text
);
\copy clarity_4y."ADT_Feed" from '/home/ubuntu/zad/mnt/clarity-4y/adt.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ADT_Feed" from '/home/ubuntu/zad/mnt/clarity-4y/adt.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ADT_Feed" from '/home/ubuntu/zad/mnt/clarity-4y/adt.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ADT_Feed" from '/home/ubuntu/zad/mnt/clarity-4y/adt.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists clarity_4y."Diagnoses";
drop index if exists diag_idx_code;
drop index if exists diag_idx_name;
create table clarity_4y."Diagnoses"
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
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index diag_idx_code on clarity_4y."Diagnoses" ("Code");
create index diag_idx_name on clarity_4y."Diagnoses" ("diagName");

drop index if exists flt_lda_idx_name;
drop index if exists flt_lda_idx_id;
drop table if exists clarity_4y."FlowsheetValue-LDA";
create table clarity_4y."FlowsheetValue-LDA"
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
\copy clarity_4y."FlowsheetValue-LDA" from '/home/ubuntu/zad/mnt/clarity-4y/flt_lda.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue-LDA" from '/home/ubuntu/zad/mnt/clarity-4y/flt_lda.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue-LDA" from '/home/ubuntu/zad/mnt/clarity-4y/flt_lda.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue-LDA" from '/home/ubuntu/zad/mnt/clarity-4y/flt_lda.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index flt_lda_idx_name on clarity_4y."FlowsheetValue-LDA" ("FLO_MEAS_NAME");
create index flt_lda_idx_id on clarity_4y."FlowsheetValue-LDA" ("FLO_MEAS_ID");

drop index if exists flt_idx_name;
drop index if exists flt_idx_id;
drop table if exists clarity_4y."FlowsheetValue";
create table clarity_4y."FlowsheetValue"
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
\copy clarity_4y."FlowsheetValue" from '/home/ubuntu/zad/mnt/clarity-4y/flt.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue" from '/home/ubuntu/zad/mnt/clarity-4y/flt.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue" from '/home/ubuntu/zad/mnt/clarity-4y/flt.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue" from '/home/ubuntu/zad/mnt/clarity-4y/flt.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index flt_idx_name on clarity_4y."FlowsheetValue" ("FLO_MEAS_NAME");
create index flt_idx_id on clarity_4y."FlowsheetValue" ("FLO_MEAS_ID");

drop index if exists flt_643_idx_name;
drop index if exists flt_643_idx_id;
drop table if exists clarity_4y."FlowsheetValue_643";
create table clarity_4y."FlowsheetValue_643"
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
\copy clarity_4y."FlowsheetValue_643" from '/home/ubuntu/zad/mnt/clarity-4y/flt_new.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue_643" from '/home/ubuntu/zad/mnt/clarity-4y/flt_new.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue_643" from '/home/ubuntu/zad/mnt/clarity-4y/flt_new.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FlowsheetValue_643" from '/home/ubuntu/zad/mnt/clarity-4y/flt_new.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index flt_643_idx_name on clarity_4y."FlowsheetValue_643" ("FLO_MEAS_NAME");
create index flt_643_idx_id on clarity_4y."FlowsheetValue_643" ("FLO_MEAS_ID");

-- start to use rpt format (i.e., delimiter is tab) because there are double quote in the data which makes the csv format hard to import to postgresql
-- remember to remove the last two lines in rpt files before importing to the database

drop index if exists labs_idx_name;
drop index if exists labs_idx_id;
drop table if exists clarity_4y."Labs";
create table clarity_4y."Labs"
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
\copy clarity_4y."Labs" from '/home/ubuntu/zad/mnt/clarity-4y/labs.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Labs" from '/home/ubuntu/zad/mnt/clarity-4y/labs.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Labs" from '/home/ubuntu/zad/mnt/clarity-4y/labs.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Labs" from '/home/ubuntu/zad/mnt/clarity-4y/labs.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index labs_idx_name on clarity_4y."Labs" ("BASE_NAME");
create index labs_idx_id on clarity_4y."Labs" ("COMPONENT_ID");

drop index if exists labs_643_idx_name;
drop index if exists labs_643_idx_id;
drop table if exists clarity_4y."Labs_643";
create table clarity_4y."Labs_643"
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
\copy clarity_4y."Labs_643" from '/home/ubuntu/zad/mnt/clarity-4y/labs_new.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Labs_643" from '/home/ubuntu/zad/mnt/clarity-4y/labs_new.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Labs_643" from '/home/ubuntu/zad/mnt/clarity-4y/labs_new.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Labs_643" from '/home/ubuntu/zad/mnt/clarity-4y/labs_new.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index labs_643_idx_name on clarity_4y."Labs_643" ("BASE_NAME");
create index labs_643_idx_id on clarity_4y."Labs_643" ("COMPONENT_ID");

drop index if exists ldas_idx_name;
drop table if exists clarity_4y."LDAs";
create table clarity_4y."LDAs"
(
 "PAT_ID"              text,
 "PLACEMENT_INSTANT"   timestamp with    time zone,
 "FLO_MEAS_NAME"       text,
 "DISP_NAME"           text,
 "PROPERTIES_DISPLAY"  text,
 "SITE"                text,
 "REMOVAL_DTTM"        timestamp with    time zone
);
\copy clarity_4y."LDAs" from '/home/ubuntu/zad/mnt/clarity-4y/lda.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."LDAs" from '/home/ubuntu/zad/mnt/clarity-4y/lda.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."LDAs" from '/home/ubuntu/zad/mnt/clarity-4y/lda.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."LDAs" from '/home/ubuntu/zad/mnt/clarity-4y/lda.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ldas_idx_name on clarity_4y."LDAs" ("FLO_MEAS_NAME");

drop index if exists mar_idx_name;
drop index if exists mar_idx_id;
drop table if exists clarity_4y."MedicationAdministration";
create table clarity_4y."MedicationAdministration"
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
\copy clarity_4y."MedicationAdministration" from '/home/ubuntu/zad/mnt/clarity-4y/mar.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."MedicationAdministration" from '/home/ubuntu/zad/mnt/clarity-4y/mar.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."MedicationAdministration" from '/home/ubuntu/zad/mnt/clarity-4y/mar.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."MedicationAdministration" from '/home/ubuntu/zad/mnt/clarity-4y/mar.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index mar_idx_name on clarity_4y."MedicationAdministration" ("display_name");
create index mar_idx_id on clarity_4y."MedicationAdministration" ("MEDICATION_ID");

drop index if exists hist_idx;
drop table if exists clarity_4y."MedicalHistory";
create table clarity_4y."MedicalHistory"
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
\copy clarity_4y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-4y/hist.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-4y/hist.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-4y/hist.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-4y/hist.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index hist_idx on clarity_4y."MedicalHistory" ("Code");

drop table if exists clarity_4y."Notes";
create table clarity_4y."Notes"
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
\copy clarity_4y."Notes" from '/home/ubuntu/zad/mnt/clarity-4y/note.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Notes" from '/home/ubuntu/zad/mnt/clarity-4y/note.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Notes" from '/home/ubuntu/zad/mnt/clarity-4y/note.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."Notes" from '/home/ubuntu/zad/mnt/clarity-4y/note.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists clarity_4y."OrderMed";
create table clarity_4y."OrderMed"
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
\copy clarity_4y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists clarity_4y."OrderMedHome";
create table clarity_4y."OrderMedHome"
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
\copy clarity_4y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed_home.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed_home.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed_home.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-4y/ordermed_home.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop index if exists op_idx_name;
drop index if exists op_idx_id;
drop index if exists op_idx_cat;
drop table if exists clarity_4y."OrderProcs";
create table clarity_4y."OrderProcs"
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
\copy clarity_4y."OrderProcs" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcs" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcs" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcs" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_idx_name on clarity_4y."OrderProcs" ("display_name");
create index op_idx_id on clarity_4y."OrderProcs" ("OrderProcId");
create index op_idx_cat on clarity_4y."OrderProcs" ("proc_cat_name");

drop index if exists op_img_idx_name;
drop index if exists op_img_idx_id;
drop index if exists op_img_idx_cat;
drop table if exists clarity_4y."OrderProcsImage";
create table clarity_4y."OrderProcsImage"
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
\copy clarity_4y."OrderProcsImage" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_img.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcsImage" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_img.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcsImage" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_img.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcsImage" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_img.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_img_idx_name on clarity_4y."OrderProcsImage" ("display_name");
create index op_img_idx_id on clarity_4y."OrderProcsImage" ("OrderProcId");
create index op_img_idx_cat on clarity_4y."OrderProcsImage" ("proc_cat_name");

drop index if exists op_643_idx_name;
drop index if exists op_643_idx_id;
drop index if exists op_643_idx_cat;
drop table if exists clarity_4y."OrderProcs_643";
create table clarity_4y."OrderProcs_643"
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
\copy clarity_4y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_new.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_new.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_new.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-4y/orderproc_new.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_643_idx_name on clarity_4y."OrderProcs_643" ("display_name");
create index op_643_idx_id on clarity_4y."OrderProcs_643" ("OrderProcId");
create index op_643_idx_cat on clarity_4y."OrderProcs_643" ("proc_cat_name");

drop index if exists prob_idx_name;
drop index if exists prob_idx_code;
drop table if exists clarity_4y."ProblemList";
create table clarity_4y."ProblemList"
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
\copy clarity_4y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-4y/prob.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-4y/prob.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-4y/prob.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-4y/prob.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index prob_idx_name on clarity_4y."ProblemList" ("diagname");
create index prob_idx_code on clarity_4y."ProblemList" ("code");


drop index if exists cc_idx_id;
drop index if exists cc_idx_name;
drop index if exists cc_idx_display;
drop table if exists clarity_4y."ChiefComplaint";
create table clarity_4y."ChiefComplaint"
(
 csn_id              text
 ,LINE               int
 ,CONTACT_DATE       date
 ,ENC_REASON_ID      int
 ,ENC_REASON_NAME    text
 ,DISPLAY_TEXT       text
 ,COMMENTS           text
);
\copy clarity_4y."ChiefComplaint" from '/home/ubuntu/zad/mnt/clarity-4y/chief_complaint.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ChiefComplaint" from '/home/ubuntu/zad/mnt/clarity-4y/chief_complaint.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ChiefComplaint" from '/home/ubuntu/zad/mnt/clarity-4y/chief_complaint.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."ChiefComplaint" from '/home/ubuntu/zad/mnt/clarity-4y/chief_complaint.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index cc_idx_id on clarity_4y."ChiefComplaint" ("enc_reason_id");
create index cc_idx_name on clarity_4y."ChiefComplaint" ("enc_reason_name");
create index cc_idx_display on clarity_4y."ChiefComplaint" ("display_text");


drop index if exists ede_idx_id;
drop index if exists ede_idx_event_id;
drop index if exists ede_idx_event_disp_name;
drop table if exists clarity_4y."EdEvents";
create table clarity_4y."EdEvents"
(
 CSN_ID                text
 ,EVENT_ID             bigint
 ,LINE                 int
 ,EVENT_DISPLAY_NAME   text
 ,EVENT_TIME           timestamp without time zone
 ,EVENT_RECORD_TIME    timestamp without time zone
);
\copy clarity_4y."EdEvents" from '/home/ubuntu/zad/mnt/clarity-4y/ed_events.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."EdEvents" from '/home/ubuntu/zad/mnt/clarity-4y/ed_events.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."EdEvents" from '/home/ubuntu/zad/mnt/clarity-4y/ed_events.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."EdEvents" from '/home/ubuntu/zad/mnt/clarity-4y/ed_events.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ede_idx_id on clarity_4y."EdEvents" ("csn_id");
create index ede_idx_event_id on clarity_4y."EdEvents" ("event_id");
create index ede_idx_event_disp_name on clarity_4y."EdEvents" ("event_display_name");

drop index if exists fd_idx_id;
drop index if exists fd_idx_icd9;
drop index if exists fd_idx_icd10;
create table clarity_4y."FinalDiagnosis"
(
  CSN_ID    text,
  line      int,
  dx_id     int,
  icd9      text,
  icd10     text
);
\copy clarity_4y."FinalDiagnosis" from '/home/ubuntu/zad/mnt/clarity-4y/final_dx.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FinalDiagnosis" from '/home/ubuntu/zad/mnt/clarity-4y/final_dx.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FinalDiagnosis" from '/home/ubuntu/zad/mnt/clarity-4y/final_dx.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."FinalDiagnosis" from '/home/ubuntu/zad/mnt/clarity-4y/final_dx.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index fd_idx_id on clarity_4y."FinalDiagnosis"(csn_id);
create index fd_idx_icd9 on clarity_4y."FinalDiagnosis"(icd9);
create index fd_idx_icd10 on clarity_4y."FinalDiagnosis"(icd10);

drop table if exists clarity_4y.flowsheet_dict;
create table clarity_4y.flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text,
 DISP_NAME text
 );
\copy clarity_4y.flowsheet_dict from '/home/ubuntu/zad/mnt/clarity-4y/flowsheet_dict.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists clarity_4y.lab_dict;
create table clarity_4y.lab_dict
(
 component_id text,
 name text,
 base_name text,
 external_name text
 );
\copy clarity_4y.lab_dict from '/home/ubuntu/zad/mnt/clarity-4y/lab_dict.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists clarity_4y.lab_proc_dict;
create table clarity_4y.lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy clarity_4y.lab_proc_dict from '/home/ubuntu/zad/mnt/clarity-4y/lab_proc.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists clarity_4y.med_dict;
create table clarity_4y.med_dict
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
\copy clarity_4y.med_dict from '/home/ubuntu/zad/mnt/clarity-4y/med_dict.rpt' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';
