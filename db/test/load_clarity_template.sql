CREATE SCHEMA IF NOT EXISTS {workspace};

drop table if exists {workspace}."Demographics";
create table {workspace}."Demographics"
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
\copy {workspace}."Demographics" from '{folder}demo.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Demographics" from '{folder}demo.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Demographics" from '{folder}demo.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Demographics" from '{folder}demo.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes


drop table if exists {workspace}."ADT_Feed";
create table {workspace}."ADT_Feed"
(
"CSN_ID"    text,
"EventType"  text,
"effective_time"    text,
"PatientClassAtEvent"  text,
"DEPARTMENT_NAME"   text,
"ROOM_NAME"    text
);
\copy {workspace}."ADT_Feed" from '{folder}adt.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ADT_Feed" from '{folder}adt.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ADT_Feed" from '{folder}adt.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ADT_Feed" from '{folder}adt.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists {workspace}."Diagnoses";
drop index if exists diag_idx_code;
drop index if exists diag_idx_name;
create table {workspace}."Diagnoses"
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
\copy {workspace}."Diagnoses" from '{folder}diag.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Diagnoses" from '{folder}diag.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Diagnoses" from '{folder}diag.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Diagnoses" from '{folder}diag.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index diag_idx_code on {workspace}."Diagnoses" ("Code");
create index diag_idx_name on {workspace}."Diagnoses" ("diagName");

drop index if exists flt_lda_idx_name;
drop index if exists flt_lda_idx_id;
drop table if exists {workspace}."FlowsheetValue-LDA";
create table {workspace}."FlowsheetValue-LDA"
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
\copy {workspace}."FlowsheetValue-LDA" from '{folder}flt_lda.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue-LDA" from '{folder}flt_lda.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue-LDA" from '{folder}flt_lda.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue-LDA" from '{folder}flt_lda.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index flt_lda_idx_name on {workspace}."FlowsheetValue-LDA" ("FLO_MEAS_NAME");
create index flt_lda_idx_id on {workspace}."FlowsheetValue-LDA" ("FLO_MEAS_ID");

drop index if exists flt_idx_name;
drop index if exists flt_idx_id;
drop table if exists {workspace}."FlowsheetValue";
create table {workspace}."FlowsheetValue"
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
\copy {workspace}."FlowsheetValue" from '{folder}flt.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue" from '{folder}flt.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue" from '{folder}flt.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue" from '{folder}flt.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index flt_idx_name on {workspace}."FlowsheetValue" ("FLO_MEAS_NAME");
create index flt_idx_id on {workspace}."FlowsheetValue" ("FLO_MEAS_ID");

drop index if exists flt_643_idx_name;
drop index if exists flt_643_idx_id;
drop table if exists {workspace}."FlowsheetValue_643";
create table {workspace}."FlowsheetValue_643"
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
\copy {workspace}."FlowsheetValue_643" from '{folder}flt_new.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue_643" from '{folder}flt_new.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue_643" from '{folder}flt_new.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FlowsheetValue_643" from '{folder}flt_new.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index flt_643_idx_name on {workspace}."FlowsheetValue_643" ("FLO_MEAS_NAME");
create index flt_643_idx_id on {workspace}."FlowsheetValue_643" ("FLO_MEAS_ID");

-- start to use {ext} format (i.e., delimiter is tab) because there are double quote in the data which makes the csv format hard to import to postgresql
-- remember to remove the last two lines in {ext} files before importing to the database

drop index if exists labs_idx_name;
drop index if exists labs_idx_id;
drop table if exists {workspace}."Labs";
create table {workspace}."Labs"
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
\copy {workspace}."Labs" from '{folder}labs.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Labs" from '{folder}labs.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Labs" from '{folder}labs.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Labs" from '{folder}labs.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index labs_idx_name on {workspace}."Labs" ("BASE_NAME");
create index labs_idx_id on {workspace}."Labs" ("COMPONENT_ID");

drop index if exists labs_643_idx_name;
drop index if exists labs_643_idx_id;
drop table if exists {workspace}."Labs_643";
create table {workspace}."Labs_643"
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
\copy {workspace}."Labs_643" from '{folder}labs_new.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Labs_643" from '{folder}labs_new.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Labs_643" from '{folder}labs_new.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Labs_643" from '{folder}labs_new.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index labs_643_idx_name on {workspace}."Labs_643" ("BASE_NAME");
create index labs_643_idx_id on {workspace}."Labs_643" ("COMPONENT_ID");

drop index if exists ldas_idx_name;
drop table if exists {workspace}."LDAs";
create table {workspace}."LDAs"
(
 "PAT_ID"              text,
 "PLACEMENT_INSTANT"   timestamp with    time zone,
 "FLO_MEAS_NAME"       text,
 "DISP_NAME"           text,
 "PROPERTIES_DISPLAY"  text,
 "SITE"                text,
 "REMOVAL_DTTM"        timestamp with    time zone
);
\copy {workspace}."LDAs" from '{folder}lda.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."LDAs" from '{folder}lda.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."LDAs" from '{folder}lda.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."LDAs" from '{folder}lda.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ldas_idx_name on {workspace}."LDAs" ("FLO_MEAS_NAME");

drop index if exists mar_idx_name;
drop index if exists mar_idx_id;
drop table if exists {workspace}."MedicationAdministration";
create table {workspace}."MedicationAdministration"
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
\copy {workspace}."MedicationAdministration" from '{folder}mar.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicationAdministration" from '{folder}mar.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicationAdministration" from '{folder}mar.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicationAdministration" from '{folder}mar.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index mar_idx_name on {workspace}."MedicationAdministration" ("display_name");
create index mar_idx_id on {workspace}."MedicationAdministration" ("MEDICATION_ID");

drop index if exists hist_idx;
drop table if exists {workspace}."MedicalHistory";
create table {workspace}."MedicalHistory"
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
\copy {workspace}."MedicalHistory" from '{folder}hist.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicalHistory" from '{folder}hist.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicalHistory" from '{folder}hist.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicalHistory" from '{folder}hist.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index hist_idx on {workspace}."MedicalHistory" ("Code");

drop table if exists {workspace}."Notes";
create table {workspace}."Notes"
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
\copy {workspace}."Notes" from '{folder}note.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Notes" from '{folder}note.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Notes" from '{folder}note.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."Notes" from '{folder}note.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists {workspace}."OrderMed";
create table {workspace}."OrderMed"
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
\copy {workspace}."OrderMed" from '{folder}ordermed.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMed" from '{folder}ordermed.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMed" from '{folder}ordermed.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMed" from '{folder}ordermed.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists {workspace}."OrderMedHome";
create table {workspace}."OrderMedHome"
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
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop index if exists op_idx_name;
drop index if exists op_idx_id;
drop index if exists op_idx_cat;
drop table if exists {workspace}."OrderProcs";
create table {workspace}."OrderProcs"
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
\copy {workspace}."OrderProcs" from '{folder}orderproc.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs" from '{folder}orderproc.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs" from '{folder}orderproc.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs" from '{folder}orderproc.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_idx_name on {workspace}."OrderProcs" ("display_name");
create index op_idx_id on {workspace}."OrderProcs" ("OrderProcId");
create index op_idx_cat on {workspace}."OrderProcs" ("proc_cat_name");

drop index if exists op_img_idx_name;
drop index if exists op_img_idx_id;
drop index if exists op_img_idx_cat;
drop table if exists {workspace}."OrderProcsImage";
create table {workspace}."OrderProcsImage"
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
\copy {workspace}."OrderProcsImage" from '{folder}orderproc_img.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcsImage" from '{folder}orderproc_img.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcsImage" from '{folder}orderproc_img.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcsImage" from '{folder}orderproc_img.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_img_idx_name on {workspace}."OrderProcsImage" ("display_name");
create index op_img_idx_id on {workspace}."OrderProcsImage" ("OrderProcId");
create index op_img_idx_cat on {workspace}."OrderProcsImage" ("proc_cat_name");

drop index if exists op_643_idx_name;
drop index if exists op_643_idx_id;
drop index if exists op_643_idx_cat;
drop table if exists {workspace}."OrderProcs_643";
create table {workspace}."OrderProcs_643"
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
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_643_idx_name on {workspace}."OrderProcs_643" ("display_name");
create index op_643_idx_id on {workspace}."OrderProcs_643" ("OrderProcId");
create index op_643_idx_cat on {workspace}."OrderProcs_643" ("proc_cat_name");

drop index if exists prob_idx_name;
drop index if exists prob_idx_code;
drop table if exists {workspace}."ProblemList";
create table {workspace}."ProblemList"
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
\copy {workspace}."ProblemList" from '{folder}prob.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ProblemList" from '{folder}prob.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ProblemList" from '{folder}prob.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ProblemList" from '{folder}prob.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index prob_idx_name on {workspace}."ProblemList" ("diagname");
create index prob_idx_code on {workspace}."ProblemList" ("code");


drop index if exists cc_idx_id;
drop index if exists cc_idx_name;
drop index if exists cc_idx_display;
drop table if exists {workspace}."ChiefComplaint";
create table {workspace}."ChiefComplaint"
(
 csn_id              text
 ,LINE               int
 ,CONTACT_DATE       date
 ,ENC_REASON_ID      int
 ,ENC_REASON_NAME    text
 ,DISPLAY_TEXT       text
 ,COMMENTS           text
);
\copy {workspace}."ChiefComplaint" from '{folder}chief_complaint.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ChiefComplaint" from '{folder}chief_complaint.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ChiefComplaint" from '{folder}chief_complaint.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ChiefComplaint" from '{folder}chief_complaint.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index cc_idx_id on {workspace}."ChiefComplaint" ("enc_reason_id");
create index cc_idx_name on {workspace}."ChiefComplaint" ("enc_reason_name");
create index cc_idx_display on {workspace}."ChiefComplaint" ("display_text");


drop index if exists ede_idx_id;
drop index if exists ede_idx_event_id;
drop index if exists ede_idx_event_disp_name;
drop table if exists {workspace}."EdEvents";
create table {workspace}."EdEvents"
(
 CSN_ID                text
 ,EVENT_ID             bigint
 ,LINE                 int
 ,EVENT_DISPLAY_NAME   text
 ,EVENT_TIME           timestamp without time zone
 ,EVENT_RECORD_TIME    timestamp without time zone
);
\copy {workspace}."EdEvents" from '{folder}ed_events.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."EdEvents" from '{folder}ed_events.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."EdEvents" from '{folder}ed_events.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."EdEvents" from '{folder}ed_events.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index ede_idx_id on {workspace}."EdEvents" ("csn_id");
create index ede_idx_event_id on {workspace}."EdEvents" ("event_id");
create index ede_idx_event_disp_name on {workspace}."EdEvents" ("event_display_name");

drop index if exists fd_idx_id;
drop index if exists fd_idx_icd9;
drop index if exists fd_idx_icd10;
drop table if exists {workspace}."FinalDiagnosis";
create table {workspace}."FinalDiagnosis"
(
  CSN_ID    text,
  line      int,
  dx_id     int,
  icd9      text,
  icd10     text
);
\copy {workspace}."FinalDiagnosis" from '{folder}final_dx.201402.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FinalDiagnosis" from '{folder}final_dx.201503.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FinalDiagnosis" from '{folder}final_dx.201604.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."FinalDiagnosis" from '{folder}final_dx.201704.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index fd_idx_id on {workspace}."FinalDiagnosis"(csn_id);
create index fd_idx_icd9 on {workspace}."FinalDiagnosis"(icd9);
create index fd_idx_icd10 on {workspace}."FinalDiagnosis"(icd10);

drop table if exists {workspace}.flowsheet_dict;
create table {workspace}.flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text,
 DISP_NAME text
 );
\copy {workspace}.flowsheet_dict from '{folder}flowsheet_dict.{ext}' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists {workspace}.lab_dict;
create table {workspace}.lab_dict
(
 component_id text,
 name text,
 base_name text,
 external_name text
 );
\copy {workspace}.lab_dict from '{folder}lab_dict.{ext}' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists {workspace}.lab_proc_dict;
create table {workspace}.lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy {workspace}.lab_proc_dict from '{folder}lab_proc.{ext}' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists {workspace}.med_dict;
create table {workspace}.med_dict
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
\copy {workspace}.med_dict from '{folder}med_dict.{ext}' with csv delimiter as E'\t' NULL 'NULL' QUOTE E'\b';