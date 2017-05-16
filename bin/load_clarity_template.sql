CREATE SCHEMA IF NOT EXISTS {workspace};

drop table if exists {workspace}."Demographics";
create table {workspace}."Demographics"
(
 "pat_id"                text,
 "ADT_ARRIVAL_TIME"      timestamp with    time zone,
 "ED_DEPARTURE_TIME"     timestamp with    time zone,
 "HOSP_ADMSN_TIME"       timestamp with    time zone,
 "HOSP_DISCH_TIME"       timestamp with    time zone,
 "AgeDuringVisit"        text,
 "Gender"                text not  null,
 "IsEDPatient"           integer not  null,
 "DischargeDepartment"   text,
 "DischargeDisposition"  text,
 "CSN_ID"                text
 );
\copy {workspace}."Demographics" from '{folder}demo.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

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
\copy {workspace}."ADT_Feed" from '{folder}adt.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

drop table if exists {workspace}."Diagnoses";
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
);
\copy {workspace}."Diagnoses" from '{folder}diag.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

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
\copy {workspace}."FlowsheetValue-LDA" from '{folder}flt_lda.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

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
 "ConvertedWeightValue" real      ,
 "Value"                text      ,
 "UNITS"                text      ,
 "TEMPLATE_ID"          text      ,
 "TEMPLATE_NAME"        text      ,
 "TEMPLATE_DISP_NAME"   text
);
\copy {workspace}."FlowsheetValue" from '{folder}flt.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

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
 "ConvertedWeightValue" real      ,
 "Value"                text      ,
 "UNITS"                text      ,
 "TEMPLATE_ID"          text      ,
 "TEMPLATE_NAME"        text      ,
 "TEMPLATE_DISP_NAME"   text
);
\copy {workspace}."FlowsheetValue_643" from '{folder}flt_new.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

-- start to use {ext} format (i.e., delimiter is tab) because there are double quote in the data which makes the csv format hard to import to postgresql
-- remember to remove the last two lines in {ext} files before importing to the database

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
\copy {workspace}."Labs" from '{folder}labs.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251';

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
\copy {workspace}."Labs_643" from '{folder}labs_new.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251';

drop table if exists {workspace}."LDAs";
create table {workspace}."LDAs"
(
 "PAT_ID"              text,
 "PLACEMENT_INSTANT"   timestamp without time zone,
 "FLO_MEAS_NAME"       text,
 "DISP_NAME"           text,
 "PROPERTIES_DISPLAY"  text,
 "SITE"                text,
 "REMOVAL_DTTM"        timestamp without time zone
);
\copy {workspace}."LDAs" from '{folder}lda.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quotes

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
 "Dose"               real,
 "MedUnit"            text,
 "AdminSite"          text,
 "INFUSION_RATE"      real,
 "MAR_INF_RATE_UNIT"  text,
 "mar_duration"       real,
 "MAR_DURATION_UNIT"  text,
 "MIN_DISCRETE_DOSE"  real,
 "MAX_DISCRETE_DOSE"  real
);
\copy {workspace}."MedicationAdministration" from '{folder}mar.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quote's

drop table if exists {workspace}."MedicalHistory";
create table {workspace}."MedicalHistory"
(
 "CSN_ID"               text,
 "PATIENTID"            text,
 "DEPARTMENTID"         text,
 "diagName"             text,
 "Code"                 text,
 "COMMENTS"             text,
 "Annotation"           text,
 "Medical_Hx_Date"      text,
 "ENC_Date"             timestamp without time zone
);
\copy {workspace}."MedicalHistory" from '{folder}hist.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quotes

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
\copy {workspace}."Notes" from '{folder}note.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quotes

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
\copy {workspace}."OrderMed" from '{folder}ordermed.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quote encoding 'windows-1251's

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
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quotes

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
\copy {workspace}."OrderProcs" from '{folder}orderproc.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quote

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
\copy {workspace}."OrderProcsImage" from '{folder}orderproc_img.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quote

drop table if exists {workspace}."OrderProcs_643";
create table {workspace}."OrderProcs_643"
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
 order_id     text,
 line       text,
 ord_quest_id       text,
 IS_ANSWR_BYPROC_YN         boolean,
 ord_quest_resp     text,
 quest_name         text,
 question       text
);
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quote

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
 code                   text
);
\copy {workspace}."ProblemList" from '{folder}prob.{ext}' with NULL '' csv header delimiter as E'\t' QUOTE E'\b' encoding 'windows-1251'; -- a ugly but working solution to ignore quote

drop table if exists flowsheet_dict;
create table flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text,
 DISP_NAME text
 );
\copy flowsheet_dict from '{folder}flowsheet_dict.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

drop table if exists lab_dict;
create table lab_dict
(
 component_id text,
 name text,
 base_name text,
 external_name text
 );
\copy lab_dict from '{folder}lab_dict.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';

drop table if exists lab_proc_dict;
create table lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy lab_proc_dict from '{folder}lab_proc.{ext}' with csv header delimiter as E'\t' QUOTE E'\b' NULL '' encoding 'windows-1251';