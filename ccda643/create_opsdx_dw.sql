drop table if exists "Demographics";
create table "Demographics"
(
 "CSN_ID"                uuid,
 "pat_id"                uuid,
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
\copy "Demographics" from '~/clarity-dw/processed/demo.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists "ADT_Feed";
create table "ADT_Feed"
(
"CSN_ID"    uuid,
"EventType"  text,
"effective_time"    text,
"PatientClassAtEvent"  text,
"DEPARTMENT_NAME"   text,
"ROOM_NAME"    text
);
\copy "ADT_Feed" from '~/clarity-dw/processed/adt.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists "Diagnoses";
create table "Diagnoses"
(
 "CSN_ID"         uuid
 ,"DX_ID"         numeric
 ,"DX_ED_YN"      text
 ,"PRIMARY_DX_YN" text
 ,"line"          integer
 ,"diagName"      text
 ,"Code"          text
 ,"Annotation"    text
 ,"COMMENTS"      text
 ,"DX_CHRONIC_YN" text
 ,"ICD-9          Code    category" text
);
\copy "Diagnoses" from '~/clarity-dw/processed/diag.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists "FlowsheetValue-LDA";
create table "FlowsheetValue-LDA"
(
 "CSN_ID"               uuid      ,
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
\copy "FlowsheetValue-LDA" from '~/clarity-dw/processed/flt_lda.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists "FlowsheetValue";
create table "FlowsheetValue"
(
 "CSN_ID"               uuid      ,
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
\copy "FlowsheetValue" from '~/clarity-dw/processed/flt.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists "FlowsheetValue_643";
create table "FlowsheetValue_643"
(
 "CSN_ID"               uuid      ,
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
\copy "FlowsheetValue_643" from '~/clarity-dw/processed/flt_new.rpt' with csv header delimiter as E'\t' NULL 'NULL';

-- start to use rpt format (i.e., delimiter is tab) because there are double quote in the data which makes the csv format hard to import to postgresql
-- remember to remove the last two lines in rpt files before importing to the database

drop table if exists "Labs";
create table "Labs"
(
 "CSN_ID"            uuid      ,
 "COMPONENT_ID" text,
 "BASE_NAME"         text,
 "NAME"              text,
 "EXTERNAL_NAME"     text,
 "RESULT_TIME"       timestamp without time zone,
 "REFERENCE_UNIT"    text,
 "ResultValue"       text      ,
 "COMPONENT_COMMENT" text,
 "ORDER_PROC_ID"     uuid
);
\copy "Labs" from '~/clarity-dw/labs.rpt' with NULL 'NULL' csv header delimiter as E'\t';

drop table if exists "Labs_643";
create table "Labs_643"
(
 "CSN_ID"            uuid      ,
 "COMPONENT_ID" text,
 "BASE_NAME"         text,
 "NAME"              text,
 "EXTERNAL_NAME"     text,
 "RESULT_TIME"       timestamp without time zone,
 "REFERENCE_UNIT"    text,
 "ResultValue"       text      ,
 "COMPONENT_COMMENT" text,
 "ORDER_PROC_ID"     uuid
);
\copy "Labs_643" from '~/clarity-dw/labs_new.rpt' with NULL 'NULL' csv header delimiter as E'\t';

drop table if exists "LDAs";
create table "LDAs"
(
 "PAT_ID"              uuid,
 "PLACEMENT_INSTANT"   timestamp with    time zone,
 "FLO_MEAS_NAME"       text,
 "DISP_NAME"           text,
 "PROPERTIES_DISPLAY"  text,
 "SITE"                text,
 "REMOVAL_DTTM"        timestamp with    time zone
);
\copy "LDAs" from '~/clarity-dw/processed/lda.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "MedicationAdministration";
create table "MedicationAdministration"
(
 "CSN_ID"             uuid,
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
\copy "MedicationAdministration" from '~/clarity-dw/processed/mar.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "MedicalHistory";
create table "MedicalHistory"
(
 "CSN_ID"               uuid,
 "PATIENTID"            uuid,
 "DEPARTMENTID"         text,
 "diagName"             text,
 "Code"                 text,
 "ICD-9 Code category"  text,
 "COMMENTS"             text,
 "Annotation"           text,
 "Medical_Hx_Date"      text,
 "ENC_Date"             timestamp with time zone
);
\copy "MedicalHistory" from '~/clarity-dw/processed/hist.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "Notes";
create table "Notes"
(
 "CSN_ID"               uuid,
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
\copy "Notes" from '~/clarity-dw/processed/note.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "OrderMed";
create table "OrderMed"
(
 "CSN_ID"            uuid                       ,
 "display_name"      text     ,
 "ORDER_INST"        timestamp without time zone,
 "MedRoute"          text     ,
 "Dose"              text     ,
 "MedUnit"           text     ,
 "MIN_DISCRETE_DOSE" real                       ,
 "MAX_DISCRETE_DOSE" real
);
\copy "OrderMed" from '~/clarity-dw/processed/ordermed.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "OrderMedHome";
create table "OrderMedHome"
(
 "CSN_ID"            uuid                       ,
 "display_name"      text     ,
 "ORDER_INST"        timestamp without time zone,
 "MedRoute"          text     ,
 "Dose"              text     ,
 "MedUnit"           text     ,
 "MIN_DISCRETE_DOSE" real                       ,
 "MAX_DISCRETE_DOSE" real
);
\copy "OrderMedHome" from '~/clarity-dw/processed/ordermed_home.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "OrderProcs";
create table "OrderProcs"
(
 "CSN_ID"          uuid                        ,
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
 "LabStatus"     text
);
\copy "OrderProcs" from '~/clarity-dw/orderproc.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "OrderProcsImage";
create table "OrderProcsImage"
(
 "CSN_ID"            uuid                       ,
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
\copy "OrderProcsImage" from '~/clarity-dw/orderproc_img.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "OrderProcs_643";
create table "OrderProcs_643"
(
 "CSN_ID"          uuid                        ,
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
\copy "OrderProcs_643" from '~/clarity-dw/orderproc_new.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists "ProblemList";
create table "ProblemList"
(
 pat_id              uuid                       ,
 csn_id              uuid                       ,
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
\copy "ProblemList" from '~/clarity-dw/processed/prob.rpt' with NULL 'NULL' csv header delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes