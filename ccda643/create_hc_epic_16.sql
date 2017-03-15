drop table if exists "Demographics";
create table "Demographics"
(
 "CSN_ID"                uuid,
 "pat_id"                uuid,
 "ADT_ARRIVAL_TIME"      timestamp with    time zone,
 "ED_DEPARTURE_TIME"     timestamp with    time zone,
 "HOSP_ADMSN_TIME"       timestamp with    time zone,
 "HOSP_DISCH_TIME"       timestamp with    time zone,
 "AgeDuringVisit"        character varying,
 "Gender"                character varying not  null,
 "IsEDPatient"           integer not  null,
 "DischargeDepartment"   character varying,
 "DischargeDisposition"  character varying
 );
\copy "Demographics" from '/udata/zad/hc_epic_16/demo.csv' with csv header delimiter as ',' NULL 'NULL';

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
\copy "ADT_Feed" from '/udata/zad/hc_epic_16/adt.csv' with csv header delimiter as ',' NULL 'NULL';

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
\copy "Diagnoses" from '/udata/zad/hc_epic_16/diag.csv' with csv header delimiter as ',' NULL 'NULL';

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
\copy "FlowsheetValue-LDA" from '/udata/zad/hc_epic_16/flt_lda.csv' with csv header delimiter as ',' NULL 'NULL';

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
\copy "FlowsheetValue" from '/udata/zad/hc_epic_16/flt.csv' with csv header delimiter as ',' NULL 'NULL';