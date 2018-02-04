CREATE SCHEMA IF NOT EXISTS {workspace};


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
\copy {workspace}."Diagnoses" from '{folder}diag.201309.{ext}' with csv delimiter as E'\t' NULL 'NULL';
\copy {workspace}."Diagnoses" from '{folder}diag.201406.{ext}' with csv delimiter as E'\t' NULL 'NULL';
\copy {workspace}."Diagnoses" from '{folder}diag.201506.{ext}' with csv delimiter as E'\t' NULL 'NULL';
\copy {workspace}."Diagnoses" from '{folder}diag.201606.{ext}' with csv delimiter as E'\t' NULL 'NULL';
create index diag_idx_code on {workspace}."Diagnoses" ("Code");
create index diag_idx_name on {workspace}."Diagnoses" ("diagName");


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
\copy {workspace}."MedicalHistory" from '{folder}hist.201309.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicalHistory" from '{folder}hist.201406.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicalHistory" from '{folder}hist.201506.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."MedicalHistory" from '{folder}hist.201606.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index hist_idx on {workspace}."MedicalHistory" ("Code");


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
\copy {workspace}."OrderMed" from '{folder}ordermed.201309.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMed" from '{folder}ordermed.201406.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMed" from '{folder}ordermed.201506.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMed" from '{folder}ordermed.201606.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

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
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201309.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201406.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201506.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderMedHome" from '{folder}ordermed_home.201606.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes



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
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201309.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201406.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201506.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."OrderProcs_643" from '{folder}orderproc_new.201606.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
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
\copy {workspace}."ProblemList" from '{folder}prob.201309.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ProblemList" from '{folder}prob.201406.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ProblemList" from '{folder}prob.201506.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy {workspace}."ProblemList" from '{folder}prob.201606.{ext}' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index prob_idx_name on {workspace}."ProblemList" ("diagname");
create index prob_idx_code on {workspace}."ProblemList" ("code");


