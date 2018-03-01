CREATE SCHEMA IF NOT EXISTS clarity_4y;


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
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201402.rpt' with csv delimiter as E'\t' NULL 'NULL';
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201503.rpt' with csv delimiter as E'\t' NULL 'NULL';
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201604.rpt' with csv delimiter as E'\t' NULL 'NULL';
\copy clarity_4y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-4y/diag.201704.rpt' with csv delimiter as E'\t' NULL 'NULL';
create index diag_idx_code on clarity_4y."Diagnoses" ("Code");
create index diag_idx_name on clarity_4y."Diagnoses" ("diagName");


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

drop index if exists clin_ev_idx_name;
drop index if exists clin_ev_idx_id;
drop index if exists clin_ev_idx_cat;
drop table if exists clarity_4y."clinical_events";
create table clarity_4y."clinical_events"
(
"csn_id"      text,
"event_id"    text, 
"event_type"  text,
"event_display_name" text,
"event_time" timestamp without time zone,
"event_record_time" timestamp without time zone,
"user_id"     text,
"event_comment" text,
"department_name" text,
"flo_meas_id" text,
"disp_name"   text,
"meas_comment" text,
"meas_value"  text 
);
\copy clarity_4y."clinical_events" from '/home/ubuntu/zad/mnt/clarity-4y/code_rrt_events.201402.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."clinical_events" from '/home/ubuntu/zad/mnt/clarity-4y/code_rrt_events.201503.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."clinical_events" from '/home/ubuntu/zad/mnt/clarity-4y/code_rrt_events.201604.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_4y."clinical_events" from '/home/ubuntu/zad/mnt/clarity-4y/code_rrt_events.201704.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index clin_ev_idx_name on clarity_4y."clinical_events" ("event_display_name");
create index clin_ev_idx_id on clarity_4y."clinical_events" ("event_id");
create index clin_ev_idx_cat on clarity_4y."clinical_events" ("event_type");


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


