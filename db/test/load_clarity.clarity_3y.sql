CREATE SCHEMA IF NOT EXISTS clarity_3y;


drop table if exists clarity_3y."Diagnoses";
drop index if exists diag_idx_code;
drop index if exists diag_idx_name;
create table clarity_3y."Diagnoses"
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
\copy clarity_3y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/diag.201309.rpt' with csv delimiter as E'\t' NULL 'NULL';
\copy clarity_3y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/diag.201406.rpt' with csv delimiter as E'\t' NULL 'NULL';
\copy clarity_3y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/diag.201506.rpt' with csv delimiter as E'\t' NULL 'NULL';
\copy clarity_3y."Diagnoses" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/diag.201606.rpt' with csv delimiter as E'\t' NULL 'NULL';
create index diag_idx_code on clarity_3y."Diagnoses" ("Code");
create index diag_idx_name on clarity_3y."Diagnoses" ("diagName");


drop index if exists hist_idx;
drop table if exists clarity_3y."MedicalHistory";
create table clarity_3y."MedicalHistory"
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
\copy clarity_3y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/hist.201309.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/hist.201406.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/hist.201506.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."MedicalHistory" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/hist.201606.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index hist_idx on clarity_3y."MedicalHistory" ("Code");


drop table if exists clarity_3y."OrderMed";
create table clarity_3y."OrderMed"
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
\copy clarity_3y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed.201309.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed.201406.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed.201506.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderMed" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed.201606.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes

drop table if exists clarity_3y."OrderMedHome";
create table clarity_3y."OrderMedHome"
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
\copy clarity_3y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed_home.201309.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed_home.201406.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed_home.201506.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderMedHome" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/ordermed_home.201606.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes



drop index if exists op_643_idx_name;
drop index if exists op_643_idx_id;
drop index if exists op_643_idx_cat;
drop table if exists clarity_3y."OrderProcs_643";
create table clarity_3y."OrderProcs_643"
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
 IS_ANSWR_BYPROC_YN   boolean,
 ord_quest_resp     text,
 quest_name         text,
 question       text,
 comment        text
);
\copy clarity_3y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/orderproc_new.201309.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/orderproc_new.201406.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/orderproc_new.201506.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."OrderProcs_643" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/orderproc_new.201606.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index op_643_idx_name on clarity_3y."OrderProcs_643" ("display_name");
create index op_643_idx_id on clarity_3y."OrderProcs_643" ("OrderProcId");
create index op_643_idx_cat on clarity_3y."OrderProcs_643" ("proc_cat_name");

drop index if exists prob_idx_name;
drop index if exists prob_idx_code;
drop table if exists clarity_3y."ProblemList";
create table clarity_3y."ProblemList"
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
\copy clarity_3y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/prob.201309.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/prob.201406.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/prob.201506.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
\copy clarity_3y."ProblemList" from '/home/ubuntu/zad/mnt/clarity-3y/02-03-2018/prob.201606.rpt' with NULL 'NULL' csv delimiter as E'\t' QUOTE E'\b'; -- a ugly but working solution to ignore quotes
create index prob_idx_name on clarity_3y."ProblemList" ("diagname");
create index prob_idx_code on clarity_3y."ProblemList" ("code");


