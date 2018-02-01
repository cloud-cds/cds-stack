-- Special instruction for vent data

--1) write vent_info into a temp table

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


-- 2) delete any row already in orderprocs_643 that has vent data
delete
from clarity_1y."OrderProcs_643"
where
lower(proc_name)='bipap' or
lower(proc_name) like 'wall cpap - adult%' or
lower(proc_name) like 'cpap continuous%' or
lower(proc_name) like 'mechanical ventilation - adult cpap%';


-- 3) write rows from tmp_vent into orderprocs_643 and make a new json column

insert into clarity_1y."OrderProcs_643"
("CSN_ID",
"OrderProcId",
"INSTNTD_ORDER_ID",
"parent_order_id",
"chng_order_Proc_id",
"display_name",
"proc_name",
"proc_cat_name",
"FrequencyOfOrder",
"ORDER_TIME",
"RESULT_TIME",
"ParentOrderTime",
"PROC_START_TIME",
"PROC_ENDING_TIME",
"ParentStartTime",
"ParentEndingTime",
"OrderStatus",
"LabStatus",
order_id,
line,
ord_quest_id,
IS_ANSWR_BYPROC_YN,
ord_quest_resp,
quest_name,
question,
comment)
select
 "CSN_ID",
 "OrderProcId",
 "INSTNTD_ORDER_ID",
 "parent_order_id",
 "chng_order_Proc_id",
 "display_name",
 "proc_name",
 "proc_cat_name",
 json_build_object('priority', "PRIORITY", 'FrequencyOfOrder', "FrequencyOfOrder", 'ORDER_CLASS_NAME', "ORDER_CLASS_NAME")::text,
 "ORDER_TIME",
 "RESULT_TIME",
 "ParentOrderTime",
 "PROC_START_TIME",
 "PROC_ENDING_TIME",
 "ParentStartTime",
 "ParentEndingTime",
 "OrderStatus",
 "LabStatus",
 order_id,
 line,
 ord_quest_id,
 IS_ANSWR_BYPROC_YN,
 ord_quest_resp,
 quest_name,
 question,
 comment
from clarity_1y."tmp_vent";
