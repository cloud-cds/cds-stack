drop table if exists {workspace}.order_narrative;
create table {workspace}.order_narrative
(
 CSN_ID text,
 OrderProcId text, -- selected to be the same as OrderProcs, though this may create confusion.
 order_display_name text,
 ORDER_PROC_ID text,
 LINE bigint,
 CM_CT_OWNER_ID text,
 NARRATIVE text,
 ORD_DATE_REAL float,
 CONTACT_DATE timestamptz,
 IS_ARCHIVED_YN text
 );
\copy {workspace}.order_narrative from '{folder}order_narrative.{ext}' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

--  coalesce(hsp.PAT_ENC_CSN_ID, patadmlink.OR_LINK_CSN) as csn_id,
--  orcase.OR_CASE_ID,
--  orlog.LOG_ID,
--  -- Items assocuated with the cases
--  orcase.CASE_BEGIN_INSTANT,
--  orcase.CASE_END_INSTANT,
--  orcase.SCHED_STATUS_C,
--  -- Items assocuated with the logs
--  patinor.TRACKING_STAT_INST as enter_or_room_instant,
--  patoutor.TRACKING_STAT_INST as leave_or_room_instant,
--  CLARITY.dbo.V_LOG_PROCEDURES.SCHEDULED_YN,
--  CLARITY.dbo.V_LOG_PROCEDURES.PERFORMED_YN,
--  CLARITY.dbo.V_LOG_PROCEDURES.PROCEDURE_NM,
--  CLARITY.dbo.V_LOG_PROCEDURES.PROCEDURE_DISPLAY_NM


drop table if exists {workspace}.surgery_info;
create table {workspace}.surgery_info
(
 csn_id                  text,
 or_case_id              text,
 or_log_id               text,
 case_begin_instant      timestamptz,
 case_end_instant        timestamptz,
 SCHED_STATUS_C          integer,
 enter_or_room_instant   timestamptz,
 leave_or_room_instant   timestamptz,
 scheduled_yn            text,
 preformed_yn            text,
 procedure_name          text,
 procedure_display_name  text
 );
\copy {workspace}.surgery_info from '{folder}surgery_info.{ext}' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';