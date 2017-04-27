--  ===========================
--  User Web Log
--  ===========================

with a1 as (
  select pat_id, tsp, event, event#>>'{name}' as name, event#>>'{event_type}' as type, event#>>'{override_value}' as overide_value
  from criteria_log )
select  pat_id, tsp, event#>>'{uid}' as doc_id, overide_value
from a1
where name = 'suspicion_of_infection' and
      type = 'override' and
      (overide_value is null or overide_value not like '%No Infection%');

--  ===========================
--  Notifications
--  ===========================

with
flat_notifications as (
  select
    pat_id,
    to_timestamp(cast(message#>>'{{timestamp}}' as numeric)) as tsp,
    cast(message#>>'{{read}}' as boolean) as read,
    cast(message#>>'{{alert_code}}' as integer) alert_code
  from notifications
  ),
num_notes_at_once as (
  select pat_id, tsp, count(distinct(alert_code)) as number_of_unread_notifications
  from
  flat_notifications
  where not read and tsp BETWEEN '{start}'::timestamptz and '{end}'::timestamptz
  group by pat_id, tsp
),
max_notes_at_once as (
  select pat_id, max(number_of_unread_notifications) as max_unread_notes
  from num_notes_at_once
  group by pat_id
)
select
  max_unread_notes,
  count(distinct(pat_id)) as number_of_pats
from max_notes_at_once
group by max_unread_notes
order by max_unread_notes;

--  ===========================
--  Pat state
--  ===========================

--     when sirs_count > 1 and organ_count > 0 and sus_null_count = 1 then 10 -- sev_sep w.o. sus
--     when sirs_count > 1 and organ_count > 0 and sus_noinf_count = 1 then 12 -- sev_sep w.o. sus

select pat_id,
  case when flag >= 0 then flag else flag + 1000 END as pat_state,
  update_date
from
criteria_events
where flag != -1

--  ===========================
--  Threshold Crossings
--  ===========================

with
	trews_thresh as (
	select value from trews_parameters where name = 'trews_threshold'
	),
trews_detections as (
	select enc_id, tsp, trewscore, trewscore > trews_thresh.value as thresh_crossing
	from
	trews, trews_thresh ),
pat_crossed_thresh as (
	select pe.pat_id, max(thresh_crossing::int) as crossed_threshold
	from trews_detections td
	inner join pat_enc  pe
	on pe.enc_id = td.enc_id
	group by pat_id)
select count(distinct pat_id) as num_pats
from pat_crossed_thresh
where crossed_threshold = 1



max_state as (
	select pat_id, max(pat_state) as max_state
	from historical_criteria
	group by pat_id )
select ms.max_state, sum(pct.crossed_threshold) as num_pats_with_crossings, count( distinct ms.pat_id ) num_pats_in_state
from
	pat_crossed_thresh pct
	inner join
	max_state ms
	on
	ms.pat_id = pct.pat_id
group by ms.max_state
order by ms.max_state
limit 100;

