
CREATE OR REPLACE FUNCTION get_weekly_individual_metrics()
RETURNS table (
sid integer,
enc_id integer,
age real,
male integer,
sepsis_case integer,
time_ed_admit timestamp with time zone,
still_in_hospital_as_of_date timestamp with time zone,
discharge_date timestamp with time zone,
department text,
disposition text,
still_in_hospital integer,
ed_discharge integer,
died_in_hosp integer,
transfer_acute_care integer,
discharged_home integer,
discharged_other integer,
past_admit_win_30days integer,
time_inhosp_admit timestamp with time zone,
num_days_inhosp_los integer,
num_days_abx integer,
num_days_vasopressors integer,
num_days_ventilation integer,
dept_first_triage text,
ever_icu integer,
sepsis_proxy_label timestamp with time zone,
soi_time timestamp with time zone,
soi_user text,
soi_value text,
entered_no_infection integer,
no_action_taken integer,
manual_override timestamp with time zone,
manual_override_user text,
trews_alert timestamp with time zone,
cms_alert timestamp with time zone,
alert_time timestamp with time zone,
alert_in_ed integer,
alert_prior_abx_cx integer,
trews_alert_deactivated timestamp with time zone,
trews_num_alert_state_changes integer,
hours_until_alert_deactivated real,
hours_admit_to_override real,
hours_admit_to_trews real,
hours_admit_to_cms real,
hours_admit_to_sirs real,
hours_override_to_trews real,
hours_override_to_cms real,
hours_trews_to_cms real,
septic_shock_criteria timestamp with time zone,
hypotensive_within_1hr_shock integer,
first_sirs timestamp with time zone,
max_temperature real,
first_lab_time timestamp with time zone,
time_first_orgdf timestamp with time zone,
hours_lab_to_alert real,
hours_orgdf_to_alert real,
bundle_abx timestamp with time zone,
bundle_initial_lactate timestamp with time zone,
bundle_blood_cx timestamp with time zone,
bundle_repeat_lactate timestamp with time zone,
bundle_fluids timestamp with time zone,
initial_abx_order timestamp with time zone,
initial_lactate_order timestamp with time zone,
initial_lactate_value real,
repeat_lactate_order timestamp with time zone,
repeat_lactate_value real,
num_lactate_orders integer,
initial_blood_cx_order timestamp with time zone,
cx_prior_to_abx integer,
initial_fluid_order timestamp with time zone,
initial_pressor_order timestamp with time zone,
hours_alert_to_discharge real,
hours_alert_to_inpatient_admit real,
hours_alert_to_abx real,
hours_alert_to_cx real,
hours_alert_to_lactate real,
hours_alert_to_fluids real,
hours_alert_to_soi real,
hours_sirs_to_alert real,
hours_soi_to_abx real,
hours_soi_to_cx real,
hours_soi_to_lactate real,
hours_soi_to_fluids real,
hours_soi_to_rep_lactate real,
hours_shock_to_pressors real,
bundle_3hr_complete real,
bundle_6hr_complete real,
bundle_septic_shock_complete real,
orgdf_cms_bilirubin timestamp with time zone,
orgdf_trews_bilirubin timestamp with time zone,
orgdf_cms_creatinine timestamp with time zone,
orgdf_trews_creatinine timestamp with time zone,
orgdf_cms_lactate timestamp with time zone,
orgdf_trews_lactate timestamp with time zone,
orgdf_cms_inr timestamp with time zone,
orgdf_trews_inr timestamp with time zone,
orgdf_cms_platelet timestamp with time zone,
orgdf_trews_platelet timestamp with time zone,
orgdf_cms_resp_fail timestamp with time zone,
orgdf_trews_vent timestamp with time zone,
orgdf_trews_vasopressors timestamp with time zone,
orgdf_trews_gcs timestamp with time zone,
orgdf_cms_delta_sbp timestamp with time zone,
orgdf_cms_hypo_map timestamp with time zone,
orgdf_trews_delta_sbp timestamp with time zone,
orgdf_trews_map timestamp with time zone
)
LANGUAGE plpgsql
AS
$$
begin

drop table if exists indv_alert_metrics_temp;
create temporary table indv_alert_metrics_temp as
select
(select max(ia.sid) from indv_alert_metrics ia) + 1 sid,
ce.enc_id,
age.age,
xy.male,
(ce.enc_id in (select distinct cd.enc_id
                    from cdm_labels cd
                    where label_id=(select max(label_id) from label_version)))::integer sepsis_case,
ad.time_ed_admit,
current_timestamp still_in_hospital_as_of_date,
dc.discharge_date,
dc.department,
dc.disposition,
(dc.discharge_date is null)::integer still_in_hospital,
(dc.department ='HCGH EMERGENCY-ADULTS')::integer ed_discharge,
(dc.disposition ~ 'Expired')::integer died_in_hosp,
(dc.disposition ~ 'Acute Care|Federal Health Care Facility')::integer transfer_acute_care,
(dc.disposition ~ 'Home')::integer discharged_home,
(not dc.disposition ~ 'Expired|Acute Care|Federal Health Care Facility|Home')::integer discharged_other
from (
  select cr.enc_id, min(update_date) time_first_state_change
  from criteria_events cr
  where ((flag >= -990 and flag < 0 )
        or
        (cr.enc_id in (select distinct cd.enc_id
                    from cdm_labels cd
                    where label_id=(select max(label_id) from label_version)
                    )
        )
  )
  and not cr.enc_id in
    (select distinct ct.enc_id
    from cdm_t ct
    where fid='care_unit'
    and value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU',
    'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU', 'HCGH 1N PSYCH'))
  and cr.enc_id in
    (select distinct ct.enc_id
    from cdm_t ct
    where fid='care_unit'
    and value in ('HCGH EMERGENCY-ADULTS'))
  group by cr.enc_id
) ce
join (select cs.enc_id, cs.time_ed_admit from (select ct.enc_id, min(tsp) time_ed_admit
from cdm_t ct
where fid='heart_rate'
group by ct.enc_id) cs
where cs.time_ed_admit::date < current_date - interval '7 days'
and cs.time_ed_admit::date >= current_date - interval '14 days' ) ad
on ce.enc_id=ad.enc_id
left join (select ct.enc_id, tsp discharge_date,
  value::json->>'department' department,
  value::json->>'disposition' disposition
  from cdm_t ct where fid='discharge'
  --- and tsp::date <= current_date - interval '7 days'
  --- and tsp::date >= current_date - interval '14 days'
  and value::json->>'department' ~'HCGH') dc
on ce.enc_id=dc.enc_id
join (select cs.enc_id, value::real as age
  from cdm_s cs where fid='age') age
on ce.enc_id=age.enc_id
join (select cs.enc_id, value::integer male
  from cdm_s cs where fid='gender') xy
on ce.enc_id=xy.enc_id
where age.age >= 18
order by ad.time_ed_admit
;

update indv_alert_metrics_temp ia set still_in_hospital_as_of_date=NULL where ia.still_in_hospital =0;


alter table indv_alert_metrics_temp
add column past_admit_win_30days integer default 0,
add column time_inhosp_admit timestamp with time zone,
add column num_days_inhosp_los integer default 0,
add column num_days_abx integer default 0,
add column num_days_vasopressors integer default 0,
add column num_days_ventilation integer default 0,
add column dept_first_triage text,
add column ever_icu integer default 0,
add column sepsis_proxy_label timestamp with time zone,
add column soi_time timestamp with time zone,
add column soi_user text,
add column soi_value text,
add column entered_no_infection integer,
add column no_action_taken integer default 1,
add column manual_override timestamp with time zone,
add column manual_override_user text,
add column trews_alert timestamp with time zone,
add column cms_alert timestamp with time zone,
add column alert_time timestamp with time zone,
add column alert_in_ed integer,
add column alert_prior_abx_cx integer,
add column trews_alert_deactivated timestamp with time zone,
add column trews_num_alert_state_changes integer,
add column hours_until_alert_deactivated real,
add column hours_admit_to_override real,
add column hours_admit_to_trews real,
add column hours_admit_to_cms real,
add column hours_admit_to_sirs real,
add column hours_override_to_trews real,
add column hours_override_to_cms real,
add column hours_trews_to_cms real,
add column septic_shock_criteria timestamp with time zone,
add column hypotensive_within_1hr_shock integer,
add column first_sirs timestamp with time zone,
add column max_temperature real,
add column first_lab_time timestamp with time zone,
add column time_first_orgdf timestamp with time zone,
add column hours_lab_to_alert real,
add column hours_orgdf_to_alert real,
add column bundle_abx timestamp with time zone,
add column bundle_initial_lactate timestamp with time zone,
add column bundle_blood_cx timestamp with time zone,
add column bundle_repeat_lactate timestamp with time zone,
add column bundle_fluids timestamp with time zone,
add column initial_abx_order timestamp with time zone,
add column initial_lactate_order timestamp with time zone,
add column initial_lactate_value real,
add column repeat_lactate_order timestamp with time zone,
add column repeat_lactate_value real,
add column num_lactate_orders integer,
add column initial_blood_cx_order timestamp with time zone,
add column cx_prior_to_abx integer,
add column initial_fluid_order timestamp with time zone,
add column initial_pressor_order timestamp with time zone,
add column hours_alert_to_discharge real,
add column hours_alert_to_inpatient_admit real,
add column hours_alert_to_abx real,
add column hours_alert_to_cx real,
add column hours_alert_to_lactate real,
add column hours_alert_to_fluids real,
add column hours_alert_to_soi real,
add column hours_sirs_to_alert real,
add column hours_soi_to_abx real,
add column hours_soi_to_cx real,
add column hours_soi_to_lactate real,
add column hours_soi_to_fluids real,
add column hours_soi_to_rep_lactate real,
add column hours_shock_to_pressors real,
add column bundle_3hr_complete real,
add column bundle_6hr_complete real,
add column bundle_septic_shock_complete real,
add column orgdf_cms_bilirubin timestamp with time zone,
add column orgdf_trews_bilirubin timestamp with time zone,
add column orgdf_cms_creatinine timestamp with time zone,
add column orgdf_trews_creatinine timestamp with time zone,
add column orgdf_cms_lactate timestamp with time zone,
add column orgdf_trews_lactate timestamp with time zone,
add column orgdf_cms_inr timestamp with time zone,
add column orgdf_trews_inr timestamp with time zone,
add column orgdf_cms_platelet timestamp with time zone,
add column orgdf_trews_platelet timestamp with time zone,
add column orgdf_cms_resp_fail timestamp with time zone,
add column orgdf_trews_vent timestamp with time zone,
add column orgdf_trews_vasopressors timestamp with time zone,
add column orgdf_trews_gcs timestamp with time zone,
add column orgdf_cms_delta_sbp timestamp with time zone,
add column orgdf_cms_hypo_map timestamp with time zone,
add column orgdf_trews_delta_sbp timestamp with time zone,
add column orgdf_trews_map timestamp with time zone
;



update indv_alert_metrics_temp im
set time_inhosp_admit = x.min,
num_days_inhosp_los = x.num_days,
dept_first_triage = x.triage_unit
from (
select ad.enc_id,
  first(value order by ad.enc_id, inhosp_admit_time) triage_unit,
  min(inhosp_admit_time),
  max(discharge_time::date) - min(inhosp_admit_time::date) num_days
  from
  (select ct.enc_id, tsp inhosp_admit_time, value
    from cdm_t ct
    where fid='care_unit' and not value ~ 'EMERGENCY'
    and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  ) ad
  left join
  (select ct.enc_id, tsp discharge_time
    from cdm_t  ct
    where fid='discharge'
    and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  ) di
  on ad.enc_id=di.enc_id
  group by ad.enc_id) x
where im.enc_id=x.enc_id
;



update indv_alert_metrics_temp im
set num_days_abx = x.num_days
from (select ct.enc_id, min(tsp::date), max(tsp::date), max(tsp::date) - min(tsp::date) +1 num_days
  from cdm_t ct
  where fid='cms_antibiotics'
  and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ct.enc_id) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set num_days_vasopressors = x.num_days
from (select ct.enc_id, min(tsp::date), max(tsp::date), max(tsp::date) - min(tsp::date) +1 num_days
  from cdm_t ct
  where fid~'dopamine_dose|levophed_infusion_dose|epinephrine_dose|neosynephrine_dose|vasopressin_dose'
  and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ct.enc_id) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set num_days_ventilation = x.num_days
from (select ct.enc_id, min(tsp::date), max(tsp::date), max(tsp::date) - min(tsp::date) +1 num_days
  from cdm_t ct
  where fid='vent'
  and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ct.enc_id) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set ever_icu = x.val
from (select ct.enc_id, 1 val
  from cdm_t ct
  where fid='care_unit'
  and value = 'HCGH 3C ICU'
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set soi_time = x.override_time,
soi_user = x.override_user,
soi_value = x.override_value,
entered_no_infection = x.no_inf::integer,
no_action_taken=0
from (select ce.enc_id, min(override_time) override_time,
  first(override_user order by ce.enc_id, override_time) override_user,
  first(override_value->0#>>'{text}' order by ce.enc_id, override_time) override_value,
  first(override_value->0#>>'{text}' order by ce.enc_id, override_time) ~ 'No Infection' no_inf
  from criteria_events ce
  where name='suspicion_of_infection'
  and not override_time is null
  and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ce.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set manual_override = x.override_time,
manual_override_user = x.override_user,
no_action_taken = 0
from (select ce.enc_id, first(override_user order by ce.enc_id, update_date) override_user, min(override_time) override_time
  from criteria_events ce
  where name='ui_severe_sepsis'
  and is_met
  and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ce.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set trews_alert = x.tsp
from (
  select ce.enc_id, min(update_date) tsp
  from criteria_events ce
  where name='trews_subalert'
  and is_met
  and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ce.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set trews_alert_deactivated = x.tsp
from (
  select y.enc_id, min(update_date) tsp
  from (
       select ce.enc_id, update_date from
      (select cr.enc_id, update_date
      from criteria_events cr
      where name='trews_subalert'
      and not is_met
      and cr.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
      ) ce
      join
      indv_alert_metrics_temp ia
      on ia.enc_id=ce.enc_id
      where ce.update_date > ia.trews_alert
      ) y
  group by y.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set hours_until_alert_deactivated =
round((extract(epoch from im.trews_alert_deactivated - im.trews_alert)/3600)::numeric,1);

--- May want to modify this to only include alert changes over the course of ED stay
update indv_alert_metrics_temp im
set trews_num_alert_state_changes = x.count
from (
select y.enc_id, count(*) from
(  select ce.enc_id,
   lag(ce.enc_id) over (order by ce.enc_id, update_date) prev_enc_id,
   update_date,
   is_met,
   lag(is_met) over (order by ce.enc_id, update_date) prev_is_met
   from criteria_events ce
   where name='trews_subalert'
   and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
   order by ce.enc_id, update_date
) y
where y.enc_id=prev_enc_id
and not is_met
and prev_is_met
group by y.enc_id
) x
where im.enc_id=x.enc_id
;


update indv_alert_metrics_temp im
set trews_alert_deactivated = x.tsp
from (
  select ce.enc_id, min(update_date) tsp from
      (select cr.enc_id, update_date
      from criteria_events cr
      where name='trews_subalert'
      and not is_met
      and cr.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
      ) ce
      join
      indv_alert_metrics_temp ia
      on ia.enc_id=ce.enc_id
      where ce.update_date > ia.trews_alert
      group by ce.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set trews_alert = x.tsp
from (
  select ce.enc_id, min(update_date) tsp
  from criteria_events ce
  where name='trews_subalert'
  and is_met
  and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ce.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set cms_alert = x.update_date
from (
select c1.enc_id, c1.update_date, num_sirs, (num_sirs >=2 and num_cms_orgdf >=1)::integer cms_alert
  from
  ( select x.enc_id, update_date, sum(is_met::integer) num_sirs
    from
    (select distinct ce.enc_id, update_date, name, is_met
     from criteria_events ce
     where name ~'sirs_temp|heart_rate|respiratory_rate|wbc'
     and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
    ) x
  group by x.enc_id, update_date
) c1
join
(select x.enc_id, update_date, sum(is_met::integer) num_cms_orgdf
  from
  ( select distinct ce.enc_id, update_date, name, is_met
      from criteria_events ce
      where name ~'mean_arterial_pressure|bilirubin|creatinine|decrease_in_sbp|hypotension_map|hypotesion_dsbp|lactate|creatinine|inr|platelet|respiratory_failure'
      and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
   ) x
  group by x.enc_id, update_date
) c2
on c1.enc_id=c2.enc_id and c1.update_date=c2.update_date
) x
where im.enc_id=x.enc_id
and x.cms_alert=1
;

update indv_alert_metrics_temp im
set alert_time = least(im.trews_alert,im.cms_alert)
;



update indv_alert_metrics_temp im
set first_sirs = x.tsp
from (
  select x.enc_id, min(update_date) tsp from
    (select x.enc_id, update_date, sum(is_met::integer)>=2 sirs
       from
         (select distinct ce.enc_id, update_date, name, is_met
           from criteria_events ce
           where name ~'sirs_temp|heart_rate|respiratory_rate|wbc'
           and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
         ) x
       group by x.enc_id, update_date
    ) x
    where sirs
    group by x.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set septic_shock_criteria = x.tsp
from (
  select ce.enc_id, min(update_date) tsp
  from criteria_events ce
  where ((flag >= -970 and flag < -950) or (flag >= -940 and flag < -930))
  and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by ce.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set hours_admit_to_override = round((extract(epoch from im.manual_override - im.time_ed_admit)/3600)::numeric, 1),
hours_admit_to_trews = round((extract(epoch from im.trews_alert - im.time_ed_admit)/3600)::numeric, 1),
hours_admit_to_cms = round((extract(epoch from im.cms_alert - im.time_ed_admit)/3600)::numeric, 1),
hours_override_to_trews = round((extract(epoch from im.trews_alert - im.manual_override)/3600)::numeric, 1),
hours_override_to_cms = round((extract(epoch from im.cms_alert - im.manual_override)/3600)::numeric, 1),
hours_trews_to_cms = round((extract(epoch from im.cms_alert - im.trews_alert)/3600)::numeric, 1)
;

update indv_alert_metrics_temp im
set sepsis_proxy_label = x.tsp
from (
  select distinct cl.enc_id, tsp
  from cdm_labels cl
  where label_id=(select max(label_id) from label_version)
  and cl.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set first_lab_time = x.tsp
from (
  select tj.enc_id, min(tsp) tsp
  from trews_jit_score tj
  where orgdf_details::json->>'no_lab'='true'
  and tj.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
  group by tj.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set past_admit_win_30days = x.val
from (
  select c1.enc_id, 1 val from
    (select pat_id, cs.enc_id, admit_time from
       (select cs.enc_id, value::timestamp with time zone admit_time
          from cdm_s cs
          where fid='admittime'
          and cs.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
        ) cs
        join pat_enc pe
        on pe.enc_id=cs.enc_id
    ) c1
    join
    (select pat_id, ct.enc_id, discharge_time from
       (select ct.enc_id, tsp discharge_time
         from cdm_t ct
         where fid='discharge'
         and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
       ) ct
       join pat_enc pe
       on pe.enc_id=ct.enc_id
    ) c2
    on c1.pat_id=c2.pat_id
    and discharge_time < admit_time
    and discharge_time >= admit_time - interval '30 days'
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set bundle_abx = x.abx,
bundle_initial_lactate = x.init_lactate,
bundle_blood_cx = x.blood_cx,
bundle_repeat_lactate = x.rep_lactate,
bundle_fluids = x.fluids
from (
  select y.enc_id, min(y.update_date) filter(where name='antibiotics_order') as abx,
  min(y.update_date) filter(where name='initial_lactate_order') as init_lactate,
  min(y.update_date) filter(where name='blood_culture_order') as blood_cx,
  min(y.update_date) filter(where name='repeat_lactate_order') as rep_lactate,
  min(y.update_date) filter(where name='crystalloid_fluid_order') as fluids
  from (
    select ce.enc_id, name,  min(update_date) update_date
    from criteria_events ce
    where name ~ 'order'
    and is_met
    and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
    group by ce.enc_id, name
  ) y
  group by y.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set initial_abx_order = x.abx,
initial_lactate_order = x.init_lactate,
initial_blood_cx_order = x.blood_cx,
initial_lactate_value = x.lactate_val,
num_lactate_orders = x.lactate_num,
initial_fluid_order = x.fluids,
max_temperature = x.max_temp
from (
  select y.enc_id, min(y.tsp) filter(where fid='cms_antibiotics_order') as abx,
  min(y.tsp) filter(where fid='lactate_order') as init_lactate,
  first(y.value::real order by y.enc_id, tsp) filter(where fid='lactate') as lactate_val,
  count(y.tsp) filter(where fid='lactate_order') as lactate_num,
  min(y.tsp) filter(where fid='blood_culture_order') as blood_cx,
  min(y.tsp) filter(where fid='crystalloid_fluid_order') as fluids,
  max(y.value::real) filter(where fid='temperature') as max_temp
  from (
    select ct.enc_id, fid,  tsp, value
    from cdm_t ct
    where fid ~ 'lactate|cms_antibiotics_order|crystalloid_fluid_order|blood_culture_order|temperature'
    and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
    and ct.tsp >= time_ed_admit - interval '12 hours'
  ) y
  group by y.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set repeat_lactate_order = x.tsp,
repeat_lactate_value = x.val
from (
 select lr.enc_id,
 min(lr.tsp) tsp,
 first(lr.value order by lr.enc_id, lr.tsp) val
  from (
    select ct.enc_id, fid,  tsp, value::real 
    from cdm_t ct
    where fid ~ 'lactate'
    and ct.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
    and ct.tsp >= time_ed_admit - interval '12 hours'
  ) lr
  join
  (select ia.enc_id, ia.initial_lactate_order, ia.alert_time, ia.soi_time
  from indv_alert_metrics_temp ia
  ) li
  on li.enc_id=lr.enc_id
  and li.initial_lactate_order < lr.tsp
  and lr.tsp >= li.alert_time - interval '3 hours'
  group by lr.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im
set initial_pressor_order = x.vp
from (
  select y.enc_id, min(y.tsp) vp
  from (
    select ct.enc_id, fid,  tsp, value
    from cdm_t ct
    where fid ~ 'levophed_infusion_dose_order|vasopressin_dose_order|dopamine_dose_order|neosynephrine_dose|epinephrine_dose_order'
    and ct.enc_id in (select ct.enc_id from indv_alert_metrics_temp)
  ) y
  group by y.enc_id
) x
where im.enc_id=x.enc_id
;



update indv_alert_metrics_temp im
set orgdf_cms_bilirubin = x.cms_bilirubin,
orgdf_trews_bilirubin = x.trews_bilirubin,
orgdf_cms_creatinine = x.cms_creatinine,
orgdf_trews_creatinine = x.trews_creatinine,
orgdf_cms_lactate = x.cms_lactate,
orgdf_trews_lactate = x.trews_lactate,
orgdf_cms_inr = x.cms_inr,
orgdf_trews_inr = x.trews_inr,
orgdf_cms_platelet = x.cms_platelet,
orgdf_trews_platelet = x.trews_platelet,
orgdf_cms_resp_fail = x.cms_resp,
orgdf_trews_vent = x.trews_vent,
orgdf_trews_vasopressors = x.trews_pressor,
orgdf_trews_gcs = x.trews_gcs,
orgdf_cms_delta_sbp = x.cms_delta_sbp,
orgdf_cms_hypo_map = x.cms_map,
orgdf_trews_delta_sbp = x.trews_delta_sbp,
orgdf_trews_map = x.trews_map
from (
  select y.enc_id,
  min(y.update_date) filter(where name='bilirubin') as cms_bilirubin,
  min(y.update_date) filter(where name='trews_bilirubin') as trews_bilirubin,
  min(y.update_date) filter(where name='creatinine') as cms_creatinine ,
  min(y.update_date) filter(where name='trews_creatinine') as trews_creatinine,
  min(y.update_date) filter(where name='lactate') as cms_lactate,
  min(y.update_date) filter(where name='trews_lactate') as trews_lactate,
  min(y.update_date) filter(where name='inr') as cms_inr,
  min(y.update_date) filter(where name='trews_inr') as trews_inr,
  min(y.update_date) filter(where name='platelet') as cms_platelet,
  min(y.update_date) filter(where name='trews_platelet') as trews_platelet,
  min(y.update_date) filter(where name='respiratory_failure') as cms_resp,
  min(y.update_date) filter(where name='trews_vent') as trews_vent,
  min(y.update_date) filter(where name='trews_vasopressor') as trews_pressor,
  min(y.update_date) filter(where name='trews_gcs') as trews_gcs,
  min(y.update_date) filter(where name='decrease_in_sbp') as cms_delta_sbp,
  min(y.update_date) filter(where name='hypotension_map|mean_arterial_pressure') as cms_map,
  min(y.update_date) filter(where name='trews_dsbp') as trews_delta_sbp,
  min(y.update_date) filter(where name='trews_map') as trews_map
  from (
    select ce.enc_id, name,  min(update_date) update_date
      from criteria_events ce
      where name ~'trews|bilirubin|creatinine|mean_arterial_pressure|decrease_in_sbp|hypotension_map|hypotesion_dsbp|^lactate|creatinine|inr|platelet|respiratory_failure'
      and not name ='trews_subalert'
      and is_met
      and ce.enc_id in (select ia.enc_id from indv_alert_metrics_temp ia)
      group by ce.enc_id, name
    ) y
    group by y.enc_id
) x
where im.enc_id=x.enc_id
;

update indv_alert_metrics_temp im set time_first_orgdf =
least(im.orgdf_trews_bilirubin, im.orgdf_trews_creatinine, im.orgdf_trews_lactate,
   im.orgdf_trews_inr, im.orgdf_trews_platelet, im.orgdf_trews_vent, im.orgdf_trews_vasopressors,
   im.orgdf_trews_gcs, im.orgdf_trews_map, im.orgdf_trews_delta_sbp,
   im.orgdf_cms_bilirubin, im.orgdf_cms_creatinine, im.orgdf_cms_lactate,
   im.orgdf_cms_inr, im.orgdf_cms_platelet, im.orgdf_cms_resp_fail,
   im.orgdf_cms_hypo_map, im.orgdf_cms_delta_sbp)
;

update indv_alert_metrics_temp im set
cx_prior_to_abx = (im.initial_blood_cx_order <= im.initial_abx_order)::integer,
alert_in_ed = (im.alert_time < im.time_inhosp_admit or time_inhosp_admit is null)::integer,
alert_prior_abx_cx = (im.alert_time  < (least(im.initial_abx_order, im.initial_blood_cx_order) - interval '1 hour'))::integer,
hours_alert_to_discharge = round((extract(epoch from im.discharge_date - im.alert_time)/3600)::numeric, 1),
hours_alert_to_inpatient_admit = round((extract(epoch from im.time_inhosp_admit - im.alert_time)/3600)::numeric, 1),
hours_alert_to_abx = round((extract(epoch from im.initial_abx_order - im.alert_time)/3600)::numeric, 1),
hours_alert_to_cx = round((extract(epoch from im.initial_blood_cx_order - im.alert_time)/3600)::numeric, 1),
hours_alert_to_lactate = round((extract(epoch from im.initial_lactate_order - im.alert_time)/3600)::numeric, 1),
hours_alert_to_fluids = round((extract(epoch from im.initial_fluid_order - im.alert_time)/3600)::numeric, 1),
hours_alert_to_soi = round((extract(epoch from im.soi_time - im.alert_time)/3600)::numeric, 1),
hours_lab_to_alert = round((extract(epoch from im.alert_time - im.first_lab_time)/3600)::numeric, 1),
hours_orgdf_to_alert = round((extract(epoch from im.alert_time - im.time_first_orgdf)/3600)::numeric, 1),
hours_sirs_to_alert = round((extract(epoch from im.alert_time - im.first_sirs)/3600)::numeric, 1),
hours_admit_to_sirs = round((extract(epoch from im.first_sirs - im.time_ed_admit)/3600)::numeric, 1),
hours_soi_to_abx = round((extract(epoch from im.initial_abx_order - coalesce(im.soi_time, im.manual_override))/3600)::numeric, 1),
hours_soi_to_cx = round((extract(epoch from im.initial_blood_cx_order - coalesce(im.soi_time, im.manual_override))/3600)::numeric, 1),
hours_soi_to_lactate = round((extract(epoch from im.initial_lactate_order - coalesce(im.soi_time, im.manual_override))/3600)::numeric, 1),
hours_soi_to_fluids = round((extract(epoch from im.initial_fluid_order - coalesce(im.soi_time, im.manual_override))/3600)::numeric, 1),
hours_soi_to_rep_lactate = round((extract(epoch from im.bundle_repeat_lactate - coalesce(im.soi_time, im.manual_override))/3600)::numeric, 1),
hours_shock_to_pressors = round((extract(epoch from im.orgdf_trews_vasopressors - im.septic_shock_criteria)/3600)::numeric, 1)
;

update indv_alert_metrics_temp im set
bundle_3hr_complete = ((im.hours_soi_to_abx <= 3)::integer +
   (im.hours_soi_to_cx <= 3)::integer +
   (im.hours_soi_to_lactate <= 3)::integer +
   (im.hours_soi_to_fluids <= 3 )::integer)::real  ,
bundle_6hr_complete = (im.hours_soi_to_rep_lactate <= 6 or im.initial_lactate_value <= 2)::integer::real
where not im.soi_time is null
      and not im.soi_value ~ 'No Infection'
;

update indv_alert_metrics_temp im set
bundle_septic_shock_complete = (im.hours_shock_to_pressors <= 6)::integer::real,
hypotensive_within_1hr_shock = ((im.orgdf_trews_delta_sbp < im.septic_shock_criteria - interval '1 hour'
    and im.orgdf_trews_delta_sbp > im.septic_shock_criteria - interval '1 hour') or
    (im.orgdf_trews_map < im.septic_shock_criteria - interval '1 hour'
    and im.orgdf_trews_map > im.septic_shock_criteria - interval '1 hour'))::integer
where not im.septic_shock_criteria is null
;



 insert into indv_alert_metrics
  select * from indv_alert_metrics_temp;

return query select * from indv_alert_metrics_temp;
END;
$$;

--- select get_weekly_individual_metrics();
