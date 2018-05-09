CREATE OR REPLACE FUNCTION calculate_criteria_on_batch(enc_query text, ts_start timestamptz, ts_end timestamptz)
 RETURNS table(enc_id                               int,
               name                                 varchar(50),
               measurement_time                     timestamptz,
               value                                text,
               override_time                        timestamptz,
               override_user                        text,
               override_value                       json,
               is_met                               boolean,
               update_date                          timestamptz,
               is_acute                             boolean,
               severe_sepsis_onset                  timestamptz,
               severe_sepsis_wo_infection_onset     timestamptz,
               severe_sepsis_wo_infection_initial   timestamptz,
               septic_shock_onset                   timestamptz
               )
 LANGUAGE plpgsql
AS $function$
DECLARE
  cms_on                               boolean  := get_parameter('cms_on');
  trews_model_id                       integer  := get_trews_parameter('trews_jit_model_id')::integer;
BEGIN
raise notice 'enc_query:%', enc_query;

execute 'create temporary table enc_ids as ' || enc_query;
analyze enc_ids;

create temp table pat_bp_sys as
  select enc_ids.enc_id, avg(t.value::numeric) as value
  from enc_ids
  inner join cdm_t t on enc_ids.enc_id = t.enc_id
  where isnumeric(t.value) and (t.fid = 'abp_sys' or t.fid = 'nbp_sys')
  group by enc_ids.enc_id
;

create temp table pat_weights as
  select ordered.enc_id, first(ordered.value) as value
  from (
      select enc_ids.enc_id, weights.value::numeric as value
      from enc_ids
      inner join cdm_t weights on enc_ids.enc_id = weights.enc_id
      where weights.fid = 'weight'
      order by weights.tsp
  ) as ordered
  group by ordered.enc_id
;

create temp table pat_cdc as
  select enc_ids.enc_id,
         cd.name,
         cd.fid,
         cd.category,
         c.override_time as c_otime,
         c.override_user as c_ouser,
         c.override_value as c_ovalue,
         cd.override_value as d_ovalue,
         c.is_met as c_ois_met
  from enc_ids
  cross join criteria_default as cd
  left join criteria c on enc_ids.enc_id = c.enc_id and cd.name = c.name
;

--create index pat_cdc_by_enf ON pat_cdc(enc_id, name, fid);
analyze pat_cdc;


create temp table pat_cvalues as
  select pat_cdc.enc_id,
         pat_cdc.name,
         t.fid,
         pat_cdc.category,
         t.tsp,
         t.value,
         pat_cdc.c_otime,
         pat_cdc.c_ouser,
         pat_cdc.c_ovalue,
         pat_cdc.d_ovalue,
         pat_cdc.c_ois_met
  from pat_cdc
  left join cdm_t t
      on pat_cdc.enc_id = t.enc_id and t.fid = pat_cdc.fid
      and (
          t.tsp is null);

--create index pat_cvalues_by_en ON pat_cdc(enc_id, name);
analyze pat_cvalues;

create temp table infection as
  select
      ordered.enc_id,
      ordered.name,
      first(ordered.measurement_time order by ordered.measurement_time) as measurement_time,
      first(ordered.value order by ordered.measurement_time)::text as value,
      first(ordered.c_otime order by ordered.measurement_time) as override_time,
      first(ordered.c_ouser order by ordered.measurement_time) as override_user,
      first(ordered.c_ovalue order by ordered.measurement_time) as override_value,
      coalesce(bool_or(ordered.is_met), false) as is_met,
      now() as update_date
  from (
      select  pat_cvalues.enc_id,
              pat_cvalues.name,
              pat_cvalues.tsp as measurement_time,
              pat_cvalues.value as value,
              pat_cvalues.c_otime,
              pat_cvalues.c_ouser,
              pat_cvalues.c_ovalue,
              (coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value) <> 'No Infection') as is_met
      from pat_cvalues
      where pat_cvalues.name = 'suspicion_of_infection'
      order by pat_cvalues.tsp
  ) as ordered
  group by ordered.enc_id, ordered.name
;

create temp table ui_severe_sepsis as
  select
      ordered.enc_id,
      ordered.name,
      (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
      (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
      (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
      (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
      (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
      coalesce(bool_or(ordered.is_met), false) as is_met,
      now() as update_date
  from (
      select  pat_cvalues.enc_id,
              pat_cvalues.name,
              pat_cvalues.tsp as measurement_time,
              pat_cvalues.value as value,
              pat_cvalues.c_otime,
              pat_cvalues.c_ouser,
              pat_cvalues.c_ovalue,
              pat_cvalues.c_ois_met as is_met
      from pat_cvalues
      where pat_cvalues.name = 'ui_severe_sepsis'
      order by pat_cvalues.tsp
  ) as ordered
  group by ordered.enc_id, ordered.name
;

create temp table ui_septic_shock as
  select
      ordered.enc_id,
      ordered.name,
      (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
      (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
      (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
      (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
      (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
      coalesce(bool_or(ordered.is_met), false) as is_met,
      now() as update_date
  from (
      select  pat_cvalues.enc_id,
              pat_cvalues.name,
              pat_cvalues.tsp as measurement_time,
              pat_cvalues.value as value,
              pat_cvalues.c_otime,
              pat_cvalues.c_ouser,
              pat_cvalues.c_ovalue,
              pat_cvalues.c_ois_met as is_met
      from pat_cvalues
      where pat_cvalues.name = 'ui_septic_shock'
      order by pat_cvalues.tsp
  ) as ordered
  group by ordered.enc_id, ordered.name
;


create temp table severe_sepsis as
  with
  ui_deactivate as (
      select
          ordered.enc_id,
          ordered.name,
          (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
          (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
          (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
          (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
          (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from (
          select  pat_cvalues.enc_id,
                  pat_cvalues.name,
                  pat_cvalues.tsp as measurement_time,
                  pat_cvalues.value as value,
                  pat_cvalues.c_otime,
                  pat_cvalues.c_ouser,
                  pat_cvalues.c_ovalue,
                  (case when pat_cvalues.c_ovalue#>>'{0,type}' = 'uncertain'
                      then now() < (pat_cvalues.c_ovalue#>>'{0,until}')::timestamptz
                      else false end) as is_met
          from pat_cvalues
          where pat_cvalues.name = 'ui_deactivate'
          order by pat_cvalues.tsp
      ) as ordered
      group by ordered.enc_id, ordered.name
  )
  select *, null::boolean as is_acute from infection
  union all select *, null::boolean as is_acute from sirs
  -- union all select *, null::boolean as is_acute from respiratory_failures
  -- union all select *, null::boolean as is_acute from organ_dysfunction_except_rf
  union all select *, null::boolean as is_acute from organ_dysfunction

  union all select * from trews
  union all select *, null::boolean as is_acute from ui_severe_sepsis
  union all select *, null::boolean as is_acute from ui_deactivate
;

create temp table severe_sepsis_criteria as
  /*
  with organ_dysfunction as (
      select * from respiratory_failures
      union all select * from organ_dysfunction_except_rf
  )
  */
  select IC.enc_id,
         sum(IC.cnt) > 0 as suspicion_of_infection,
         sum(TA.cnt) > 0 as trews_subalert,
         /*sum(SC.cnt)*/ 0 as sirs_cnt,
         /*sum(OC.cnt)*/ 0 as org_df_cnt,
         max(IC.onset) as inf_onset,
         max(TA.onset) as trews_subalert_onset,
         /*max(SC.onset)*/ null::timestamptz as sirs_onset,
         /*min(SC.initial)*/ null::timestamptz as sirs_initial,
         /*max(OC.onset)*/ null::timestamptz as org_df_onset,
         sum(UISS1.cnt) as ui_ss1_cnt,
         max(UISS1.onset) as ui_ss1_onset,
         sum(UISS2.cnt) as ui_ss2_cnt,
         max(UISS2.onset) as ui_ss2_onset
  from
  (
    select infection.enc_id,
           sum(case when infection.is_met then 1 else 0 end) as cnt,
           max(infection.override_time) as onset
    from infection
    group by infection.enc_id
  ) IC
  left join
  (
      select trews.enc_id,
             sum(case when trews.is_met then 1 else 0 end) as cnt,
             max(trews.measurement_time) as onset
      from trews where trews.name = 'trews_subalert'
      group by trews.enc_id
  ) TA on TA.enc_id = IC.enc_id
  -- left join
  -- (
  --   select sirs.enc_id,
  --          sum(case when sirs.is_met then 1 else 0 end) as cnt,
  --          (array_agg(sirs.measurement_time order by sirs.measurement_time))[2] as onset,
  --          (array_agg(sirs.measurement_time order by sirs.measurement_time))[1] as initial
  --   from sirs
  --   where cms_on
  --   group by sirs.enc_id
  -- ) SC on IC.enc_id = SC.enc_id
  -- left join
  -- (
  --   select organ_dysfunction.enc_id,
  --          sum(case when organ_dysfunction.is_met then 1 else 0 end) as cnt,
  --          min(organ_dysfunction.measurement_time) as onset
  --   from organ_dysfunction
  --   where cms_on
  --   group by organ_dysfunction.enc_id
  -- ) OC on IC.enc_id = OC.enc_id
  left join
  (
    select ui_severe_sepsis.enc_id,
           sum(case when ui_severe_sepsis.is_met then 1 else 0 end) as cnt,
           min(ui_severe_sepsis.override_time) as onset
    from ui_severe_sepsis
    group by ui_severe_sepsis.enc_id
  ) UISS1 on IC.enc_id = UISS1.enc_id
  left join
  (
    select ui_septic_shock.enc_id,
           sum(case when ui_septic_shock.is_met then 1 else 0 end) as cnt,
           min(ui_septic_shock.override_time) as onset
    from ui_septic_shock
    group by ui_septic_shock.enc_id
  ) UISS2 on IC.enc_id = UISS2.enc_id
  group by IC.enc_id
;

create temp table severe_sepsis_now as
  select sspm.enc_id,
         sspm.ui_ss2_is_met or sspm.ui_ss1_is_met or sspm.trews_severe_sepsis_is_met or sspm.severe_sepsis_is_met severe_sepsis_is_met,
         (case when sspm.ui_ss1_is_met then sspm.ui_ss1_onset
          when not sspm.severe_sepsis_is_met and not sspm.trews_severe_sepsis_is_met and sspm.ui_ss2_is_met
            then sspm.ui_ss2_onset
          when sspm.trews_severe_sepsis_wo_infection_is_met then
            (case when sspm.trews_severe_sepsis_onset <> 'infinity'::timestamptz
                    then sspm.trews_severe_sepsis_onset
                    else null end
             )
          else
             (case when sspm.severe_sepsis_onset <> 'infinity'::timestamptz
                   then sspm.severe_sepsis_onset
                   else null end
             )
         end) as severe_sepsis_onset,
         (case when sspm.ui_ss1_is_met then sspm.ui_ss1_onset
          when not sspm.severe_sepsis_is_met and not sspm.trews_severe_sepsis_is_met and sspm.ui_ss2_is_met
            then sspm.ui_ss2_onset
          when sspm.trews_severe_sepsis_wo_infection_is_met then
            (case when sspm.trews_severe_sepsis_wo_infection_onset <> 'infinity'::timestamptz
                   then sspm.trews_severe_sepsis_wo_infection_onset
                   else null end
             )
          else
             (case when sspm.severe_sepsis_wo_infection_onset <> 'infinity'::timestamptz
                   then sspm.severe_sepsis_wo_infection_onset
                   else null end
             )
         end) as severe_sepsis_wo_infection_onset,
         (case when sspm.ui_ss1_is_met then sspm.ui_ss1_onset
          when not sspm.severe_sepsis_is_met and not sspm.trews_severe_sepsis_is_met and sspm.ui_ss2_is_met
            then sspm.ui_ss2_onset
          when sspm.trews_severe_sepsis_wo_infection_is_met then sspm.trews_severe_sepsis_wo_infection_initial
          else sspm.severe_sepsis_wo_infection_initial
         end) severe_sepsis_wo_infection_initial,
         (case when sspm.ui_ss1_is_met then sspm.ui_ss1_onset
          when not sspm.severe_sepsis_is_met and not sspm.trews_severe_sepsis_is_met and sspm.ui_ss2_is_met
            then sspm.ui_ss2_onset
          when sspm.trews_severe_sepsis_wo_infection_is_met then sspm.trews_severe_sepsis_lead_time
          else sspm.severe_sepsis_lead_time
         end) severe_sepsis_lead_time
  from (
    select stats.enc_id,
           coalesce(bool_or(stats.suspicion_of_infection
                              and stats.sirs_cnt > 1
                              and stats.org_df_cnt > 0)
                    , false
                    ) as severe_sepsis_is_met,
           coalesce(bool_or(stats.suspicion_of_infection
                              and stats.trews_subalert)
                    , false
                    ) as trews_severe_sepsis_is_met,
           coalesce(bool_or(stats.sirs_cnt > 1 and stats.org_df_cnt > 0)
                    , false
                    ) as severe_sepsis_wo_infection_is_met,
           coalesce(bool_or(stats.trews_subalert)
                    , false
                    ) as trews_severe_sepsis_wo_infection_is_met,
           coalesce(bool_or(stats.ui_ss1_cnt > 0)
                    , false
                    ) as ui_ss1_is_met,
           max(stats.ui_ss1_onset) as ui_ss1_onset,
           coalesce(bool_or(stats.ui_ss2_cnt > 0)
                    , false
                    ) as ui_ss2_is_met,
           max(stats.ui_ss2_onset) as ui_ss2_onset,
           max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                        coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                        coalesce(stats.org_df_onset, 'infinity'::timestamptz))
               ) as severe_sepsis_onset,
           max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                        coalesce(stats.trews_subalert_onset, 'infinity'::timestamptz))
               ) as trews_severe_sepsis_onset,
           max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                        coalesce(stats.org_df_onset, 'infinity'::timestamptz))
               ) as severe_sepsis_wo_infection_onset,
           max(greatest(coalesce(stats.trews_subalert_onset, 'infinity'::timestamptz))
               ) as trews_severe_sepsis_wo_infection_onset,
           min(least(stats.sirs_initial, stats.org_df_onset)
               ) as severe_sepsis_wo_infection_initial,
           min(least(stats.trews_subalert_onset)
               ) as trews_severe_sepsis_wo_infection_initial,
           min(least(stats.inf_onset, stats.sirs_onset, stats.org_df_onset))
              as severe_sepsis_lead_time,
           min(least(stats.inf_onset, stats.trews_subalert_onset))
              as trews_severe_sepsis_lead_time
    from severe_sepsis_criteria stats
    group by stats.enc_id
  ) sspm
;


/**
 * Septic shock subqueries
 **/

create temp table crystalloid_fluid as
  select
      ordered.enc_id,
      ordered.name,
      (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
      (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
      (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
      (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
      (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
      coalesce(bool_or(ordered.is_met), false) as is_met,
      now() as update_date
  from
  (
      select  pat_cvalues.enc_id,
              pat_cvalues.name,
              pat_cvalues.tsp as measurement_time,
              pat_cvalues.value as value,
              pat_cvalues.c_otime,
              pat_cvalues.c_ouser,
              pat_cvalues.c_ovalue,
              (case when coalesce(pat_cvalues.c_ovalue#>>'{0,text}' = 'Not Indicated' or pat_cvalues.c_ovalue#>>'{0,text}' ~* 'Clinically Inappropriate', false)
                    then criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                    else pat_cvalues.fid = 'crystalloid_fluid'
                          and criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
               end)
              and ssn.severe_sepsis_is_met
                      and coalesce(pat_cvalues.c_otime, pat_cvalues.tsp) >= ssn.severe_sepsis_onset
              as is_met
      from pat_cvalues
      left join severe_sepsis_now ssn on pat_cvalues.enc_id = ssn.enc_id
      where pat_cvalues.name = 'crystalloid_fluid'
      order by pat_cvalues.tsp
  ) as ordered
  group by ordered.enc_id, ordered.name
;

create temp table hypotension as
  select
      ordered.enc_id,
      ordered.name,
      (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
      (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
      (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
      (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
      (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
      coalesce(bool_or(ordered.is_met), false) as is_met,
      now() as update_date
  from
  (
      with pat_fluid_overrides as (
        select CFL.enc_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated' or CFL.override_value#>>'{0,text}' ~* 'Clinically Inappropriate'), false) as override
        from crystalloid_fluid CFL
        group by CFL.enc_id
      ),
      pats_fluid_after_severe_sepsis as (
        select  MFL.enc_id,
                MFL.tsp,
                sum(MFL.value::numeric) as total_fluid,
                -- Fluids are met if they are overriden or if we have more than
                -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
                (coalesce(bool_or(OV.override), false)
                    or coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.value))
                ) as is_met,
                coalesce(bool_or(OV.override), false) as override_is_met
        from cdm_t MFL
        left join pat_weights PW on MFL.enc_id = PW.enc_id
        left join severe_sepsis_now SSPN on MFL.enc_id = SSPN.enc_id
        left join pat_fluid_overrides OV on MFL.enc_id = OV.enc_id
        where isnumeric(MFL.value)
        and SSPN.severe_sepsis_is_met
        and MFL.tsp >= (SSPN.severe_sepsis_onset - orders_lookback)
        and (MFL.fid = 'crystalloid_fluid' or coalesce(OV.override, false))
        group by MFL.enc_id, MFL.tsp
      )
      select PC.enc_id,
             PC.name,
             PC.tsp as measurement_time,
             PC.value as value,
             PC.c_otime,
             PC.c_ouser,
             PC.c_ovalue,
             (SSPN.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp) >= SSPN.severe_sepsis_onset)
             and
             (case when PC.category = 'hypotension' then
                     PFL.is_met
                     and
                     (
                      (PFL.override_is_met and SSPN.severe_sepsis_onset < PC.tsp and NEXT.tsp < SSPN.severe_sepsis_onset + interval '1 hour')
                      or
                      (not PFL.override_is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                     )
                     and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                     and criteria_value_met(NEXT.value, PC.c_ovalue, PC.d_ovalue)

                   when PC.category = 'hypotension_dsbp' then
                     PFL.is_met
                     and
                     (
                      (PFL.override_is_met and SSPN.severe_sepsis_onset < PC.tsp and NEXT.tsp < SSPN.severe_sepsis_onset + interval '1 hour')
                      or
                      (not PFL.override_is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                     )
                     and decrease_in_sbp_met(PBPSYS.value, PC.value, PC.c_ovalue, PC.d_ovalue)
                     and decrease_in_sbp_met(PBPSYS.value, NEXT.value, PC.c_ovalue, PC.d_ovalue)

                  else false
              end) as is_met
      from pat_cvalues PC
      left join severe_sepsis_now SSPN on PC.enc_id = SSPN.enc_id

      left join pats_fluid_after_severe_sepsis PFL
        on PC.enc_id = PFL.enc_id

      left join lateral (
        select t.enc_id, t.fid, t.tsp, t.value
        from cdm_t t
        where PC.enc_id = t.enc_id and PC.fid = t.fid and PC.tsp < t.tsp
        order by t.tsp
        limit 1
      ) NEXT on PC.enc_id = NEXT.enc_id and PC.fid = NEXT.fid

      left join lateral (
        select BP.enc_id, max(BP.value::numeric) as value
        from pat_bp_sys BP where PC.enc_id = BP.enc_id
        group by BP.enc_id
      ) PBPSYS on PC.enc_id = PBPSYS.enc_id

      where PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
      order by PC.tsp
  ) as ordered
  group by ordered.enc_id, ordered.name
;

create temp table hypoperfusion as
  select
      ordered.enc_id,
      ordered.name,
      (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
      (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
      (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
      (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
      (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
      coalesce(bool_or(ordered.is_met), false) as is_met,
      now() as update_date
  from
  (
      select  pat_cvalues.enc_id,
              pat_cvalues.name,
              pat_cvalues.tsp as measurement_time,
              pat_cvalues.value as value,
              pat_cvalues.c_otime,
              pat_cvalues.c_ouser,
              pat_cvalues.c_ovalue,
              criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                  and (ssn.severe_sepsis_is_met and
                      coalesce(pat_cvalues.c_otime, pat_cvalues.tsp)
                              between ssn.severe_sepsis_onset - initial_lactate_order_lookback and ssn.severe_sepsis_onset + '3 hours'::interval)
                  as is_met
      from pat_cvalues
      left join severe_sepsis_now ssn on pat_cvalues.enc_id = ssn.enc_id
      where pat_cvalues.name = 'initial_lactate'
      order by pat_cvalues.tsp
  ) as ordered
  group by ordered.enc_id, ordered.name
;

create temp table septic_shock as
  select * from crystalloid_fluid
  union all select * from hypotension
  union all select * from hypoperfusion
  union all select * from ui_septic_shock
;

create temp table septic_shock_now as
  select stats.enc_id,
         coalesce(bool_or(ui.ui_cnt > 0), bool_or(stats.cnt > 0)) as septic_shock_is_met,
         bool_or(stats.hypotension_cnt > 0) septic_shock_hypotension_is_met,
         (case when bool_or(ui.ui_cnt > 0) then min(ui.ui_onset)
          else greatest(min(stats.onset), max(ssn.severe_sepsis_onset)) end) as septic_shock_onset
  from (
      (select hypotension.enc_id,
              sum(case when hypotension.is_met then 1 else 0 end) as cnt,
              sum(case when hypotension.is_met then 1 else 0 end) as hypotension_cnt,
              min(hypotension.measurement_time) as onset
       from hypotension
       group by hypotension.enc_id)
      union
      (select hypoperfusion.enc_id,
              sum(case when hypoperfusion.is_met then 1 else 0 end) as cnt,
              0 as hypotension_cnt,
              min(hypoperfusion.measurement_time) as onset
       from hypoperfusion
       group by hypoperfusion.enc_id)
  ) stats
  left join severe_sepsis_now ssn on stats.enc_id = ssn.enc_id
  left join
  (select ui_septic_shock.enc_id,
      sum(case when ui_septic_shock.is_met then 1 else 0 end) as ui_cnt,
      min(ui_septic_shock.override_time) as ui_onset
   from ui_septic_shock
   group by ui_septic_shock.enc_id) ui on stats.enc_id = ui.enc_id
  group by stats.enc_id
;


return query
  with
  orders_criteria_raw as (
      select
          ordered.enc_id,
          ordered.name,
          coalesce(   (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)),
                      last(ordered.measurement_time order by ordered.measurement_time)
          ) as measurement_time,
          coalesce(   (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text,
                      last(ordered.value order by ordered.measurement_time)::text
          ) as value,
          coalesce(   (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)),
                      last(ordered.c_otime order by ordered.measurement_time)
          ) as override_time,
          coalesce(   (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)),
                      last(ordered.c_ouser order by ordered.measurement_time)
          ) as override_user,
          coalesce(
              (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)),
              last(ordered.c_ovalue order by ordered.measurement_time)
          ) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date,
          max(ordered.measurement_time) max_meas_time
      from
      (
          with past_criteria as (
              select  PO.enc_id, SNP.state,
                      min(SNP.severe_sepsis_onset) as severe_sepsis_onset,
                      min(SNP.septic_shock_onset) as septic_shock_onset,
                      bool_or(SNP.septic_shock_hypotension_is_met) as septic_shock_hypotension_is_met
              from enc_ids PO
              inner join lateral get_states_snapshot(PO.enc_id) SNP on PO.enc_id = SNP.enc_id
              where SNP.state >= 20
              group by PO.enc_id, SNP.state
          ),
          orders_severe_sepsis_onsets as (
              select LT.enc_id, min(coalesce(LT.severe_sepsis_onset, now())) - orders_lookback as severe_sepsis_onset_for_order,
              min(coalesce(LT.severe_sepsis_onset, now())) - initial_lactate_order_lookback as severe_sepsis_onset_for_initial_lactate_order,
              min(coalesce(LT.severe_sepsis_onset, now())) - blood_culture_order_lookback as severe_sepsis_onset_for_blood_culture_order,
              min(coalesce(LT.severe_sepsis_onset, now())) - antibiotics_order_lookback as severe_sepsis_onset_for_antibiotics_order
              from (
                  select  SSPN.enc_id,
                          min(case when SSPN.severe_sepsis_is_met then SSPN.severe_sepsis_onset else null end)
                              as severe_sepsis_onset
                      from severe_sepsis_now SSPN
                      group by SSPN.enc_id
                  union all
                  select PC.enc_id, min(PC.severe_sepsis_onset) as severe_sepsis_onset
                      from past_criteria PC
                      where PC.state >= 20
                      group by PC.enc_id
              ) LT
              group by LT.enc_id
          ),
          orders_septic_shock_onsets as (
              select ONST.enc_id, min(ONST.septic_shock_onset) as septic_shock_onset
              , bool_or(ONST.septic_shock_hypotension_is_met) as septic_shock_hypotension_is_met
              from (
                  select  SSHN.enc_id,
                          min(case when SSHN.septic_shock_is_met then SSHN.septic_shock_onset else null end)
                              as septic_shock_onset,
                          bool_or(SSHN.septic_shock_hypotension_is_met) as septic_shock_hypotension_is_met
                      from septic_shock_now SSHN
                      group by SSHN.enc_id
                  union all
                  select PC.enc_id, min(PC.septic_shock_onset) as septic_shock_onset,
                      bool_or(PC.septic_shock_hypotension_is_met) as septic_shock_hypotension_is_met
                      from past_criteria PC
                      where PC.state >= 30
                      group by PC.enc_id
              ) ONST
              group by ONST.enc_id
          ),
          antibiotics_comb as (
              with antibiotics_comb_therapy as (
                  select  pat_cvalues.enc_id,
                          pat_cvalues.tsp as measurement_time,
                          json_build_object('status', dose_order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}'),
                                             'fid', pat_cvalues.fid, 'result', pat_cvalues.value::json, 'tsp', pat_cvalues.tsp) as value,
                          pat_cvalues.c_otime,
                          pat_cvalues.c_ouser,
                          pat_cvalues.c_ovalue,
                          coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OLT.severe_sepsis_onset_for_antibiotics_order
                                  and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value,
                                          coalesce(
                                              (pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                              (pat_cvalues.d_ovalue#>>'{lower}')::numeric
                                          )
                                      )), false) as is_met
                  from pat_cvalues
                      left join orders_severe_sepsis_onsets OLT
                          on pat_cvalues.enc_id = OLT.enc_id
                  where pat_cvalues.name = 'antibiotics_order' and pat_cvalues.category = 'antibiotics_comb')
              select  act.enc_id,
                      'antibiotics_order'::text as name,
                      (case when (count(*) filter (where act.name = 'comb1' and act.is_met)) > 0
                              and (count(*) filter (where act.name = 'comb2' and act.is_met)) > 0 -- Completed
                          then greatest(
                                  min(act.measurement_time) filter (where act.name = 'comb1' and act.is_met),
                                  min(act.measurement_time) filter (where act.name = 'comb2' and act.is_met)
                              )
                        when (count(*) filter (where act.name = 'comb1' and act.value#>>'{status}' is not null)) > 0
                          and (count(*) filter (where act.name = 'comb2' and act.value#>>'{status}' is not null)) > 0 -- Ordered
                          then greatest(
                                  min(act.measurement_time) filter (where act.name = 'comb1' and act.value#>>'{status}' is not null),
                                  min(act.measurement_time) filter (where act.name = 'comb2' and (act.value::json)#>>'{status}' is not null)
                              )
                        else null end
                          ) as measurement_time,
                      (case when (count(*) filter (where act.name = 'comb1' and act.is_met)) > 0
                              and (count(*) filter (where act.name = 'comb2' and act.is_met)) > 0
                          then    json_build_object('status', 'Completed',
                                      'fid', json_build_array(first(act.value#>>'{fid}' order by act.measurement_time) filter (where act.name = 'comb1' and act.is_met),
                                                              first(act.value#>>'{fid}' order by act.measurement_time) filter (where act.name = 'comb2' and act.is_met)),
                                      'result', json_build_array(first(act.value#>'{result}' order by act.measurement_time) filter (where act.name = 'comb1' and act.is_met)::json,
                                                                 first(act.value#>'{result}' order by act.measurement_time) filter (where act.name = 'comb2' and act.is_met))::json,
                                      'tsp', json_build_array(first(act.value#>>'{tsp}' order by act.measurement_time) filter (where act.name = 'comb1' and act.is_met),
                                                                 first(act.value#>>'{tsp}' order by act.measurement_time) filter (where act.name = 'comb2' and act.is_met))
                                                                  )
                        when (count(*) filter (where act.name = 'comb1' and act.value#>>'{status}' is not null)) > 0
                          and (count(*) filter (where act.name = 'comb2' and act.value#>>'{status}' is not null)) > 0
                          then    json_build_object('status', 'Ordered',
                                      'fid', json_build_array(first(act.value#>>'{fid}' order by act.measurement_time) filter (where act.name = 'comb1' and act.value#>>'{status}' is not null),
                                                              first(act.value#>>'{fid}' order by act.measurement_time) filter (where act.name = 'comb2' and act.value#>>'{status}' is not null)),
                                      'result', json_build_array(first(act.value#>>'{result}' order by act.measurement_time) filter (where act.name = 'comb1' and act.value#>>'{status}' is not null),
                                                                 first(act.value#>>'{result}' order by act.measurement_time) filter (where act.name = 'comb2' and act.value#>>'{status}' is not null)),
                                      'tsp', json_build_array(first(act.value#>>'{tsp}' order by act.measurement_time) filter (where act.name = 'comb1' and act.value#>>'{status}' is not null),
                                                                 first(act.value#>>'{tsp}' order by act.measurement_time) filter (where act.name = 'comb2' and act.value#>>'{status}' is not null))
                                                                  )
                        else null end
                          )::text as value,
                      last(act.c_otime),
                      last(act.c_ouser),
                      last(act.c_ovalue),
                      (count(*) filter (where act.name = 'comb1' and act.is_met)) > 0
                          and (count(*) filter (where act.name = 'comb2' and act.is_met)) > 0 as is_met
              from antibiotics_comb_therapy act
              where act.name is not null
              group by act.enc_id
          ),
          orders as (
              select  pat_cvalues.enc_id,
                      pat_cvalues.name,
                      (case when (pat_cvalues.name = 'initial_lactate_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_initial_lactate_order)
                              then pat_cvalues.tsp
                          else null end) as measurement_time,
                      (case when (pat_cvalues.name = 'initial_lactate_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_initial_lactate_order)
                              then (case when pat_cvalues.category in ('after_severe_sepsis_dose', 'after_septic_shock_dose')
                                          then dose_order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}')
                                        else order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}')
                                   end)
                          else null end) as value,
                      pat_cvalues.c_otime,
                      pat_cvalues.c_ouser,
                      pat_cvalues.c_ovalue,
                      (case
                          when pat_cvalues.category = 'after_severe_sepsis' then
                              ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > (case when pat_cvalues.name = 'initial_lactate_order' then OLT.severe_sepsis_onset_for_initial_lactate_order when pat_cvalues.name = 'blood_culture_order' then OLT.severe_sepsis_onset_for_blood_culture_order when pat_cvalues.name = 'antibiotics_order' then OLT.severe_sepsis_onset_for_antibiotics_order else OLT.severe_sepsis_onset_for_order end), false) )
                              and ( order_met(pat_cvalues.name, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}'))

                          when pat_cvalues.category = 'after_severe_sepsis_dose' then
                              ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > (case when pat_cvalues.name = 'initial_lactate_order' then OLT.severe_sepsis_onset_for_initial_lactate_order when pat_cvalues.name = 'blood_culture_order' then OLT.severe_sepsis_onset_for_blood_culture_order when pat_cvalues.name = 'antibiotics_order' then OLT.severe_sepsis_onset_for_antibiotics_order else OLT.severe_sepsis_onset_for_order end), false) )
                              and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value,
                                      coalesce((pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                               (pat_cvalues.d_ovalue#>>'{lower}')::numeric)))

                          when pat_cvalues.category = 'after_septic_shock' then
                              ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OST.septic_shock_onset, false) )
                              and ( order_met(pat_cvalues.name, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}'))

                          when pat_cvalues.category = 'after_septic_shock_dose' then
                              (case when OST.septic_shock_onset is null then false
                                  else
                                      -- if septic shock and hypotension is_met, vasopressor is not needed.
                                      not OST.septic_shock_hypotension_is_met
                                      or
                                      (( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OST.septic_shock_onset, false) )
                                      and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value,
                                              coalesce((pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                                       (pat_cvalues.d_ovalue#>>'{lower}')::numeric)) ))
                              end)
                          else criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                          end
                      ) as is_met
              from pat_cvalues
                left join orders_severe_sepsis_onsets OLT
                  on pat_cvalues.enc_id = OLT.enc_id
                left join orders_septic_shock_onsets OST
                  on pat_cvalues.enc_id = OST.enc_id
              where (pat_cvalues.name = 'antibiotics_order' and pat_cvalues.category = 'after_severe_sepsis_dose')
          )
          select * from orders
          union all select * from antibiotics_comb
      ) as ordered
      group by ordered.enc_id, ordered.name
  ),
  orders_criteria as (
      select ocr.enc_id, ocr.name, ocr.measurement_time, ocr.value,
      (case when ocr.override_value#>>'{0,text}' = 'Ordered' and ocr.override_time < ocr.max_meas_time then null else ocr.override_time end) override_time,
      (case when ocr.override_value#>>'{0,text}' = 'Ordered' and ocr.override_time < ocr.max_meas_time then null else ocr.override_user end) override_user,
      (case when ocr.override_value#>>'{0,text}' = 'Ordered' and ocr.override_time < ocr.max_meas_time then null else ocr.override_value end) override_value,
      ocr.is_met, ocr.update_date
      from orders_criteria_raw ocr
  ),
  repeat_lactate_raw as (
      select
          ordered.enc_id,
          ordered.name,
          coalesce(   (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)),
                      last(ordered.measurement_time order by ordered.measurement_time)
          ) as measurement_time,
          coalesce(   (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text,
                      last(ordered.value order by ordered.measurement_time)::text
          ) as value,
          coalesce(   (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)),
                      last(ordered.c_otime order by ordered.measurement_time)
          ) as override_time,
          coalesce(   (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)),
                      last(ordered.c_ouser order by ordered.measurement_time)
          ) as override_user,
          coalesce(
              (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)),
              last(ordered.c_ovalue order by ordered.measurement_time)
          ) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          max(ordered.measurement_time) max_meas_time,
          now() as update_date
      from
      (
          select  pat_cvalues.enc_id,
                  pat_cvalues.name,
                  (case when initial_lactate_order.tsp < pat_cvalues.tsp then pat_cvalues.tsp else null end) as measurement_time,
                  (case when initial_lactate_order.tsp < pat_cvalues.tsp then order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}') else null end) as value,
                  pat_cvalues.c_otime,
                  pat_cvalues.c_ouser,
                  pat_cvalues.c_ovalue,
                  (case when initial_lactate_order.is_met
                      and (
                          not initial_lactate_order.res_is_met or
                          (
                              order_met(pat_cvalues.name, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}')
                              and pat_cvalues.tsp > initial_lactate_order.tsp
                              and pat_cvalues.tsp > coalesce(initial_lactate_order.res_tsp, pat_cvalues.tsp)
                          )

                      )
                      then true
                      else false end) is_met
          from pat_cvalues
          left join (
              select oc.enc_id,
                     max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                     bool_or(oc.is_met) as is_met,
                     coalesce(min(oc.value) = 'Completed', false) as is_completed,
                     first(p3.tsp order by p3.tsp) res_tsp,
                     coalesce(first(p3.value::numeric order by p3.tsp) > 2.0, false) res_is_met
              from orders_criteria oc left join pat_cvalues p3
                  on oc.enc_id = p3.enc_id and p3.name = 'initial_lactate'
                      and p3.tsp >= oc.measurement_time
              where oc.name = 'initial_lactate_order'
              group by oc.enc_id
          ) initial_lactate_order on pat_cvalues.enc_id = initial_lactate_order.enc_id
          where pat_cvalues.name = 'repeat_lactate_order'
          order by pat_cvalues.tsp
      )
      as ordered
      group by ordered.enc_id, ordered.name
  ),
  repeat_lactate as (
      select rlr.enc_id, rlr.name, rlr.measurement_time, rlr.value,
      (case when rlr.override_value#>>'{0,text}' = 'Ordered' and rlr.override_time < rlr.max_meas_time then null else rlr.override_time end) override_time,
      (case when rlr.override_value#>>'{0,text}' = 'Ordered' and rlr.override_time < rlr.max_meas_time then null else rlr.override_user end) override_user,
      (case when rlr.override_value#>>'{0,text}' = 'Ordered' and rlr.override_time < rlr.max_meas_time then null else rlr.override_value end) override_value,
      rlr.is_met, rlr.update_date
      from repeat_lactate_raw rlr
  )
  select new_criteria.*,
         severe_sepsis_now.severe_sepsis_onset,
         severe_sepsis_now.severe_sepsis_wo_infection_onset,
         severe_sepsis_now.severe_sepsis_wo_infection_initial,
         septic_shock_now.septic_shock_onset
  from (
      select * from severe_sepsis
      union all select *, null as is_acute from septic_shock
      union all select *, null as is_acute from orders_criteria
      union all select *, null as is_acute from repeat_lactate
  ) new_criteria
  left join severe_sepsis_now on new_criteria.enc_id = severe_sepsis_now.enc_id
  left join septic_shock_now on new_criteria.enc_id = septic_shock_now.enc_id
;

drop table septic_shock_now;
drop table septic_shock;
drop table severe_sepsis_now;
drop table severe_sepsis_criteria;
drop table severe_sepsis;
drop table hypoperfusion;
drop table hypotension;
drop table crystalloid_fluid;
drop table ui_septic_shock;
drop table ui_severe_sepsis;
drop table trews;
drop table infection;
--drop index pat_cvalues_by_en;
drop table pat_cvalues;
--drop index pat_cdc_by_enf;
drop table pat_cdc;
drop table pat_bp_sys;
drop table pat_weights;
drop table enc_ids;
return;
END; $function$;



CREATE OR REPLACE FUNCTION get_states_batch(table_name text, where_clause text default '')
    RETURNS table(
        enc_id                               int,
        state                                int,
        severe_sepsis_onset                  timestamptz,
        severe_sepsis_wo_infection_onset     timestamptz,
        severe_sepsis_wo_infection_initial   timestamptz,
        septic_shock_onset                   timestamptz
    )
AS $func$ BEGIN RETURN QUERY EXECUTE
format('select stats.enc_id,
    (
    case
    when ui_deactivate_cnt = 1 then 16
    when ui_severe_sepsis_cnt = 0 and ui_septic_shock_cnt = 1 then (
        -- trews severe sepsis ON
        case when sus_count = 1 and trews_subalert_met > 0 then
            (case
            -- septic shock
            when now() - GREATEST(sus_onset, trews_subalert_onset)  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- ui_sev_sep_3hr_exp
            when now() - GREATEST(sus_onset, trews_subalert_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- ui_sev_sep_6hr_exp
            when now() - ui_septic_shock_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- ui_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 61 -- ui_sev_sep_3hr_com
            else
            60 end)
        -- cms severe sepsis ON
        when sus_count = 1 and sirs_count > 1 and organ_count > 0 then
        -- use ui_septic_shock ON
            (case
            -- septic shock
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- ui_sev_sep_3hr_exp
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- ui_sev_sep_6hr_exp
            when now() - ui_septic_shock_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- ui_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 61 -- ui_sev_sep_3hr_com
            else
            60 end)
        else
            (case
            -- septic shock
            when now() - ui_septic_shock_onset  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- ui_sev_sep_3hr_exp
            when now() - ui_septic_shock_onset  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- ui_sev_sep_6hr_exp
            when now() - ui_septic_shock_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- ui_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 61 -- ui_sev_sep_3hr_com
            else
            60 end)
        end
    )
    when ui_severe_sepsis_cnt = 1 then (
        (
        case
        when ui_septic_shock_cnt = 1 then
            (case
            -- septic shock
            when now() - ui_severe_sepsis_onset  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- ui_sev_sep_3hr_exp
            when now() - ui_severe_sepsis_onset  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- ui_sev_sep_6hr_exp
            when now() - ui_septic_shock_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- ui_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 61 -- ui_sev_sep_3hr_com
            else
            60 end)
        when (fluid_count = 1 and hypotension_count > 0) and hypoperfusion_count = 1 then
            (case
                -- septic shock
                when now() - ui_severe_sepsis_onset  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- ui_sev_sep_3hr_exp
                when now() - ui_severe_sepsis_onset  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- ui_sev_sep_6hr_exp
                when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
                when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
                when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- ui_sev_sep_6hr_com
                when sev_sep_3hr_count = 4 then 61 -- ui_sev_sep_3hr_com
                else
                60 end)
        when (fluid_count = 1 and hypotension_count > 0) then
            (case
                -- septic shock
                when now() - ui_severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- ui_sev_sep_3hr_exp
                when now() - ui_severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- ui_sev_sep_6hr_exp
                when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
                when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
                when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- ui_sev_sep_6hr_com
                when sev_sep_3hr_count = 4 then 61 -- ui_sev_sep_3hr_com
                else
                60 end)
        when hypoperfusion_count = 1 then
            (case
                -- septic shock
                when now() - ui_severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 62 -- trews_sev_sep_3hr_exp
                when now() - ui_severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 64 -- trews_sev_sep_6hr_exp
                when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 66 -- trews_sep_sho_6hr_exp
                when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 65 -- trews_sep_sho_6hr_com
                when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 63 -- trews_sev_sep_6hr_com
                when sev_sep_3hr_count = 4 then 61 -- trews_sev_sep_3hr_com
                else
                60 end)
        when now() - ui_severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 52 -- ui_sev_sep_3hr_exp
        when now() - ui_severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 54 -- ui_sev_sep_6hr_exp
        when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 53 -- ui_sev_sep_6hr_com
        when sev_sep_3hr_count = 4 then 51 -- ui_sev_sep_3hr_com
        else
        -- severe sepsis
        50
        end)
    )
    when sus_count = 1 then
        (
        -- trews severe sepsis already on
        case when state between 25 and 29 and not (trews_orgdf_met = 0 and trews_orgdf_override > 0) then (
            (
            case
            when (fluid_count = 1 and hypotension_count > 0) and hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - severe_sepsis_onset  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
                    when now() - severe_sepsis_onset  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
                    else
                    40 end)
            when (fluid_count = 1 and hypotension_count > 0) then
                (case
                    -- septic shock
                    when now() - severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
                    when now() - severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
                    else
                    40 end)
            when hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
                    when now() - severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
                    else
                    40 end)
            when now() - severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 27 -- trews_sev_sep_3hr_exp
            when now() - severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 29 -- trews_sev_sep_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 28 -- trews_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 26 -- trews_sev_sep_3hr_com
            else
            -- severe sepsis
            25
            end)
        )
        -- trews septic shock already on
        when state between 40 and 46 and not (trews_orgdf_met = 0 and trews_orgdf_override > 0) then (
            (
            case
            -- septic shock
            when now() - severe_sepsis_onset > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
            when now() - severe_sepsis_onset > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
            when now() - septic_shock_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
            else
            40 end
            )
        )
        when trews_subalert_met > 0 then (
            (
            case
            when (fluid_count = 1 and hypotension_count > 0) and hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, trews_subalert_onset)  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, trews_subalert_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
                    else
                    40 end)
            when (fluid_count = 1 and hypotension_count > 0) then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, trews_subalert_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, trews_subalert_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
                    else
                    40 end)
            when hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, trews_subalert_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 42 -- trews_sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, trews_subalert_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 44 -- trews_sev_sep_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 46 -- trews_sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 45 -- trews_sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 43 -- trews_sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 41 -- trews_sev_sep_3hr_com
                    else
                    40 end)
            when now() - GREATEST(sus_onset, trews_subalert_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 27 -- trews_sev_sep_3hr_exp
            when now() - GREATEST(sus_onset, trews_subalert_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 29 -- trews_sev_sep_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 28 -- trews_sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 26 -- trews_sev_sep_3hr_com
            else
            -- severe sepsis
            25
            end)
        )
        when sirs_count > 1 and organ_count > 0 then (
            (
            case
            when (fluid_count = 1 and hypotension_count > 0) and hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 33 -- sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when (fluid_count = 1 and hypotension_count > 0) then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 33 -- sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 33 -- sev_sep_6hr_com
                    when sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
            when sev_sep_6hr_count = 1 and sev_sep_3hr_count = 4 then 23 -- sev_sep_6hr_com
            when sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com
            else
            -- severe sepsis
            20
            end)
        )
        else
            -- inactive
            0
        end
        )
    -- no sus
    when trews_subalert_met > 0 and sus_null_count = 1 then 11 -- trews_sev_sep w.o. sus
    when (trews_subalert_met > 0 and sus_noinf_count = 1) or (trews_orgdf_met = 0 and trews_orgdf_override > 0) then 13 -- trews_sev_sep w.o. sus
    when sirs_count > 1 and organ_count > 0 and sus_null_count = 1 then 10 -- sev_sep w.o. sus
    when sirs_count > 1 and ((organ_count > 0 and sus_noinf_count = 1) or (organ_count = 0 and orgdf_override > 0)) then 12 -- sev_sep w.o. sus
    else 0 -- health case
    end) as state,
    new_severe_sepsis_onset,
    new_severe_sepsis_wo_infection_onset,
    new_severe_sepsis_wo_infection_initial,
    new_septic_shock_onset
from
(
select %I.enc_id
from %I
left join get_states_snapshot(%I.enc_id) GSS on GSS.enc_id = %I.enc_id
%s
group by %I.enc_id
) stats',
table_name, table_name, table_name, table_name, table_name,
table_name, table_name, table_name,
where_clause, table_name)
; END $func$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION advance_criteria_snapshot_batch(enc_query text, func_mode text default 'advance')
RETURNS void AS $$
DECLARE
    ts_end timestamptz := now();
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    execute 'create temporary table adv_enc_ids as ' || enc_query;
    analyze adv_enc_ids;

    perform auto_deactivate(enc_id) from adv_enc_ids R(enc_id);

    create temporary table new_criteria as
        select * from calculate_criteria_on_batch(enc_query, ts_end - window_size, ts_end);

    analyze new_criteria;

    insert into criteria (enc_id, name, measurement_time, value, override_time, override_user, override_value, is_met, update_date, is_acute)
    select enc_id, name, measurement_time, value, override_time, override_user, override_value, is_met, update_date, is_acute
    from new_criteria
    on conflict (enc_id, name) do update
    set is_met              = excluded.is_met,
        measurement_time    = excluded.measurement_time,
        value               = excluded.value,
        update_date         = excluded.update_date,
        is_acute            = excluded.is_acute,
        override_time       = (case when criteria.override_value#>>'{0,text}' = 'Ordered'
                                 then excluded.override_time
                                when criteria.override_value#>>'{0,text}' = 'Ordering' and now() - criteria.override_time > '5 minutes'::interval
                                 then null
                                else criteria.override_time end),
        override_user       = (case when criteria.override_value#>>'{0,text}' = 'Ordered'
                                 then excluded.override_user
                                when criteria.override_value#>>'{0,text}' = 'Ordering' and now() - criteria.override_time > '5 minutes'::interval
                                 then null
                                else criteria.override_user end),
        override_value      = (case when criteria.override_value#>>'{0,text}' = 'Ordered'
                                 then excluded.override_value
                                when criteria.override_value#>>'{0,text}' = 'Ordering' and now() - criteria.override_time > '5 minutes'::interval
                                 then null
                                else criteria.override_value end)
    ;

    create temporary table state_change as
    select coalesce(snapshot.enc_id, live.enc_id) as enc_id,
           coalesce(snapshot.event_id, 0) as from_event_id,
           coalesce(snapshot.state, 0) as state_from,
           live.state as state_to,
           live.severe_sepsis_onset,
           live.severe_sepsis_wo_infection_onset,
           live.severe_sepsis_wo_infection_initial,
           live.septic_shock_onset
    from get_states_batch('new_criteria') live
    left join get_states_snapshot(live.enc_id) snapshot on snapshot.enc_id = live.enc_id
    where snapshot.state is null
    or ( snapshot.state < 10 and live.state > snapshot.state)
    or ( snapshot.state = 16 and live.state <> snapshot.state)
    or ( snapshot.state = 10 and (snapshot.severe_sepsis_wo_infection_onset < now() - window_size or live.state > 10))
    or ( snapshot.state = 11 and live.state <> snapshot.state)
    -- once on cms severe sepsis path way, keep in cms path way
    or ( snapshot.state between 20 and 24
        and (
                (live.state between 30 and 36) or
                (live.state between 20 and 24 and snapshot.state <> 23) or
                (live.state between 60 and 66)
            )
        and snapshot.state < live.state)
    -- once on cms septic shock path way, keep in cms path way
    or ( snapshot.state between 30 and 36 and snapshot.state <> 35
        and live.state between 30 and 36
        and snapshot.state < live.state)
    -- once on trews severe sepsis path way, keep in trews path way
    or ( snapshot.state between 25 and 29
        and (
                (live.state between 40 and 46) or
                (live.state between 25 and 29 and snapshot.state <> 28) or
                (live.state between 60 and 66))
        and snapshot.state < live.state)
    -- once on trews septic shock path way, keep in trews path way
    or ( snapshot.state between 40 and 46 and snapshot.state <> 45
        and live.state between 40 and 46
        and snapshot.state < live.state)
    -- once on ui severe sepsis path way, keep in ui path way
    or ( snapshot.state between 50 and 54
        and (
                (live.state between 50 and 54 and snapshot.state <> 53) or
                (live.state between 60 and 66)
            ) and snapshot.state < live.state)
    -- once on ui septic shock path way, keep in ui path way
    or ( snapshot.state between 60 and 66 and snapshot.state <> 65 and live.state between 60 and 66 and snapshot.state < live.state)
    ;

    analyze state_change;

    update criteria_events
    set flag = flag - 1000
    from state_change
    where criteria_events.event_id = state_change.from_event_id
    and criteria_events.enc_id = state_change.enc_id
    and state_change.state_from >= 0
    ;

    with notified_patients as (
        select distinct si.enc_id
        from state_change si
        left join lateral update_notifications(si.enc_id,
            flag_to_alert_codes(si.state_to),
            si.severe_sepsis_onset,
            si.septic_shock_onset,
            si.severe_sepsis_wo_infection_onset,
            si.severe_sepsis_wo_infection_initial,
            func_mode
            ) n
        on si.enc_id = n.enc_id
    )
    insert into criteria_events (event_id, enc_id, name, measurement_time, value,
                                 override_time, override_user, override_value, is_met, update_date, is_acute, flag)
    select s.event_id, c.enc_id, c.name, c.measurement_time, c.value,
           (case when c.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else c.override_time end),
           (case when c.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else c.override_user end),
           (case when c.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else c.override_value end),
           c.is_met, c.update_date, c.is_acute,
           s.state_to as flag
    from ( select ssid.event_id, si.enc_id, si.state_to
           from state_change si
           cross join (select nextval('criteria_event_ids') event_id) ssid
    ) as s
    inner join new_criteria c on s.enc_id = c.enc_id
    left join notified_patients as np on s.enc_id = np.enc_id
    where not c.name like '%_order';

    drop table state_change;
    drop table new_criteria;

    perform order_event_update(enc_id) from adv_enc_ids R(enc_id);
    drop table adv_enc_ids;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

