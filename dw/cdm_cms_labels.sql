DROP TABLE IF EXISTS label_version CASCADE;
CREATE TABLE label_version (
    label_id        serial primary key,
    created         timestamptz,
    description     text
);

DROP TABLE IF EXISTS cdm_labels;
CREATE TABLE cdm_labels (
    dataset_id          integer references dw_version(dataset_id),
    label_id            integer references label_version(label_id),
    pat_id              text,
    tsp                 timestamptz,
    label_type          text,
    label               integer,
    primary key         (dataset_id, label_id, pat_id, tsp)
);


-- get_cms_labels_for_window
-- Duplicate of current DW calculate_criteria
CREATE OR REPLACE FUNCTION get_cms_labels_for_window(
                this_pat_id                      text,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                use_clarity_notes                boolean default false
  )
  RETURNS table(pat_id                           varchar(50),
                name                             varchar(50),
                measurement_time                 timestamptz,
                value                            text,
                override_time                    timestamptz,
                override_user                    text,
                override_value                   json,
                is_met                           boolean,
                update_date                      timestamptz,
                severe_sepsis_onset              timestamptz,
                severe_sepsis_wo_infection_onset timestamptz,
                septic_shock_onset               timestamptz
  )
 LANGUAGE plpgsql
AS $function$
DECLARE
  window_size interval := ts_end - ts_start;

  -- Lookback before the initial severe sepsis indicator.
  orders_lookback interval := interval '6 hours';
BEGIN

  select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
  -- raise notice 'Running calculate criteria on pat %, dataset_id % between %, %',this_pat_id, _dataset_id, ts_start , ts_end;

  return query
  with pat_visit_ids as (
    select distinct P.pat_id, P.visit_id from pat_enc P
    where P.pat_id = coalesce(this_pat_id, P.pat_id) and P.dataset_id = _dataset_id
  ),
  pat_ids as (
    select distinct P.pat_id from pat_visit_ids P
  ),
  suspicion_of_infection_buff as (
    -- Use either clarity or cdm notes for now.
    -- We implement this as a union over two queries, each gated
    -- by a simple where clause based on an function argument.
    with clarity_matches as (
        select _dataset_id as dataset_id,
               P.pat_id as pat_id,
               'suspicion_of_infection'::text as name,
               true as is_met,
               min(M.start_ts) as measurement_time,
               min(M.start_ts) as override_time,
               'NLP'::text as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               min(M.ngram) as value,
               now() as update_date
        from pat_visit_ids P
        inner join lateral match_clarity_infections(P.visit_id, 3, 3) M on P.visit_id = M.csn_id
        where use_clarity_notes
        group by P.pat_id
    ),
    cdm_matches as (
        -- TODO: we have picked an arbitrary time interval for notes. Refine.
        select _dataset_id as dataset_id,
               P.pat_id as pat_id,
               'suspicion_of_infection'::text as name,
               true as is_met,
               min(M.start_ts) as measurement_time,
               min(M.start_ts) as override_time,
               'NLP'::text as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               min(M.ngram) as value,
               now() as update_date
        from pat_ids P
        inner join lateral match_cdm_infections(P.pat_id, _dataset_id, 3, 3) M
          on P.pat_id = M.pat_id
          and M.start_ts between ts_start - interval '1 days' and ts_end + interval '1 days'
        where not use_clarity_notes
        group by P.pat_id
    )
    select * from clarity_matches
    union all
    select * from cdm_matches
  ),
  pat_cvalues as (
    select pat_ids.pat_id,
           cd.name,
           meas.fid,
           cd.category,
           meas.tsp,
           meas.value,
           c.override_time as c_otime,
           c.override_user as c_ouser,
           c.override_value as c_ovalue,
           cd.override_value as d_ovalue
    from pat_ids
    cross join criteria_default as cd
    left join suspicion_of_infection_buff c
      on pat_ids.pat_id = c.pat_id
      and cd.name = c.name
      and cd.dataset_id = c.dataset_id
    left join criteria_meas meas
        on pat_ids.pat_id = meas.pat_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null or meas.tsp between ts_start - window_size and ts_end)
    where cd.dataset_id = _dataset_id
  ),
  pat_bp_sys as (
    select pat_ids.pat_id, avg(sbp_meas.value::numeric) as value
    from pat_ids
    inner join criteria_meas sbp_meas on pat_ids.pat_id = sbp_meas.pat_id
    where isnumeric(sbp_meas.value) and sbp_meas.fid = 'bp_sys' and sbp_meas.dataset_id = _dataset_id
    group by pat_ids.pat_id
  ),
  pat_urine_output as (
      select P.pat_id, sum(uo.value::numeric) as value
      from pat_ids P
      inner join criteria_meas uo on P.pat_id = uo.pat_id
      where uo.fid = 'urine_output' and uo.dataset_id = _dataset_id
      and isnumeric(uo.value)
      and ts_end - uo.tsp < interval '2 hours'
      group by P.pat_id
  ),
  pat_weights as (
    select ordered.pat_id, first(ordered.value) as value
    from (
        select P.pat_id, weights.value::numeric as value
        from pat_ids P
        inner join criteria_meas weights on P.pat_id = weights.pat_id
        where weights.fid = 'weight'  and weights.dataset_id = _dataset_id
        order by weights.tsp
    ) as ordered
    group by ordered.pat_id
  ),
  infection as (
      select
          ordered.pat_id,
          ordered.name,
          first(ordered.measurement_time) as measurement_time,
          first(ordered.value)::text as value,
          first(ordered.c_otime) as override_time,
          first(ordered.c_ouser) as override_user,
          first(ordered.c_ovalue) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from (
          select  PC.pat_id,
                  PC.name,
                  PC.tsp as measurement_time,
                  PC.value as value,
                  PC.c_otime,
                  PC.c_ouser,
                  PC.c_ovalue,
                  (coalesce(PC.c_ovalue#>>'{0,text}', PC.value) <> 'No Infection') as is_met
          from pat_cvalues PC
          where PC.name = 'suspicion_of_infection'
          order by PC.tsp
      ) as ordered
      group by ordered.pat_id, ordered.name
  ),
  sirs as (
      select
          ordered.pat_id,
          ordered.name,
          first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
          first(case when ordered.is_met then ordered.value else null end)::text as value,
          first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
          first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
          first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from (
          select  PC.pat_id,
                  PC.name,
                  PC.tsp as measurement_time,
                  PC.value as value,
                  PC.c_otime,
                  PC.c_ouser,
                  PC.c_ovalue,
                  criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue) as is_met
          from pat_cvalues PC
          where PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
          order by PC.tsp
      ) as ordered
      group by ordered.pat_id, ordered.name
  ),
  respiratory_failures as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.tsp else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from (
        select
            PC.pat_id,
            PC.name,
            PC.tsp,
            (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value))) as value,
            PC.c_otime,
            PC.c_ouser,
            PC.c_ovalue,
            (coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null) as is_met
        from pat_cvalues PC
        where PC.category = 'respiratory_failure'
        order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  organ_dysfunction_except_rf as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from (
        select  PC.pat_id,
                PC.name,
                PC.tsp as measurement_time,
                PC.value as value,
                PC.c_otime,
                PC.c_ouser,
                PC.c_ovalue,
                (case
                    when PC.category = 'decrease_in_sbp' then
                        decrease_in_sbp_met(
                            (select max(PBP.value) from pat_bp_sys PBP where PBP.pat_id = PC.pat_id),
                            PC.value, PC.c_ovalue, PC.d_ovalue)

                    when PC.category = 'urine_output' then
                        urine_output_met(
                            (select max(pat_urine_output.value) from pat_urine_output where pat_urine_output.pat_id = PC.pat_id),
                            (select max(pat_weights.value) from pat_weights where pat_weights.pat_id = PC.pat_id),
                            _dataset_id
                        )

                    else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                    end
                ) as is_met
        from pat_cvalues PC
        where PC.name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate')
        order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  severe_sepsis as (
    select * from infection
    union all select * from sirs
    union all select * from respiratory_failures
    union all select * from organ_dysfunction_except_rf
  ),

  -- Calculate severe sepsis in an extended window, for use in
  -- any criteria that has requirements after severe sepsis is met.
  severe_sepsis_criteria as (
      with organ_dysfunction as (
          select * from respiratory_failures
          union all select * from organ_dysfunction_except_rf
      )
      select IC.pat_id,
             sum(IC.cnt) > 0 as suspicion_of_infection,
             sum(SC.cnt) as sirs_cnt,
             sum(OC.cnt) as org_df_cnt,
             max(IC.onset) as inf_onset,
             max(SC.initial) as sirs_initial,
             max(SC.onset) as sirs_onset,
             max(OC.onset) as org_df_onset
      from
      (
        select infection.pat_id,
               sum(case when infection.is_met then 1 else 0 end) as cnt,
               max(infection.override_time) as onset
        from infection
        group by infection.pat_id
      ) IC
      left join
      (
        select sirs.pat_id,
               sum(case when sirs.is_met then 1 else 0 end) as cnt,
               (array_agg(sirs.measurement_time order by sirs.measurement_time))[1] as initial,
               (array_agg(sirs.measurement_time order by sirs.measurement_time))[2] as onset
        from sirs
        group by sirs.pat_id
      ) SC on IC.pat_id = SC.pat_id
      left join
      (
        select organ_dysfunction.pat_id,
               sum(case when organ_dysfunction.is_met then 1 else 0 end) as cnt,
               min(organ_dysfunction.measurement_time) as onset
        from organ_dysfunction
        group by organ_dysfunction.pat_id
      ) OC on IC.pat_id = OC.pat_id
      where greatest(SC.onset, OC.onset) - least(SC.initial, OC.onset) < window_size
      group by IC.pat_id
  ),
  severe_sepsis_onsets as (
    select sspm.pat_id,
           sspm.severe_sepsis_is_met,
           (case when sspm.severe_sepsis_onset <> 'infinity'::timestamptz
                 then sspm.severe_sepsis_onset
                 else null end
           ) as severe_sepsis_onset,
           (case when sspm.severe_sepsis_wo_infection_onset <> 'infinity'::timestamptz
                 then sspm.severe_sepsis_wo_infection_onset
                 else null end
           ) as severe_sepsis_wo_infection_onset,
           sspm.severe_sepsis_lead_time
    from (
      select stats.pat_id,
             coalesce(bool_or(stats.suspicion_of_infection
                                and stats.sirs_cnt > 1
                                and stats.org_df_cnt > 0)
                      , false
                      ) as severe_sepsis_is_met,

             max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                          coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_onset,

             max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_wo_infection_onset,

             min(least(stats.inf_onset, stats.sirs_initial, stats.org_df_onset))
                as severe_sepsis_lead_time

      from severe_sepsis_criteria stats
      group by stats.pat_id
    ) sspm
  ),

  crystalloid_fluid as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
      select  PC.pat_id,
              PC.name,
              PC.tsp as measurement_time,
              PC.value as value,
              PC.c_otime,
              PC.c_ouser,
              PC.c_ovalue,
              ( case
                when coalesce(PC.c_ovalue#>>'{0,text}' = 'Not Indicated', false)
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                else PC.fid = 'crystalloid_fluid' and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                end )
              and (SSP.severe_sepsis_is_met
                    and coalesce(PC.c_otime, PC.tsp) >= SSP.severe_sepsis_onset)
              as is_met
      from pat_cvalues PC
      left join severe_sepsis_onsets SSP on PC.pat_id = SSP.pat_id
      where PC.name = 'crystalloid_fluid'
      order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  hypotension as (
      select
          ordered.pat_id,
          ordered.name,
          first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
          first(case when ordered.is_met then ordered.value else null end)::text as value,
          first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
          first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
          first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from
      (
          with pat_fluid_overrides as (
            select CFL.pat_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated'), false) as override
            from crystalloid_fluid CFL
            group by CFL.pat_id
          ),
          pats_fluid_after_severe_sepsis as (
            select  MFL.pat_id,
                    MFL.tsp,
                    sum(MFL.value::numeric) as total_fluid,
                    -- Fluids are met if they are overriden or if we have more than
                    -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
                    (coalesce(bool_or(OV.override), false)
                        or coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.value))
                    ) as is_met
            from criteria_meas MFL
            left join pat_weights PW on MFL.pat_id = PW.pat_id
            left join severe_sepsis_onsets SSPN on MFL.pat_id = SSPN.pat_id
            left join pat_fluid_overrides OV on MFL.pat_id = OV.pat_id
            where isnumeric(MFL.value)
            and SSPN.severe_sepsis_is_met
            and MFL.tsp >= (SSPN.severe_sepsis_onset - orders_lookback)
            and (MFL.fid = 'crystalloid_fluid' or coalesce(OV.override, false))
            group by MFL.pat_id, MFL.tsp
          )
          select PC.pat_id,
                 PC.name,
                 PC.tsp as measurement_time,
                 PC.value as value,
                 PC.c_otime,
                 PC.c_ouser,
                 PC.c_ovalue,
                 (SSPN.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp) >= SSPN.severe_sepsis_onset)
                 and
                 (case when PC.category = 'hypotension' then
                         (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                         and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                         and criteria_value_met(NEXT.value, PC.c_ovalue, PC.d_ovalue)

                       when PC.category = 'hypotension_dsbp' then
                         (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                         and decrease_in_sbp_met(PBPSYS.value, PC.value, PC.c_ovalue, PC.d_ovalue)
                         and decrease_in_sbp_met(PBPSYS.value, NEXT.value, PC.c_ovalue, PC.d_ovalue)

                      else false
                  end) as is_met
          from pat_cvalues PC
          left join severe_sepsis_onsets SSPN on PC.pat_id = SSPN.pat_id

          left join pats_fluid_after_severe_sepsis PFL
            on PC.pat_id = PFL.pat_id

          left join lateral (
            select meas.pat_id, meas.fid, meas.tsp, meas.value
            from criteria_meas meas
            where PC.pat_id = meas.pat_id and PC.fid = meas.fid and PC.tsp < meas.tsp
            order by meas.tsp
            limit 1
          ) NEXT on PC.pat_id = NEXT.pat_id and PC.fid = NEXT.fid

          left join lateral (
            select BP.pat_id, max(BP.value) as value
            from pat_bp_sys BP where PC.pat_id = BP.pat_id
            group by BP.pat_id
          ) PBPSYS on PC.pat_id = PBPSYS.pat_id

          where PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
          order by PC.tsp
      ) as ordered
      group by ordered.pat_id, ordered.name
  ),
  hypoperfusion as (
      select
          ordered.pat_id,
          ordered.name,
          first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
          first(case when ordered.is_met then ordered.value else null end)::text as value,
          first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
          first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
          first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from
      (
          select  PC.pat_id,
                  PC.name,
                  PC.tsp as measurement_time,
                  PC.value as value,
                  PC.c_otime,
                  PC.c_ouser,
                  PC.c_ovalue,
                  criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                      and (SSP.severe_sepsis_onset is not null
                              and coalesce(PC.c_otime, PC.tsp) >= SSP.severe_sepsis_onset)
                      as is_met
          from pat_cvalues PC
          left join severe_sepsis_onsets SSP on PC.pat_id = SSP.pat_id
          where PC.name = 'initial_lactate'
          order by PC.tsp
      ) as ordered
      group by ordered.pat_id, ordered.name
  ),
  septic_shock as (
    select * from crystalloid_fluid
    union all select * from hypotension
    union all select * from hypoperfusion
  ),

  -- Calculate septic shock in an extended window, for use in
  -- any criteria that has requirements after severe sepsis is met.
  septic_shock_onsets as (
    select stats.pat_id,
           bool_or(stats.cnt > 0) as septic_shock_is_met,
           greatest(min(stats.onset), max(SSP.severe_sepsis_onset)) as septic_shock_onset
    from (
        -- Hypotension and hypoperfusion subqueries individually check
        -- that they occur after severe sepsis onset.
        (select hypotension.pat_id,
                sum(case when hypotension.is_met then 1 else 0 end) as cnt,
                min(hypotension.measurement_time) as onset
         from hypotension
         group by hypotension.pat_id)
        union
        (select hypoperfusion.pat_id,
                sum(case when hypoperfusion.is_met then 1 else 0 end) as cnt,
                min(hypoperfusion.measurement_time) as onset
         from hypoperfusion
         group by hypoperfusion.pat_id)
    ) stats
    left join severe_sepsis_onsets SSP on stats.pat_id = SSP.pat_id
    group by stats.pat_id
  ),

  orders_criteria as (
    select
        ordered.pat_id,
        ordered.name,
        coalesce(   first(case when ordered.is_met then ordered.measurement_time else null end),
                    last(ordered.measurement_time)
        ) as measurement_time,
        coalesce(   first(case when ordered.is_met then ordered.value else null end)::text,
                    last(ordered.value)::text
        ) as value,
        coalesce(   first(case when ordered.is_met then ordered.c_otime else null end),
                    last(ordered.c_otime)
        ) as override_time,
        coalesce(   first(case when ordered.is_met then ordered.c_ouser else null end),
                    last(ordered.c_ouser)
        ) as override_user,
        coalesce(
            first(case when ordered.is_met then ordered.c_ovalue else null end),
            last(ordered.c_ovalue)
        ) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
      -- We calculate extended_pat_cvalues for orders only based on the
      -- severe_sepsis_lead_time above. This lets us search for orders
      -- before the initial indicator of sepsis.
      --
      -- We cannot calculate pat_cvalues in one go to address both the
      -- need for severe_sepsis_lead_time and orders, since there is a
      -- dependency between the two calculations.
      --
      with orders_cvalues as (
        select * from pat_cvalues CV
        where CV.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
        union all
        select pat_ids.pat_id,
               cd.name,
               meas.fid,
               cd.category,
               meas.tsp,
               meas.value,
               c.override_time as c_otime,
               c.override_user as c_ouser,
               c.override_value as c_ovalue,
               cd.override_value as d_ovalue
        from pat_ids
        cross join criteria_default as cd
        left join severe_sepsis_onsets SSP
          on pat_ids.pat_id = SSP.pat_id
        left join suspicion_of_infection_buff c
          on pat_ids.pat_id = c.pat_id
          and cd.name = c.name
          and cd.dataset_id = c.dataset_id
        left join criteria_meas meas
            on pat_ids.pat_id = meas.pat_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              -- This predicate safely returns no rows if
              -- severe_sepsis_lead_time - orders_lookback
              -- is chronologically before ts_start - window_size
              or meas.tsp between SSP.severe_sepsis_lead_time - orders_lookback
                          and ts_start - window_size
            )
        where cd.dataset_id = _dataset_id
        and cd.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
      )
      select  CV.pat_id,
              CV.name,
              CV.tsp as measurement_time,
              (case when CV.category in ('after_severe_sepsis_dose', 'after_septic_shock_dose')
                      then dose_order_status(CV.fid, CV.c_ovalue#>>'{0,text}')
                    else order_status(CV.fid, CV.value, CV.c_ovalue#>>'{0,text}')
               end) as value,
              CV.c_otime,
              CV.c_ouser,
              CV.c_ovalue,
              (case
                  when CV.category = 'after_severe_sepsis' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (SSP.severe_sepsis_lead_time - orders_lookback))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.pat_id = coalesce(this_pat_id, SSP.pat_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_severe_sepsis_dose' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (SSP.severe_sepsis_lead_time - orders_lookback))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.pat_id = coalesce(this_pat_id, SSP.pat_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                            coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                     (CV.d_ovalue#>>'{lower}')::numeric)) )

                  when CV.category = 'after_septic_shock' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.pat_id = coalesce(this_pat_id, SSH.pat_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_septic_shock_dose' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.pat_id = coalesce(this_pat_id, SSH.pat_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                            coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                     (CV.d_ovalue#>>'{lower}')::numeric)) )

                  else criteria_value_met(CV.value, CV.c_ovalue, CV.d_ovalue)
                  end
              ) as is_met
      from orders_cvalues CV
      order by CV.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  repeat_lactate as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
        select  pat_cvalues.pat_id,
                pat_cvalues.name,
                pat_cvalues.tsp as measurement_time,
                order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}') as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                ((
                  coalesce(initial_lactate_order.is_met and lactate_results.is_met, false)
                    and order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value))
                    and (coalesce(pat_cvalues.tsp > initial_lactate_order.tsp, false)
                            and coalesce(lactate_results.tsp > initial_lactate_order.tsp, false))
                ) or
                (
                  not( coalesce(initial_lactate_order.is_completed
                                  and ( lactate_results.is_met or pat_cvalues.tsp <= initial_lactate_order.tsp )
                                , false) )
                )) is_met
        from pat_cvalues
        left join (
            select oc.pat_id,
                   max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                   coalesce(bool_or(oc.is_met), false) as is_met,
                   coalesce(min(oc.value) = 'Completed', false) as is_completed
            from orders_criteria oc
            where oc.name = 'initial_lactate_order'
            group by oc.pat_id
        ) initial_lactate_order on pat_cvalues.pat_id = initial_lactate_order.pat_id
        left join (
            select p3.pat_id,
                   max(case when p3.value::numeric > 2.0 then p3.tsp else null end) tsp,
                   coalesce(bool_or(p3.value::numeric > 2.0), false) is_met
            from pat_cvalues p3
            where p3.name = 'initial_lactate'
            group by p3.pat_id
        ) lactate_results on pat_cvalues.pat_id = lactate_results.pat_id
        where pat_cvalues.name = 'repeat_lactate_order'
        order by pat_cvalues.tsp
    )
    as ordered
    group by ordered.pat_id, ordered.name
  )
  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         SSH.septic_shock_onset
  from (
      select * from severe_sepsis SSP
      union all select * from septic_shock SSH
      union all select * from orders_criteria
      union all select * from repeat_lactate
  ) new_criteria
  left join severe_sepsis_onsets SSP on new_criteria.pat_id = SSP.pat_id
  left join septic_shock_onsets SSH on new_criteria.pat_id = SSH.pat_id;

  return;
END; $function$;


-- get_cms_labels_for_window_inlined
-- Inlined version of calculate_criteria, using fewer passes
-- TODO: compare to get_cms_labels_for_window
CREATE OR REPLACE FUNCTION get_cms_labels_for_window_inlined(
                this_pat_id                      text,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                use_app_infections               boolean default false,
                use_clarity_notes                boolean default false
  )
  RETURNS table(pat_id                           varchar(50),
                name                             varchar(50),
                measurement_time                 timestamptz,
                value                            text,
                override_time                    timestamptz,
                override_user                    text,
                override_value                   json,
                is_met                           boolean,
                update_date                      timestamptz,
                severe_sepsis_onset              timestamptz,
                severe_sepsis_wo_infection_onset timestamptz,
                septic_shock_onset               timestamptz
  )
 LANGUAGE plpgsql
AS $function$
DECLARE
  window_size interval := ts_end - ts_start;

  -- Lookback before the initial severe sepsis indicator.
  orders_lookback interval := interval '6 hours';
BEGIN

  select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
  -- raise notice 'Running calculate criteria on pat %, dataset_id % between %, %',this_pat_id, _dataset_id, ts_start , ts_end;

  return query
  with pat_visit_ids as (
    select distinct P.pat_id, P.visit_id from pat_enc P
    where P.pat_id = coalesce(this_pat_id, P.pat_id) and P.dataset_id = _dataset_id
  ),
  pat_ids as (
    select distinct P.pat_id from pat_visit_ids P
  ),
  user_overrides as (
    -- TODO: dispatch to override generator
    select * from (
      values (null::integer, null::varchar, null::varchar, null::varchar, null::varchar, null::timestamptz, null::text, null::timestamptz, null::text, null::json)
    ) as criteria (dataset_id, pat_id, name, fid, category, tsp, value, override_time, override_user, override_value)
  ),
  pat_cvalues as (
    select pat_ids.pat_id,
           cd.name,
           meas.fid,
           cd.category,
           meas.tsp,
           meas.value,
           c.override_time as c_otime,
           c.override_user as c_ouser,
           c.override_value as c_ovalue,
           cd.override_value as d_ovalue
    from pat_ids
    cross join criteria_default as cd
    left join user_overrides c
      on pat_ids.pat_id = c.pat_id
      and cd.name = c.name
      and cd.dataset_id = c.dataset_id
    left join criteria_meas meas
        on pat_ids.pat_id = meas.pat_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null or meas.tsp between ts_start - window_size and ts_end)
    where cd.dataset_id = _dataset_id
  ),
  pat_aggregates as (
    select ordered.pat_id,
           avg(ordered.bp_sys) as bp_sys,
           first(ordered.weight) as weight,
           sum(ordered.urine_output) as urine_output
    from (
        select P.pat_id,
               (case when meas.fid = 'bp_sys' then meas.value::numeric else null end) as bp_sys,
               (case when meas.fid = 'weight' then meas.value::numeric else null end) as weight,
               (case when meas.fid = 'urine_output'
                     and ts_end - meas.tsp < interval '2 hours'
                     then meas.value::numeric else null end
                ) as urine_output
        from pat_ids P
        inner join criteria_meas meas on P.pat_id = meas.pat_id
        where meas.fid in ('bp_sys', 'urine_output', 'weight')
        and isnumeric(meas.value)
        and meas.dataset_id = _dataset_id
        order by meas.tsp
    ) as ordered
    group by ordered.pat_id
  ),
  sirs_and_org_df_criteria as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from (
      select  PC.pat_id,
              PC.name,
              PC.tsp as measurement_time,
              (case
                when PC.name = 'respiratory_failure'
                then (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value)))
                else PC.value
               end) as value,
              PC.c_otime,
              PC.c_ouser,
              PC.c_ovalue,
              (case
                when PC.name in (
                  'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp',
                  'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                )
                then
                  (case
                    when PC.category = 'decrease_in_sbp' then
                      decrease_in_sbp_met(
                        (select max(PBP.bp_sys) from pat_aggregates PBP where PBP.pat_id = PC.pat_id),
                        PC.value, PC.c_ovalue, PC.d_ovalue
                      )

                    when PC.category = 'urine_output' then
                      urine_output_met(
                          (select max(PUO.urine_output) from pat_aggregates PUO where PUO.pat_id = PC.pat_id),
                          (select max(PW.weight) from pat_aggregates PW where PW.pat_id = PC.pat_id),
                          _dataset_id
                      )

                    else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                    end
                  )

                else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
               end) as is_met
      from pat_cvalues PC
      where PC.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
      order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  severe_sepsis_wo_infection as (
    select CO.pat_id,
           sum(CO.sirs_cnt) as sirs_cnt,
           sum(CO.org_df_cnt) as org_df_cnt,
           max(CO.sirs_initial) as sirs_initial,
           max(CO.sirs_onset) as sirs_onset,
           max(CO.org_df_onset) as org_df_onset
    from (
      select S.pat_id,

             sum(case when S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
                      then 1
                      else 0
                 end) as sirs_cnt,

             (array_agg(
                (case
                  when S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
                  then S.measurement_time else null end
                ) order by S.measurement_time)
              )[1] as sirs_initial,

             (array_agg(
                (case
                  when S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
                  then S.measurement_time else null end
                ) order by S.measurement_time)
              )[2] as sirs_onset,

             sum(case when S.name in (
                        'respiratory_failure',
                        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                      )
                      and S.is_met
                      then 1
                      else 0
                 end
             ) as org_df_cnt,

             min(case when S.name in (
                        'respiratory_failure',
                        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                      )
                      and S.is_met
                      then S.measurement_time
                      else null
                 end
              ) as org_df_onset
      from sirs_and_org_df_criteria S
      group by S.pat_id
    ) CO
    where greatest(CO.sirs_onset, CO.org_df_onset) - least(CO.sirs_initial, CO.org_df_onset) < window_size
    group by CO.pat_id
  ),
  app_infections as (
      select
          ordered.pat_id,
          ordered.name,
          first(ordered.measurement_time) as measurement_time,
          first(ordered.value)::text as value,
          first(ordered.c_otime) as override_time,
          first(ordered.c_ouser) as override_user,
          first(ordered.c_ovalue) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from (
          select  PC.pat_id,
                  PC.name,
                  PC.tsp as measurement_time,
                  PC.value as value,
                  PC.c_otime,
                  PC.c_ouser,
                  PC.c_ovalue,
                  (coalesce(PC.c_ovalue#>>'{0,text}', PC.value) <> 'No Infection') as is_met
          from pat_cvalues PC
          where PC.name = 'suspicion_of_infection'
          and use_app_infections
          order by PC.tsp
      ) as ordered
      group by ordered.pat_id, ordered.name
  ),
  extracted_infections as (
    -- Use either clarity or cdm notes for now.
    -- We implement this as a union over two queries, each gated
    -- by a simple where clause based on an function argument.
    with notes_candidates as (
      select distinct SPWOI.pat_id
      from severe_sepsis_wo_infection SPWOI
      where SPWOI.sirs_cnt > 1 and SPWOI.org_df_cnt > 0
      group by SPWOI.pat_id
    ),
    clarity_matches as (
        select P.pat_id as pat_id,
               'suspicion_of_infection'::text as name,
               min(M.start_ts) as measurement_time,
               min(M.ngram) as value,
               min(M.start_ts) as override_time,
               'NLP'::text as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true as is_met,
               now() as update_date
        from notes_candidates NC
        inner join pat_visit_ids P on NC.pat_id = P.pat_id
        inner join lateral match_clarity_infections(P.visit_id, 3, 3) M on P.visit_id = M.csn_id
        where use_clarity_notes
        and not use_app_infections
        group by P.pat_id
    ),
    cdm_matches as (
        -- TODO: we have picked an arbitrary time interval for notes. Refine.
        select NC.pat_id as pat_id,
               'suspicion_of_infection'::text as name,
               min(M.start_ts) as measurement_time,
               min(M.ngram) as value,
               min(M.start_ts) as override_time,
               'NLP'::text as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true as is_met,
               now() as update_date
        from notes_candidates NC
        inner join lateral match_cdm_infections(NC.pat_id, _dataset_id, 3, 3) M
          on NC.pat_id = M.pat_id
          and M.start_ts between ts_start - interval '1 days' and ts_end + interval '1 days'
        where not use_clarity_notes
        and not use_app_infections
        group by NC.pat_id
    )
    select * from clarity_matches
    union all
    select * from cdm_matches
  ),
  infections as (
    select * from app_infections
    union all select * from extracted_infections
  ),
  severe_sepsis as (
    select * from sirs_and_org_df_criteria
    union all select * from infections
  ),
  severe_sepsis_criteria_stats as (
    select CO.pat_id,
           sum(I.infection_cnt) > 0 as suspicion_of_infection,
           sum(CO.sirs_cnt) as sirs_cnt,
           sum(CO.org_df_cnt) as org_df_cnt,
           max(I.infection_onset) as inf_onset,
           max(CO.sirs_initial) as sirs_initial,
           max(CO.sirs_onset) as sirs_onset,
           max(CO.org_df_onset) as org_df_onset
    from severe_sepsis_wo_infection CO
    inner join (
      select I.pat_id,
             sum(case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             max(case when I.name = 'suspicion_of_infection' then I.override_time else null end) as infection_onset
      from infections I
      group by I.pat_id
    ) I on CO.pat_id = I.pat_id
    group by CO.pat_id
  ),
  severe_sepsis_onsets as (
    select sspm.pat_id,
           sspm.severe_sepsis_is_met,
           (case when sspm.severe_sepsis_onset <> 'infinity'::timestamptz
                 then sspm.severe_sepsis_onset
                 else null end
           ) as severe_sepsis_onset,
           (case when sspm.severe_sepsis_wo_infection_onset <> 'infinity'::timestamptz
                 then sspm.severe_sepsis_wo_infection_onset
                 else null end
           ) as severe_sepsis_wo_infection_onset,
           sspm.severe_sepsis_lead_time
    from (
      select stats.pat_id,
             coalesce(bool_or(stats.suspicion_of_infection
                                and stats.sirs_cnt > 1
                                and stats.org_df_cnt > 0)
                      , false
                      ) as severe_sepsis_is_met,

             max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                          coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_onset,

             max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_wo_infection_onset,

             min(least(stats.inf_onset, stats.sirs_initial, stats.org_df_onset))
                as severe_sepsis_lead_time

      from severe_sepsis_criteria_stats stats
      group by stats.pat_id
    ) sspm
  ),

  crystalloid_fluid_and_hypoperfusion as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
      select  PC.pat_id,
              PC.name,
              PC.tsp as measurement_time,
              PC.value as value,
              PC.c_otime,
              PC.c_ouser,
              PC.c_ovalue,
              (case
                when PC.name = 'initial_lactate'
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                else (
                  case
                  when coalesce(PC.c_ovalue#>>'{0,text}' = 'Not Indicated', false)
                  then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  else PC.fid = 'crystalloid_fluid' and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  end
                )
                end
              ) and (SSP.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp) >= SSP.severe_sepsis_onset)
              as is_met
      from pat_cvalues PC
      left join severe_sepsis_onsets SSP on PC.pat_id = SSP.pat_id
      where PC.name in ( 'crystalloid_fluid', 'initial_lactate' )
      order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  hypotension as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
        with pat_fluid_overrides as (
          select CFL.pat_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated'), false) as override
          from crystalloid_fluid_and_hypoperfusion CFL
          where CFL.name = 'crystalloid_fluid'
          group by CFL.pat_id
        ),
        pats_fluid_after_severe_sepsis as (
          select  MFL.pat_id,
                  MFL.tsp,
                  sum(MFL.value::numeric) as total_fluid,
                  -- Fluids are met if they are overriden or if we have more than
                  -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
                  (coalesce(bool_or(OV.override), false)
                      or coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.weight))
                  ) as is_met
          from criteria_meas MFL
          left join pat_aggregates PW on MFL.pat_id = PW.pat_id
          left join severe_sepsis_onsets SSPN on MFL.pat_id = SSPN.pat_id
          left join pat_fluid_overrides OV on MFL.pat_id = OV.pat_id
          where isnumeric(MFL.value)
          and SSPN.severe_sepsis_is_met
          and MFL.tsp >= (SSPN.severe_sepsis_onset - orders_lookback)
          and (MFL.fid = 'crystalloid_fluid' or coalesce(OV.override, false))
          group by MFL.pat_id, MFL.tsp
        )
        select PC.pat_id,
               PC.name,
               PC.tsp as measurement_time,
               PC.value as value,
               PC.c_otime,
               PC.c_ouser,
               PC.c_ovalue,
               (SSPN.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp) >= SSPN.severe_sepsis_onset)
               and
               (case when PC.category = 'hypotension' then
                       (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                       and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                       and criteria_value_met(NEXT.value, PC.c_ovalue, PC.d_ovalue)

                     when PC.category = 'hypotension_dsbp' then
                       (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                       and decrease_in_sbp_met(PBPSYS.value, PC.value, PC.c_ovalue, PC.d_ovalue)
                       and decrease_in_sbp_met(PBPSYS.value, NEXT.value, PC.c_ovalue, PC.d_ovalue)

                    else false
                end) as is_met
        from pat_cvalues PC
        left join severe_sepsis_onsets SSPN on PC.pat_id = SSPN.pat_id

        left join pats_fluid_after_severe_sepsis PFL
          on PC.pat_id = PFL.pat_id

        left join lateral (
          select meas.pat_id, meas.fid, meas.tsp, meas.value
          from criteria_meas meas
          where PC.pat_id = meas.pat_id and PC.fid = meas.fid and PC.tsp < meas.tsp
          order by meas.tsp
          limit 1
        ) NEXT on PC.pat_id = NEXT.pat_id and PC.fid = NEXT.fid

        left join lateral (
          select BP.pat_id, max(BP.bp_sys) as value
          from pat_aggregates BP where PC.pat_id = BP.pat_id
          group by BP.pat_id
        ) PBPSYS on PC.pat_id = PBPSYS.pat_id

        where PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
        order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),

  septic_shock as (
    select * from crystalloid_fluid_and_hypoperfusion
    union all select * from hypotension
  ),

  -- Calculate septic shock in an extended window, for use in
  -- any criteria that has requirements after severe sepsis is met.
  septic_shock_onsets as (
    select stats.pat_id,
           bool_or(stats.cnt > 0) as septic_shock_is_met,
           greatest(min(stats.onset), max(SSP.severe_sepsis_onset)) as septic_shock_onset
    from (
        -- Hypotension and hypoperfusion subqueries individually check
        -- that they occur after severe sepsis onset.
        (select hypotension.pat_id,
                sum(case when hypotension.is_met then 1 else 0 end) as cnt,
                min(hypotension.measurement_time) as onset
         from hypotension
         group by hypotension.pat_id)
        union
        (select HPF.pat_id,
                sum(case when HPF.is_met then 1 else 0 end) as cnt,
                min(HPF.measurement_time) as onset
         from crystalloid_fluid_and_hypoperfusion HPF
         where HPF.name = 'initial_lactate'
         group by HPF.pat_id)
    ) stats
    left join severe_sepsis_onsets SSP on stats.pat_id = SSP.pat_id
    group by stats.pat_id
  ),

  orders_criteria as (
    select
        ordered.pat_id,
        ordered.name,
        coalesce(   first(case when ordered.is_met then ordered.measurement_time else null end),
                    last(ordered.measurement_time)
        ) as measurement_time,
        coalesce(   first(case when ordered.is_met then ordered.value else null end)::text,
                    last(ordered.value)::text
        ) as value,
        coalesce(   first(case when ordered.is_met then ordered.c_otime else null end),
                    last(ordered.c_otime)
        ) as override_time,
        coalesce(   first(case when ordered.is_met then ordered.c_ouser else null end),
                    last(ordered.c_ouser)
        ) as override_user,
        coalesce(
            first(case when ordered.is_met then ordered.c_ovalue else null end),
            last(ordered.c_ovalue)
        ) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
      -- We calculate extended_pat_cvalues for orders only based on the
      -- severe_sepsis_lead_time above. This lets us search for orders
      -- before the initial indicator of sepsis.
      --
      -- We cannot calculate pat_cvalues in one go to address both the
      -- need for severe_sepsis_lead_time and orders, since there is a
      -- dependency between the two calculations.
      --
      with orders_cvalues as (
        select * from pat_cvalues CV
        where CV.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
        union all
        select pat_ids.pat_id,
               cd.name,
               meas.fid,
               cd.category,
               meas.tsp,
               meas.value,
               c.override_time as c_otime,
               c.override_user as c_ouser,
               c.override_value as c_ovalue,
               cd.override_value as d_ovalue
        from pat_ids
        cross join criteria_default as cd
        left join severe_sepsis_onsets SSP
          on pat_ids.pat_id = SSP.pat_id
        left join infections c
          on pat_ids.pat_id = c.pat_id
          and cd.name = c.name
        left join criteria_meas meas
            on pat_ids.pat_id = meas.pat_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              -- This predicate safely returns no rows if
              -- severe_sepsis_lead_time - orders_lookback
              -- is chronologically before ts_start - window_size
              or meas.tsp between SSP.severe_sepsis_lead_time - orders_lookback
                          and ts_start - window_size
            )
        where cd.dataset_id = _dataset_id
        and cd.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
      )
      select  CV.pat_id,
              CV.name,
              CV.tsp as measurement_time,
              (case when CV.category in ('after_severe_sepsis_dose', 'after_septic_shock_dose')
                      then dose_order_status(CV.fid, CV.c_ovalue#>>'{0,text}')
                    else order_status(CV.fid, CV.value, CV.c_ovalue#>>'{0,text}')
               end) as value,
              CV.c_otime,
              CV.c_ouser,
              CV.c_ovalue,
              (case
                  when CV.category = 'after_severe_sepsis' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (SSP.severe_sepsis_lead_time - orders_lookback))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.pat_id = coalesce(this_pat_id, SSP.pat_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_severe_sepsis_dose' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (SSP.severe_sepsis_lead_time - orders_lookback))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.pat_id = coalesce(this_pat_id, SSP.pat_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                            coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                     (CV.d_ovalue#>>'{lower}')::numeric)) )

                  when CV.category = 'after_septic_shock' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.pat_id = coalesce(this_pat_id, SSH.pat_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_septic_shock_dose' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.pat_id = coalesce(this_pat_id, SSH.pat_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                            coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                     (CV.d_ovalue#>>'{lower}')::numeric)) )

                  else criteria_value_met(CV.value, CV.c_ovalue, CV.d_ovalue)
                  end
              ) as is_met
      from orders_cvalues CV
      order by CV.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  repeat_lactate as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
        select  pat_cvalues.pat_id,
                pat_cvalues.name,
                pat_cvalues.tsp as measurement_time,
                order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}') as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                ((
                  coalesce(initial_lactate_order.is_met and lactate_results.is_met, false)
                    and order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value))
                    and (coalesce(pat_cvalues.tsp > initial_lactate_order.tsp, false)
                            and coalesce(lactate_results.tsp > initial_lactate_order.tsp, false))
                ) or
                (
                  not( coalesce(initial_lactate_order.is_completed
                                  and ( lactate_results.is_met or pat_cvalues.tsp <= initial_lactate_order.tsp )
                                , false) )
                )) is_met
        from pat_cvalues
        left join (
            select oc.pat_id,
                   max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                   coalesce(bool_or(oc.is_met), false) as is_met,
                   coalesce(min(oc.value) = 'Completed', false) as is_completed
            from orders_criteria oc
            where oc.name = 'initial_lactate_order'
            group by oc.pat_id
        ) initial_lactate_order on pat_cvalues.pat_id = initial_lactate_order.pat_id
        left join (
            select p3.pat_id,
                   max(case when p3.value::numeric > 2.0 then p3.tsp else null end) tsp,
                   coalesce(bool_or(p3.value::numeric > 2.0), false) is_met
            from pat_cvalues p3
            where p3.name = 'initial_lactate'
            group by p3.pat_id
        ) lactate_results on pat_cvalues.pat_id = lactate_results.pat_id
        where pat_cvalues.name = 'repeat_lactate_order'
        order by pat_cvalues.tsp
    )
    as ordered
    group by ordered.pat_id, ordered.name
  )
  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         SSH.septic_shock_onset
  from (
      select * from severe_sepsis SSP
      union all select * from septic_shock SSH
      union all select * from orders_criteria
      union all select * from repeat_lactate
  ) new_criteria
  left join severe_sepsis_onsets SSP on new_criteria.pat_id = SSP.pat_id
  left join septic_shock_onsets SSH on new_criteria.pat_id = SSH.pat_id;

  return;
END; $function$;



-- get_prospective_cms_labels_for_window
-- Prospective (i.e., forward-looking) inlined version of calculate_criteria.
-- TODO: compare to get_cms_labels_for_window
CREATE OR REPLACE FUNCTION get_prospective_cms_labels_for_window(
                this_pat_id                      text,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                use_app_infections               boolean default false,
                use_clarity_notes                boolean default false
  )
  RETURNS table(pat_id                           varchar(50),
                name                             varchar(50),
                measurement_time                 timestamptz,
                value                            text,
                override_time                    timestamptz,
                override_user                    text,
                override_value                   json,
                is_met                           boolean,
                update_date                      timestamptz,
                severe_sepsis_onset              timestamptz,
                severe_sepsis_wo_infection_onset timestamptz,
                septic_shock_onset               timestamptz
  )
 LANGUAGE plpgsql
AS $function$
DECLARE
  window_size interval := ts_end - ts_start;
  orders_lookback interval := interval '6 hours';
  orders_lookahead interval := interval '6 hours';
BEGIN

  select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
  -- raise notice 'Running calculate criteria on pat %, dataset_id % between %, %',this_pat_id, _dataset_id, ts_start , ts_end;

  return query
  with pat_visit_ids as (
    select distinct P.pat_id, P.visit_id from pat_enc P
    where P.pat_id = coalesce(this_pat_id, P.pat_id) and P.dataset_id = _dataset_id
  ),
  pat_ids as (
    select distinct P.pat_id from pat_visit_ids P
  ),
  user_overrides as (
    -- TODO: dispatch to override generator
    select * from (
      values (null::integer, null::varchar, null::varchar, null::varchar, null::varchar, null::timestamptz, null::text, null::timestamptz, null::text, null::json)
    ) as criteria (dataset_id, pat_id, name, fid, category, tsp, value, override_time, override_user, override_value)
  ),
  pat_cvalues as (
    select pat_ids.pat_id,
           cd.name,
           meas.fid,
           cd.category,
           meas.tsp,
           meas.value,
           c.override_time as c_otime,
           c.override_user as c_ouser,
           c.override_value as c_ovalue,
           cd.override_value as d_ovalue
    from pat_ids
    cross join criteria_default as cd
    left join user_overrides c
      on pat_ids.pat_id = c.pat_id
      and cd.name = c.name
      and cd.dataset_id = c.dataset_id
    left join criteria_meas meas
        on pat_ids.pat_id = meas.pat_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null or meas.tsp between ts_start - window_size and ts_end)
    where cd.dataset_id = _dataset_id
  ),
  pat_aggregates as (
    select ordered.pat_id,
           avg(ordered.bp_sys) as bp_sys,
           first(ordered.weight) as weight,
           sum(ordered.urine_output) as urine_output
    from (
        select P.pat_id,
               (case when meas.fid = 'bp_sys' then meas.value::numeric else null end) as bp_sys,
               (case when meas.fid = 'weight' then meas.value::numeric else null end) as weight,
               (case when meas.fid = 'urine_output'
                     and ts_end - meas.tsp < interval '2 hours'
                     then meas.value::numeric else null end
                ) as urine_output
        from pat_ids P
        inner join criteria_meas meas on P.pat_id = meas.pat_id
        where meas.fid in ('bp_sys', 'urine_output', 'weight')
        and isnumeric(meas.value)
        and meas.dataset_id = _dataset_id
        order by meas.tsp
    ) as ordered
    group by ordered.pat_id
  ),
  sirs_and_org_df_criteria as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from (
      select  PC.pat_id,
              PC.name,
              PC.tsp as measurement_time,
              (case
                when PC.name = 'respiratory_failure'
                then (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value)))
                else PC.value
               end) as value,
              PC.c_otime,
              PC.c_ouser,
              PC.c_ovalue,
              (case
                when PC.name in (
                  'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp',
                  'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                )
                then
                  (case
                    when PC.category = 'decrease_in_sbp' then
                      decrease_in_sbp_met(
                        (select max(PBP.bp_sys) from pat_aggregates PBP where PBP.pat_id = PC.pat_id),
                        PC.value, PC.c_ovalue, PC.d_ovalue
                      )

                    when PC.category = 'urine_output' then
                      urine_output_met(
                          (select max(PUO.urine_output) from pat_aggregates PUO where PUO.pat_id = PC.pat_id),
                          (select max(PW.weight) from pat_aggregates PW where PW.pat_id = PC.pat_id),
                          _dataset_id
                      )

                    else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                    end
                  )

                else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
               end) as is_met
      from pat_cvalues PC
      where PC.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
      order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  severe_sepsis_wo_infection as (
    select CO.pat_id,
           sum(CO.sirs_cnt) as sirs_cnt,
           sum(CO.org_df_cnt) as org_df_cnt,
           max(CO.sirs_initial) as sirs_initial,
           max(CO.sirs_onset) as sirs_onset,
           max(CO.org_df_onset) as org_df_onset
    from (
      select S.pat_id,

             sum(case when S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
                      then 1
                      else 0
                 end) as sirs_cnt,

             (array_agg(
                (case
                  when S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
                  then S.measurement_time else null end
                ) order by S.measurement_time)
              )[1] as sirs_initial,

             (array_agg(
                (case
                  when S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
                  then S.measurement_time else null end
                ) order by S.measurement_time)
              )[2] as sirs_onset,

             sum(case when S.name in (
                        'respiratory_failure',
                        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                      )
                      and S.is_met
                      then 1
                      else 0
                 end
             ) as org_df_cnt,

             min(case when S.name in (
                        'respiratory_failure',
                        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                      )
                      and S.is_met
                      then S.measurement_time
                      else null
                 end
              ) as org_df_onset
      from sirs_and_org_df_criteria S
      group by S.pat_id
    ) CO
    where greatest(CO.sirs_onset, CO.org_df_onset) - least(CO.sirs_initial, CO.org_df_onset) < window_size
    group by CO.pat_id
  ),
  app_infections as (
      select
          ordered.pat_id,
          ordered.name,
          first(ordered.measurement_time) as measurement_time,
          first(ordered.value)::text as value,
          first(ordered.c_otime) as override_time,
          first(ordered.c_ouser) as override_user,
          first(ordered.c_ovalue) as override_value,
          coalesce(bool_or(ordered.is_met), false) as is_met,
          now() as update_date
      from (
          select  PC.pat_id,
                  PC.name,
                  PC.tsp as measurement_time,
                  PC.value as value,
                  PC.c_otime,
                  PC.c_ouser,
                  PC.c_ovalue,
                  (coalesce(PC.c_ovalue#>>'{0,text}', PC.value) <> 'No Infection') as is_met
          from pat_cvalues PC
          where PC.name = 'suspicion_of_infection'
          and use_app_infections
          order by PC.tsp
      ) as ordered
      group by ordered.pat_id, ordered.name
  ),
  extracted_infections as (
    -- Use either clarity or cdm notes for now.
    -- We implement this as a union over two queries, each gated
    -- by a simple where clause based on an function argument.
    with initial_notes_candidates as (
      select distinct SPWOI.pat_id
      from severe_sepsis_wo_infection SPWOI
      where SPWOI.sirs_cnt > 1 and SPWOI.org_df_cnt > 0
      group by SPWOI.pat_id
    ),
    -- Prospective infections based on additional orders in a forward-looking window
    orders_cvalues as (
      select PC.*
      from initial_notes_candidates NC
      inner join pat_cvalues PC on NC.pat_id = PC.pat_id
      where PC.name in (
        'blood_culture_order',
        'antibiotics_order'
      )
      union all
      select NC.pat_id,
             cd.name,
             meas.fid,
             cd.category,
             meas.tsp,
             meas.value,
             null as c_otime,
             null as c_ouser,
             null as c_ovalue,
             cd.override_value as d_ovalue
      from initial_notes_candidates NC
      cross join criteria_default as cd
      left join criteria_meas meas
          on NC.pat_id = meas.pat_id
          and meas.fid = cd.fid
          and cd.dataset_id = meas.dataset_id
          and (meas.tsp is null or meas.tsp between ts_end and ts_end + orders_lookahead)
      where cd.dataset_id = _dataset_id
      and cd.name in (
        'blood_culture_order',
        'antibiotics_order'
      )
    ),
    notes_candidates as (
      select distinct CV.pat_id
      from orders_cvalues CV
      where
        (case
          when CV.category = 'after_severe_sepsis' then
            ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

          when CV.category = 'after_severe_sepsis_dose' then
            ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                    coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                             (CV.d_ovalue#>>'{lower}')::numeric)) )

          when CV.category = 'after_septic_shock' then
            ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

          when CV.category = 'after_septic_shock_dose' then
            ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                    coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                             (CV.d_ovalue#>>'{lower}')::numeric)) )

          else criteria_value_met(CV.value, CV.c_ovalue, CV.d_ovalue)
          end
        )
    ),
    clarity_matches as (
        select P.pat_id as pat_id,
               'suspicion_of_infection'::text as name,
               min(M.start_ts) as measurement_time,
               min(M.ngram) as value,
               min(M.start_ts) as override_time,
               'NLP'::text as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true as is_met,
               now() as update_date
        from notes_candidates NC
        inner join pat_visit_ids P on NC.pat_id = P.pat_id
        inner join lateral match_clarity_infections(P.visit_id, 3, 3) M on P.visit_id = M.csn_id
        where use_clarity_notes
        and not use_app_infections
        group by P.pat_id
    ),
    cdm_matches as (
        -- TODO: we have picked an arbitrary time interval for notes. Refine.
        select NC.pat_id as pat_id,
               'suspicion_of_infection'::text as name,
               min(M.start_ts) as measurement_time,
               min(M.ngram) as value,
               min(M.start_ts) as override_time,
               'NLP'::text as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true as is_met,
               now() as update_date
        from notes_candidates NC
        inner join lateral match_cdm_infections(NC.pat_id, _dataset_id, 3, 3) M
          on NC.pat_id = M.pat_id
          and M.start_ts between ts_start - interval '1 days' and ts_end + interval '1 days'
        where not use_clarity_notes
        and not use_app_infections
        group by NC.pat_id
    )
    select * from clarity_matches
    union all
    select * from cdm_matches
  ),
  infections as (
    select * from app_infections
    union all select * from extracted_infections
  ),
  severe_sepsis as (
    select * from sirs_and_org_df_criteria
    union all select * from infections
  ),
  severe_sepsis_criteria_stats as (
    select CO.pat_id,
           sum(I.infection_cnt) > 0 as suspicion_of_infection,
           sum(CO.sirs_cnt) as sirs_cnt,
           sum(CO.org_df_cnt) as org_df_cnt,
           max(I.infection_onset) as inf_onset,
           max(CO.sirs_initial) as sirs_initial,
           max(CO.sirs_onset) as sirs_onset,
           max(CO.org_df_onset) as org_df_onset
    from severe_sepsis_wo_infection CO
    inner join (
      select I.pat_id,
             sum(case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             max(case when I.name = 'suspicion_of_infection' then I.override_time else null end) as infection_onset
      from infections I
      group by I.pat_id
    ) I on CO.pat_id = I.pat_id
    group by CO.pat_id
  ),
  severe_sepsis_onsets as (
    select sspm.pat_id,
           sspm.severe_sepsis_is_met,
           (case when sspm.severe_sepsis_onset <> 'infinity'::timestamptz
                 then sspm.severe_sepsis_onset
                 else null end
           ) as severe_sepsis_onset,
           (case when sspm.severe_sepsis_wo_infection_onset <> 'infinity'::timestamptz
                 then sspm.severe_sepsis_wo_infection_onset
                 else null end
           ) as severe_sepsis_wo_infection_onset,
           sspm.severe_sepsis_lead_time
    from (
      select stats.pat_id,
             coalesce(bool_or(stats.suspicion_of_infection
                                and stats.sirs_cnt > 1
                                and stats.org_df_cnt > 0)
                      , false
                      ) as severe_sepsis_is_met,

             max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                          coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_onset,

             max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_wo_infection_onset,

             min(least(stats.inf_onset, stats.sirs_initial, stats.org_df_onset))
                as severe_sepsis_lead_time

      from severe_sepsis_criteria_stats stats
      group by stats.pat_id
    ) sspm
  ),

  crystalloid_fluid_and_hypoperfusion as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
      select  PC.pat_id,
              PC.name,
              PC.tsp as measurement_time,
              PC.value as value,
              PC.c_otime,
              PC.c_ouser,
              PC.c_ovalue,
              (case
                when PC.name = 'initial_lactate'
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                else (
                  case
                  when coalesce(PC.c_ovalue#>>'{0,text}' = 'Not Indicated', false)
                  then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  else PC.fid = 'crystalloid_fluid' and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  end
                )
                end
              ) and (SSP.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp) >= SSP.severe_sepsis_onset)
              as is_met
      from pat_cvalues PC
      left join severe_sepsis_onsets SSP on PC.pat_id = SSP.pat_id
      where PC.name in ( 'crystalloid_fluid', 'initial_lactate' )
      order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  hypotension as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
        with pat_fluid_overrides as (
          select CFL.pat_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated'), false) as override
          from crystalloid_fluid_and_hypoperfusion CFL
          where CFL.name = 'crystalloid_fluid'
          group by CFL.pat_id
        ),
        pats_fluid_after_severe_sepsis as (
          select  MFL.pat_id,
                  MFL.tsp,
                  sum(MFL.value::numeric) as total_fluid,
                  -- Fluids are met if they are overriden or if we have more than
                  -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
                  (coalesce(bool_or(OV.override), false)
                      or coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.weight))
                  ) as is_met
          from criteria_meas MFL
          left join pat_aggregates PW on MFL.pat_id = PW.pat_id
          left join severe_sepsis_onsets SSPN on MFL.pat_id = SSPN.pat_id
          left join pat_fluid_overrides OV on MFL.pat_id = OV.pat_id
          where isnumeric(MFL.value)
          and SSPN.severe_sepsis_is_met
          and MFL.tsp >= (SSPN.severe_sepsis_onset - orders_lookback)
          and (MFL.fid = 'crystalloid_fluid' or coalesce(OV.override, false))
          group by MFL.pat_id, MFL.tsp
        )
        select PC.pat_id,
               PC.name,
               PC.tsp as measurement_time,
               PC.value as value,
               PC.c_otime,
               PC.c_ouser,
               PC.c_ovalue,
               (SSPN.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp) >= SSPN.severe_sepsis_onset)
               and
               (case when PC.category = 'hypotension' then
                       (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                       and criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                       and criteria_value_met(NEXT.value, PC.c_ovalue, PC.d_ovalue)

                     when PC.category = 'hypotension_dsbp' then
                       (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                       and decrease_in_sbp_met(PBPSYS.value, PC.value, PC.c_ovalue, PC.d_ovalue)
                       and decrease_in_sbp_met(PBPSYS.value, NEXT.value, PC.c_ovalue, PC.d_ovalue)

                    else false
                end) as is_met
        from pat_cvalues PC
        left join severe_sepsis_onsets SSPN on PC.pat_id = SSPN.pat_id

        left join pats_fluid_after_severe_sepsis PFL
          on PC.pat_id = PFL.pat_id

        left join lateral (
          select meas.pat_id, meas.fid, meas.tsp, meas.value
          from criteria_meas meas
          where PC.pat_id = meas.pat_id and PC.fid = meas.fid and PC.tsp < meas.tsp
          order by meas.tsp
          limit 1
        ) NEXT on PC.pat_id = NEXT.pat_id and PC.fid = NEXT.fid

        left join lateral (
          select BP.pat_id, max(BP.bp_sys) as value
          from pat_aggregates BP where PC.pat_id = BP.pat_id
          group by BP.pat_id
        ) PBPSYS on PC.pat_id = PBPSYS.pat_id

        where PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
        order by PC.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),

  septic_shock as (
    select * from crystalloid_fluid_and_hypoperfusion
    union all select * from hypotension
  ),

  -- Calculate septic shock in an extended window, for use in
  -- any criteria that has requirements after severe sepsis is met.
  septic_shock_onsets as (
    select stats.pat_id,
           bool_or(stats.cnt > 0) as septic_shock_is_met,
           greatest(min(stats.onset), max(SSP.severe_sepsis_onset)) as septic_shock_onset
    from (
        -- Hypotension and hypoperfusion subqueries individually check
        -- that they occur after severe sepsis onset.
        (select hypotension.pat_id,
                sum(case when hypotension.is_met then 1 else 0 end) as cnt,
                min(hypotension.measurement_time) as onset
         from hypotension
         group by hypotension.pat_id)
        union
        (select HPF.pat_id,
                sum(case when HPF.is_met then 1 else 0 end) as cnt,
                min(HPF.measurement_time) as onset
         from crystalloid_fluid_and_hypoperfusion HPF
         where HPF.name = 'initial_lactate'
         group by HPF.pat_id)
    ) stats
    left join severe_sepsis_onsets SSP on stats.pat_id = SSP.pat_id
    group by stats.pat_id
  ),

  orders_criteria as (
    select
        ordered.pat_id,
        ordered.name,
        coalesce(   first(case when ordered.is_met then ordered.measurement_time else null end),
                    last(ordered.measurement_time)
        ) as measurement_time,
        coalesce(   first(case when ordered.is_met then ordered.value else null end)::text,
                    last(ordered.value)::text
        ) as value,
        coalesce(   first(case when ordered.is_met then ordered.c_otime else null end),
                    last(ordered.c_otime)
        ) as override_time,
        coalesce(   first(case when ordered.is_met then ordered.c_ouser else null end),
                    last(ordered.c_ouser)
        ) as override_user,
        coalesce(
            first(case when ordered.is_met then ordered.c_ovalue else null end),
            last(ordered.c_ovalue)
        ) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
      -- We calculate extended_pat_cvalues for orders only based on the
      -- severe_sepsis_lead_time above. This lets us search for orders
      -- before the initial indicator of sepsis.
      --
      -- We cannot calculate pat_cvalues in one go to address both the
      -- need for severe_sepsis_lead_time and orders, since there is a
      -- dependency between the two calculations.
      --
      with orders_cvalues as (
        select * from pat_cvalues CV
        where CV.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
        union all
        select pat_ids.pat_id,
               cd.name,
               meas.fid,
               cd.category,
               meas.tsp,
               meas.value,
               c.override_time as c_otime,
               c.override_user as c_ouser,
               c.override_value as c_ovalue,
               cd.override_value as d_ovalue
        from pat_ids
        cross join criteria_default as cd
        left join severe_sepsis_onsets SSP
          on pat_ids.pat_id = SSP.pat_id
        left join infections c
          on pat_ids.pat_id = c.pat_id
          and cd.name = c.name
        left join criteria_meas meas
            on pat_ids.pat_id = meas.pat_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              -- This predicate safely returns no rows if
              -- severe_sepsis_lead_time - orders_lookback
              -- is chronologically before ts_start - window_size
              or meas.tsp between SSP.severe_sepsis_lead_time - orders_lookback
                          and ts_start - window_size
            )
        where cd.dataset_id = _dataset_id
        and cd.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
      )
      select  CV.pat_id,
              CV.name,
              CV.tsp as measurement_time,
              (case when CV.category in ('after_severe_sepsis_dose', 'after_septic_shock_dose')
                      then dose_order_status(CV.fid, CV.c_ovalue#>>'{0,text}')
                    else order_status(CV.fid, CV.value, CV.c_ovalue#>>'{0,text}')
               end) as value,
              CV.c_otime,
              CV.c_ouser,
              CV.c_ovalue,
              (case
                  when CV.category = 'after_severe_sepsis' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (SSP.severe_sepsis_lead_time - orders_lookback))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.pat_id = coalesce(this_pat_id, SSP.pat_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_severe_sepsis_dose' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (SSP.severe_sepsis_lead_time - orders_lookback))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.pat_id = coalesce(this_pat_id, SSP.pat_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                            coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                     (CV.d_ovalue#>>'{lower}')::numeric)) )

                  when CV.category = 'after_septic_shock' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.pat_id = coalesce(this_pat_id, SSH.pat_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_septic_shock_dose' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.pat_id = coalesce(this_pat_id, SSH.pat_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}', CV.value::numeric,
                            coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                     (CV.d_ovalue#>>'{lower}')::numeric)) )

                  else criteria_value_met(CV.value, CV.c_ovalue, CV.d_ovalue)
                  end
              ) as is_met
      from orders_cvalues CV
      order by CV.tsp
    ) as ordered
    group by ordered.pat_id, ordered.name
  ),
  repeat_lactate as (
    select
        ordered.pat_id,
        ordered.name,
        first(case when ordered.is_met then ordered.measurement_time else null end) as measurement_time,
        first(case when ordered.is_met then ordered.value else null end)::text as value,
        first(case when ordered.is_met then ordered.c_otime else null end) as override_time,
        first(case when ordered.is_met then ordered.c_ouser else null end) as override_user,
        first(case when ordered.is_met then ordered.c_ovalue else null end) as override_value,
        coalesce(bool_or(ordered.is_met), false) as is_met,
        now() as update_date
    from
    (
        select  pat_cvalues.pat_id,
                pat_cvalues.name,
                pat_cvalues.tsp as measurement_time,
                order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}') as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                ((
                  coalesce(initial_lactate_order.is_met and lactate_results.is_met, false)
                    and order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value))
                    and (coalesce(pat_cvalues.tsp > initial_lactate_order.tsp, false)
                            and coalesce(lactate_results.tsp > initial_lactate_order.tsp, false))
                ) or
                (
                  not( coalesce(initial_lactate_order.is_completed
                                  and ( lactate_results.is_met or pat_cvalues.tsp <= initial_lactate_order.tsp )
                                , false) )
                )) is_met
        from pat_cvalues
        left join (
            select oc.pat_id,
                   max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                   coalesce(bool_or(oc.is_met), false) as is_met,
                   coalesce(min(oc.value) = 'Completed', false) as is_completed
            from orders_criteria oc
            where oc.name = 'initial_lactate_order'
            group by oc.pat_id
        ) initial_lactate_order on pat_cvalues.pat_id = initial_lactate_order.pat_id
        left join (
            select p3.pat_id,
                   max(case when p3.value::numeric > 2.0 then p3.tsp else null end) tsp,
                   coalesce(bool_or(p3.value::numeric > 2.0), false) is_met
            from pat_cvalues p3
            where p3.name = 'initial_lactate'
            group by p3.pat_id
        ) lactate_results on pat_cvalues.pat_id = lactate_results.pat_id
        where pat_cvalues.name = 'repeat_lactate_order'
        order by pat_cvalues.tsp
    )
    as ordered
    group by ordered.pat_id, ordered.name
  )
  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         SSH.septic_shock_onset
  from (
      select * from severe_sepsis SSP
      union all select * from septic_shock SSH
      union all select * from orders_criteria
      union all select * from repeat_lactate
  ) new_criteria
  left join severe_sepsis_onsets SSP on new_criteria.pat_id = SSP.pat_id
  left join septic_shock_onsets SSH on new_criteria.pat_id = SSH.pat_id;

  return;
END; $function$;




CREATE OR REPLACE FUNCTION get_cms_label_series(
        label_description       text,
        this_pat_id             text,
        _dataset_id             INTEGER DEFAULT NULL,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all',
        prospective_cms         integer,
        use_app_infections      boolean default false,
        use_clarity_notes       boolean default false
  )
--   @Peter, positive inf time and negative inf time?
--   passing in a null value will calculate historical criteria over all patientis
  returns void
  LANGUAGE plpgsql
AS $function$
DECLARE
    window_size            interval := get_parameter('lookbackhours')::interval;
    pat_id_str             text;
    window_fn              text;
    use_app_infections_str text;
    use_clarity_notes_str  text;
    generated_label_id     integer;
BEGIN

    select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
    raise notice 'Running calculate historical criteria on dataset_id %', _dataset_id;

    pat_id_str = case when this_pat_id is null then 'NULL'
                      else format('''%s''',this_pat_id) end;

    window_fn = case prospective_cms
                  when 0 then 'get_cms_labels_for_window_inlined'
                  when 1 then 'get_prospective_cms_labels_for_window'
                  else 'get_cms_labels_for_window' end;

    use_app_infections_str = case when use_app_infections then 'True' else 'False' end;
    use_clarity_notes_str = case when use_clarity_notes then 'True' else 'False' end;


    drop table if exists new_criteria_windows;

     EXECUTE format(
       'create temporary table new_criteria_windows as
        with
        pat_start as(
          select pat_id, min(tsp) as min_time
          from criteria_meas meas
          where dataset_id = %s
          group by pat_id
        ),
        meas_bins as (
          select distinct meas.pat_id, meas.tsp ,
            floor(extract(EPOCH FROM meas.tsp - pat_start.min_time) /
            extract(EPOCH from interval ''1 hour''))+1 as bin
          from
            criteria_meas meas
            inner join pat_start
            on pat_start.pat_id = meas.pat_id
          where
            meas.pat_id = coalesce(%s, meas.pat_id) and
            meas.dataset_id = %s and
            meas.tsp between ''%s''::timestamptz and ''%s''::timestamptz
        ),
        window_ends as (
          select
            pat_id, max(tsp) as tsp
          from
            meas_bins
          group by pat_id, bin
          order by pat_id, tsp
          limit %s)
        select window_ends.tsp as ts, new_criteria.*
        from
          window_ends
          inner join lateral
          %s(coalesce(%s, window_ends.pat_id),
             window_ends.tsp - ''%s''::interval,
             window_ends.tsp,
             %s, %s, %s) new_criteria
        on window_ends.pat_id = new_criteria.pat_id;'
        , _dataset_id,  pat_id_str, _dataset_id, ts_start, ts_end, window_limit, window_fn
        , pat_id_str, window_size, _dataset_id, use_app_infections_str, use_clarity_notes_str);


    insert into label_version (created, description)
        values (now(), label_description)
        returning label_id into generated_label_id;

    insert into cdm_labels (dataset_id, label_id, pat_id, tsp, label_type, label)
      select _dataset_id, generated_label_id, sw.pat_id, sw.ts, 'cms state', sw.state
      from get_window_states('new_criteria_windows', this_pat_id) sw
    on conflict (dataset_id, label_id, pat_id, tsp) do update
      set label_type = excluded.label_type,
          label = excluded.label;

    drop table new_criteria_windows;
    return;
END; $function$;


