--------------------------------------------------------------------------------------------
-- Label calculation.
--
-- Known issues:
-- all get_cms_*_for_window_* functions should return a dataset_id to properly
-- support calculations over multiple datasets.
--
-- all callers of get_cms_*_for_window_* should appropriately join on dataset_id
--

-------------------------------------------------------
-- Per-window criteria calculation

-- Calculates criteria for only SIRS and Organ Dysfunction.
-- This is useful for identifying candidates for whom we can
-- evaluate suspicion of infection.
CREATE OR REPLACE FUNCTION get_cms_candidates_for_window(
                this_enc_id                      int,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                _ignored                         boolean default false,
                use_clarity_notes                boolean default false,
                save_criteria_windows            boolean default false
  )
  RETURNS table(enc_id                           integer,
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
BEGIN
  select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
  -- raise notice 'Running calculate criteria on pat %, dataset_id % between %, %',this_enc_id, _dataset_id, ts_start , ts_end;

  return query
  with pat_ids as (
    select distinct P.enc_id from pat_enc P
    where P.enc_id = coalesce(this_enc_id, P.enc_id) and P.dataset_id = _dataset_id
  ),
  user_overrides as (
    -- TODO: dispatch to override generator
    select * from (
      values (null::integer, null::int, null::varchar, null::varchar, null::varchar, null::timestamptz, null::text, null::timestamptz, null::text, null::json)
    ) as criteria (dataset_id, enc_id, name, fid, category, tsp, value, override_time, override_user, override_value)
  ),
  pat_cvalues as (
    select pat_ids.enc_id,
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
      on pat_ids.enc_id = c.enc_id
      and cd.name = c.name
      and cd.dataset_id = c.dataset_id
    left join cdm_t meas
        on pat_ids.enc_id = meas.enc_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null or meas.tsp between ts_start - window_size and ts_end)
    where cd.dataset_id = _dataset_id
    and meas.dataset_id = _dataset_id
    and meas.value <> 'nan'
    and meas.fid in (
      'temperature', 'heart_rate', 'resp_rate', 'wbc', 'bands', 'nbp_sys', 'abp_sys', 'map', 'bilirubin', 'platelets', 'inr',
      'ptt', 'lactate', 'vent', 'bipap', 'cpap', 'creatinine'
    )
  ),
  pat_aggregates as (
    select aggs.enc_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.enc_id,
               meas.tsp as measurement_time,
               (case when meas.fid in ('nbp_sys', 'abp_sys') then meas.value::numeric else null end) as bp_sys,
               (case when meas.fid = 'weight' then meas.value::numeric else null end) as weight,
               (case when meas.fid = 'urine_output'
                     and ts_end - meas.tsp < interval '2 hours'
                     then meas.value::numeric else null end
                ) as urine_output
        from pat_ids P
        inner join cdm_t meas on P.enc_id = meas.enc_id
        where meas.fid in ('nbp_sys', 'abp_sys', 'urine_output', 'weight')
        and isnumeric(meas.value)
        and meas.dataset_id = _dataset_id
    ) as aggs
    group by aggs.enc_id
  ),
  all_sirs_org as (
    select  PC.enc_id,
            PC.name,
            PC.tsp as measurement_time,
            (case
              when PC.name = 'respiratory_failure'
              then (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value)))
              else PC.value
             end) as value,
            PC.c_otime  as override_time,
            PC.c_ouser  as override_user,
            PC.c_ovalue as override_value,
            (case
              when PC.name = 'respiratory_failure'
              then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

              when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
              then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

              else
              (case
                  when PC.category = 'decrease_in_sbp' then
                    decrease_in_sbp_met(
                      (select max(PBP.bp_sys) from pat_aggregates PBP where PBP.enc_id = PC.enc_id),
                      PC.value, PC.c_ovalue, PC.d_ovalue
                    )

                  when PC.category = 'urine_output' then
                    urine_output_met(
                        (select max(PUO.urine_output) from pat_aggregates PUO where PUO.enc_id = PC.enc_id),
                        (select max(PW.weight) from pat_aggregates PW where PW.enc_id = PC.enc_id),
                        _dataset_id
                    )

                  else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  end
                )
             end) as is_met
    from pat_cvalues PC
    where PC.name in (
      'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
      'respiratory_failure',
      'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
    )
  ),
  all_sirs_org_triples as (
    with sirs as (
      select * from all_sirs_org S
      where S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
    ),
    org_df as (
      select * from all_sirs_org S
      where S.name in (
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
      and S.is_met
    )
    select SO.enc_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset
    from (
      select S1.enc_id,
             S1.name as sirs1_name,
             S2.name as sirs2_name,
             D.name as odf_name,
             coalesce(S1.measurement_time, S1.override_time) as sirs_initial,
             coalesce(S2.measurement_time, S2.override_time) as sirs_onset,
             coalesce(D.measurement_time, D.override_time) as org_df_onset
      from sirs S1
      inner join sirs S2
        on S1.enc_id = S2.enc_id
        and S1.name <> S2.name
        and coalesce(S1.measurement_time, S1.override_time) <= coalesce(S2.measurement_time, S2.override_time)
      inner join org_df D on S1.enc_id = D.enc_id
    ) SO
    where not (SO.sirs_initial is null or SO.sirs_onset is null or SO.org_df_onset is null)
    and greatest(SO.sirs_onset, SO.org_df_onset) - least(SO.sirs_initial, SO.org_df_onset) < window_size
  ),
  null_infections as (
    -- This is necessary for get_window_labels_from_criteria
    select P.enc_id,
           'suspicion_of_infection'::varchar as name,
           null::timestamptz                 as measurement_time,
           null::text                        as value,
           null::timestamptz                 as override_time,
           null::text                        as override_user,
           null::json                        as override_value,
           false                             as is_met,
           now()                             as update_date
    from pat_ids P
  ),
  severe_sepsis_candidates as (
    select SO.enc_id,
           bool_or(coalesce(I.infection_cnt, 0) > 0) as suspicion_of_infection,
           coalesce(
              first(I.infection_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where I.infection_onset is not null),
              'infinity'::timestamptz
           ) as inf_onset,

           coalesce(
              first(SO.sirs1_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs1_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs1_name,

           coalesce(
              first(SO.sirs2_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs2_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs2_name,

           coalesce(
              first(SO.odf_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.odf_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as odf_name,

           coalesce(
              first(SO.sirs_initial order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs_initial order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs_initial,

           coalesce(
              first(SO.sirs_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs_onset,

           coalesce(
              first(SO.org_df_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.org_df_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as org_df_onset

    from all_sirs_org_triples SO
    left join (
      select I.enc_id,
             (case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             (case when I.name = 'suspicion_of_infection' then I.override_time else null end) as infection_onset
      from null_infections I
    ) I
      on SO.enc_id = I.enc_id
      and (greatest(
                greatest(SO.sirs_onset, SO.org_df_onset) - I.infection_onset,
                I.infection_onset - least(SO.sirs_onset, SO.org_df_onset)
              ) < interval '6 hours')

    group by SO.enc_id
  ),

  severe_sepsis as (
    select
        CR.enc_id,
        CR.name,
        (first(CR.measurement_time order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as measurement_time,
        (first(CR.value            order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met))::text as value,
        (first(CR.override_time    order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_time,
        (first(CR.override_user    order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_user,
        (first(CR.override_value   order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_value,
        coalesce(bool_or(CR.is_met), false) as is_met,
        now() as update_date

    from (
      select * from all_sirs_org
      union all
      select I.enc_id, I.name, I.measurement_time, I.value, I.override_time, I.override_user, I.override_value, I.is_met
      from null_infections I
    ) CR
    left join severe_sepsis_candidates CD
      on CR.enc_id = CD.enc_id
      and CR.name in ( CD.sirs1_name, CD.sirs2_name, CD.odf_name, 'suspicion_of_infection' )

    where ( coalesce(CD.sirs1_name, CD.sirs2_name, CD.odf_name) is null )
    or (    ( CD.sirs1_name is not null and CD.sirs_initial = coalesce(CR.measurement_time, CR.override_time) )
         or ( CD.sirs2_name is not null and CD.sirs_onset   = coalesce(CR.measurement_time, CR.override_time) )
         or ( CD.odf_name   is not null and CD.org_df_onset = coalesce(CR.measurement_time, CR.override_time) )
         or ( CR.name = 'suspicion_of_infection' and (CD.inf_onset = 'infinity'::timestamptz or CR.override_time = CD.inf_onset) )
    )
    group by CR.enc_id, CR.name
  ),

  severe_sepsis_onsets as (
    select sspm.enc_id,
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
      select stats.enc_id,
             coalesce(bool_or(stats.suspicion_of_infection), false) as severe_sepsis_is_met,

             max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                          coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_onset,

             max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_wo_infection_onset,

             min(least(stats.inf_onset, stats.sirs_initial, stats.org_df_onset))
                as severe_sepsis_lead_time

      from severe_sepsis_candidates stats
      group by stats.enc_id
    ) sspm
  )

  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         null::timestamptz as septic_shock_onset
  from severe_sepsis new_criteria
  left join severe_sepsis_onsets SSP on new_criteria.enc_id = SSP.enc_id;

  return;
END; $function$;


-- get_cms_labels_for_window:
--   a) inlined version of calculate_criteria, using fewer passes
--   b) uses a preprocessed notes table for cdm_notes
--   c) looks for any combination of sirs/orgdf/notes within the window,
--      rather than looking at the combination of the first occurences.
--
CREATE OR REPLACE FUNCTION get_cms_labels_for_window(
                this_enc_id                      int,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                use_app_infections               boolean default false,
                use_clarity_notes                boolean default false,
                save_criteria_windows            boolean default false
  )
  RETURNS table(enc_id                           int,
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

  -- Criteria Lookbacks.
  initial_lactate_lookback       interval := interval '6 hours';
  orders_lookback                interval := interval '6 hours';
  blood_culture_order_lookback   interval := interval '48 hours';
  antibiotics_order_lookback     interval := interval '24 hours';
BEGIN

  select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
  -- raise notice 'Running calculate criteria on pat %, dataset_id % between %, %',this_enc_id, _dataset_id, ts_start , ts_end;

  return query
  with pat_ids as (
    select distinct P.enc_id from pat_enc P
    where P.enc_id = coalesce(this_enc_id, P.enc_id) and P.dataset_id = _dataset_id
  ),
  user_overrides as (
    -- TODO: dispatch to override generator
    select * from (
      values (null::integer, null::int, null::varchar, null::varchar, null::varchar, null::timestamptz, null::text, null::timestamptz, null::text, null::json)
    ) as criteria (dataset_id, enc_id, name, fid, category, tsp, value, override_time, override_user, override_value)
  ),
  pat_cvalues as (
    select pat_ids.enc_id,
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
      on pat_ids.enc_id = c.enc_id
      and cd.name = c.name
      and cd.dataset_id = c.dataset_id
    left join cdm_t meas
        on pat_ids.enc_id = meas.enc_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null or meas.tsp between ts_start - window_size and ts_end)
    where cd.dataset_id = _dataset_id
    and meas.dataset_id = _dataset_id
    and meas.value <> 'nan'
  ),
  pat_aggregates as (
    select aggs.enc_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.enc_id,
               meas.tsp as measurement_time,
               (case when meas.fid in ('nbp_sys', 'abp_sys') then meas.value::numeric else null end) as bp_sys,
               (case when meas.fid = 'weight' then meas.value::numeric else null end) as weight,
               (case when meas.fid = 'urine_output'
                     and ts_end - meas.tsp < interval '2 hours'
                     then meas.value::numeric else null end
                ) as urine_output
        from pat_ids P
        inner join cdm_t meas on P.enc_id = meas.enc_id
        where meas.fid in ('nbp_sys', 'abp_sys', 'urine_output', 'weight')
        and isnumeric(meas.value)
        and meas.dataset_id = _dataset_id
    ) as aggs
    group by aggs.enc_id
  ),
  all_sirs_org as (
    select  PC.enc_id,
            PC.name,
            PC.tsp as measurement_time,
            (case
              when PC.name = 'respiratory_failure'
              then (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value)))
              else PC.value
             end) as value,
            PC.c_otime  as override_time,
            PC.c_ouser  as override_user,
            PC.c_ovalue as override_value,
            (case
              when PC.name = 'respiratory_failure'
              then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

              when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
              then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

              else
              (case
                  when PC.category = 'decrease_in_sbp' then
                    decrease_in_sbp_met(
                      (select max(PBP.bp_sys) from pat_aggregates PBP where PBP.enc_id = PC.enc_id),
                      PC.value, PC.c_ovalue, PC.d_ovalue
                    )

                  when PC.category = 'urine_output' then
                    urine_output_met(
                        (select max(PUO.urine_output) from pat_aggregates PUO where PUO.enc_id = PC.enc_id),
                        (select max(PW.weight) from pat_aggregates PW where PW.enc_id = PC.enc_id),
                        _dataset_id
                    )

                  else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  end
                )
             end) as is_met
    from pat_cvalues PC
    where PC.name in (
      'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
      'respiratory_failure',
      'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
    )
  ),
  all_sirs_org_triples as (
    with sirs as (
      select * from all_sirs_org S
      where S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
    ),
    org_df as (
      select * from all_sirs_org S
      where S.name in (
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
      and S.is_met
    )
    select SO.enc_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset
    from (
      select S1.enc_id,
             S1.name as sirs1_name,
             S2.name as sirs2_name,
             D.name as odf_name,
             coalesce(S1.measurement_time, S1.override_time) as sirs_initial,
             coalesce(S2.measurement_time, S2.override_time) as sirs_onset,
             coalesce(D.measurement_time, D.override_time) as org_df_onset
      from sirs S1
      inner join sirs S2
        on S1.enc_id = S2.enc_id
        and S1.name <> S2.name
        and coalesce(S1.measurement_time, S1.override_time) <= coalesce(S2.measurement_time, S2.override_time)
      inner join org_df D on S1.enc_id = D.enc_id
    ) SO
    where not (SO.sirs_initial is null or SO.sirs_onset is null or SO.org_df_onset is null)
    and greatest(SO.sirs_onset, SO.org_df_onset) - least(SO.sirs_initial, SO.org_df_onset) < window_size
  ),

  -- TODO/BUG: all infection subqueries should return non-grouped infections
  -- for all possible matches, in the same way as sirs/orgdf triples.
  --
  app_infections as (
      select
          ainf.enc_id,
          ainf.name,
          first((ainf.measurement_time) order by coalesce(ainf.measurement_time, ainf.c_otime)) as measurement_time,
          first((ainf.value)            order by coalesce(ainf.measurement_time, ainf.c_otime))::text as value,
          first((ainf.c_otime)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_time,
          first((ainf.c_ouser)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_user,
          first((ainf.c_ovalue)         order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_value,
          coalesce(bool_or(ainf.is_met), false) as is_met,
          now() as update_date
      from (
          select  PC.enc_id,
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
      ) as ainf
      group by ainf.enc_id, ainf.name
  ),
  extracted_infections as (
    -- Use either clarity or cdm notes for now.
    -- We implement this as a union over two queries, each gated
    -- by a simple where clause based on an function argument.
    with notes_candidates as (
      select distinct T.enc_id from all_sirs_org_triples T
    ),
    null_infections as (
      -- get_window_labels_from_criteria explicitly requires a null value to yield states 10 and 12
      select P.enc_id,
             'suspicion_of_infection'::text    as name,
             null::timestamptz                 as measurement_time,
             null::text                        as value,
             null::timestamptz                 as override_time,
             null::text                        as override_user,
             null::json                        as override_value,
             false                             as is_met,
             now()                             as update_date
      from pat_ids P
    ),
    clarity_matches as (
        select P.enc_id                                                 as enc_id,
               'suspicion_of_infection'::text                           as name,
               min(M.start_ts)                                          as measurement_time,
               min(M.ngram)                                             as value,
               min(M.start_ts)                                          as override_time,
               'NLP'::text                                              as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true                                                     as is_met,
               now()                                                    as update_date
        from notes_candidates NC
        inner join pat_enc P on NC.enc_id = P.enc_id
        inner join lateral match_clarity_infections(P.visit_id, 3, 3) M on P.visit_id = M.csn_id
        where use_clarity_notes
        and not use_app_infections
        group by P.enc_id
    ),
    cdm_matches as (
        -- TODO: we have picked an arbitrary time interval for notes. Refine.
        select NC.enc_id                                                as enc_id,
               'suspicion_of_infection'::text                           as name,
               min(NTG.tsp)                                             as measurement_time,
               min(NTG.ngram)                                           as value,
               min(NTG.tsp)                                             as override_time,
               'NLP'::text                                              as override_user,
               json_agg(json_build_object('text'::text, NTG.ngram))     as override_value,
               true                                                     as is_met,
               now()                                                    as update_date
        from notes_candidates NC
        inner join lateral (
          select M.enc_id, M.dataset_id, N.*
          from cdm_processed_notes M, lateral unnest(M.tsps, M.ngrams) N(tsp, ngram)
          where NC.enc_id = M.enc_id
          and M.dataset_id = _dataset_id
          and N.tsp between ts_start - interval '1 days' and ts_end
          and not use_app_infections
        ) NTG
          on NC.enc_id = NTG.enc_id
          and NTG.dataset_id = _dataset_id
          and NTG.tsp between ts_start - interval '1 days' and ts_end
          and not use_app_infections
        group by NC.enc_id
    )
    select NI.enc_id,
           coalesce(MTCH.name,             NI.name             ) as name,
           coalesce(MTCH.measurement_time, NI.measurement_time ) as measurement_time,
           coalesce(MTCH.value,            NI.value            ) as value,
           coalesce(MTCH.override_time,    NI.override_time    ) as override_time,
           coalesce(MTCH.override_user,    NI.override_user    ) as override_user,
           coalesce(MTCH.override_value,   NI.override_value   ) as override_value,
           coalesce(MTCH.is_met,           NI.is_met           ) as is_met,
           coalesce(MTCH.update_date,      NI.update_date      ) as update_date
    from null_infections NI
    left join (
      select * from clarity_matches
      union all
      select * from cdm_matches
    ) MTCH on NI.enc_id = MTCH.enc_id
    where not use_app_infections
  ),
  infections as (
    select * from app_infections
    union all select * from extracted_infections
  ),


  severe_sepsis_candidates as (
    select SO.enc_id,
           bool_or(coalesce(I.infection_cnt, 0) > 0) as suspicion_of_infection,
           coalesce(
              first(I.infection_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where I.infection_onset is not null),
              'infinity'::timestamptz
           ) as inf_onset,

           coalesce(
              first(SO.sirs1_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs1_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs1_name,

           coalesce(
              first(SO.sirs2_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs2_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs2_name,

           coalesce(
              first(SO.odf_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.odf_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as odf_name,

           coalesce(
              first(SO.sirs_initial order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs_initial order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs_initial,

           coalesce(
              first(SO.sirs_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs_onset,

           coalesce(
              first(SO.org_df_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.org_df_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as org_df_onset

    from all_sirs_org_triples SO
    left join (
      select I.enc_id,
             (case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             (case when I.name = 'suspicion_of_infection' then I.override_time else null end) as infection_onset
      from infections I
    ) I
      on SO.enc_id = I.enc_id
      and (greatest(
                greatest(SO.sirs_onset, SO.org_df_onset) - I.infection_onset,
                I.infection_onset - least(SO.sirs_onset, SO.org_df_onset)
              ) < interval '6 hours')

    group by SO.enc_id
  ),

  severe_sepsis as (
    select
        CR.enc_id,
        CR.name,
        (first(CR.measurement_time order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as measurement_time,
        (first(CR.value            order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met))::text as value,
        (first(CR.override_time    order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_time,
        (first(CR.override_user    order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_user,
        (first(CR.override_value   order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_value,
        coalesce(bool_or(CR.is_met), false) as is_met,
        now() as update_date

    from (
      select * from all_sirs_org
      union all
      select I.enc_id, I.name, I.measurement_time, I.value, I.override_time, I.override_user, I.override_value, I.is_met
      from infections I
    ) CR
    left join severe_sepsis_candidates CD
      on CR.enc_id = CD.enc_id
      and CR.name in ( CD.sirs1_name, CD.sirs2_name, CD.odf_name, 'suspicion_of_infection' )

    where ( coalesce(CD.sirs1_name, CD.sirs2_name, CD.odf_name) is null )
    or (    ( CD.sirs1_name is not null and CD.sirs_initial = coalesce(CR.measurement_time, CR.override_time) )
         or ( CD.sirs2_name is not null and CD.sirs_onset   = coalesce(CR.measurement_time, CR.override_time) )
         or ( CD.odf_name   is not null and CD.org_df_onset = coalesce(CR.measurement_time, CR.override_time) )
         or ( CR.name = 'suspicion_of_infection' and (CD.inf_onset = 'infinity'::timestamptz or CR.override_time = CD.inf_onset) )
    )
    group by CR.enc_id, CR.name
  ),

  severe_sepsis_onsets as (
    select sspm.enc_id,
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
      select stats.enc_id,
             coalesce(bool_or(stats.suspicion_of_infection), false) as severe_sepsis_is_met,

             max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                          coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_onset,

             max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_wo_infection_onset,

             min(least(stats.inf_onset, stats.sirs_initial, stats.org_df_onset))
                as severe_sepsis_lead_time

      from severe_sepsis_candidates stats
      group by stats.enc_id
    ) sspm
  ),

  crystalloid_fluid_and_hypoperfusion as (
    select
        cfhf.enc_id,
        cfhf.name,
        first((case when cfhf.is_met then cfhf.measurement_time else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as measurement_time,
        first((case when cfhf.is_met then cfhf.value            else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime))::text as value,
        first((case when cfhf.is_met then cfhf.c_otime          else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as override_time,
        first((case when cfhf.is_met then cfhf.c_ouser          else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as override_user,
        first((case when cfhf.is_met then cfhf.c_ovalue         else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as override_value,
        coalesce(bool_or(cfhf.is_met), false) as is_met,
        now() as update_date
    from
    (
      with cf_and_hpf_cvalues as (
        select * from pat_cvalues CV where CV.name in ( 'crystalloid_fluid', 'initial_lactate' )
        union all
        select pat_ids.enc_id,
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
          on pat_ids.enc_id = SSP.enc_id
        left join infections c
          on pat_ids.enc_id = c.enc_id
          and cd.name = c.name
        left join cdm_t meas
            on pat_ids.enc_id = meas.enc_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              or (case when cd.name = 'initial_lactate_order'  then meas.value <> 'nan' and meas.tsp between SSP.severe_sepsis_onset - initial_lactate_lookback and ts_start - window_size else false end)
              or (case when cd.name <> 'initial_lactate_order' then meas.tsp between SSP.severe_sepsis_onset - orders_lookback and ts_start - window_size else false end)
            )
        where cd.dataset_id = _dataset_id
        and cd.name in ( 'crystalloid_fluid', 'initial_lactate' )
        and meas.value <> 'nan'
      )
      select  PC.enc_id,
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
              ) and (SSP.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp)
                      >= (case
                            when PC.name = 'initial_lactate'
                            then (SSP.severe_sepsis_onset - initial_lactate_lookback)
                            else (SSP.severe_sepsis_onset - orders_lookback) end))
              as is_met
      from cf_and_hpf_cvalues PC
      left join severe_sepsis_onsets SSP on PC.enc_id = SSP.enc_id
      where PC.name in ( 'crystalloid_fluid', 'initial_lactate' )
    ) as cfhf
    group by cfhf.enc_id, cfhf.name
  ),
  hypotension as (
    select
        ht.enc_id,
        ht.name,
        first((case when ht.is_met then ht.measurement_time else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as measurement_time,
        first((case when ht.is_met then ht.value            else null end) order by coalesce(ht.measurement_time, ht.c_otime))::text as value,
        first((case when ht.is_met then ht.c_otime          else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as override_time,
        first((case when ht.is_met then ht.c_ouser          else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as override_user,
        first((case when ht.is_met then ht.c_ovalue         else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as override_value,
        coalesce(bool_or(ht.is_met), false) as is_met,
        now() as update_date
    from
    (
        with pat_fluid_overrides as (
          select CFL.enc_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated'), false) as override
          from crystalloid_fluid_and_hypoperfusion CFL
          where CFL.name = 'crystalloid_fluid'
          group by CFL.enc_id
        ),
        pats_fluid_after_severe_sepsis as (
          select  MFL.enc_id,
                  MFL.tsp,
                  sum(MFL.value::numeric) as total_fluid,
                  -- Fluids are met if they are overriden or if we have more than
                  -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
                  (coalesce(bool_or(OV.override), false)
                      or coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.weight))
                  ) as is_met
          from cdm_t MFL
          left join pat_aggregates PW on MFL.enc_id = PW.enc_id
          left join severe_sepsis_onsets SSPN on MFL.enc_id = SSPN.enc_id
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
        left join severe_sepsis_onsets SSPN on PC.enc_id = SSPN.enc_id

        left join pats_fluid_after_severe_sepsis PFL
          on PC.enc_id = PFL.enc_id

        left join lateral (
          select meas.enc_id, meas.fid, meas.tsp, meas.value
          from cdm_t meas
          where PC.enc_id = meas.enc_id and PC.fid = meas.fid and PC.tsp < meas.tsp
          and meas.value <> 'nan'
          order by meas.tsp
          limit 1
        ) NEXT on PC.enc_id = NEXT.enc_id and PC.fid = NEXT.fid

        left join lateral (
          select BP.enc_id, max(BP.bp_sys) as value
          from pat_aggregates BP where PC.enc_id = BP.enc_id
          group by BP.enc_id
        ) PBPSYS on PC.enc_id = PBPSYS.enc_id

        where PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
    ) as ht
    group by ht.enc_id, ht.name
  ),

  septic_shock as (
    select * from crystalloid_fluid_and_hypoperfusion
    union all select * from hypotension
  ),

  -- Calculate septic shock in an extended window, for use in
  -- any criteria that has requirements after severe sepsis is met.
  septic_shock_onsets as (
    select stats.enc_id,
           bool_or(stats.cnt > 0) as septic_shock_is_met,
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
    from (
        -- Hypotension and hypoperfusion subqueries individually check
        -- that they occur after severe sepsis onset.
        (select hypotension.enc_id,
                sum(case when hypotension.is_met then 1 else 0 end) as cnt,
                min(hypotension.measurement_time) as onset
         from hypotension
         group by hypotension.enc_id)
        union
        (select HPF.enc_id,
                sum(case when HPF.is_met then 1 else 0 end) as cnt,
                min(HPF.measurement_time) as onset
         from crystalloid_fluid_and_hypoperfusion HPF
         where HPF.name = 'initial_lactate'
         group by HPF.enc_id)
    ) stats
    left join severe_sepsis_onsets SSP on stats.enc_id = SSP.enc_id
    group by stats.enc_id
  ),

  orders_criteria as (
    select
        ordc.enc_id,
        ordc.name,
        coalesce(   first((case when ordc.is_met then ordc.measurement_time else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last(ordc.measurement_time order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as measurement_time,
        coalesce(   first((case when ordc.is_met then ordc.value else null end) order by coalesce(ordc.measurement_time, ordc.c_otime))::text,
                    last(ordc.value order by coalesce(ordc.measurement_time, ordc.c_otime))::text
        ) as value,
        coalesce(   first((case when ordc.is_met then ordc.c_otime else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last((ordc.c_otime) order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as override_time,
        coalesce(   first((case when ordc.is_met then ordc.c_ouser else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last(ordc.c_ouser order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as override_user,
        coalesce(
            first((case when ordc.is_met then ordc.c_ovalue else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
            last(ordc.c_ovalue order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as override_value,
        coalesce(bool_or(ordc.is_met), false) as is_met,
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
        select pat_ids.enc_id,
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
          on pat_ids.enc_id = SSP.enc_id
        left join infections c
          on pat_ids.enc_id = c.enc_id
          and cd.name = c.name
        left join cdm_t meas
            on pat_ids.enc_id = meas.enc_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              or (cd.name = 'blood_culture_order'     and meas.value <> 'nan' and meas.tsp between SSP.severe_sepsis_onset - blood_culture_order_lookback   and ts_start - window_size)
              or (cd.name = 'antibiotics_order'       and meas.value <> 'nan' and meas.tsp between SSP.severe_sepsis_onset - antibiotics_order_lookback     and ts_start - window_size)
              or (cd.name = 'initial_lactate_order'   and meas.tsp between SSP.severe_sepsis_onset - initial_lactate_lookback       and ts_start - window_size)
              or (cd.name = 'crystalloid_fluid_order' and meas.tsp between SSP.severe_sepsis_onset - orders_lookback                and ts_start - window_size)
              or (case
                    when cd.name not in ('blood_culture_order', 'initial_lactate_order', 'antibiotics_order', 'crystalloid_fluid_order')
                    then meas.tsp between SSP.severe_sepsis_onset and ts_start - window_size
                    else false end)  -- NOTE: this lookback is weaker than CMS criteria.
            )
        where cd.dataset_id = _dataset_id
        and cd.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
        and meas.value <> 'nan'
      )
      select  CV.enc_id,
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
                                              > (case
                                                  when CV.name = 'blood_culture_order'
                                                  then SSP.severe_sepsis_onset - blood_culture_order_lookback
                                                  when CV.name = 'initial_lactate_order'
                                                  then SSP.severe_sepsis_onset - initial_lactate_lookback
                                                  else SSP.severe_sepsis_onset
                                                  end))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.enc_id = coalesce(this_enc_id, SSP.enc_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_severe_sepsis_dose' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (case
                                                  when CV.name = 'antibiotics_order'
                                                  then SSP.severe_sepsis_onset - antibiotics_order_lookback
                                                  when CV.name = 'crystalloid_fluid_order'
                                                  then SSP.severe_sepsis_onset - orders_lookback
                                                  else SSP.severe_sepsis_onset
                                                  end))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.enc_id = coalesce(this_enc_id, SSP.enc_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}',
                                         (case when isnumeric(CV.value) then CV.value::numeric else null::numeric end),
                                         coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                                  (CV.d_ovalue#>>'{lower}')::numeric)) )

                  when CV.category = 'after_septic_shock' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.enc_id = coalesce(this_enc_id, SSH.enc_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_septic_shock_dose' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.enc_id = coalesce(this_enc_id, SSH.enc_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}',
                                         (case when isnumeric(CV.value) then CV.value::numeric else null::numeric end),
                                         coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                                  (CV.d_ovalue#>>'{lower}')::numeric)) )

                  else criteria_value_met(CV.value, CV.c_ovalue, CV.d_ovalue)
                  end
              ) as is_met
      from orders_cvalues CV
    ) as ordc
    group by ordc.enc_id, ordc.name
  ),
  repeat_lactate as (
    select
        rlc.enc_id,
        rlc.name,
        first((case when rlc.is_met then rlc.measurement_time else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as measurement_time,
        first((case when rlc.is_met then rlc.value            else null end) order by coalesce(rlc.measurement_time, rlc.c_otime))::text as value,
        first((case when rlc.is_met then rlc.c_otime          else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as override_time,
        first((case when rlc.is_met then rlc.c_ouser          else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as override_user,
        first((case when rlc.is_met then rlc.c_ovalue         else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as override_value,
        coalesce(bool_or(rlc.is_met), false) as is_met,
        now() as update_date
    from
    (
        select  pat_cvalues.enc_id,
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
            select oc.enc_id,
                   max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                   coalesce(bool_or(oc.is_met), false) as is_met,
                   coalesce(min(oc.value) = 'Completed', false) as is_completed
            from orders_criteria oc
            where oc.name = 'initial_lactate_order'
            group by oc.enc_id
        ) initial_lactate_order on pat_cvalues.enc_id = initial_lactate_order.enc_id
        left join (
            select p3.enc_id,
                   max(case when p3.value::numeric > 2.0 then p3.tsp else null end) tsp,
                   coalesce(bool_or(p3.value::numeric > 2.0), false) is_met
            from pat_cvalues p3
            where p3.name = 'initial_lactate'
            group by p3.enc_id
        ) lactate_results on pat_cvalues.enc_id = lactate_results.enc_id
        where pat_cvalues.name = 'repeat_lactate_order'
    )
    as rlc
    group by rlc.enc_id, rlc.name
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
  left join severe_sepsis_onsets SSP on new_criteria.enc_id = SSP.enc_id
  left join septic_shock_onsets SSH on new_criteria.enc_id = SSH.enc_id;

  return;
END; $function$;



-- Variant of get_cms_labels_for_window with labeling for new order values in JSON format.
-- Here we explicitly pull out the collected time for blood cultures,
-- and the result time for lactates
CREATE OR REPLACE FUNCTION get_cms_labels_for_window_with_complex_orders(
                this_enc_id                      int,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                use_app_infections               boolean default false,
                use_clarity_notes                boolean default false,
                save_criteria_windows            boolean default false
  )
  RETURNS table(enc_id                           int,
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

  -- Criteria Lookbacks.
  initial_lactate_lookback       interval := interval '6 hours';
  orders_lookback                interval := interval '6 hours';
  blood_culture_order_lookback   interval := interval '48 hours';
  antibiotics_order_lookback     interval := interval '24 hours';
BEGIN

  select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
  -- raise notice 'Running get_cms_labels_for_window_with_complex_orders on pat %, dataset_id % between %, %',this_enc_id, _dataset_id, ts_start , ts_end;

  return query
  with pat_ids as (
    select distinct P.enc_id from pat_enc P
    where P.enc_id = coalesce(this_enc_id, P.enc_id) and P.dataset_id = _dataset_id
  ),
  user_overrides as (
    -- TODO: dispatch to override generator
    select * from (
      values (null::integer, null::int, null::varchar, null::varchar, null::varchar, null::timestamptz, null::text, null::timestamptz, null::text, null::json)
    ) as criteria (dataset_id, enc_id, name, fid, category, tsp, value, override_time, override_user, override_value)
  ),
  pat_cvalues as (
    select pat_ids.enc_id,
           cd.name,
           meas.fid,
           cd.category,
           meas.tsp,
           (case when cd.name = 'vasopressors_order' and meas.value like '%-%' then substring(meas.value from '[0-9\.]*') else meas.value end) as value,
           c.override_time as c_otime,
           c.override_user as c_ouser,
           c.override_value as c_ovalue,
           cd.override_value as d_ovalue
    from pat_ids
    cross join criteria_default as cd
    left join user_overrides c
      on pat_ids.enc_id = c.enc_id
      and cd.name = c.name
      and cd.dataset_id = c.dataset_id
    left join cdm_t meas
        on pat_ids.enc_id = meas.enc_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null or meas.tsp between ts_start - window_size and ts_end)
    where cd.dataset_id = _dataset_id
    and meas.dataset_id = _dataset_id
    and meas.value <> 'nan'
  ),
  pat_aggregates as (
    select aggs.enc_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.enc_id,
               meas.tsp as measurement_time,
               (case when meas.fid in ('nbp_sys', 'abp_sys') then meas.value::numeric else null end) as bp_sys,
               (case when meas.fid = 'weight' then meas.value::numeric else null end) as weight,
               (case when meas.fid = 'urine_output'
                     and ts_end - meas.tsp < interval '2 hours'
                     then meas.value::numeric else null end
                ) as urine_output
        from pat_ids P
        inner join cdm_t meas on P.enc_id = meas.enc_id
        where meas.fid in ('nbp_sys', 'abp_sys', 'urine_output', 'weight')
        and isnumeric(meas.value)
        and meas.dataset_id = _dataset_id
    ) as aggs
    group by aggs.enc_id
  ),
  all_sirs_org as (
    select  PC.enc_id,
            PC.name,
            PC.tsp as measurement_time,
            (case
              when PC.name = 'respiratory_failure'
              then (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value)))
              else PC.value
             end) as value,
            PC.c_otime  as override_time,
            PC.c_ouser  as override_user,
            PC.c_ovalue as override_value,
            (case
              when PC.name = 'respiratory_failure'
              then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

              when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
              then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

              else
              (case
                  when PC.category = 'decrease_in_sbp' then
                    decrease_in_sbp_met(
                      (select max(PBP.bp_sys) from pat_aggregates PBP where PBP.enc_id = PC.enc_id),
                      PC.value, PC.c_ovalue, PC.d_ovalue
                    )

                  when PC.category = 'urine_output' then
                    urine_output_met(
                        (select max(PUO.urine_output) from pat_aggregates PUO where PUO.enc_id = PC.enc_id),
                        (select max(PW.weight) from pat_aggregates PW where PW.enc_id = PC.enc_id),
                        _dataset_id
                    )

                  else criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)
                  end
                )
             end) as is_met
    from pat_cvalues PC
    where PC.name in (
      'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
      'respiratory_failure',
      'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
    )
  ),
  all_sirs_org_triples as (
    with sirs as (
      select * from all_sirs_org S
      where S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
    ),
    org_df as (
      select * from all_sirs_org S
      where S.name in (
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
      and S.is_met
    )
    select SO.enc_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset
    from (
      select S1.enc_id,
             S1.name as sirs1_name,
             S2.name as sirs2_name,
             D.name as odf_name,
             coalesce(S1.measurement_time, S1.override_time) as sirs_initial,
             coalesce(S2.measurement_time, S2.override_time) as sirs_onset,
             coalesce(D.measurement_time, D.override_time) as org_df_onset
      from sirs S1
      inner join sirs S2
        on S1.enc_id = S2.enc_id
        and S1.name <> S2.name
        and coalesce(S1.measurement_time, S1.override_time) <= coalesce(S2.measurement_time, S2.override_time)
      inner join org_df D on S1.enc_id = D.enc_id
    ) SO
    where not (SO.sirs_initial is null or SO.sirs_onset is null or SO.org_df_onset is null)
    and greatest(SO.sirs_onset, SO.org_df_onset) - least(SO.sirs_initial, SO.org_df_onset) < window_size
  ),

  -- TODO/BUG: all infection subqueries should return non-grouped infections
  -- for all possible matches, in the same way as sirs/orgdf triples.
  --
  app_infections as (
      select
          ainf.enc_id,
          ainf.name,
          first((ainf.measurement_time) order by coalesce(ainf.measurement_time, ainf.c_otime)) as measurement_time,
          first((ainf.value)            order by coalesce(ainf.measurement_time, ainf.c_otime))::text as value,
          first((ainf.c_otime)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_time,
          first((ainf.c_ouser)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_user,
          first((ainf.c_ovalue)         order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_value,
          coalesce(bool_or(ainf.is_met), false) as is_met,
          now() as update_date
      from (
          select  PC.enc_id,
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
      ) as ainf
      group by ainf.enc_id, ainf.name
  ),
  extracted_infections as (
    -- Use either clarity or cdm notes for now.
    -- We implement this as a union over two queries, each gated
    -- by a simple where clause based on an function argument.
    with notes_candidates as (
      select distinct T.enc_id from all_sirs_org_triples T
    ),
    null_infections as (
      -- get_window_labels_from_criteria explicitly requires a null value to yield states 10 and 12
      select P.enc_id,
             'suspicion_of_infection'::text    as name,
             null::timestamptz                 as measurement_time,
             null::text                        as value,
             null::timestamptz                 as override_time,
             null::text                        as override_user,
             null::json                        as override_value,
             false                             as is_met,
             now()                             as update_date
      from pat_ids P
    ),
    clarity_matches as (
        select P.enc_id                                                 as enc_id,
               'suspicion_of_infection'::text                           as name,
               min(M.start_ts)                                          as measurement_time,
               min(M.ngram)                                             as value,
               min(M.start_ts)                                          as override_time,
               'NLP'::text                                              as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true                                                     as is_met,
               now()                                                    as update_date
        from notes_candidates NC
        inner join pat_enc P on NC.enc_id = P.enc_id
        inner join lateral match_clarity_infections(P.visit_id, 3, 3) M on P.visit_id = M.csn_id
        where use_clarity_notes
        and not use_app_infections
        group by P.enc_id
    ),
    cdm_matches as (
        -- TODO: we have picked an arbitrary time interval for notes. Refine.
        select NC.enc_id                                                as enc_id,
               'suspicion_of_infection'::text                           as name,
               min(NTG.tsp)                                             as measurement_time,
               min(NTG.ngram)                                           as value,
               min(NTG.tsp)                                             as override_time,
               'NLP'::text                                              as override_user,
               json_agg(json_build_object('text'::text, NTG.ngram))     as override_value,
               true                                                     as is_met,
               now()                                                    as update_date
        from notes_candidates NC
        inner join lateral (
          select M.enc_id, M.dataset_id, N.*
          from cdm_processed_notes M, lateral unnest(M.tsps, M.ngrams) N(tsp, ngram)
          where NC.enc_id = M.enc_id
          and M.dataset_id = _dataset_id
          and N.tsp between ts_start - interval '1 days' and ts_end
          and not use_app_infections
        ) NTG
          on NC.enc_id = NTG.enc_id
          and NTG.dataset_id = _dataset_id
          and NTG.tsp between ts_start - interval '1 days' and ts_end
          and not use_app_infections
        group by NC.enc_id
    )
    select NI.enc_id,
           coalesce(MTCH.name,             NI.name             ) as name,
           coalesce(MTCH.measurement_time, NI.measurement_time ) as measurement_time,
           coalesce(MTCH.value,            NI.value            ) as value,
           coalesce(MTCH.override_time,    NI.override_time    ) as override_time,
           coalesce(MTCH.override_user,    NI.override_user    ) as override_user,
           coalesce(MTCH.override_value,   NI.override_value   ) as override_value,
           coalesce(MTCH.is_met,           NI.is_met           ) as is_met,
           coalesce(MTCH.update_date,      NI.update_date      ) as update_date
    from null_infections NI
    left join (
      select * from clarity_matches
      union all
      select * from cdm_matches
    ) MTCH on NI.enc_id = MTCH.enc_id
    where not use_app_infections
  ),
  infections as (
    select * from app_infections
    union all select * from extracted_infections
  ),


  severe_sepsis_candidates as (
    select SO.enc_id,
           bool_or(coalesce(I.infection_cnt, 0) > 0) as suspicion_of_infection,
           coalesce(
              first(I.infection_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where I.infection_onset is not null),
              'infinity'::timestamptz
           ) as inf_onset,

           coalesce(
              first(SO.sirs1_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs1_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs1_name,

           coalesce(
              first(SO.sirs2_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs2_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs2_name,

           coalesce(
              first(SO.odf_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.odf_name order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as odf_name,

           coalesce(
              first(SO.sirs_initial order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs_initial order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs_initial,

           coalesce(
              first(SO.sirs_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.sirs_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as sirs_onset,

           coalesce(
              first(SO.org_df_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
                filter (where coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null),
              first(SO.org_df_onset order by greatest(SO.sirs_initial, SO.sirs_onset, SO.org_df_onset))
           ) as org_df_onset

    from all_sirs_org_triples SO
    left join (
      select I.enc_id,
             (case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             (case when I.name = 'suspicion_of_infection' then I.override_time else null end) as infection_onset
      from infections I
    ) I
      on SO.enc_id = I.enc_id
      and (greatest(
                greatest(SO.sirs_onset, SO.org_df_onset) - I.infection_onset,
                I.infection_onset - least(SO.sirs_onset, SO.org_df_onset)
              ) < interval '6 hours')

    group by SO.enc_id
  ),

  severe_sepsis as (
    select
        CR.enc_id,
        CR.name,
        (first(CR.measurement_time order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as measurement_time,
        (first(CR.value            order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met))::text as value,
        (first(CR.override_time    order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_time,
        (first(CR.override_user    order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_user,
        (first(CR.override_value   order by coalesce(CR.measurement_time, CR.override_time)) filter (where save_criteria_windows or CR.is_met)) as override_value,
        coalesce(bool_or(CR.is_met), false) as is_met,
        now() as update_date

    from (
      select * from all_sirs_org
      union all
      select I.enc_id, I.name, I.measurement_time, I.value, I.override_time, I.override_user, I.override_value, I.is_met
      from infections I
    ) CR
    left join severe_sepsis_candidates CD
      on CR.enc_id = CD.enc_id
      and CR.name in ( CD.sirs1_name, CD.sirs2_name, CD.odf_name, 'suspicion_of_infection' )

    where ( coalesce(CD.sirs1_name, CD.sirs2_name, CD.odf_name) is null )
    or (    ( CD.sirs1_name is not null and CD.sirs_initial = coalesce(CR.measurement_time, CR.override_time) )
         or ( CD.sirs2_name is not null and CD.sirs_onset   = coalesce(CR.measurement_time, CR.override_time) )
         or ( CD.odf_name   is not null and CD.org_df_onset = coalesce(CR.measurement_time, CR.override_time) )
         or ( CR.name = 'suspicion_of_infection' and (CD.inf_onset = 'infinity'::timestamptz or CR.override_time = CD.inf_onset) )
    )
    group by CR.enc_id, CR.name
  ),

  severe_sepsis_onsets as (
    select sspm.enc_id,
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
      select stats.enc_id,
             coalesce(bool_or(stats.suspicion_of_infection), false) as severe_sepsis_is_met,

             max(greatest(coalesce(stats.inf_onset, 'infinity'::timestamptz),
                          coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_onset,

             max(greatest(coalesce(stats.sirs_onset, 'infinity'::timestamptz),
                          coalesce(stats.org_df_onset, 'infinity'::timestamptz))
                 ) as severe_sepsis_wo_infection_onset,

             min(least(stats.inf_onset, stats.sirs_initial, stats.org_df_onset))
                as severe_sepsis_lead_time

      from severe_sepsis_candidates stats
      group by stats.enc_id
    ) sspm
  ),

  crystalloid_fluid_and_hypoperfusion as (
    select
        cfhf.enc_id,
        cfhf.name,
        first((case when cfhf.is_met then cfhf.measurement_time else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as measurement_time,
        first((case when cfhf.is_met then cfhf.value            else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime))::text as value,
        first((case when cfhf.is_met then cfhf.c_otime          else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as override_time,
        first((case when cfhf.is_met then cfhf.c_ouser          else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as override_user,
        first((case when cfhf.is_met then cfhf.c_ovalue         else null end) order by coalesce(cfhf.measurement_time, cfhf.c_otime)) as override_value,
        coalesce(bool_or(cfhf.is_met), false) as is_met,
        now() as update_date
    from
    (
      with cf_and_hpf_cvalues as (
        select * from pat_cvalues CV where CV.name in ( 'crystalloid_fluid', 'initial_lactate' )
        union all
        select pat_ids.enc_id,
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
          on pat_ids.enc_id = SSP.enc_id
        left join infections c
          on pat_ids.enc_id = c.enc_id
          and cd.name = c.name
        left join cdm_t meas
            on pat_ids.enc_id = meas.enc_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              or (case when cd.name = 'initial_lactate_order'  then meas.value <> 'nan' and meas.value::json#>>'{result_tsp}' <> 'NaT' and (meas.value::json#>>'{result_tsp}')::timestamptz between SSP.severe_sepsis_onset - initial_lactate_lookback and ts_start - window_size else false end)
              or (case when cd.name <> 'initial_lactate_order' then meas.tsp between SSP.severe_sepsis_onset - orders_lookback and ts_start - window_size else false end)
            )
        where cd.dataset_id = _dataset_id
        and cd.name in ( 'crystalloid_fluid', 'initial_lactate' )
        and meas.value <> 'nan'
      )
      select  PC.enc_id,
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
              ) and (SSP.severe_sepsis_is_met and coalesce(PC.c_otime, PC.tsp)
                      >= (case
                            when PC.name = 'initial_lactate'
                            then (SSP.severe_sepsis_onset - initial_lactate_lookback)
                            else (SSP.severe_sepsis_onset - orders_lookback) end))
              as is_met
      from cf_and_hpf_cvalues PC
      left join severe_sepsis_onsets SSP on PC.enc_id = SSP.enc_id
      where PC.name in ( 'crystalloid_fluid', 'initial_lactate' )
    ) as cfhf
    group by cfhf.enc_id, cfhf.name
  ),
  hypotension as (
    select
        ht.enc_id,
        ht.name,
        first((case when ht.is_met then ht.measurement_time else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as measurement_time,
        first((case when ht.is_met then ht.value            else null end) order by coalesce(ht.measurement_time, ht.c_otime))::text as value,
        first((case when ht.is_met then ht.c_otime          else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as override_time,
        first((case when ht.is_met then ht.c_ouser          else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as override_user,
        first((case when ht.is_met then ht.c_ovalue         else null end) order by coalesce(ht.measurement_time, ht.c_otime)) as override_value,
        coalesce(bool_or(ht.is_met), false) as is_met,
        now() as update_date
    from
    (
        with pat_fluid_overrides as (
          select CFL.enc_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated'), false) as override
          from crystalloid_fluid_and_hypoperfusion CFL
          where CFL.name = 'crystalloid_fluid'
          group by CFL.enc_id
        ),
        pats_fluid_after_severe_sepsis as (
          select  MFL.enc_id,
                  MFL.tsp,
                  sum(MFL.value::numeric) as total_fluid,
                  -- Fluids are met if they are overriden or if we have more than
                  -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
                  (coalesce(bool_or(OV.override), false)
                      or coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.weight))
                  ) as is_met
          from cdm_t MFL
          left join pat_aggregates PW on MFL.enc_id = PW.enc_id
          left join severe_sepsis_onsets SSPN on MFL.enc_id = SSPN.enc_id
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
        left join severe_sepsis_onsets SSPN on PC.enc_id = SSPN.enc_id

        left join pats_fluid_after_severe_sepsis PFL
          on PC.enc_id = PFL.enc_id

        left join lateral (
          select meas.enc_id, meas.fid, meas.tsp, meas.value
          from cdm_t meas
          where PC.enc_id = meas.enc_id and PC.fid = meas.fid and PC.tsp < meas.tsp
          and meas.value <> 'nan'
          order by meas.tsp
          limit 1
        ) NEXT on PC.enc_id = NEXT.enc_id and PC.fid = NEXT.fid

        left join lateral (
          select BP.enc_id, max(BP.bp_sys) as value
          from pat_aggregates BP where PC.enc_id = BP.enc_id
          group by BP.enc_id
        ) PBPSYS on PC.enc_id = PBPSYS.enc_id

        where PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
    ) as ht
    group by ht.enc_id, ht.name
  ),

  septic_shock as (
    select * from crystalloid_fluid_and_hypoperfusion
    union all select * from hypotension
  ),

  -- Calculate septic shock in an extended window, for use in
  -- any criteria that has requirements after severe sepsis is met.
  septic_shock_onsets as (
    select stats.enc_id,
           bool_or(stats.cnt > 0) as septic_shock_is_met,
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
    from (
        -- Hypotension and hypoperfusion subqueries individually check
        -- that they occur after severe sepsis onset.
        (select hypotension.enc_id,
                sum(case when hypotension.is_met then 1 else 0 end) as cnt,
                min(hypotension.measurement_time) as onset
         from hypotension
         group by hypotension.enc_id)
        union
        (select HPF.enc_id,
                sum(case when HPF.is_met then 1 else 0 end) as cnt,
                min(HPF.measurement_time) as onset
         from crystalloid_fluid_and_hypoperfusion HPF
         where HPF.name = 'initial_lactate'
         group by HPF.enc_id)
    ) stats
    left join severe_sepsis_onsets SSP on stats.enc_id = SSP.enc_id
    group by stats.enc_id
  ),

  orders_criteria as (
    select
        ordc.enc_id,
        ordc.name,
        coalesce(   first((case when ordc.is_met then ordc.measurement_time else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last(ordc.measurement_time order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as measurement_time,
        coalesce(   first((case when ordc.is_met then ordc.value else null end) order by coalesce(ordc.measurement_time, ordc.c_otime))::text,
                    last(ordc.value order by coalesce(ordc.measurement_time, ordc.c_otime))::text
        ) as value,
        coalesce(   first((case when ordc.is_met then ordc.c_otime else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last((ordc.c_otime) order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as override_time,
        coalesce(   first((case when ordc.is_met then ordc.c_ouser else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last(ordc.c_ouser order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as override_user,
        coalesce(
            first((case when ordc.is_met then ordc.c_ovalue else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
            last(ordc.c_ovalue order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as override_value,
        coalesce(bool_or(ordc.is_met), false) as is_met,
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
        select CV.enc_id,
               CV.name,
               CV.fid,
               CV.category,
               (case when CV.name = 'blood_culture_order' then (CV.value::json#>>'{collect_tsp}')::timestamptz
                     when CV.name = 'initial_lactate_order' then (CV.value::json#>>'{result_tsp}')::timestamptz
                     else CV.tsp
                end) as tsp,
               (case when CV.name = 'blood_culture_order' then CV.value::json#>>'{status}'
                     when CV.name = 'initial_lactate_order' then CV.value::json#>>'{status}'
                     when CV.name = 'vasopressors_order' and CV.value like '%-%' then substring(CV.value from '[0-9\.]*')
                     else CV.value
                end) as value,
               CV.c_otime,
               CV.c_ouser,
               CV.c_ovalue,
               CV.d_ovalue
        from pat_cvalues CV
        where CV.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
        and (case when CV.name = 'blood_culture_order' then CV.value::json#>>'{collect_tsp}' <> 'NaT'
                  when CV.name = 'initial_lactate_order' then CV.value::json#>>'{result_tsp}' <> 'NaT'
                  else true
              end)
        union all
        select pat_ids.enc_id,
               cd.name,
               meas.fid,
               cd.category,
               (case when cd.name = 'blood_culture_order' then (meas.value::json#>>'{collect_tsp}')::timestamptz
                     when cd.name = 'initial_lactate_order' then (meas.value::json#>>'{result_tsp}')::timestamptz
                     else meas.tsp
                end) as tsp,
               (case when cd.name = 'blood_culture_order' then meas.value::json#>>'{status}'
                     when cd.name = 'initial_lactate_order' then meas.value::json#>>'{status}'
                     when cd.name = 'vasopressors_order' and meas.value like '%-%' then substring(meas.value from '[0-9\.]*')
                     else meas.value
                end) as value,
               c.override_time as c_otime,
               c.override_user as c_ouser,
               c.override_value as c_ovalue,
               cd.override_value as d_ovalue
        from pat_ids
        cross join criteria_default as cd
        left join severe_sepsis_onsets SSP
          on pat_ids.enc_id = SSP.enc_id
        left join infections c
          on pat_ids.enc_id = c.enc_id
          and cd.name = c.name
        left join cdm_t meas
            on pat_ids.enc_id = meas.enc_id
            and meas.fid = cd.fid
            and cd.dataset_id = meas.dataset_id
            and (meas.tsp is null
              or (case when cd.name = 'blood_culture_order'     then meas.value <> 'nan' and meas.value::json#>>'{collect_tsp}' <> 'NaT' and (meas.value::json#>>'{collect_tsp}')::timestamptz between SSP.severe_sepsis_onset - blood_culture_order_lookback   and ts_start - window_size else false end)
              or (case when cd.name = 'initial_lactate_order'   then meas.value <> 'nan' and meas.value::json#>>'{result_tsp}'  <> 'NaT' and (meas.value::json#>>'{result_tsp}')::timestamptz  between SSP.severe_sepsis_onset - initial_lactate_lookback       and ts_start - window_size else false end)
              or (cd.name = 'antibiotics_order'       and meas.tsp between SSP.severe_sepsis_onset - antibiotics_order_lookback     and ts_start - window_size)
              or (cd.name = 'crystalloid_fluid_order' and meas.tsp between SSP.severe_sepsis_onset - orders_lookback                and ts_start - window_size)
              or (case
                  when cd.name not in ('blood_culture_order', 'initial_lactate_order', 'antibiotics_order', 'crystalloid_fluid_order')
                  then meas.tsp between SSP.severe_sepsis_onset and ts_start - window_size
                  else false end) -- NOTE: this lookback is weaker than CMS criteria.
            )
        where cd.dataset_id = _dataset_id
        and meas.value <> 'nan'
        and cd.name in (
          'initial_lactate_order',
          'blood_culture_order',
          'antibiotics_order',
          'crystalloid_fluid_order',
          'vasopressors_order'
        )
      )
      select  CV.enc_id,
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
                                              > (case
                                                  when CV.name = 'blood_culture_order'
                                                  then SSP.severe_sepsis_onset - blood_culture_order_lookback
                                                  when CV.name = 'initial_lactate_order'
                                                  then SSP.severe_sepsis_onset - initial_lactate_lookback
                                                  else SSP.severe_sepsis_onset
                                                  end))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.enc_id = coalesce(this_enc_id, SSP.enc_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_severe_sepsis_dose' then
                    (select coalesce(
                              bool_or(SSP.severe_sepsis_is_met
                                        and greatest(CV.c_otime, CV.tsp)
                                              > (case
                                                  when CV.name = 'antibiotics_order'
                                                  then SSP.severe_sepsis_onset - antibiotics_order_lookback
                                                  when CV.name = 'crystalloid_fluid_order'
                                                  then SSP.severe_sepsis_onset - orders_lookback
                                                  else SSP.severe_sepsis_onset
                                                  end))
                              , false)
                      from severe_sepsis_onsets SSP
                      where SSP.enc_id = coalesce(this_enc_id, SSP.enc_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}',
                                         (case when isnumeric(CV.value) then CV.value::numeric else null::numeric end),
                                         coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                                  (CV.d_ovalue#>>'{lower}')::numeric)) )

                  when CV.category = 'after_septic_shock' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.enc_id = coalesce(this_enc_id, SSH.enc_id)
                    )
                    and ( order_met(CV.name, coalesce(CV.c_ovalue#>>'{0,text}', CV.value)) )

                  when CV.category = 'after_septic_shock_dose' then
                    (select coalesce(
                              bool_or(SSH.septic_shock_is_met
                                        and greatest(CV.c_otime, CV.tsp) > SSH.septic_shock_onset)
                              , false)
                      from septic_shock_onsets SSH
                      where SSH.enc_id = coalesce(this_enc_id, SSH.enc_id)
                    )
                    and ( dose_order_met(CV.fid, CV.c_ovalue#>>'{0,text}',
                                         (case when isnumeric(CV.value) then CV.value::numeric else null::numeric end),
                                         coalesce((CV.c_ovalue#>>'{0,lower}')::numeric,
                                                  (CV.d_ovalue#>>'{lower}')::numeric)) )

                  else criteria_value_met(CV.value, CV.c_ovalue, CV.d_ovalue)
                  end
              ) as is_met
      from orders_cvalues CV
    ) as ordc
    group by ordc.enc_id, ordc.name
  ),
  repeat_lactate as (
    select
        rlc.enc_id,
        rlc.name,
        first((case when rlc.is_met then rlc.measurement_time else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as measurement_time,
        first((case when rlc.is_met then rlc.value            else null end) order by coalesce(rlc.measurement_time, rlc.c_otime))::text as value,
        first((case when rlc.is_met then rlc.c_otime          else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as override_time,
        first((case when rlc.is_met then rlc.c_ouser          else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as override_user,
        first((case when rlc.is_met then rlc.c_ovalue         else null end) order by coalesce(rlc.measurement_time, rlc.c_otime)) as override_value,
        coalesce(bool_or(rlc.is_met), false) as is_met,
        now() as update_date
    from
    (
        select  pat_cvalues.enc_id,
                pat_cvalues.name,
                (pat_cvalues.value::json#>>'{result_tsp}')::timestamptz as measurement_time,
                order_status(pat_cvalues.fid, pat_cvalues.value::json#>>'{status}', pat_cvalues.c_ovalue#>>'{0,text}') as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                ((
                  coalesce(initial_lactate_order.is_met and lactate_results.is_met, false)
                    and order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value::json#>>'{status}'))
                    and (coalesce((pat_cvalues.value::json#>>'{result_tsp}')::timestamptz > initial_lactate_order.tsp, false)
                            and coalesce(lactate_results.tsp > initial_lactate_order.tsp, false))
                ) or
                (
                  not( coalesce(initial_lactate_order.is_completed
                                  and ( lactate_results.is_met or
                                        (pat_cvalues.value::json#>>'{result_tsp}')::timestamptz <= initial_lactate_order.tsp )
                                , false) )
                )) is_met
        from pat_cvalues
        left join (
            select oc.enc_id,
                   max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                   coalesce(bool_or(oc.is_met), false) as is_met,
                   coalesce(min(oc.value) = 'Completed', false) as is_completed
            from orders_criteria oc
            where oc.name = 'initial_lactate_order'
            group by oc.enc_id
        ) initial_lactate_order on pat_cvalues.enc_id = initial_lactate_order.enc_id
        left join (
            select p3.enc_id,
                   max(case when p3.value::numeric > 2.0 then p3.tsp else null end) tsp,
                   coalesce(bool_or(p3.value::numeric > 2.0), false) is_met
            from pat_cvalues p3
            where p3.name = 'initial_lactate'
            group by p3.enc_id
        ) lactate_results on pat_cvalues.enc_id = lactate_results.enc_id
        where pat_cvalues.name = 'repeat_lactate_order'
        and   pat_cvalues.value::json#>>'{result_tsp}' <> 'NaT'
    )
    as rlc
    group by rlc.enc_id, rlc.name
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
  left join severe_sepsis_onsets SSP on new_criteria.enc_id = SSP.enc_id
  left join septic_shock_onsets SSH on new_criteria.enc_id = SSH.enc_id;

  return;
END; $function$;



----------------------------------------------------------
-- Per-window label calculation

CREATE OR REPLACE FUNCTION get_window_labels_from_criteria(table_name text, _enc_id int)
  RETURNS table( ts timestamptz, enc_id int, state int)
AS $func$ BEGIN
  RETURN QUERY EXECUTE format(
  'select stats.ts, stats.enc_id,
      (
        case
        when ssp_present and ssh_present then (
          case
          when stats.ts - stats.severe_sepsis_onset >= ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
          when stats.ts - stats.severe_sepsis_onset >= ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
          when stats.ts - stats.septic_shock_onset  >= ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
          when stats.ts - stats.septic_shock_onset  >= ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
          when stats.ts - stats.severe_sepsis_onset >= ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
          when stats.ts - stats.severe_sepsis_onset >= ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com

          else 30
          end
        )

        when ssp_present then (
          case
          when stats.ts - stats.severe_sepsis_onset >= ''3 hours''::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
          when stats.ts - stats.severe_sepsis_onset >= ''6 hours''::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
          when stats.ts - stats.severe_sepsis_onset >= ''6 hours''::interval and sev_sep_6hr_count = 1 then 23 -- sev_sep_6hr_com
          when stats.ts - stats.severe_sepsis_onset >= ''3 hours''::interval and sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com

          else 20
          end
        )

        when sspwoi_present and sus_count = 1 then 13 -- sirs, orgdf and note, but no ssp (i.e., not within 6 hrs)
        when sspwoi_present and sus_null_count  = 1 then 10 -- sirs, orgdf, no sus
        when sspwoi_present and sus_noinf_count = 1 then 12 -- sirs, orgdf, no sus

        -- States 5-8: documented infection, incomplete vitals for SIRS+OrgDF.
        when sus_count = 1 and sirs_count > 0 and organ_count > 0 then 8
        when sus_count = 1 and sirs_count > 1 then 7
        when sus_count = 1 and sirs_count > 0 then 5
        when sus_count = 1 and organ_count > 0 then 6

        -- States 1-4: no documented infection, incomplete vitals for SIRS+OrgDF.
        when sirs_count > 0 and organ_count > 0 then 4
        when sirs_count > 1 then 3
        when sirs_count > 0 then 1
        when organ_count > 0 then 2

        else 0
        end
      ) as state
  from
  (
  select %I.ts, %I.enc_id,
      bool_or(severe_sepsis_wo_infection_onset is not null) as sspwoi_present,
      bool_or(severe_sepsis_onset is not null)              as ssp_present,
      bool_or(septic_shock_onset is not null)               as ssh_present,

      max(severe_sepsis_wo_infection_onset)                 as severe_sepsis_wo_infection_onset,
      max(severe_sepsis_onset)                              as severe_sepsis_onset,
      max(septic_shock_onset)                               as septic_shock_onset,

      count(*) filter (where name = ''suspicion_of_infection'' and is_met)                                                            as sus_count,
      count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value#>>''{0,text}'' = ''No Infection'') as sus_noinf_count,
      count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value is null)                           as sus_null_count,

      count(*) filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met )                         as sirs_count,
      count(*) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',
                                      ''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'')
                          and is_met )                                                                                                as organ_count,

      count(*) filter (where name in (''initial_lactate_order'',''blood_culture_order'',
                                      ''antibiotics_order'', ''crystalloid_fluid_order'')
                          and is_met )                                                                                                as sev_sep_3hr_count,
      count(*) filter (where name = ''repeat_lactate_order'' and is_met )                                                             as sev_sep_6hr_count,
      count(*) filter (where name = ''vasopressors_order'' and is_met )                                                               as sep_sho_6hr_count

  from %I
  where %I.enc_id = coalesce($1, %I.enc_id)
  group by %I.ts, %I.enc_id
  ) stats', table_name, table_name, table_name, table_name, table_name, table_name, table_name)
  USING _enc_id;
END $func$ LANGUAGE plpgsql;


----------------------------------------------------------
-- Top-level series labeling functions

-- Returns every measurement as a window candidate.
CREATE OR REPLACE FUNCTION get_all_meas_timestamps(
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(pat_id varchar(50), tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    select enc_id, tsp
      from cdm_t meas
    where
      meas.enc_id = coalesce(%s, meas.enc_id) and
      meas.dataset_id = %s and
      meas.tsp between ''%s''::timestamptz and ''%s''::timestamptz'
    , enc_id_str, _dataset_id, ts_start, ts_end
    );
END; $function$;


-- Returns binned timestamps for window boundaries
-- based on timestamps at which measurements are available.
CREATE OR REPLACE FUNCTION get_meas_periodic_timestamps(
        _enc_id                 integer,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(enc_id int, tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    with pat_start as(
      select enc_id, min(tsp) as min_time
      from cdm_t meas
      where dataset_id = %s
      group by enc_id
    ),
    meas_bins as (
      select distinct meas.enc_id, meas.tsp ,
        floor(extract(EPOCH FROM meas.tsp - pat_start.min_time) /
        extract(EPOCH from interval ''1 hour''))+1 as bin
      from
        cdm_t meas
        inner join pat_start
        on pat_start.enc_id = meas.enc_id
      where
        meas.enc_id = coalesce(%s, meas.enc_id) and
        meas.dataset_id = %s and
        meas.tsp between ''%s''::timestamptz and ''%s''::timestamptz
    )
    select enc_id, max(tsp) as tsp
    from meas_bins
    group by enc_id, bin
    order by enc_id, tsp
    limit %s'
    , _dataset_id,  enc_id_str, _dataset_id, ts_start, ts_end, window_limit
    );
END; $function$;


-- Returns every 15 min interval within a +/- 6-hr window of
-- every measurement that meets either a SIRS or OrgDF criteria
-- as a window candidate.
CREATE OR REPLACE FUNCTION get_meas_timestamps_for_bpas(
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(pat_id varchar(50), tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    select distinct meas.pat_id, W.tsp
    from cdm_t meas
    inner join criteria_default cd on meas.fid = cd.fid
    cross join lateral
      generate_series(
        date_trunc(''hour'', meas.tsp - interval ''6 hours''),
        date_trunc(''hour'', meas.tsp + interval ''6 hours'') + interval ''1 hour'',
        interval ''15 minutes'') W(tsp)
    where
      cd.dataset_id = %s and
      meas.dataset_id = %s and
      meas.pat_id = coalesce(%s, meas.pat_id) and
      meas.tsp between ''%s''::timestamptz and ''%s''::timestamptz
      and cd.name in (
          ''sirs_temp'', ''heart_rate'', ''respiratory_rate'', ''wbc'',
          ''respiratory_failure'',
          ''blood_pressure'', ''mean_arterial_pressure'', ''decrease_in_sbp'', ''creatinine'', ''bilirubin'', ''platelet'', ''inr'', ''lactate''
        )
      and
      (case
        when cd.name = ''respiratory_failure'' then meas.value is not null

        when cd.name in (''sirs_temp'', ''heart_rate'', ''respiratory_rate'', ''wbc'')
        then criteria_value_met(meas.value, null, cd.override_value)

        else
        (case
            when cd.category = ''decrease_in_sbp'' or cd.category = ''urine_output'' then true
            else criteria_value_met(meas.value, null, cd.override_value)
            end
          )
       end)
    order by meas.pat_id, W.tsp
    limit %s'
    , _dataset_id, _dataset_id, enc_id_str, ts_start, ts_end, window_limit
    );
END; $function$;


-- Returns timestamps for window boundaries based on changes of patient
-- state observed in a label series.
CREATE OR REPLACE FUNCTION get_label_change_timestamps(
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(pat_id varchar(50), tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    with state_changes as (
      select pat_id, tsp, label,
             last_value(label)  over (order by pat_id, tsp rows between current row and 1 following) as next,
             first_value(label) over (order by pat_id, tsp rows 1 preceding) as previous
      from cdm_labels
      where pat_id = coalesce(%s, pat_id)
      and dataset_id = %s
      and label_id = %s
      and tsp between ''%s''::timestamptz and ''%s''::timestamptz
    )
    select pat_id::varchar(50), tsp
    from state_changes
    where label > previous or label > next
    order by pat_id, tsp
    limit %s'
    , enc_id_str, _dataset_id, _label_id, ts_start, ts_end, window_limit
    );
END; $function$;


-- Returns timestamps for patient states >= 10, picking at most
-- one timestamp per hour.
CREATE OR REPLACE FUNCTION get_hourly_active_timestamps(
        _enc_id                 int,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(enc_id int, tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    select enc_id::int, min(tsp)
    from cdm_labels
    where enc_id = coalesce(%s, enc_id)
    and dataset_id = %s
    and label_id = %s
    and tsp between ''%s''::timestamptz and ''%s''::timestamptz
    and label >= 10
    group by dataset_id, label_id, enc_id, label_type, date_trunc(''hour'', tsp)
    limit %s'
    , enc_id_str, _dataset_id, _label_id, ts_start, ts_end, window_limit
    );
END; $function$;


-- Returns windows based on timestamps where a different label series
-- is active (i.e., non-zero label).
CREATE OR REPLACE FUNCTION get_pop1_label_timestamps(
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(pat_id varchar(50), tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    with interesting_pats as (
      select distinct pat_id from cdm_labels where dataset_id = %s and label_id = 152
    )
    select C.pat_id::varchar(50), C.tsp
    from interesting_pats P inner join cdm_labels C
      on P.pat_id = C.pat_id and C.dataset_id = %s
    where C.pat_id = coalesce(%s, C.pat_id)
    and C.dataset_id = %s
    and C.label_id = %s
    and C.tsp between ''%s''::timestamptz and ''%s''::timestamptz
    limit %s'
    , _dataset_id, _dataset_id, enc_id_str, _dataset_id, _label_id, ts_start, ts_end, window_limit
    );
END; $function$;


CREATE OR REPLACE FUNCTION get_pop2_label_timestamps(
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(pat_id varchar(50), tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    select C.pat_id::varchar(50), C.tsp
    from cdm_labels C
    where C.pat_id = coalesce(%s, C.pat_id)
    and C.dataset_id = %s
    and C.label_id = %s
    and C.tsp between ''%s''::timestamptz and ''%s''::timestamptz
    and C.pat_id not in ( select distinct S.pat_id from cdm_labels S where dataset_id = %s and S.label_id = 152 )
    limit %s'
    , enc_id_str, _dataset_id, _label_id, ts_start, ts_end, _dataset_id, window_limit
    );
END; $function$;


CREATE OR REPLACE FUNCTION get_pop3_label_timestamps(
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer default 0,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all'
  )
  returns table(pat_id varchar(50), tsp timestamptz)
  LANGUAGE plpgsql
AS $function$
DECLARE
    enc_id_str text;
BEGIN
  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  return query execute format('
    with interesting_pats as (
      select distinct c1.pat_id
      from (select pat_id, min(tsp) min_tsp from cdm_labels where label_id=151 and label=10 group by pat_id) c1
      inner join
      (select pat_id, min(tsp) min_tsp from cdm_labels where label_id=152 and label=20 group by pat_id) c2
      on c1.pat_id=c2.pat_id and c1.min_tsp > c2.min_tsp
    )
    select distinct meas.pat_id, W.tsp
    from interesting_pats P
    inner join cdm_t meas on P.pat_id = meas.pat_id
    inner join criteria_default cd on meas.fid = cd.fid
    cross join lateral
      generate_series(
        date_trunc(''hour'', meas.tsp - interval ''6 hours''),
        date_trunc(''hour'', meas.tsp + interval ''6 hours'') + interval ''1 hour'',
        interval ''15 minutes'') W(tsp)
    where
      cd.dataset_id = %s and
      meas.dataset_id = %s and
      meas.pat_id = coalesce(%s, meas.pat_id) and
      meas.tsp between ''%s''::timestamptz and ''%s''::timestamptz
      and cd.name in (
          ''sirs_temp'', ''heart_rate'', ''respiratory_rate'', ''wbc'',
          ''respiratory_failure'',
          ''blood_pressure'', ''mean_arterial_pressure'', ''decrease_in_sbp'', ''creatinine'', ''bilirubin'', ''platelet'', ''inr'', ''lactate''
        )
      and
      (case
        when cd.name = ''respiratory_failure'' then meas.value is not null

        when cd.name in (''sirs_temp'', ''heart_rate'', ''respiratory_rate'', ''wbc'')
        then criteria_value_met(meas.value, null, cd.override_value)

        else
        (case
            when cd.category = ''decrease_in_sbp'' or cd.category = ''urine_output'' then true
            else criteria_value_met(meas.value, null, cd.override_value)
            end
          )
       end)
    order by meas.pat_id, W.tsp
    limit %s'
    , _dataset_id, _dataset_id, enc_id_str, ts_start, ts_end, window_limit
    );
END; $function$;



--
-- Returns sspwoi, ssp, ssh onset times for a label series.
-- TODO/BUG: this should return onset times for each encounter, rather than for
-- only the first onset times in a label series per patient.
create or replace function get_label_series_onset_timestamps(_dataset_id integer, _label_id integer)
  returns table ( dataset_id                         integer,
                  label_id                           integer,
                  pat_id                             text,
                  severe_sepsis_wo_infection_onset   timestamptz,
                  severe_sepsis_onset                timestamptz,
                  septic_shock_onset                 timestamptz,
                  w_severe_sepsis_wo_infection_onset timestamptz,
                  w_severe_sepsis_onset              timestamptz,
                  w_septic_shock_onset               timestamptz )
language plpgsql
as $func$
declare
  window_size interval := interval '6 hours';
begin
  return query
    -- Earliest occurrence of each state
    with earliest_occurrences as (
      select L.dataset_id, L.label_id, L.pat_id, L.label, min(L.tsp) as tsp
      from cdm_labels L
      where L.dataset_id = coalesce(_dataset_id, L.dataset_id)
      and   L.label_id   = coalesce(_label_id, L.label_id)
      and   L.label_type = 'cms state'
      group by L.dataset_id, L.label_id, L.pat_id, L.label
    )
    select I.dataset_id, I.label_id, I.pat_id,

           -- Onset times
           least(min(L10.sspwoi), min(L20.sspwoi), min(L30.sspwoi)) as severe_sepsis_wo_infection_onset,
           least(min(L20.ssp), min(L30.ssp)) as severe_sepsis_onset,
           min(L30.ssh) as septic_shock_onset,

           -- Window ends used to calculate onset times
           least(min(L10.w_sspwoi), min(L20.w_ssp), min(L30.w_ssh)) as w_severe_sepsis_wo_infection_onset,
           least(min(L20.w_ssp), min(L30.w_ssh)) as w_severe_sepsis_onset,
           min(L30.w_ssh) as w_septic_shock_onset
    from
    ( select distinct I.dataset_id, I.label_id, I.pat_id from earliest_occurrences I ) I

    left join
    (
      -- Earliest occurrence of sspwoi.
      select WL10.dataset_id, WL10.label_id, WL10.pat_id,
             min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
             min(WL10.tsp) as w_sspwoi
      from (
        select L10.dataset_id, L10.label_id, L10.pat_id, min(L10.tsp) as tsp
        from earliest_occurrences L10
        where L10.label >= 10 and L10.label < 20
        group by L10.pat_id, L10.dataset_id, L10.label_id
      ) WL10
      -- Retrieve exact timestamp of onset within the window.
      inner join lateral get_cms_labels_for_window(
        WL10.pat_id, WL10.tsp - window_size, WL10.tsp, WL10.dataset_id
      ) LWindow
        on WL10.pat_id = LWindow.pat_id
      group by WL10.pat_id, WL10.dataset_id, WL10.label_id
    ) L10
      on I.dataset_id = L10.dataset_id
      and I.label_id = L10.label_id
      and I.pat_id = L10.pat_id

    left join (
      -- Earliest occurrence of ssp.
      select WL20.dataset_id, WL20.label_id, WL20.pat_id,
             min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
             min(LWindow.severe_sepsis_onset) as ssp,
             min(WL20.tsp) as w_ssp
      from (
        select L20.dataset_id, L20.label_id, L20.pat_id, min(L20.tsp) as tsp
        from earliest_occurrences L20
        where L20.label >= 20 and L20.label < 30
        group by L20.pat_id, L20.dataset_id, L20.label_id
      ) WL20
      -- Retrieve exact timestamp of onset within the window.
      inner join lateral get_cms_labels_for_window(
        WL20.pat_id, WL20.tsp - window_size, WL20.tsp, WL20.dataset_id
      ) LWindow
        on WL20.pat_id = LWindow.pat_id
      group by WL20.pat_id, WL20.dataset_id, WL20.label_id
    ) L20
      on I.dataset_id = L20.dataset_id
      and I.label_id = L20.label_id
      and I.pat_id = L20.pat_id

    left join (
      -- Earliest occurrence of ssh.
      select WL30.dataset_id, WL30.label_id, WL30.pat_id,
             min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
             min(LWindow.severe_sepsis_onset) as ssp,
             min(LWindow.septic_shock_onset) as ssh,
             min(WL30.tsp) as w_ssh
      from (
        select L30.dataset_id, L30.label_id, L30.pat_id, min(L30.tsp) as tsp
        from earliest_occurrences L30
        where L30.label >= 30
        group by L30.pat_id, L30.dataset_id, L30.label_id
      ) WL30
      -- Retrieve exact timestamp of onset within the window.
      inner join lateral get_cms_labels_for_window(
        WL30.pat_id, WL30.tsp - window_size, WL30.tsp, WL30.dataset_id
      ) LWindow
        on WL30.pat_id = LWindow.pat_id
      group by WL30.pat_id, WL30.dataset_id, WL30.label_id
    ) L30
      on I.dataset_id = L30.dataset_id
      and I.label_id = L30.label_id
      and I.pat_id = L30.pat_id

    group by I.dataset_id, I.label_id, I.pat_id;

end; $func$;


--
-- Computes a label series using windows defined by the 'window_generator' UDF.
-- This version parallelizes label calculation over windows, by partition windows by pat_id.
--
-- Note this function is not transactional due to its use of dblink.
--
CREATE OR REPLACE FUNCTION get_cms_label_series_for_windows_in_parallel(
        label_description            text,
        window_generator             text,
        label_function               integer,
        _enc_id                      integer,
        _dataset_id                  integer,
        _label_id                    integer,
        parallel_dblink              text,
        num_partitions               integer,
        ts_start                     timestamptz default '-infinity'::timestamptz,
        ts_end                       timestamptz default 'infinity'::timestamptz,
        window_limit                 text default 'all',
        use_app_infections           boolean default false,
        use_clarity_notes            boolean default false,
        with_bundle_compliance       boolean default true,
        output_label_series          integer default null,
        save_criteria_windows        boolean default false
  )
  returns integer
  LANGUAGE plpgsql
AS $function$
DECLARE
    window_size                interval := get_parameter('lookbackhours')::interval;
    window_queries             text[];
    window_union_query         text;
    window_label_query         text;
    window_vacuum_query        text;
    bundle_window_queries      text[];
    bundle_union_query         text;
    bundle_label_query         text;
    enc_id_str                 text;
    label_id_str               text;
    window_fn                  text;
    generate_label_query       text;
    generated_label_id         integer;
    use_app_infections_str     text;
    use_clarity_notes_str      text;
    save_criteria_windows_str  text;
    num_windows                integer;
BEGIN

    select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
    raise notice 'Running get_cms_label_series_for_windows_in_parallel on dataset_id %', _dataset_id;

    enc_id_str = case when _enc_id is null then 'NULL'
                      else format('%s', _enc_id) end;

    label_id_str = case when _label_id is null then 'NULL'
                        else format('%s', _label_id) end;

    window_fn = case label_function
                  when 0 then 'get_cms_candidates_for_window'
                  when 1 then 'get_cms_labels_for_window'
                  else 'get_cms_labels_for_window_with_complex_orders'
                  end;

    use_app_infections_str = case when use_app_infections then 'True' else 'False' end;
    use_clarity_notes_str = case when use_clarity_notes then 'True' else 'False' end;
    save_criteria_windows_str = case when save_criteria_windows then 'True' else 'False' end;

    -- Populate criteria for each window into a temp table.
    select array(select format(
       'drop table if exists new_criteria_windows_%s;
        create unlogged table new_criteria_windows_%s as
        with window_ends as (
          select *
          from %s(%s::int, %s::integer, %s::integer, ''%s''::timestamptz, ''%s''::timestamptz, ''%s''::text)
        )
        select partition.tsp as ts, new_criteria.*
        from
          ( select * from window_ends
            where
            enc_id %% %s = (%s - 1) -- update partition based on enc_id
          ) partition
          inner join lateral
          %s(coalesce(%s, partition.enc_id),
             partition.tsp - ''%s''::interval,
             partition.tsp,
             %s, %s, %s, %s) new_criteria
        on partition.enc_id = new_criteria.enc_id;'
        , partition_id, partition_id
        , window_generator, enc_id_str, _dataset_id, label_id_str, ts_start, ts_end, window_limit
        , num_partitions, partition_id
        , window_fn, enc_id_str, window_size, _dataset_id, use_app_infections_str, use_clarity_notes_str, save_criteria_windows_str)
      into window_queries
      from generate_series(1, num_partitions) R(partition_id));

    perform distribute(parallel_dblink, window_queries, num_partitions);

    window_union_query :=
      'drop table if exists new_criteria_windows; create unlogged table new_criteria_windows as '
      || (
        select string_agg(format('select * from new_criteria_windows_%s', partition_id), ' union all ')
        from generate_series(1, num_partitions) R(partition_id)
      );

    if output_label_series is not null then
      -- Reuse existing label series.
      generated_label_id := output_label_series;
    else
      -- Register a new label id.
      generate_label_query := format(
        'insert into label_version (created, description) values (now(), ''%s'')',
        label_description
      );

      perform distribute(parallel_dblink, ARRAY[generate_label_query]::text[], 1);

      generated_label_id := max(label_id) from label_version;
    end if;

    -- Populate CMS label series.
    window_label_query := format(
     'insert into cdm_labels (dataset_id, label_id, enc_id, tsp, label_type, label)
        select %s, %s, sw.enc_id, sw.ts, ''cms state'', sw.state
        from get_window_labels_from_criteria(''new_criteria_windows'', %s) sw
      on conflict (dataset_id, label_id, enc_id, tsp) do update
        set label_type = excluded.label_type,
            label = excluded.label;'
      , _dataset_id, generated_label_id, enc_id_str);

    window_vacuum_query := 'vacuum analyze verbose cdm_labels;';

    perform distribute(parallel_dblink, ARRAY[window_union_query, window_label_query, window_vacuum_query]::text[], 1);

    if with_bundle_compliance then
      -- Add and process additional windows for exact bundle compliance.
      raise notice 'Running bundle compliance windows for dataset_id %, label_id %', _dataset_id, generated_label_id;

      select array(select format(
       'drop table if exists bundle_compliance_windows_%s;
        create unlogged table bundle_compliance_windows_%s as
          with onset_times as (
            select * from get_label_series_onset_timestamps(%s, %s) T
            where
            enc_id %% %s = (%s - 1) -- update partition based on enc_id
          ),
          severe_sepsis as (
            select T.w_severe_sepsis_onset as ts, SSP.*
            from onset_times T
            inner join lateral %s(coalesce(%s, T.enc_id), T.w_severe_sepsis_onset - interval ''6 hours'' , T.w_severe_sepsis_onset, %s, %s, %s, %s) SSP
            on SSP.enc_id = T.enc_id
            where not(T.severe_sepsis_onset is null or T.w_severe_sepsis_onset is null)
            and (T.septic_shock_onset is null or T.severe_sepsis_onset <> T.septic_shock_onset)
          ),
          septic_shock as (
            select T.w_septic_shock_onset as ts, SSH.*
            from onset_times T
            inner join lateral %s(coalesce(%s, T.enc_id), T.w_septic_shock_onset - interval ''6 hours'', T.w_septic_shock_onset, %s, %s, %s, %s) SSH
            on SSH.enc_id = T.enc_id
            where not(T.septic_shock_onset is null or T.w_septic_shock_onset is null)
          ),
          severe_sepsis_6hr_bundle as (
            select T.severe_sepsis_onset + interval ''6 hours'' as ts, SSP.*
            from onset_times T
            inner join lateral %s(coalesce(%s, T.enc_id), T.severe_sepsis_onset, T.severe_sepsis_onset + interval ''6 hours'', %s, %s, %s, %s) SSP
            on SSP.enc_id = T.enc_id
            where T.severe_sepsis_onset is not null
            and (T.septic_shock_onset is null or T.severe_sepsis_onset <> T.septic_shock_onset)
          ),
          septic_shock_6hr_bundle as (
            select T.septic_shock_onset + interval ''6 hours'' as ts, SSH.*
            from onset_times T
            inner join lateral %s(coalesce(%s, T.enc_id), T.septic_shock_onset, T.septic_shock_onset + interval ''6 hours'', %s, %s, %s, %s) SSH
            on SSH.enc_id = T.enc_id
            where T.septic_shock_onset is not null
          )
          select PB.ts,
                 SSP.enc_id,
                 SSP.name,
                 (case when SSP.name like ''%%_order'' then PB.measurement_time else SSP.measurement_time end) as measurement_time,
                 (case when SSP.name like ''%%_order'' then PB.value            else SSP.value            end) as value,
                 (case when SSP.name like ''%%_order'' then PB.override_time    else SSP.override_time    end) as override_time,
                 (case when SSP.name like ''%%_order'' then PB.override_user    else SSP.override_user    end) as override_user,
                 (case when SSP.name like ''%%_order'' then PB.override_value   else SSP.override_value   end) as override_value,
                 (case when SSP.name like ''%%_order'' then PB.is_met           else SSP.is_met           end) as is_met,
                 (case when SSP.name like ''%%_order'' then PB.update_date      else SSP.update_date      end) as update_date,
                 SSP.severe_sepsis_onset,
                 SSP.severe_sepsis_wo_infection_onset,
                 SSP.septic_shock_onset
          from severe_sepsis SSP
          inner join severe_sepsis_6hr_bundle PB on SSP.enc_id = PB.enc_id and SSP.name = PB.name
          union all
          select HB.ts,
                 SSH.enc_id,
                 SSH.name,
                 (case when SSH.name like ''%%_order'' then HB.measurement_time else SSH.measurement_time end) as measurement_time,
                 (case when SSH.name like ''%%_order'' then HB.value            else SSH.value            end) as value,
                 (case when SSH.name like ''%%_order'' then HB.override_time    else SSH.override_time    end) as override_time,
                 (case when SSH.name like ''%%_order'' then HB.override_user    else SSH.override_user    end) as override_user,
                 (case when SSH.name like ''%%_order'' then HB.override_value   else SSH.override_value   end) as override_value,
                 (case when SSH.name like ''%%_order'' then HB.is_met           else SSH.is_met           end) as is_met,
                 (case when SSH.name like ''%%_order'' then HB.update_date      else SSH.update_date      end) as update_date,
                 SSH.severe_sepsis_onset,
                 SSH.severe_sepsis_wo_infection_onset,
                 SSH.septic_shock_onset
          from septic_shock SSH
          inner join septic_shock_6hr_bundle HB on SSH.enc_id = HB.enc_id and SSH.name = HB.name;'
        , partition_id, partition_id
        , _dataset_id, generated_label_id, num_partitions, partition_id
        , window_fn, enc_id_str, _dataset_id, use_app_infections_str, use_clarity_notes_str, save_criteria_windows_str
        , window_fn, enc_id_str, _dataset_id, use_app_infections_str, use_clarity_notes_str, save_criteria_windows_str
        , window_fn, enc_id_str, _dataset_id, use_app_infections_str, use_clarity_notes_str, save_criteria_windows_str
        , window_fn, enc_id_str, _dataset_id, use_app_infections_str, use_clarity_notes_str, save_criteria_windows_str)
      into bundle_window_queries
      from generate_series(1, num_partitions) R(partition_id));

      perform distribute(parallel_dblink, bundle_window_queries, num_partitions);

      bundle_union_query :=
        'drop table if exists bundle_compliance_windows; create unlogged table bundle_compliance_windows as '
        || (
          select string_agg(format('select * from bundle_compliance_windows_%s', partition_id), ' union all ')
          from generate_series(1, num_partitions) R(partition_id)
        );

      -- Register a new label id and populate label series.
      bundle_label_query := format(
        'insert into cdm_labels (dataset_id, label_id, enc_id, tsp, label_type, label)
          select %s, %s, sw.enc_id, sw.ts, ''bundle compliance state'', sw.state
          from get_window_labels_from_criteria(''bundle_compliance_windows'', %s) sw
        on conflict (dataset_id, label_id, enc_id, tsp) do update
          set label_type = excluded.label_type,
              label = excluded.label;

        drop table bundle_compliance_windows;'
        , _dataset_id, generated_label_id, enc_id_str);

      perform distribute(parallel_dblink, ARRAY[bundle_union_query, bundle_label_query]::text[], 1);

      raise notice 'Cleaning bundle temporaries for dataset_id %, label_id %', _dataset_id, generated_label_id;

      execute (
        select string_agg(format('drop table bundle_compliance_windows_%s', partition_id), ';')
        from generate_series(1, num_partitions) R(partition_id)
      );
    end if;

    raise notice 'Cleaning criteria temporaries for dataset_id %, label_id %', _dataset_id, generated_label_id;

    if not save_criteria_windows then
      execute ( 'drop table new_criteria_windows;' );
    else
      execute ( 'delete from new_criteria_windows where measurement_time is null and value is null;' );
    end if;

    execute (
      ( select string_agg(format('drop table new_criteria_windows_%s', partition_id), ';')
        from generate_series(1, num_partitions) R(partition_id) )
    );

    return generated_label_id;
END; $function$;



CREATE OR REPLACE FUNCTION get_cms_label_series_for_windows_block_parallel(
        label_description       text,
        window_generator        text,
        label_function          integer,
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer,
        parallel_dblink         text,
        num_partitions          integer,
        ts_start                timestamptz,
        ts_end                  timestamptz,
        ts_step                 interval,
        window_limit            text default 'all',
        use_app_infections      boolean default false,
        use_clarity_notes       boolean default false,
        with_bundle_compliance  boolean default true
  )
  returns integer
  LANGUAGE plpgsql
AS $function$
DECLARE
  ts_block                   record;
  ts_end_epoch               bigint;
  ts_end_upper               timestamptz;
  block_query                text;
  first_iter                 boolean := true;
  current_label_id           int := 0;
  generated_label_id         int := 0;
  enc_id_str                 text;
  label_id_str               text;
  use_app_infections_str     text;
  use_clarity_notes_str      text;
  with_bundle_compliance_str text;
  output_label_series_str    text;
BEGIN

  ts_end_epoch = ceil(extract(epoch from ts_end) / extract(epoch from ts_step)) * extract(epoch from ts_step);
  ts_end_upper = 'epoch'::timestamptz + (ts_end_epoch::text || ' sec')::interval;

  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  label_id_str = case when _label_id is null then 'NULL'
                      else format('%s', _label_id) end;

  use_app_infections_str     = case when use_app_infections then 'True' else 'False' end;
  use_clarity_notes_str      = case when use_clarity_notes then 'True' else 'False' end;
  with_bundle_compliance_str = case when with_bundle_compliance then 'True' else 'False' end;

  -- First block does not reuse.
  -- Subsequent block reuses.
  for ts_block in select * from generate_series(ts_start + ts_step, ts_end_upper, ts_step) R(ts_block_end)
  loop
    raise notice 'Processing block %s %s', ts_block.ts_block_end - ts_step, ts_block.ts_block_end;

    output_label_series_str = case when first_iter then 'null' else format('%s', generated_label_id) end;

    block_query := format(
      'select get_cms_label_series_for_windows_in_parallel(
      ''%s'', ''%s'', %s, %s, %s, %s, ''%s'', %s,
      ''%s''::timestamptz, ''%s''::timestamptz,
      ''%s'', %s, %s, %s, %s
      )'
      , label_description, window_generator, label_function, enc_id_str, _dataset_id, label_id_str, parallel_dblink, num_partitions
      , ts_block.ts_block_end - ts_step, ts_block.ts_block_end
      , window_limit, use_app_infections_str, use_clarity_notes_str, with_bundle_compliance_str, output_label_series_str
    );

    perform distribute(parallel_dblink, ARRAY[block_query]::text[], 1);
    current_label_id := max(label_id) from label_version;

    if first_iter then
      generated_label_id := current_label_id;
    else
      if current_label_id <> generated_label_id then
        -- Clean up.
        delete from cdm_labels where label_id = generated_label_id;
        delete from cdm_labels where label_id = current_label_id;
        generated_label_id := null;

        -- Log an error, and break the loop.
        raise exception 'Failed to reuse label series during get_cms_label_series_for_windows_block_parallel';
        exit;
      end if;
    end if;

    first_iter := false;
  end loop;
  return generated_label_id;
END; $function$;


--
-- Populates the cdm_processed_notes table for a set of windows.
CREATE OR REPLACE FUNCTION get_cms_note_matches_for_windows(
        label_description       text,
        window_generator        text,
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer,
        parallel_dblink         text,
        num_partitions          integer,
        ts_start                timestamptz default '-infinity'::timestamptz,
        ts_end                  timestamptz default 'infinity'::timestamptz,
        window_limit            text default 'all',
        clean_matches           boolean default true
  )
  returns void
  LANGUAGE plpgsql
AS $function$
DECLARE
    notes_queries           text[];
    window_queries          text[];
    clean_notes_query       text;
    merge_query             text;
    clean_table_query       text;
    window_size             interval := get_parameter('lookbackhours')::interval;
    enc_id_str              text;
    label_id_str            text;
BEGIN
    select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
    raise notice 'Running get_cms_note_matches_for_windows on dataset_id %, label_id %', _dataset_id, _label_id;

    enc_id_str = case when _enc_id is null then 'NULL'
                      else format('%s', _enc_id) end;

    label_id_str = case when _label_id is null then 'NULL'
                        else format('%s', _label_id) end;

    -- Generate candidate note ids.
    select array(select format(
       'drop table if exists note_candidates_%s;
        create unlogged table note_candidates_%s as
        with window_ends as (
          select *
          from %s(%s::int, %s::integer, %s::integer, ''%s''::timestamptz, ''%s''::timestamptz, ''%s''::text)
        ),
        partition as (
          select * from window_ends
          where
            (case
              when pat_id like ''JH%%''
              then (substring(pat_id from 3)::int) %% %s = (%s - 1)
              else (substring(pat_id from 2)::int) %% %s = (%s - 1)
              end)
        )
        select distinct N.note_id
        from partition PT inner join cdm_notes N on PT.pat_id = N.pat_id
        where N.dataset_id = %s
        and note_date(N.dates) between (PT.tsp - ''%s''::interval) - interval ''1 days'' and PT.tsp + interval ''1 days'';'
        , partition_id, partition_id
        , window_generator, enc_id_str, _dataset_id, label_id_str, ts_start, ts_end, window_limit
        , num_partitions, partition_id
        , num_partitions, partition_id
        , _dataset_id, window_size)
      into notes_queries
      from generate_series(1, num_partitions) R(partition_id));

    perform distribute(parallel_dblink, notes_queries, num_partitions);

    -- Match within candidate note ids.
    select array(select format(
       'drop table if exists window_notes_%s;
        create unlogged table window_notes_%s as
        select * from match_cdm_infections_from_candidates(coalesce(%s, null), %s, ''note_candidates_%s'', 3, 3) M;'
        , partition_id, partition_id
        , enc_id_str, _dataset_id, partition_id)
      into window_queries
      from generate_series(1, num_partitions) R(partition_id));

    perform distribute(parallel_dblink, window_queries, num_partitions);

    if clean_matches then
      clean_notes_query := format('delete from cdm_processed_notes where dataset_id = %s', _dataset_id);
      perform distribute(parallel_dblink, ARRAY[clean_notes_query], 1);
    end if;

    merge_query := 'insert into cdm_processed_notes as pn'
      || '  select dataset_id, pat_id, note_id, note_type, note_status, array_agg(start_ts) as tsps, array_agg(ngram) as ngrams'
      || '  from ('
      || (
        select string_agg(format('select * from window_notes_%s', partition_id), ' union all ')
        from generate_series(1, num_partitions) R(partition_id)
      )
      || '  ) all_notes'
      || '  group by dataset_id, pat_id, note_id, note_type, note_status'
      || '  on conflict(dataset_id, pat_id, note_id, note_type, note_status)'
      || '  do update set tsps=array_cat(pn.tsps, excluded.tsps), ngrams=array_cat(pn.ngrams, excluded.ngrams)';

    clean_table_query := (
      select string_agg(format('drop table note_candidates_%s; drop table window_notes_%s', partition_id, partition_id), ';')
      from generate_series(1, num_partitions) R(partition_id)
    );

    perform distribute(parallel_dblink, ARRAY[merge_query, clean_table_query], 1);

    return;
END; $function$;



CREATE OR REPLACE FUNCTION get_cms_note_matches_for_windows_blocked(
        label_description       text,
        window_generator        text,
        _enc_id                 text,
        _dataset_id             integer,
        _label_id               integer,
        parallel_dblink         text,
        num_partitions          integer,
        ts_start                timestamptz,
        ts_end                  timestamptz,
        ts_step                 interval,
        window_limit            text
  )
  returns void
  LANGUAGE plpgsql
AS $function$
DECLARE
  ts_block                   record;
  ts_end_epoch               bigint;
  ts_end_upper               timestamptz;
  block_query                text;
  enc_id_str                 text;
  label_id_str               text;
  first_iter                 boolean := true;
  clean_notes_str            text;
BEGIN

  ts_end_epoch = ceil(extract(epoch from ts_end) / extract(epoch from ts_step)) * extract(epoch from ts_step);
  ts_end_upper = 'epoch'::timestamptz + (ts_end_epoch::text || ' sec')::interval;

  enc_id_str = case when _enc_id is null then 'NULL'
                    else format('%s', _enc_id) end;

  label_id_str = case when _label_id is null then 'NULL'
                      else format('%s', _label_id) end;

  -- First block does not reuse.
  -- Subsequent block reuses.
  for ts_block in select * from generate_series(ts_start + ts_step, ts_end_upper, ts_step) R(ts_block_end)
  loop
    raise notice 'Processing block %s %s', ts_block.ts_block_end - ts_step, ts_block.ts_block_end;

    clean_notes_str = case when first_iter then 'True' else 'False' end;

    block_query := format(
      'select get_cms_note_matches_for_windows(
        ''%s'', ''%s'',
        %s, %s, %s, ''%s'', %s,
        ''%s''::timestamptz, ''%s''::timestamptz, ''%s'', %s
      )
      '
      , label_description, window_generator
      , enc_id_str, _dataset_id, label_id_str, parallel_dblink, num_partitions
      , ts_block.ts_block_end - ts_step, ts_block.ts_block_end, window_limit, clean_notes_str
    );

    perform distribute(parallel_dblink, ARRAY[block_query]::text[], 1);
    first_iter := false;
  end loop;
  return;
END; $function$;



--
-- Computes a label series using coarse-to-fine strategy for optimizing notes processing.
CREATE OR REPLACE FUNCTION get_cms_label_series(
        label_description       text,
        label_function          integer,
        _enc_id                 text,
        _dataset_id             integer,
        parallel_dblink         text,
        num_partitions          integer,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        ts_label_block_size     interval default interval '1 month',
        ts_notes_block_size     interval default interval '1 month',
        window_limit            text default 'all',
        use_app_infections      boolean default false,
        use_clarity_notes       boolean default false,
        with_bundle_compliance  boolean default true,
        refresh_notes           boolean default false,
        _candidate_label_id     integer default null
  )
  returns integer
  LANGUAGE plpgsql
AS $function$
DECLARE
  candidate_function integer := 0;
  candidate_label_id integer;
  result_label_id    integer;
BEGIN
  if label_function = 2 then
    -- Single pass for simulated suspicion of infection over all windows
    -- (since this skips notes processing, and should be efficient as-is)

    select get_cms_label_series_for_windows_block_parallel(
      label_description, 'get_meas_periodic_timestamps', label_function,
      _enc_id, _dataset_id, null, parallel_dblink, num_partitions, ts_start, ts_end, ts_label_block_size, window_limit, use_app_infections, use_clarity_notes, false)
    into result_label_id;
  else
    -- Coarse-grained pass, calculating sirs and org df for state changes.
    if _candidate_label_id is null then
      select get_cms_label_series_for_windows_block_parallel(
        label_description || ' (candidate series)', 'get_meas_periodic_timestamps', candidate_function,
        _enc_id, _dataset_id, null, parallel_dblink, num_partitions, ts_start, ts_end, ts_label_block_size, window_limit, use_app_infections, use_clarity_notes, false)
      into candidate_label_id;

      raise notice 'Finished first pass for get_cms_label_series on dataset_id %, label_id %', _dataset_id, candidate_label_id;
    else
      candidate_label_id := _candidate_label_id;
    end if;

    if label_function <> 0 then
      if label_function > 0 and refresh_notes then
        -- Preprocess notes.
        perform get_cms_note_matches_for_windows_blocked(
          label_description, 'get_hourly_active_timestamps',
          _enc_id, _dataset_id, candidate_label_id, parallel_dblink, num_partitions, ts_start, ts_end, ts_notes_block_size, window_limit);

        raise notice 'Finished notes for get_cms_label_series on dataset_id %, label_id %', _dataset_id, candidate_label_id;
      end if;

      -- Fine-grained pass, using windows based on state changes.
      select get_cms_label_series_for_windows_block_parallel(
        label_description, 'get_hourly_active_timestamps', label_function,
        _enc_id, _dataset_id, candidate_label_id, parallel_dblink, num_partitions,
        ts_start, ts_end, ts_label_block_size, window_limit, use_app_infections, use_clarity_notes, with_bundle_compliance)
      into result_label_id;
    else
      result_label_id := candidate_label_id;
    end if;
  end if;

  return result_label_id;
END; $function$;
