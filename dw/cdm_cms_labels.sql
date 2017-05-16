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


-- Calculates criteria for only SIRS and Organ Dysfunction.
-- This is useful for identifying candidates for whom we can
-- evaluate suspicion of infection.
CREATE OR REPLACE FUNCTION get_cms_candidates_for_window(
                this_pat_id                      text,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                _ignored                         boolean default false,
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
    select aggs.pat_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.pat_id,
               meas.tsp as measurement_time,
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
    ) as aggs
    group by aggs.pat_id
  ),
  sirs_and_org_df_criteria as (
    select
        sodf.pat_id,
        sodf.name,
        first((case when sodf.is_met then sodf.measurement_time else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as measurement_time,
        first((case when sodf.is_met then sodf.value            else null end) order by coalesce(sodf.measurement_time, sodf.c_otime))::text as value,
        first((case when sodf.is_met then sodf.c_otime          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_time,
        first((case when sodf.is_met then sodf.c_ouser          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_user,
        first((case when sodf.is_met then sodf.c_ovalue         else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_value,
        coalesce(bool_or(sodf.is_met), false) as is_met,
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
                when PC.name = 'respiratory_failure'
                then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

                when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

                else
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
               end) as is_met
      from pat_cvalues PC
      where PC.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
    ) as sodf
    group by sodf.pat_id, sodf.name
  ),
  null_infections as (
    -- This is necessary for get_window_states
    select P.pat_id,
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
  severe_sepsis_wo_infection as (
    select CO.pat_id,
           false                as suspicion_of_infection,
           sum(CO.sirs_cnt)     as sirs_cnt,
           sum(CO.org_df_cnt)   as org_df_cnt,
           null::timestamptz    as inf_onset,
           max(CO.sirs_initial) as sirs_initial,
           max(CO.sirs_onset)   as sirs_onset,
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

      from severe_sepsis_wo_infection stats
      group by stats.pat_id
    ) sspm
  )
  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         null::timestamptz as septic_shock_onset
  from (
      select * from sirs_and_org_df_criteria
      union all select * from null_infections
  ) new_criteria
  left join severe_sepsis_onsets SSP on new_criteria.pat_id = SSP.pat_id;

  return;
END; $function$;


-- get_cms_labels_for_window
-- Duplicate of current DW calculate_criteria
CREATE OR REPLACE FUNCTION get_cms_labels_for_window(
                this_pat_id                      text,
                ts_start                         timestamptz,
                ts_end                           timestamptz,
                _dataset_id                      INTEGER DEFAULT NULL,
                _ignored                         boolean default false,
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
    select weights.pat_id, first(weights.value order by weights.tsp) as value
    from (
        select P.pat_id, weights.tsp, weights.value::numeric as value
        from pat_ids P
        inner join criteria_meas weights on P.pat_id = weights.pat_id
        where weights.fid = 'weight'  and weights.dataset_id = _dataset_id
    ) as weights
    group by weights.pat_id
  ),
  infection as (
      select
          infct.pat_id,
          infct.name,
          first(infct.measurement_time order by coalesce(infct.measurement_time, infct.c_otime)) as measurement_time,
          first(infct.value            order by coalesce(infct.measurement_time, infct.c_otime))::text as value,
          first(infct.c_otime          order by coalesce(infct.measurement_time, infct.c_otime)) as override_time,
          first(infct.c_ouser          order by coalesce(infct.measurement_time, infct.c_otime)) as override_user,
          first(infct.c_ovalue         order by coalesce(infct.measurement_time, infct.c_otime)) as override_value,
          coalesce(bool_or(infct.is_met), false) as is_met,
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
      ) as infct
      group by infct.pat_id, infct.name
  ),
  sirs as (
      select
          sirs.pat_id,
          sirs.name,
          first((case when sirs.is_met then sirs.measurement_time else null end) order by coalesce(sirs.measurement_time, sirs.c_otime)) as measurement_time,
          first((case when sirs.is_met then sirs.value            else null end) order by coalesce(sirs.measurement_time, sirs.c_otime))::text as value,
          first((case when sirs.is_met then sirs.c_otime          else null end) order by coalesce(sirs.measurement_time, sirs.c_otime)) as override_time,
          first((case when sirs.is_met then sirs.c_ouser          else null end) order by coalesce(sirs.measurement_time, sirs.c_otime)) as override_user,
          first((case when sirs.is_met then sirs.c_ovalue         else null end) order by coalesce(sirs.measurement_time, sirs.c_otime)) as override_value,
          coalesce(bool_or(sirs.is_met), false) as is_met,
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
      ) as sirs
      group by sirs.pat_id, sirs.name
  ),
  respiratory_failures as (
    select
        rf.pat_id,
        rf.name,
        first((case when rf.is_met then rf.measurement_time else null end) order by coalesce(rf.measurement_time, rf.c_otime)) as measurement_time,
        first((case when rf.is_met then rf.value            else null end) order by coalesce(rf.measurement_time, rf.c_otime))::text as value,
        first((case when rf.is_met then rf.c_otime          else null end) order by coalesce(rf.measurement_time, rf.c_otime)) as override_time,
        first((case when rf.is_met then rf.c_ouser          else null end) order by coalesce(rf.measurement_time, rf.c_otime)) as override_user,
        first((case when rf.is_met then rf.c_ovalue         else null end) order by coalesce(rf.measurement_time, rf.c_otime)) as override_value,
        coalesce(bool_or(rf.is_met), false) as is_met,
        now() as update_date
    from (
        select
            PC.pat_id,
            PC.name,
            PC.tsp as measurement_time,
            (coalesce(PC.c_ovalue#>>'{0,text}', (PC.fid ||': '|| PC.value))) as value,
            PC.c_otime,
            PC.c_ouser,
            PC.c_ovalue,
            (coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null) as is_met
        from pat_cvalues PC
        where PC.category = 'respiratory_failure'
    ) as rf
    group by rf.pat_id, rf.name
  ),
  organ_dysfunction_except_rf as (
    select
        odf.pat_id,
        odf.name,
        first((case when odf.is_met then odf.measurement_time else null end) order by coalesce(odf.measurement_time, odf.c_otime)) as measurement_time,
        first((case when odf.is_met then odf.value            else null end) order by coalesce(odf.measurement_time, odf.c_otime))::text as value,
        first((case when odf.is_met then odf.c_otime          else null end) order by coalesce(odf.measurement_time, odf.c_otime)) as override_time,
        first((case when odf.is_met then odf.c_ouser          else null end) order by coalesce(odf.measurement_time, odf.c_otime)) as override_user,
        first((case when odf.is_met then odf.c_ovalue         else null end) order by coalesce(odf.measurement_time, odf.c_otime)) as override_value,
        coalesce(bool_or(odf.is_met), false) as is_met,
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
    ) as odf
    group by odf.pat_id, odf.name
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
        cfl.pat_id,
        cfl.name,
        first((case when cfl.is_met then cfl.measurement_time else null end) order by coalesce(cfl.measurement_time, cfl.c_otime)) as measurement_time,
        first((case when cfl.is_met then cfl.value            else null end) order by coalesce(cfl.measurement_time, cfl.c_otime))::text as value,
        first((case when cfl.is_met then cfl.c_otime          else null end) order by coalesce(cfl.measurement_time, cfl.c_otime)) as override_time,
        first((case when cfl.is_met then cfl.c_ouser          else null end) order by coalesce(cfl.measurement_time, cfl.c_otime)) as override_user,
        first((case when cfl.is_met then cfl.c_ovalue         else null end) order by coalesce(cfl.measurement_time, cfl.c_otime)) as override_value,
        coalesce(bool_or(cfl.is_met), false) as is_met,
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
    ) as cfl
    group by cfl.pat_id, cfl.name
  ),
  hypotension as (
      select
          ht.pat_id,
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
      ) as ht
      group by ht.pat_id, ht.name
  ),
  hypoperfusion as (
      select
          hpf.pat_id,
          hpf.name,
          first((case when hpf.is_met then hpf.measurement_time else null end) order by coalesce(hpf.measurement_time, hpf.c_otime)) as measurement_time,
          first((case when hpf.is_met then hpf.value            else null end) order by coalesce(hpf.measurement_time, hpf.c_otime))::text as value,
          first((case when hpf.is_met then hpf.c_otime          else null end) order by coalesce(hpf.measurement_time, hpf.c_otime)) as override_time,
          first((case when hpf.is_met then hpf.c_ouser          else null end) order by coalesce(hpf.measurement_time, hpf.c_otime)) as override_user,
          first((case when hpf.is_met then hpf.c_ovalue         else null end) order by coalesce(hpf.measurement_time, hpf.c_otime)) as override_value,
          coalesce(bool_or(hpf.is_met), false) as is_met,
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
      ) as hpf
      group by hpf.pat_id, hpf.name
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
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
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
        ordc.pat_id,
        ordc.name,
        coalesce(   first((case when ordc.is_met then ordc.measurement_time else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last(ordc.measurement_time order by coalesce(ordc.measurement_time, ordc.c_otime))
        ) as measurement_time,
        coalesce(   first((case when ordc.is_met then ordc.value else null end) order by coalesce(ordc.measurement_time, ordc.c_otime))::text,
                    last(ordc.value order by coalesce(ordc.measurement_time, ordc.c_otime))::text
        ) as value,
        coalesce(   first((case when ordc.is_met then ordc.c_otime else null end) order by coalesce(ordc.measurement_time, ordc.c_otime)),
                    last(ordc.c_otime order by coalesce(ordc.measurement_time, ordc.c_otime))
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
    ) as ordc
    group by ordc.pat_id, ordc.name
  ),
  repeat_lactate as (
    select
        rlc.pat_id,
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
    )
    as rlc
    group by rlc.pat_id, rlc.name
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
    select aggs.pat_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.pat_id,
               meas.tsp as measurement_time,
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
    ) as aggs
    group by aggs.pat_id
  ),
  sirs_and_org_df_criteria as (
    select
        sodf.pat_id,
        sodf.name,
        first((case when sodf.is_met then sodf.measurement_time else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as measurement_time,
        first((case when sodf.is_met then sodf.value            else null end) order by coalesce(sodf.measurement_time, sodf.c_otime))::text as value,
        first((case when sodf.is_met then sodf.c_otime          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_time,
        first((case when sodf.is_met then sodf.c_ouser          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_user,
        first((case when sodf.is_met then sodf.c_ovalue         else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_value,
        coalesce(bool_or(sodf.is_met), false) as is_met,
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
                when PC.name = 'respiratory_failure'
                then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

                when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

                else
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
               end) as is_met
      from pat_cvalues PC
      where PC.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
    ) as sodf
    group by sodf.pat_id, sodf.name
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
          ainf.pat_id,
          ainf.name,
          first((ainf.measurement_time) order by coalesce(ainf.measurement_time, ainf.c_otime)) as measurement_time,
          first((ainf.value)            order by coalesce(ainf.measurement_time, ainf.c_otime))::text as value,
          first((ainf.c_otime)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_time,
          first((ainf.c_ouser)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_user,
          first((ainf.c_ovalue)         order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_value,
          coalesce(bool_or(ainf.is_met), false) as is_met,
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
      ) as ainf
      group by ainf.pat_id, ainf.name
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
    null_infections as (
      -- get_window_states explicitly requires a null value to yield states 10 and 12
      select P.pat_id,
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
        select P.pat_id                                                 as pat_id,
               'suspicion_of_infection'::text                           as name,
               min(M.start_ts)                                          as measurement_time,
               min(M.ngram)                                             as value,
               min(M.start_ts)                                          as override_time,
               'NLP'::text                                              as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true                                                     as is_met,
               now()                                                    as update_date
        from notes_candidates NC
        inner join pat_visit_ids P on NC.pat_id = P.pat_id
        inner join lateral match_clarity_infections(P.visit_id, 3, 3) M on P.visit_id = M.csn_id
        where use_clarity_notes
        and not use_app_infections
        group by P.pat_id
    ),
    cdm_matches as (
        -- TODO: we have picked an arbitrary time interval for notes. Refine.
        select NC.pat_id                                                as pat_id,
               'suspicion_of_infection'::text                           as name,
               min(M.start_ts)                                          as measurement_time,
               min(M.ngram)                                             as value,
               min(M.start_ts)                                          as override_time,
               'NLP'::text                                              as override_user,
               json_agg(json_build_object('text'::text, M.ngram::text)) as override_value,
               true                                                     as is_met,
               now()                                                    as update_date
        from notes_candidates NC
        inner join lateral match_cdm_infections(NC.pat_id, _dataset_id, 3, 3) M
          on NC.pat_id = M.pat_id
          and M.start_ts between ts_start - interval '1 days' and ts_end + interval '1 days'
        where not use_clarity_notes
        and not use_app_infections
        group by NC.pat_id
    )
    select NI.pat_id,
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
    ) MTCH on NI.pat_id = MTCH.pat_id
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
        cfhf.pat_id,
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
    ) as cfhf
    group by cfhf.pat_id, cfhf.name
  ),
  hypotension as (
    select
        ht.pat_id,
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
    ) as ht
    group by ht.pat_id, ht.name
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
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
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
        ordc.pat_id,
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
    ) as ordc
    group by ordc.pat_id, ordc.name
  ),
  repeat_lactate as (
    select
        rlc.pat_id,
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
    )
    as rlc
    group by rlc.pat_id, rlc.name
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




-- get_cms_labels_for_window_simulated_soi
-- Simulates suspicion of infection for everyone with SIRS and Org DF,
-- rather than using notes.
-- This is primarily for debugging calculations after severe sepsis, e.g.,
-- orders, etc., and is based on the inlined version above.
CREATE OR REPLACE FUNCTION get_cms_labels_for_window_simulated_soi(
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
    select aggs.pat_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.pat_id,
               meas.tsp as measurement_time,
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
    ) as aggs
    group by aggs.pat_id
  ),
  sirs_and_org_df_criteria as (
    select
        sodf.pat_id,
        sodf.name,
        first((case when sodf.is_met then sodf.measurement_time else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as measurement_time,
        first((case when sodf.is_met then sodf.value            else null end) order by coalesce(sodf.measurement_time, sodf.c_otime))::text as value,
        first((case when sodf.is_met then sodf.c_otime          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_time,
        first((case when sodf.is_met then sodf.c_ouser          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_user,
        first((case when sodf.is_met then sodf.c_ovalue         else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_value,
        coalesce(bool_or(sodf.is_met), false) as is_met,
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
                when PC.name = 'respiratory_failure'
                then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

                when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

                else
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
               end) as is_met
      from pat_cvalues PC
      where PC.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
    ) as sodf
    group by sodf.pat_id, sodf.name
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
  simulated_infections as (
    -- get_window_states explicitly requires a null value to yield states 10 and 12
    select R.pat_id,
           'suspicion_of_infection'::text                                          as name,
           R.tsp                                                                   as measurement_time,
           (case when R.is_met then 'Simulated' else null end)                     as value,
           R.tsp                                                                   as override_time,
           (case when R.is_met then 'Simulated' else null end)::text               as override_user,
           (case when R.is_met then '[{"text": "Simulated"}]'::json else null end) as override_value,
           R.is_met                                                                as is_met,
           now()                                                                   as update_date
    from (
      select P.pat_id,
             greatest(S.sirs_onset, S.org_df_onset) as tsp,
             greatest(S.sirs_onset, S.org_df_onset) is not null as is_met
      from pat_ids P
      left join severe_sepsis_wo_infection S on P.pat_id = S.pat_id
    ) R
  ),
  severe_sepsis as (
    select * from sirs_and_org_df_criteria
    union all select * from simulated_infections
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
      from simulated_infections I
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
        cfhf.pat_id,
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
    ) as cfhf
    group by cfhf.pat_id, cfhf.name
  ),
  hypotension as (
    select
        ht.pat_id,
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
    ) as ht
    group by ht.pat_id, ht.name
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
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
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
        ordc.pat_id,
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
        left join simulated_infections c
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
    ) as ordc
    group by ordc.pat_id, ordc.name
  ),
  repeat_lactate as (
    select
        rlc.pat_id,
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
    )
    as rlc
    group by rlc.pat_id, rlc.name
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
    select aggs.pat_id,
           avg(aggs.bp_sys) as bp_sys,
           first(aggs.weight order by aggs.measurement_time) as weight,
           sum(aggs.urine_output) as urine_output
    from (
        select P.pat_id,
               meas.tsp as measurement_time,
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
    ) as aggs
    group by aggs.pat_id
  ),
  sirs_and_org_df_criteria as (
    select
        sodf.pat_id,
        sodf.name,
        first((case when sodf.is_met then sodf.measurement_time else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as measurement_time,
        first((case when sodf.is_met then sodf.value            else null end) order by coalesce(sodf.measurement_time, sodf.c_otime))::text as value,
        first((case when sodf.is_met then sodf.c_otime          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_time,
        first((case when sodf.is_met then sodf.c_ouser          else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_user,
        first((case when sodf.is_met then sodf.c_ovalue         else null end) order by coalesce(sodf.measurement_time, sodf.c_otime)) as override_value,
        coalesce(bool_or(sodf.is_met), false) as is_met,
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
                when PC.name = 'respiratory_failure'
                then coalesce(PC.c_ovalue#>>'{0,text}', PC.value) is not null

                when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
                then criteria_value_met(PC.value, PC.c_ovalue, PC.d_ovalue)

                else
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
               end) as is_met
      from pat_cvalues PC
      where PC.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
    ) as sodf
    group by sodf.pat_id, sodf.name
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
          ainf.pat_id,
          ainf.name,
          first((ainf.measurement_time) order by coalesce(ainf.measurement_time, ainf.c_otime)) as measurement_time,
          first((ainf.value)            order by coalesce(ainf.measurement_time, ainf.c_otime))::text as value,
          first((ainf.c_otime)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_time,
          first((ainf.c_ouser)          order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_user,
          first((ainf.c_ovalue)         order by coalesce(ainf.measurement_time, ainf.c_otime)) as override_value,
          coalesce(bool_or(ainf.is_met), false) as is_met,
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
      ) as ainf
      group by ainf.pat_id, ainf.name
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
    null_infections as (
      -- get_window_states explicitly requires a null value to yield states 10 and 12
      select P.pat_id,
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
    select NI.pat_id,
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
    ) MTCH on NI.pat_id = MTCH.pat_id
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
        cfhf.pat_id,
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
    ) as cfhf
    group by cfhf.pat_id, cfhf.name
  ),
  hypotension as (
    select
        ht.pat_id,
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
    ) as ht
    group by ht.pat_id, ht.name
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
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
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
        ordc.pat_id,
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
    ) as ordc
    group by ordc.pat_id, ordc.name
  ),
  repeat_lactate as (
    select
        rlc.pat_id,
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
    )
    as rlc
    group by rlc.pat_id, rlc.name
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



-- Returns binned timestamps for window boundaries
-- based on timestamps at which measuremenets are available.
CREATE OR REPLACE FUNCTION get_meas_periodic_timestamps(
        _pat_id                 text,
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
    pat_id_str text;
BEGIN
  pat_id_str = case when _pat_id is null then 'NULL'
                    else format('''%s''', _pat_id) end;

  return query execute format('
    with pat_start as(
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
    )
    select pat_id, max(tsp) as tsp
    from meas_bins
    group by pat_id, bin
    order by pat_id, tsp
    limit %s'
    , _dataset_id,  pat_id_str, _dataset_id, ts_start, ts_end, window_limit
    );
END; $function$;


-- Returns timestamps for window boundaries based on changes of patient
-- state observed in a label series.
CREATE OR REPLACE FUNCTION get_label_change_timestamps(
        _pat_id                 text,
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
    pat_id_str text;
BEGIN
  pat_id_str = case when _pat_id is null then 'NULL'
                    else format('''%s''', _pat_id) end;

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
    , pat_id_str, _dataset_id, _label_id, ts_start, ts_end, window_limit
    );
END; $function$;


--
-- Computes a label series using windows defined by the 'window_generator' UDF.
CREATE OR REPLACE FUNCTION get_cms_label_series_for_windows(
        label_description       text,
        window_generator        text,
        label_function          integer,
        _pat_id                 text,
        _dataset_id             integer,
        _label_id               integer,
        ts_start                timestamptz default '-infinity'::timestamptz,
        ts_end                  timestamptz default 'infinity'::timestamptz,
        window_limit            text default 'all',
        use_app_infections      boolean default false,
        use_clarity_notes       boolean default false
  )
  returns integer
  LANGUAGE plpgsql
AS $function$
DECLARE
    window_size            interval := get_parameter('lookbackhours')::interval;
    pat_id_str             text;
    label_id_str           text;
    window_fn              text;
    use_app_infections_str text;
    use_clarity_notes_str  text;
    generated_label_id     integer;
BEGIN

    select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
    raise notice 'Running get_cms_label_series_for_windows on dataset_id %', _dataset_id;

    pat_id_str = case when _pat_id is null then 'NULL'
                      else format('''%s''', _pat_id) end;

    label_id_str = case when _label_id is null then 'NULL'
                        else format('%s', _label_id) end;

    window_fn = case label_function
                  when 0 then 'get_cms_labels_for_window_inlined'
                  when 1 then 'get_prospective_cms_labels_for_window'
                  when 2 then 'get_cms_candidates_for_window'
                  when 3 then 'get_cms_labels_for_window_simulated_soi'
                  else 'get_cms_labels_for_window' end;

    use_app_infections_str = case when use_app_infections then 'True' else 'False' end;
    use_clarity_notes_str = case when use_clarity_notes then 'True' else 'False' end;

    -- Populate criteria for each window into a temp table.
    drop table if exists new_criteria_windows;

    execute format(
     'create temporary table new_criteria_windows as
      with window_ends as (
        select * from %s(%s::text, %s::integer, %s::integer, ''%s''::timestamptz, ''%s''::timestamptz, ''%s''::text)
      )
      select window_ends.tsp as ts, new_criteria.*
      from
        window_ends
        inner join lateral
        %s(coalesce(%s, window_ends.pat_id),
           window_ends.tsp - ''%s''::interval,
           window_ends.tsp,
           %s, %s, %s) new_criteria
      on window_ends.pat_id = new_criteria.pat_id;'
      , window_generator, pat_id_str, _dataset_id, label_id_str, ts_start, ts_end, window_limit
      , window_fn, pat_id_str, window_size, _dataset_id, use_app_infections_str, use_clarity_notes_str);

    -- Register a new label id
    insert into label_version (created, description)
        values (now(), label_description)
        returning label_id into generated_label_id;

    -- Populate label series
    insert into cdm_labels (dataset_id, label_id, pat_id, tsp, label_type, label)
      select _dataset_id, generated_label_id, sw.pat_id, sw.ts, 'cms state', sw.state
      from get_window_states('new_criteria_windows', _pat_id) sw
    on conflict (dataset_id, label_id, pat_id, tsp) do update
      set label_type = excluded.label_type,
          label = excluded.label;

    drop table new_criteria_windows;
    return generated_label_id;
END; $function$;


--
-- Computes a label series using coarse-to-fine strategy for optimizing notes processing.
CREATE OR REPLACE FUNCTION get_cms_label_series(
        label_description       text,
        label_function          integer,
        _pat_id                 text,
        _dataset_id             INTEGER DEFAULT NULL,
        ts_start                timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end                  timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit            text default 'all',
        use_app_infections      boolean default false,
        use_clarity_notes       boolean default false
  )
  returns integer
  LANGUAGE plpgsql
AS $function$
DECLARE
  candidate_function integer := 2;
  candidate_label_id integer;
  result_label_id    integer;
BEGIN
  if label_function = 3 then
    -- Single pass for simulated suspicion of infection over all windows
    -- (since this skips notes processing, and should be sufficiently efficient as-is)
    select get_cms_label_series_for_windows(
      label_description, 'get_meas_periodic_timestamps', label_function,
      _pat_id, _dataset_id, null, ts_start, ts_end, window_limit, use_app_infections, use_clarity_notes)
    into result_label_id;
  else
    -- Coarse-grained pass, calculating sirs and org df for state changes.
    select get_cms_label_series_for_windows(
      label_description || ' (candidate series)', 'get_meas_periodic_timestamps', candidate_function,
      _pat_id, _dataset_id, null, ts_start, ts_end, window_limit, use_app_infections, use_clarity_notes)
    into candidate_label_id;

    raise notice 'Finished first pass for get_cms_label_series_v2 on dataset_id %, label_id %', _dataset_id, candidate_label_id;

    if label_function <> 2 then
      -- Fine-grained pass, using windows based on state changes.
      select get_cms_label_series_for_windows(
        label_description, 'get_label_change_timestamps', label_function,
        _pat_id, _dataset_id, candidate_label_id, ts_start, ts_end, window_limit, use_app_infections, use_clarity_notes)
      into result_label_id;
    else
      result_label_id := candidate_label_id;
    end if;
  end if;

  return result_label_id;
END; $function$;




--------------------------------------------------
-- Columnar criteria report for a patient-window.
--------------------------------------------------
create or replace function humanize_report_timestamp(reference timestamptz, ts timestamptz)
  returns text language plpgsql
as $func$ begin
  return
  case when reference is null and date_part('year', ts) = date_part('year', current_timestamp)
       then to_char(ts, 'MM-DD HH24:MI')

       when date_part('day', ts) = date_part('day', reference)
            and date_part('month', ts) = date_part('month', reference)
            and date_part('year', ts) = date_part('year', reference)
       then to_char(ts, 'HH24:MI')

       when date_part('year', ts) = date_part('year', reference)
       then to_char(ts, 'MM-DD HH24:MI')

       else to_char(ts, 'MM-DD-YY HH24:MI')
  end;
end; $func$;

create or replace function humanize_report_infection(value json)
  returns text language plpgsql
as $func$
declare
  bvalue jsonb := value::jsonb;
begin
  return
    case when bvalue is null then null
         when jsonb_array_length(bvalue) > 1 and (bvalue->0) ? ('text'::text)
         then '# Matches: '
              || jsonb_array_length(bvalue)::text
              || ', First: '
              || ((bvalue->0)->>'text')

         when jsonb_array_length(bvalue) > 0 and (bvalue->0) ? ('text'::text)
         then (bvalue->0)->>'text'
         else bvalue::text
    end;
end; $func$;

create or replace function humanize_report_note(value text, t text)
  returns text language plpgsql
as $func$ begin
  return
  case when value is null or t is null
       then null
       else 'Note @' || t || ': ' || value
  end;
end; $func$;

create or replace function humanize_report_entry(tag text, value text, t text)
  returns text language plpgsql
as $func$ begin
  return
  case when value is null or t is null
       then null
       else tag || ': ' || value || ' @' || t
  end;
end; $func$;


create or replace function criteria_window_report(this_pat_id text,
                                                  ts_start    timestamptz,
                                                  ts_end      timestamptz,
                                                  dataset_id  integer)
  returns table ( pat_id                            varchar(50),
                  severe_sepsis_onset               text,
                  sirs_organ_dys_onset              text,
                  septic_shock_onset                text,
                  antibiotics_order_met             text,
                  antibiotics_order_t               text,
                  antibiotics_order_value           text,
                  bilirubin_t                       text,
                  bilirubin_value                   text,
                  blood_culture_order_met           text,
                  blood_culture_order_t             text,
                  blood_culture_order_value         text,
                  blood_pressure_t                  text,
                  blood_pressure_value              text,
                  creatinine_t                      text,
                  creatinine_value                  text,
                  crystalloid_fluid_t               text,
                  crystalloid_fluid_value           text,
                  crystalloid_fluid_order_met       text,
                  crystalloid_fluid_order_t         text,
                  crystalloid_fluid_order_value     text,
                  decrease_in_sbp_t                 text,
                  decrease_in_sbp_value             text,
                  heart_rate_t                      text,
                  heart_rate_value                  text,
                  hypotension_dsbp_t                text,
                  hypotension_dsbp_value            text,
                  hypotension_map_t                 text,
                  hypotension_map_value             text,
                  initial_lactate_t                 text,
                  initial_lactate_value             text,
                  initial_lactate_order_met         text,
                  initial_lactate_order_t           text,
                  initial_lactate_order_value       text,
                  inr_t                             text,
                  inr_value                         text,
                  lactate_t                         text,
                  lactate_value                     text,
                  mean_arterial_pressure_t          text,
                  mean_arterial_pressure_value      text,
                  platelet_t                        text,
                  platelet_value                    text,
                  repeat_lactate_order_met          text,
                  repeat_lactate_order_t            text,
                  repeat_lactate_order_value        text,
                  respiratory_failure_t             text,
                  respiratory_failure_value         text,
                  respiratory_rate_t                text,
                  respiratory_rate_value            text,
                  sirs_temp_t                       text,
                  sirs_temp_value                   text,
                  suspicion_of_infection_t          text,
                  suspicion_of_infection_value      text,
                  systolic_bp_t                     text,
                  systolic_bp_value                 text,
                  vasopressors_order_met            text,
                  vasopressors_order_t              text,
                  vasopressors_order_value          text,
                  wbc_t                             text,
                  wbc_value                         text
                )
  language plpgsql
as $func$ begin
  return query
  select  R.pat_id,
          humanize_report_timestamp(null, R.severe_sepsis_onset),
          humanize_report_timestamp(R.severe_sepsis_onset, R.severe_sepsis_wo_infection_onset) as sirs_organ_dys_onset,
          humanize_report_timestamp(R.severe_sepsis_onset, R.septic_shock_onset),
          R.o#>>'{antibiotics_order, met}'         as antibiotics_order_met,
          R.o#>>'{antibiotics_order, t}'           as antibiotics_order_t,
          R.o#>>'{antibiotics_order, value}'       as antibiotics_order_value,
          R.o#>>'{bilirubin, t}'                   as bilirubin_t,
          R.o#>>'{bilirubin, value}'               as bilirubin_value,
          R.o#>>'{blood_culture_order, met}'       as blood_culture_order_met,
          R.o#>>'{blood_culture_order, t}'         as blood_culture_order_t,
          R.o#>>'{blood_culture_order, value}'     as blood_culture_order_value,
          R.o#>>'{blood_pressure, t}'              as blood_pressure_t,
          R.o#>>'{blood_pressure, value}'          as blood_pressure_value,
          R.o#>>'{creatinine, t}'                  as creatinine_t,
          R.o#>>'{creatinine, value}'              as creatinine_value,
          R.o#>>'{crystalloid_fluid, t}'           as crystalloid_fluid_t,
          R.o#>>'{crystalloid_fluid, value}'       as crystalloid_fluid_value,
          R.o#>>'{crystalloid_fluid_order, met}'   as crystalloid_fluid_order_met,
          R.o#>>'{crystalloid_fluid_order, t}'     as crystalloid_fluid_order_t,
          R.o#>>'{crystalloid_fluid_order, value}' as crystalloid_fluid_order_value,
          R.o#>>'{decrease_in_sbp, t}'             as decrease_in_sbp_t,
          R.o#>>'{decrease_in_sbp, value}'         as decrease_in_sbp_value,
          R.o#>>'{heart_rate, t}'                  as heart_rate_t,
          R.o#>>'{heart_rate, value}'              as heart_rate_value,
          R.o#>>'{hypotension_dsbp, t}'            as hypotension_dsbp_t,
          R.o#>>'{hypotension_dsbp, value}'        as hypotension_dsbp_value,
          R.o#>>'{hypotension_map, t}'             as hypotension_map_t,
          R.o#>>'{hypotension_map, value}'         as hypotension_map_value,
          R.o#>>'{initial_lactate, t}'             as initial_lactate_t,
          R.o#>>'{initial_lactate, value}'         as initial_lactate_value,
          R.o#>>'{initial_lactate_order, met}'     as initial_lactate_order_met,
          R.o#>>'{initial_lactate_order, t}'       as initial_lactate_order_t,
          R.o#>>'{initial_lactate_order, value}'   as initial_lactate_order_value,
          R.o#>>'{inr, t}'                         as inr_t,
          R.o#>>'{inr, value}'                     as inr_value,
          R.o#>>'{lactate, t}'                     as lactate_t,
          R.o#>>'{lactate, value}'                 as lactate_value,
          R.o#>>'{mean_arterial_pressure, t}'      as mean_arterial_pressure_t,
          R.o#>>'{mean_arterial_pressure, value}'  as mean_arterial_pressure_value,
          R.o#>>'{platelet, t}'                    as platelet_t,
          R.o#>>'{platelet, value}'                as platelet_value,
          R.o#>>'{repeat_lactate_order, met}'      as repeat_lactate_order_met,
          R.o#>>'{repeat_lactate_order, t}'        as repeat_lactate_order_t,
          R.o#>>'{repeat_lactate_order, value}'    as repeat_lactate_order_value,
          R.o#>>'{respiratory_failure, t}'         as respiratory_failure_t,
          R.o#>>'{respiratory_failure, value}'     as respiratory_failure_value,
          R.o#>>'{respiratory_rate, t}'            as respiratory_rate_t,
          R.o#>>'{respiratory_rate, value}'        as respiratory_rate_value,
          R.o#>>'{sirs_temp, t}'                   as sirs_temp_t,
          R.o#>>'{sirs_temp, value}'               as sirs_temp_value,
          R.o#>>'{suspicion_of_infection, t}'      as suspicion_of_infection_t,
          R.o#>>'{suspicion_of_infection, value}'  as suspicion_of_infection_value,
          R.o#>>'{systolic_bp, t}'                 as systolic_bp_t,
          R.o#>>'{systolic_bp, value}'             as systolic_bp_value,
          R.o#>>'{vasopressors_order, met}'        as vasopressors_order_met,
          R.o#>>'{vasopressors_order, t}'          as vasopressors_order_t,
          R.o#>>'{vasopressors_order, value}'      as vasopressors_order_value,
          R.o#>>'{wbc, t}'                         as wbc_t,
          R.o#>>'{wbc, value}'                     as wbc_value
  from (
    select  C.pat_id, C.severe_sepsis_onset, C.severe_sepsis_wo_infection_onset, C.septic_shock_onset,
            json_object_agg(C.name,
              case when C.name = 'suspicion_of_infection'
                    then json_object('{t, value}', ARRAY[
                           humanize_report_timestamp(C.severe_sepsis_onset, coalesce(C.override_time, C.measurement_time)),
                           coalesce(humanize_report_infection(C.override_value), C.value)::text
                         ]::text[])

                   when C.name like '%_order'
                    then json_object('{t, value, met}', ARRAY[
                           humanize_report_timestamp(C.severe_sepsis_onset, coalesce(C.override_time, C.measurement_time)),
                           coalesce(C.override_value::text, C.value)::text,
                           C.is_met::text
                         ]::text[])

                   when C.is_met
                    then json_object('{t, value}', ARRAY[
                           humanize_report_timestamp(C.severe_sepsis_onset, coalesce(C.override_time, C.measurement_time)),
                           coalesce(C.override_value::text, C.value)::text
                         ]::text[])

                   else null
              end
            ) as o
    from get_cms_labels_for_window_inlined(this_pat_id, ts_start, ts_end, dataset_id) C
    group by C.pat_id, C.severe_sepsis_onset, C.severe_sepsis_wo_infection_onset, C.septic_shock_onset
  ) R
  order by pat_id;
end; $func$;


create or replace function criteria_report(this_pat_state  integer,
                                           this_dataset_id integer,
                                           this_label_id   integer)
  returns table ( pat_worst_state                   integer,
                  pat_id                            varchar(50),
                  severe_sepsis_onset               text,
                  sirs_organ_dys_onset              text,
                  septic_shock_onset                text,
                  infection                         text,
                  sirs_criteria                     text,
                  org_df_criteria                   text,
                  septic_shock_criteria             text,
                  orders                            text
  )
language plpgsql
as $func$ begin
  return query
    select WS.max_state as pat_worst_state,
           Report.pat_id,
           Report.severe_sepsis_onset,
           Report.sirs_organ_dys_onset,
           Report.septic_shock_onset,

           humanize_report_note(suspicion_of_infection_value, suspicion_of_infection_t)
            as infection,

           array_to_string(array_remove(
            ARRAY[
              humanize_report_entry( 'TEMP', sirs_temp_value, sirs_temp_t ),
              humanize_report_entry( 'HR', heart_rate_value, heart_rate_t ),
              humanize_report_entry( 'RR', respiratory_rate_value, respiratory_rate_t ),
              humanize_report_entry( 'WBC', wbc_value, wbc_t )
            ],
           NULL), ' ', '') as sirs_criteria,

           array_to_string(array_remove(
            ARRAY[
              humanize_report_entry( 'SBP', systolic_bp_value, systolic_bp_t),
              humanize_report_entry( 'MAP', mean_arterial_pressure_value, mean_arterial_pressure_t ),
              humanize_report_entry( 'DSBP', decrease_in_sbp_value, decrease_in_sbp_t ),
              humanize_report_entry( 'RSPF', respiratory_failure_value, respiratory_failure_t ),
              humanize_report_entry( 'CRT', creatinine_value, creatinine_t ),
              humanize_report_entry( 'BILI', bilirubin_value, bilirubin_t ),
              humanize_report_entry( 'PLT', platelet_value, platelet_t ),
              humanize_report_entry( 'INR', inr_value, inr_t ),
              humanize_report_entry( 'BP', blood_pressure_value, blood_pressure_t ),
              humanize_report_entry( 'LC', lactate_value, lactate_t )
            ]::text[],
           NULL), ' ', '') as org_df_criteria,

           array_to_string(array_remove(
            ARRAY[
              humanize_report_entry( 'FLD', crystalloid_fluid_value, crystalloid_fluid_t ),
              humanize_report_entry( 'HT_DBSP', hypotension_dsbp_value, hypotension_dsbp_t ),
              humanize_report_entry( 'HT_MAP', hypotension_map_value, hypotension_map_t ),
              humanize_report_entry( 'ILC', initial_lactate_value, initial_lactate_t )
            ]::text[],
           NULL), ' ', '') as septic_shock_criteria,

           array_to_string(array_remove(
             ARRAY[
              humanize_report_entry(
                'AB',
                antibiotics_order_value || (case when antibiotics_order_met::boolean then '✓' else '' end),
                antibiotics_order_t
                )
             ,
              humanize_report_entry(
                'BC',
                blood_culture_order_value || (case when blood_culture_order_met::boolean then '✓' else '' end),
                blood_culture_order_t
              )
             ,
              humanize_report_entry(
                'FL',
                crystalloid_fluid_order_value || (case when crystalloid_fluid_order_met::boolean then '✓' else '' end),
                crystalloid_fluid_order_t
              )
             ,
              humanize_report_entry(
                'ILC',
                initial_lactate_order_value || (case when initial_lactate_order_met::boolean then '✓' else '' end),
                initial_lactate_order_t
              )
             ,
              humanize_report_entry(
                'RLC',
                repeat_lactate_order_value || (case when repeat_lactate_order_met::boolean then '✓' else '' end),
                repeat_lactate_order_t
              )
             ,
              humanize_report_entry(
                'VP',
                vasopressors_order_value || (case when vasopressors_order_met::boolean then '✓' else '' end),
                vasopressors_order_t
              )
             ]::text[],
           NULL), ' ', '') as orders
    from (
      -- Earliest occurrence of the worst state.
      select L2.pat_id, L2.dataset_id, min(L2.tsp) as window_ts, max(L2.label) as max_state
      from (
        -- Worst state for each patient.
        select L.pat_id, max(L.label) as label
        from cdm_labels L
        where L.dataset_id = coalesce(this_dataset_id, L.dataset_id)
        and   L.label_id = coalesce(this_label_id, L.label_id)
        group by L.pat_id
      ) L1
      inner join cdm_labels L2
          on L1.pat_id = L2.pat_id
          and L1.label = L2.label
      where L1.label = coalesce(this_pat_state, L1.label)
      and L2.dataset_id = coalesce(this_dataset_id, L2.dataset_id)
      and L2.label_id = coalesce(this_label_id, L2.label_id)
      group by L2.pat_id, L2.dataset_id
    ) WS
    inner join lateral criteria_window_report(
      WS.pat_id, WS.window_ts - interval '6 hours', WS.window_ts, WS.dataset_id
    ) Report
      on WS.pat_id = Report.pat_id
    order by WS.max_state desc, Report.pat_id
    ;
end; $func$;