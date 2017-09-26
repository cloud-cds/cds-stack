import os

window_template_meas_periodic = '''
create table measurement_times_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  with pat_start as(
    select pat_id, min(tsp) as min_time
    from criteria_meas meas
    where dataset_id = %(dataset_id)s
    group by pat_id
  ),
  meas_bins as (
    select distinct meas.pat_id, meas.tsp ,
      floor(extract(EPOCH FROM meas.tsp - pat_start.min_time) / extract(EPOCH from interval '1 hour'))+1 as bin
    from
      criteria_meas meas
      inner join pat_start
      on pat_start.pat_id = meas.pat_id
    where
      meas.dataset_id = %(dataset_id)s
  )
  select pat_id, max(tsp) as tsp
  from meas_bins
  group by pat_id, bin
  order by pat_id, tsp
  ;

create table pat_partition_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  select * from measurement_times_d%(dataset_id)s T
  ;
'''

window_template_measurements = '''
create table measurement_times_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  select distinct meas.pat_id, date_trunc('hour', meas.tsp) as tsp
  from criteria_meas meas
  inner join criteria_default_flat cd on meas.fid = cd.fid
  where
    cd.dataset_id = %(dataset_id)s
    and meas.dataset_id = %(dataset_id)s
    and cd.name in (
        'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
    and
    (case
      when cd.name = 'respiratory_failure' then meas.value is not null
      when cd.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and isnumeric(meas.value)
      then coalesce( not (
            meas.value::double precision
                between coalesce(case when cd.lower = '' then null else cd.lower end, meas.value)::double precision
                and coalesce(case when cd.upper = '' then null else cd.upper end, meas.value)::double precision
          ), false)

      when cd.category = 'decrease_in_sbp' or cd.category = 'urine_output' then true

      when isnumeric(meas.value)
      then coalesce( not (
            meas.value::double precision
                between coalesce(case when cd.lower = '' then null else cd.lower end, meas.value)::double precision
                and coalesce(case when cd.upper = '' then null else cd.upper end, meas.value)::double precision
          ), false)

      else false
     end)
  ;

create table pat_partition_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  select T.pat_id, T.tsp + (O.window_offset::varchar || ' minutes')::interval as tsp
  from measurement_times_d%(dataset_id)s T
  cross join cdm_window_offsets_15mins O
  ;
'''

bpa_template = '''
create table pat_cvalues_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval as window_id,
         meas.pat_id,
         cd.name,
         meas.fid,
         cd.category,
         meas.tsp,
         meas.value,
         cd.lower as d_lower,
         cd.upper as d_upper
  from criteria_default_flat as cd

  left join criteria_meas meas
      on meas.fid = cd.fid
      and cd.dataset_id = meas.dataset_id

  cross join cdm_window_offsets_15mins O

  where cd.dataset_id = %(dataset_id)s
  and meas.dataset_id = %(dataset_id)s
  and meas.value <> 'nan'
  and cd.name in (
    'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
    'respiratory_failure',
    'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
  )
  and meas.fid in (
    'temperature', 'heart_rate', 'resp_rate', 'wbc', 'bands', 'bp_sys', 'map', 'bilirubin', 'platelets', 'inr',
    'ptt', 'lactate', 'vent', 'bipap', 'cpap', 'creatinine'
  )
  and
  (case
    when cd.name = 'respiratory_failure' then meas.value is not null
    when cd.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and isnumeric(meas.value)
    then coalesce( not (
          meas.value::double precision
              between coalesce(case when cd.lower = '' then null else cd.lower end, meas.value)::double precision
              and coalesce(case when cd.upper = '' then null else cd.upper end, meas.value)::double precision
        ), false)

    when cd.category = 'decrease_in_sbp' or cd.category = 'urine_output' then true

    when isnumeric(meas.value)
    then coalesce( not (
          meas.value::double precision
              between coalesce(case when cd.lower = '' then null else cd.lower end, meas.value)::double precision
              and coalesce(case when cd.upper = '' then null else cd.upper end, meas.value)::double precision
        ), false)

    else false
   end)
  ;


create table pat_aggregates_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select aggs.window_id,
         aggs.pat_id,
         avg(aggs.bp_sys) as bp_sys,
         avg(aggs.weight) as weight,
         sum(aggs.urine_output) as urine_output
  from (
      select date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval as window_id,
             meas.pat_id,
             meas.tsp as measurement_time,
             (case when meas.fid = 'bp_sys' then meas.value::double precision else null end) as bp_sys,
             (case when meas.fid = 'weight' then meas.value::double precision else null end) as weight,
             (case when meas.fid = 'urine_output'
                   and date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval - meas.tsp < interval '2 hours'
                   then meas.value::double precision else null end
              ) as urine_output
      from criteria_meas meas
      cross join cdm_window_offsets_15mins O
      where meas.dataset_id = %(dataset_id)s
      and meas.fid in ('bp_sys', 'urine_output', 'weight')
      and isnumeric(meas.value)
  ) as aggs
  group by aggs.window_id, aggs.pat_id
  ;


create table all_sirs_org_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select  PC.window_id,
          PC.pat_id,
          PC.name,
          PC.tsp as measurement_time,
          (case
            when PC.name = 'respiratory_failure' then PC.fid || ': ' || PC.value
            else PC.value
           end) as value,
          (case
            when PC.name = 'respiratory_failure'
            then PC.value is not null

            when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and isnumeric(PC.value)
            then coalesce( not (
                  PC.value::double precision
                      between coalesce(case when PC.d_lower = '' then null else PC.d_lower end, PC.value)::double precision
                      and coalesce(case when PC.d_upper = '' then null else PC.d_upper end, PC.value)::double precision
                ), false)

            when PC.category = 'decrease_in_sbp' then
              (case
                when isnumeric(PC.value) and isnumeric(PC.d_upper)
                then coalesce( PAGG.bp_sys - PC.value::double precision > PC.d_upper::double precision, false)

                when isnumeric(PC.value)
                then coalesce( PAGG.bp_sys - PC.value::double precision > PC.value::double precision, false)

                else false
                end)

            when PC.category = 'urine_output' then
              coalesce( PAGG.urine_output / coalesce( PAGG.weight, POPMEANS.weight_popmean ) < 0.5, false)

            when isnumeric(PC.value)
            then coalesce( not (
                  PC.value::double precision
                      between coalesce(case when PC.d_lower = '' then null else PC.d_lower end, PC.value)::double precision
                      and coalesce(case when PC.d_upper = '' then null else PC.d_upper end, PC.value)::double precision
                ), false)

            else false
            end) as is_met
  from pat_cvalues_d%(dataset_id)s PC

  left join pat_aggregates_d%(dataset_id)s PAGG
    on PC.window_id = PAGG.window_id and PC.pat_id = PAGG.pat_id

  cross join (
    select value::double precision as weight_popmean
    from cdm_g where fid = 'weight_popmean' and dataset_id = %(dataset_id)s
  ) POPMEANS
  ;


create table null_infections_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  -- This is necessary for get_window_labels_from_criteria
  select P.tsp                             as window_id,
         P.pat_id                          as pat_id,
         'suspicion_of_infection'::varchar as name,
         null::timestamptz                 as measurement_time,
         null::text                        as value,
         false                             as is_met
  from pat_partition_d%(dataset_id)s P
  ;



create table severe_sepsis_candidates_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with all_sirs_org_triples_d%(dataset_id)s as (
    with sirs as (
      select * from all_sirs_org_d%(dataset_id)s S
      where S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
    ),
    org_df as (
      select * from all_sirs_org_d%(dataset_id)s S
      where S.name in (
        'respiratory_failure',
        'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
      )
      and S.is_met
    )
    select SO.window_id,
           SO.pat_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset
    from (
      select S1.window_id,
             S1.pat_id,
             S1.name as sirs1_name,
             S2.name as sirs2_name,
             D.name as odf_name,
             S1.measurement_time as sirs_initial,
             S2.measurement_time as sirs_onset,
             D.measurement_time as org_df_onset
      from org_df D
      inner join sirs S1
        on D.window_id = S1.window_id and D.pat_id = S1.pat_id
      inner join sirs S2
        on S1.window_id = S2.window_id
        and S1.pat_id = S2.pat_id
        and S1.name <> S2.name
        and S1.measurement_time <= S2.measurement_time
    ) SO
    where not (SO.sirs_initial is null or SO.sirs_onset is null or SO.org_df_onset is null)
    and greatest(SO.sirs_onset, SO.org_df_onset)
          - least(SO.sirs_initial, SO.org_df_onset) < interval '6 hours' /*window_size*/
  ),
  indexed_triples as (
    select SO.window_id,
           SO.pat_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset,
           I.infection_cnt,
           I.infection_onset,

           row_number() over (
              partition by SO.window_id, SO.pat_id
              order by
                (coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null) desc nulls last,
                greatest(SO.sirs_initial::timestamptz, SO.sirs_onset::timestamptz, SO.org_df_onset::timestamptz)::timestamptz nulls last
            ) as row

    from all_sirs_org_triples_d%(dataset_id)s SO
    left join (
      select I.window_id,
             I.pat_id,
             (case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             (case when I.name = 'suspicion_of_infection' then I.measurement_time else null::timestamptz end) as infection_onset
      from null_infections_d%(dataset_id)s I
    ) I
      on SO.window_id = I.window_id and SO.pat_id = I.pat_id
      and greatest(SO.sirs_onset, SO.org_df_onset, I.infection_onset)
            - least(SO.sirs_onset, SO.org_df_onset, I.infection_onset) < interval '6 hours'
  )
  select I.window_id,
         I.pat_id,
         coalesce(I.infection_cnt, 0) > 0 as suspicion_of_infection,

         (case
            when coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null
            then I.infection_onset
            else 'infinity'::timestamptz end
          ) as inf_onset,

         I.sirs1_name   as sirs1_name,
         I.sirs2_name   as sirs2_name,
         I.odf_name     as odf_name,
         I.sirs_initial as sirs_initial,
         I.sirs_onset   as sirs_onset,
         I.org_df_onset as org_df_onset

  from indexed_triples I
  where I.row = 1
  ;




create table severe_sepsis_onsets_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with severe_sepsis_onsets as (
    select sspm.window_id,
           sspm.pat_id,
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
      select stats.window_id,
             stats.pat_id,
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

      from severe_sepsis_candidates_d%(dataset_id)s stats
      group by stats.window_id, stats.pat_id
    ) sspm
  )

  select * from severe_sepsis_onsets
  ;




create table severe_sepsis_criteria_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  with indexed_criteria as (
    select
        CR.window_id,
        CR.pat_id,
        CR.name,
        CR.measurement_time,
        CR.value,
        coalesce(CR.is_met, false) as is_met,
        row_number() over (
          partition by CR.window_id, CR.pat_id, CR.name
          order by (case when coalesce(CR.is_met, false) then CR.measurement_time else null end) desc nulls last
        ) as row
    from (
      select * from all_sirs_org_d%(dataset_id)s
      union all select * from null_infections_d%(dataset_id)s
    ) CR
    left join severe_sepsis_candidates_d%(dataset_id)s CD
      on CR.window_id = CD.window_id and CR.pat_id = CD.pat_id
      and CR.name in ( CD.sirs1_name, CD.sirs2_name, CD.odf_name, 'suspicion_of_infection' )

    where ( coalesce(CD.sirs1_name, CD.sirs2_name, CD.odf_name) is null )
    or (    ( CD.sirs1_name is not null and CD.sirs_initial = CR.measurement_time )
         or ( CD.sirs2_name is not null and CD.sirs_onset   = CR.measurement_time )
         or ( CD.odf_name   is not null and CD.org_df_onset = CR.measurement_time )
         or ( CR.name = 'suspicion_of_infection' and (CD.inf_onset = 'infinity'::timestamptz or CR.measurement_time = CD.inf_onset) )
    )
  )
  select C.window_id, C.pat_id, C.name, C.measurement_time, C.value, C.is_met,
         getdate()::timestamptz as update_date
  from indexed_criteria C
  where C.row = 1
  ;
'''

severe_sepsis_template = '''
create table pat_cvalues_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval as window_id,
         meas.pat_id,
         cd.name,
         meas.fid,
         cd.category,
         meas.tsp,
         meas.value,
         cd.lower as d_lower,
         cd.upper as d_upper
  from criteria_default_flat as cd

  left join criteria_meas meas
      on meas.fid = cd.fid
      and cd.dataset_id = meas.dataset_id

  cross join cdm_window_offsets_15mins O

  where cd.dataset_id = %(dataset_id)s
  and meas.dataset_id = %(dataset_id)s
  and meas.value <> 'nan'
  and cd.name in (
    'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
    'respiratory_failure',
    'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
  )
  and meas.fid in (
    'temperature', 'heart_rate', 'resp_rate', 'wbc', 'bands', 'bp_sys', 'map', 'bilirubin', 'platelets', 'inr',
    'ptt', 'lactate', 'vent', 'bipap', 'cpap', 'creatinine'
  )
  and
  (case
    when cd.name = 'respiratory_failure' then meas.value is not null
    when cd.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and isnumeric(meas.value)
    then coalesce( not (
          meas.value::double precision
              between coalesce(case when cd.lower = '' then null else cd.lower end, meas.value)::double precision
              and coalesce(case when cd.upper = '' then null else cd.upper end, meas.value)::double precision
        ), false)

    when cd.category = 'decrease_in_sbp' or cd.category = 'urine_output' then true

    when isnumeric(meas.value)
    then coalesce( not (
          meas.value::double precision
              between coalesce(case when cd.lower = '' then null else cd.lower end, meas.value)::double precision
              and coalesce(case when cd.upper = '' then null else cd.upper end, meas.value)::double precision
        ), false)

    else false
   end)
  ;


create table pat_aggregates_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select aggs.window_id,
         aggs.pat_id,
         avg(aggs.bp_sys) as bp_sys,
         avg(aggs.weight) as weight,
         sum(aggs.urine_output) as urine_output
  from (
      select date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval as window_id,
             meas.pat_id,
             meas.tsp as measurement_time,
             (case when meas.fid = 'bp_sys' then meas.value::double precision else null end) as bp_sys,
             (case when meas.fid = 'weight' then meas.value::double precision else null end) as weight,
             (case when meas.fid = 'urine_output'
                   and date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval - meas.tsp < interval '2 hours'
                   then meas.value::double precision else null end
              ) as urine_output
      from criteria_meas meas
      cross join cdm_window_offsets_15mins O
      where meas.dataset_id = %(dataset_id)s
      and meas.fid in ('bp_sys', 'urine_output', 'weight')
      and isnumeric(meas.value)
  ) as aggs
  group by aggs.window_id, aggs.pat_id
  ;


create table all_sirs_org_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select  PC.window_id,
          PC.pat_id,
          PC.name,
          PC.tsp as measurement_time,
          (case
            when PC.name = 'respiratory_failure' then PC.fid || ': ' || PC.value
            else PC.value
           end) as value,
          (case
            when PC.name = 'respiratory_failure'
            then PC.value is not null

            when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and isnumeric(PC.value)
            then coalesce( not (
                  PC.value::double precision
                      between coalesce(case when PC.d_lower = '' then null else PC.d_lower end, PC.value)::double precision
                      and coalesce(case when PC.d_upper = '' then null else PC.d_upper end, PC.value)::double precision
                ), false)

            when PC.category = 'decrease_in_sbp' then
              (case
                when isnumeric(PC.value) and isnumeric(PC.d_upper)
                then coalesce( PAGG.bp_sys - PC.value::double precision > PC.d_upper::double precision, false)

                when isnumeric(PC.value)
                then coalesce( PAGG.bp_sys - PC.value::double precision > PC.value::double precision, false)

                else false
                end)

            when PC.category = 'urine_output' then
              coalesce( PAGG.urine_output / coalesce( PAGG.weight, POPMEANS.weight_popmean ) < 0.5, false)

            when isnumeric(PC.value)
            then coalesce( not (
                  PC.value::double precision
                      between coalesce(case when PC.d_lower = '' then null else PC.d_lower end, PC.value)::double precision
                      and coalesce(case when PC.d_upper = '' then null else PC.d_upper end, PC.value)::double precision
                ), false)

            else false
            end) as is_met
  from pat_cvalues_d%(dataset_id)s PC

  left join pat_aggregates_d%(dataset_id)s PAGG
    on PC.window_id = PAGG.window_id and PC.pat_id = PAGG.pat_id

  cross join (
    select value::double precision as weight_popmean
    from cdm_g where fid = 'weight_popmean' and dataset_id = %(dataset_id)s
  ) POPMEANS
  ;


create table all_sirs_org_triples_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  with sirs as (
    select * from all_sirs_org_d%(dataset_id)s S
    where S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
  ),
  org_df as (
    select * from all_sirs_org_d%(dataset_id)s S
    where S.name in (
      'respiratory_failure',
      'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
    )
    and S.is_met
  )
  select SO.window_id,
         SO.pat_id,
         SO.sirs1_name,
         SO.sirs2_name,
         SO.odf_name,
         SO.sirs_initial,
         SO.sirs_onset,
         SO.org_df_onset
  from (
    select S1.window_id,
           S1.pat_id,
           S1.name as sirs1_name,
           S2.name as sirs2_name,
           D.name as odf_name,
           S1.measurement_time as sirs_initial,
           S2.measurement_time as sirs_onset,
           D.measurement_time as org_df_onset
    from org_df D
    inner join sirs S1
      on D.window_id = S1.window_id and D.pat_id = S1.pat_id
    inner join sirs S2
      on S1.window_id = S2.window_id
      and S1.pat_id = S2.pat_id
      and S1.name <> S2.name
      and S1.measurement_time <= S2.measurement_time
  ) SO
  where not (SO.sirs_initial is null or SO.sirs_onset is null or SO.org_df_onset is null)
  and greatest(SO.sirs_onset, SO.org_df_onset)
        - least(SO.sirs_initial, SO.org_df_onset) < interval '6 hours' /*window_size*/
  ;



create table infections_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with notes_candidates as (
    select distinct T.window_id, T.pat_id from all_sirs_org_triples_d%(dataset_id)s T
  ),

  null_infections as (
    -- This is necessary for get_window_labels_from_criteria
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'suspicion_of_infection'::varchar as name,
           null::timestamptz                 as measurement_time,
           null::text                        as value,
           false                             as is_met
    from pat_partition_d%(dataset_id)s P
  ),

  cdm_matches as (
      -- TODO: we have picked an arbitrary time interval for notes. Refine.
      select NC.window_id                             as window_id,
             NC.pat_id                                as pat_id,
             'suspicion_of_infection'::text           as name,
             min(NTG.tsp)                             as measurement_time,
             listagg(NTG.note_id, ', ')               as value,
             true                                     as is_met
      from notes_candidates NC
      inner join (
        select M.dataset_id, M.pat_id, M.start_ts as tsp, M.note_id
        from cdm_processed_notes M
        where M.dataset_id = %(dataset_id)s
        and not ( ngrams1 = '[]' and ngrams2 = '[]' and ngrams3 = '[]' )
      ) NTG
        on NC.pat_id = NTG.pat_id
        and NTG.dataset_id = %(dataset_id)s
        and NTG.tsp between NC.window_id - interval '12 hours' and NC.window_id
      group by NC.window_id, NC.pat_id
  )

  select NI.window_id,
         NI.pat_id,
         coalesce(MTCH.name,             NI.name             ) as name,
         coalesce(MTCH.measurement_time, NI.measurement_time ) as measurement_time,
         coalesce(MTCH.value,            NI.value            ) as value,
         coalesce(MTCH.is_met,           NI.is_met           ) as is_met
  from null_infections NI
  left join cdm_matches MTCH on NI.window_id = MTCH.window_id and NI.pat_id = MTCH.pat_id
  ;



create table severe_sepsis_candidates_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with indexed_triples as (
    select SO.window_id,
           SO.pat_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset,
           I.infection_cnt,
           I.infection_onset,

           row_number() over (
              partition by SO.window_id, SO.pat_id
              order by
                (coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null) desc nulls last,
                greatest(SO.sirs_initial::timestamptz, SO.sirs_onset::timestamptz, SO.org_df_onset::timestamptz)::timestamptz nulls last
            ) as row

    from all_sirs_org_triples_d%(dataset_id)s SO
    left join (
      select I.window_id,
             I.pat_id,
             (case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             (case when I.name = 'suspicion_of_infection' then I.measurement_time else null::timestamptz end) as infection_onset
      from infections_d%(dataset_id)s I
    ) I
      on SO.window_id = I.window_id and SO.pat_id = I.pat_id
      and greatest(SO.sirs_onset, SO.org_df_onset, I.infection_onset)
            - least(SO.sirs_onset, SO.org_df_onset, I.infection_onset) < interval '6 hours'
  )
  select I.window_id,
         I.pat_id,
         coalesce(I.infection_cnt, 0) > 0 as suspicion_of_infection,

         (case
            when coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null
            then I.infection_onset
            else 'infinity'::timestamptz end
          ) as inf_onset,

         I.sirs1_name   as sirs1_name,
         I.sirs2_name   as sirs2_name,
         I.odf_name     as odf_name,
         I.sirs_initial as sirs_initial,
         I.sirs_onset   as sirs_onset,
         I.org_df_onset as org_df_onset

  from indexed_triples I
  where I.row = 1
  ;



create table severe_sepsis_onsets_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with severe_sepsis_onsets as (
    select sspm.window_id,
           sspm.pat_id,
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
      select stats.window_id,
             stats.pat_id,
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

      from severe_sepsis_candidates_d%(dataset_id)s stats
      group by stats.window_id, stats.pat_id
    ) sspm
  )

  select * from severe_sepsis_onsets
  ;




create table severe_sepsis_criteria_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  with indexed_criteria as (
    select
        CR.window_id,
        CR.pat_id,
        CR.name,
        CR.measurement_time,
        CR.value,
        coalesce(CR.is_met, false) as is_met,
        row_number() over (
          partition by CR.window_id, CR.pat_id, CR.name
          order by (case when coalesce(CR.is_met, false) then CR.measurement_time else null end) desc nulls last
        ) as row
    from (
      select * from all_sirs_org_d%(dataset_id)s
      union all select * from infections_d%(dataset_id)s
    ) CR
    left join severe_sepsis_candidates_d%(dataset_id)s CD
      on CR.window_id = CD.window_id and CR.pat_id = CD.pat_id
      and CR.name in ( CD.sirs1_name, CD.sirs2_name, CD.odf_name, 'suspicion_of_infection' )

    where ( coalesce(CD.sirs1_name, CD.sirs2_name, CD.odf_name) is null )
    or (    ( CD.sirs1_name is not null and CD.sirs_initial = CR.measurement_time )
         or ( CD.sirs2_name is not null and CD.sirs_onset   = CR.measurement_time )
         or ( CD.odf_name   is not null and CD.org_df_onset = CR.measurement_time )
         or ( CR.name = 'suspicion_of_infection' and (CD.inf_onset = 'infinity'::timestamptz or CR.measurement_time = CD.inf_onset) )
    )
  )
  select C.window_id, C.pat_id, C.name, C.measurement_time, C.value, C.is_met,
         getdate()::timestamptz as update_date
  from indexed_criteria C
  where C.row = 1
  ;
'''


severe_sepsis_output_template = '''
-----------------------------------------
-- Skip if we're processing septic shock.

create table severe_sepsis_outputs_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         null::timestamptz as septic_shock_onset
  from severe_sepsis_criteria_d%(dataset_id)s new_criteria
  left join severe_sepsis_onsets_d%(dataset_id)s SSP
    on new_criteria.window_id = SSP.window_id and new_criteria.pat_id = SSP.pat_id
  order by new_criteria.pat_id, new_criteria.window_id, new_criteria.name
  ;

create table cdm_labels_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  select sw.pat_id, sw.window_id as tsp, 'cms state' as label_type, sw.state as label from (
    select stats.window_id, stats.pat_id,
        (
          case
          when ssp_present and ssh_present then (
            case
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
            when stats.window_id - stats.septic_shock_onset  >= '6 hours'::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
            when stats.window_id - stats.septic_shock_onset  >= '6 hours'::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com

            else 30
            end
          )

          when ssp_present then (
            case
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 1 then 23 -- sev_sep_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com

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
      select OCR.window_id, OCR.pat_id,
          bool_or(severe_sepsis_wo_infection_onset is not null) as sspwoi_present,
          bool_or(severe_sepsis_onset is not null)              as ssp_present,
          bool_or(septic_shock_onset is not null)               as ssh_present,

          max(severe_sepsis_wo_infection_onset)                 as severe_sepsis_wo_infection_onset,
          max(severe_sepsis_onset)                              as severe_sepsis_onset,
          max(septic_shock_onset)                               as septic_shock_onset,

          sum(case when name = 'suspicion_of_infection' and is_met then 1 else 0 end)     as sus_count,
          sum(case when name = 'suspicion_of_infection' and not is_met then 1 else 0 end) as sus_noinf_count,
          sum(case when name = 'suspicion_of_infection' and not is_met then 1 else 0 end) as sus_null_count,

          sum(case when name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met then 1 else 0 end)
            as sirs_count,

          sum(case when name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp',
                                 'respiratory_failure','creatinine','bilirubin','platelet','inr','lactate')
                   and is_met
              then 1
              else 0
              end)
            as organ_count,

          sum(case when name in ('initial_lactate_order','blood_culture_order',
                                 'antibiotics_order', 'crystalloid_fluid_order')
                   and is_met
              then 1
              else 0
              end)
            as sev_sep_3hr_count,

          sum(case when name = 'repeat_lactate_order' and is_met then 1 else 0 end)
            as sev_sep_6hr_count,

          sum(case when name = 'vasopressors_order' and is_met then 1 else 0 end)
            as sep_sho_6hr_count

      from severe_sepsis_outputs_d%(dataset_id)s OCR
      where OCR.pat_id = coalesce(null/*this_pat_id*/, OCR.pat_id)
      group by OCR.window_id, OCR.pat_id
    ) stats
  ) sw
  ;

-------------------------------------------
-- End of severe sepsis labeling
-------------------------------------------
'''



######################################
## Septic shock
##
## - Requires a <pfx>pat_partition_d<id> table to be available.

septic_shock_template = '''
create table %(prefix)spat_cvalues_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval as window_id,
         meas.pat_id,
         cd.name,
         meas.fid,
         cd.category,
         meas.tsp,
         meas.value,
         cd.lower as d_lower,
         cd.upper as d_upper
  from criteria_default_flat as cd

  left join criteria_meas meas
      on meas.fid = cd.fid
      and cd.dataset_id = meas.dataset_id

  cross join cdm_window_offsets_15mins O

  where cd.dataset_id = %(dataset_id)s
  and meas.dataset_id = %(dataset_id)s
  and meas.value <> 'nan'
  and cd.name in (
    'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
    'respiratory_failure',
    'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate',
    'systolic_bp', 'hypotension_map', 'hypotension_dsbp',
    'repeat_lactate_order'
  )
  ;


create table %(prefix)spat_aggregates_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select aggs.window_id,
         aggs.pat_id,
         avg(aggs.bp_sys) as bp_sys,
         avg(aggs.weight) as weight,
         sum(aggs.urine_output) as urine_output
  from (
      select date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval as window_id,
             meas.pat_id,
             meas.tsp as measurement_time,
             (case when meas.fid = 'bp_sys' then meas.value::double precision else null end) as bp_sys,
             (case when meas.fid = 'weight' then meas.value::double precision else null end) as weight,
             (case when meas.fid = 'urine_output'
                   and date_trunc('hour', meas.tsp) + (O.window_offset || ' minutes')::interval - meas.tsp < interval '2 hours'
                   then meas.value::double precision else null end
              ) as urine_output
      from criteria_meas meas
      cross join cdm_window_offsets_15mins O
      where meas.dataset_id = %(dataset_id)s
      and meas.fid in ('bp_sys', 'urine_output', 'weight')
      and isnumeric(meas.value)
  ) as aggs
  group by aggs.window_id, aggs.pat_id
  ;


create table %(prefix)sall_sirs_org_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select  PC.window_id,
          PC.pat_id,
          PC.name,
          PC.tsp as measurement_time,
          (case
            when PC.name = 'respiratory_failure' then PC.fid || ': ' || PC.value
            else PC.value
           end) as value,
          (case
            when PC.name = 'respiratory_failure'
            then PC.value is not null

            when PC.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and isnumeric(PC.value)
            then coalesce( not (
                  PC.value::double precision
                      between coalesce(case when PC.d_lower = '' then null else PC.d_lower end, PC.value)::double precision
                      and coalesce(case when PC.d_upper = '' then null else PC.d_upper end, PC.value)::double precision
                ), false)

            when PC.category = 'decrease_in_sbp' then
              (case
                when isnumeric(PC.value) and isnumeric(PC.d_upper)
                then coalesce( PAGG.bp_sys - PC.value::double precision > PC.d_upper::double precision, false)

                when isnumeric(PC.value)
                then coalesce( PAGG.bp_sys - PC.value::double precision > PC.value::double precision, false)

                else false
                end)

            when PC.category = 'urine_output' then
              coalesce( PAGG.urine_output / coalesce( PAGG.weight, POPMEANS.weight_popmean ) < 0.5, false)

            when isnumeric(PC.value)
            then coalesce( not (
                  PC.value::double precision
                      between coalesce(case when PC.d_lower = '' then null else PC.d_lower end, PC.value)::double precision
                      and coalesce(case when PC.d_upper = '' then null else PC.d_upper end, PC.value)::double precision
                ), false)

            else false
            end) as is_met
  from %(prefix)spat_cvalues_d%(dataset_id)s PC

  left join %(prefix)spat_aggregates_d%(dataset_id)s PAGG
    on PC.window_id = PAGG.window_id and PC.pat_id = PAGG.pat_id

  cross join (
    select value::double precision as weight_popmean
    from cdm_g where fid = 'weight_popmean' and dataset_id = %(dataset_id)s
  ) POPMEANS

  where PC.name in (
    'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
    'respiratory_failure',
    'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
  )
  ;


create table %(prefix)sall_sirs_org_triples_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  with sirs as (
    select * from %(prefix)sall_sirs_org_d%(dataset_id)s S
    where S.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and S.is_met
  ),
  org_df as (
    select * from %(prefix)sall_sirs_org_d%(dataset_id)s S
    where S.name in (
      'respiratory_failure',
      'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
    )
    and S.is_met
  )
  select SO.window_id,
         SO.pat_id,
         SO.sirs1_name,
         SO.sirs2_name,
         SO.odf_name,
         SO.sirs_initial,
         SO.sirs_onset,
         SO.org_df_onset
  from (
    select S1.window_id,
           S1.pat_id,
           S1.name as sirs1_name,
           S2.name as sirs2_name,
           D.name as odf_name,
           S1.measurement_time as sirs_initial,
           S2.measurement_time as sirs_onset,
           D.measurement_time as org_df_onset
    from org_df D
    inner join sirs S1
      on D.window_id = S1.window_id and D.pat_id = S1.pat_id
    inner join sirs S2
      on S1.window_id = S2.window_id
      and S1.pat_id = S2.pat_id
      and S1.name <> S2.name
      and S1.measurement_time <= S2.measurement_time
  ) SO
  where not (SO.sirs_initial is null or SO.sirs_onset is null or SO.org_df_onset is null)
  and greatest(SO.sirs_onset, SO.org_df_onset)
        - least(SO.sirs_initial, SO.org_df_onset) < interval '6 hours' /*window_size*/
  ;



create table %(prefix)sinfections_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with notes_candidates as (
    select distinct T.window_id, T.pat_id from %(prefix)sall_sirs_org_triples_d%(dataset_id)s T
  ),

  null_infections as (
    -- This is necessary for get_window_labels_from_criteria
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'suspicion_of_infection'::varchar as name,
           null::timestamptz                 as measurement_time,
           null::text                        as value,
           false                             as is_met
    from %(prefix)spat_partition_d%(dataset_id)s P
  ),

  cdm_matches as (
      -- TODO: we have picked an arbitrary time interval for notes. Refine.
      select NC.window_id                             as window_id,
             NC.pat_id                                as pat_id,
             'suspicion_of_infection'::text           as name,
             min(NTG.tsp)                             as measurement_time,
             listagg(NTG.note_id, ', ')               as value,
             true                                     as is_met
      from notes_candidates NC
      inner join (
        select M.dataset_id, M.pat_id, M.start_ts as tsp, M.note_id
        from cdm_processed_notes M
        where M.dataset_id = %(dataset_id)s
        and not ( ngrams1 = '[]' and ngrams2 = '[]' and ngrams3 = '[]' )
      ) NTG
        on NC.pat_id = NTG.pat_id
        and NTG.dataset_id = %(dataset_id)s
        and NTG.tsp between NC.window_id - interval '12 hours' and NC.window_id
      group by NC.window_id, NC.pat_id
  )

  select NI.window_id,
         NI.pat_id,
         coalesce(MTCH.name,             NI.name             ) as name,
         coalesce(MTCH.measurement_time, NI.measurement_time ) as measurement_time,
         coalesce(MTCH.value,            NI.value            ) as value,
         coalesce(MTCH.is_met,           NI.is_met           ) as is_met
  from null_infections NI
  left join cdm_matches MTCH on NI.window_id = MTCH.window_id and NI.pat_id = MTCH.pat_id
  ;



create table %(prefix)ssevere_sepsis_candidates_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with indexed_triples as (
    select SO.window_id,
           SO.pat_id,
           SO.sirs1_name,
           SO.sirs2_name,
           SO.odf_name,
           SO.sirs_initial,
           SO.sirs_onset,
           SO.org_df_onset,
           I.infection_cnt,
           I.infection_onset,

           row_number() over (
              partition by SO.window_id, SO.pat_id
              order by
                (coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null) desc nulls last,
                greatest(SO.sirs_initial::timestamptz, SO.sirs_onset::timestamptz, SO.org_df_onset::timestamptz)::timestamptz nulls last
            ) as row

    from %(prefix)sall_sirs_org_triples_d%(dataset_id)s SO
    left join (
      select I.window_id,
             I.pat_id,
             (case when I.name = 'suspicion_of_infection' and I.is_met then 1 else 0 end) as infection_cnt,
             (case when I.name = 'suspicion_of_infection' then I.measurement_time else null::timestamptz end) as infection_onset
      from %(prefix)sinfections_d%(dataset_id)s I
    ) I
      on SO.window_id = I.window_id and SO.pat_id = I.pat_id
      and greatest(SO.sirs_onset, SO.org_df_onset, I.infection_onset)
            - least(SO.sirs_onset, SO.org_df_onset, I.infection_onset) < interval '6 hours'
  )
  select I.window_id,
         I.pat_id,
         coalesce(I.infection_cnt, 0) > 0 as suspicion_of_infection,

         (case
            when coalesce(I.infection_cnt, 0) > 0 and I.infection_onset is not null
            then I.infection_onset
            else 'infinity'::timestamptz end
          ) as inf_onset,

         I.sirs1_name   as sirs1_name,
         I.sirs2_name   as sirs2_name,
         I.odf_name     as odf_name,
         I.sirs_initial as sirs_initial,
         I.sirs_onset   as sirs_onset,
         I.org_df_onset as org_df_onset

  from indexed_triples I
  where I.row = 1
  ;



create table %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with severe_sepsis_onsets as (
    select sspm.window_id,
           sspm.pat_id,
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
      select stats.window_id,
             stats.pat_id,
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

      from %(prefix)ssevere_sepsis_candidates_d%(dataset_id)s stats
      group by stats.window_id, stats.pat_id
    ) sspm
  )

  select * from severe_sepsis_onsets
  ;




create table %(prefix)ssevere_sepsis_criteria_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  with indexed_criteria as (
    select
        CR.window_id,
        CR.pat_id,
        CR.name,
        CR.measurement_time,
        CR.value,
        coalesce(CR.is_met, false) as is_met,
        row_number() over (
          partition by CR.window_id, CR.pat_id, CR.name
          order by (case when coalesce(CR.is_met, false) then CR.measurement_time else null end) desc nulls last
        ) as row
    from (
      select * from %(prefix)sall_sirs_org_d%(dataset_id)s
      union all select * from %(prefix)sinfections_d%(dataset_id)s
    ) CR
    left join %(prefix)ssevere_sepsis_candidates_d%(dataset_id)s CD
      on CR.window_id = CD.window_id and CR.pat_id = CD.pat_id
      and CR.name in ( CD.sirs1_name, CD.sirs2_name, CD.odf_name, 'suspicion_of_infection' )

    where ( coalesce(CD.sirs1_name, CD.sirs2_name, CD.odf_name) is null )
    or (    ( CD.sirs1_name is not null and CD.sirs_initial = CR.measurement_time )
         or ( CD.sirs2_name is not null and CD.sirs_onset   = CR.measurement_time )
         or ( CD.odf_name   is not null and CD.org_df_onset = CR.measurement_time )
         or ( CR.name = 'suspicion_of_infection' and (CD.inf_onset = 'infinity'::timestamptz or CR.measurement_time = CD.inf_onset) )
    )
  )
  select C.window_id, C.pat_id, C.name, C.measurement_time, C.value, C.is_met,
         getdate()::timestamptz as update_date
  from indexed_criteria C
  where C.row = 1
  ;



create table %(prefix)scrystalloid_fluid_and_hypoperfusion_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with null_cf_and_hpf as (
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'crystalloid_fluid'::varchar      as name,
           null::timestamptz                 as measurement_time,
           null::varchar                     as value,
           false                             as is_met
    from %(prefix)spat_partition_d%(dataset_id)s P
    union all
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'initial_lactate'::varchar        as name,
           null::timestamptz                 as measurement_time,
           null::varchar                     as value,
           false                             as is_met
    from %(prefix)spat_partition_d%(dataset_id)s P
  ),

  cf_and_hpf_cvalues as (
    select SSP.window_id,
           SSP.pat_id,
           cd.name,
           meas.fid,
           cd.category,
           meas.tsp,
           meas.value,
           cd.override_value as d_ovalue,
           SSP.severe_sepsis_is_met,
           SSP.severe_sepsis_onset
    from %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSP
    cross join criteria_default as cd
    left join criteria_meas meas
        on SSP.pat_id = meas.pat_id
        and meas.fid = cd.fid
        and cd.dataset_id = meas.dataset_id
        and (meas.tsp is null
              or (case when cd.name = 'initial_lactate_order' then meas.value <> 'nan' and json_extract_path_text(meas.value, 'result_tsp') <> 'NaT' and json_extract_path_text(meas.value, 'result_tsp')::timestamptz between SSP.severe_sepsis_onset - interval '6 hours'/*initial_lactate_lookback*/ and SSP.window_id else false end)
              or (case when cd.name <> 'initial_lactate_order' then meas.tsp between SSP.severe_sepsis_onset - interval '6 hours'/*orders_lookback*/ and SSP.window_id else false end)
        )
    where cd.dataset_id = %(dataset_id)s
    and meas.dataset_id = %(dataset_id)s
    and cd.name in ( 'crystalloid_fluid', 'initial_lactate' )
    and meas.fid in ( 'crystalloid_fluid', 'lactate' )
    and isnumeric(meas.value)
    and meas.value <> 'nan'
    and SSP.severe_sepsis_is_met
  ),

  cfhfp as (
    select  PC.window_id,
            PC.pat_id,
            PC.name,
            PC.tsp as measurement_time,
            PC.value as value,
            (case
              when PC.name = 'initial_lactate'
              then criteria_value_met(PC.value, PC.d_ovalue)
              else PC.fid = 'crystalloid_fluid' and criteria_value_met(PC.value, PC.d_ovalue)
              end
            ) and (PC.severe_sepsis_is_met and PC.tsp
                    >= (case
                          when PC.name = 'initial_lactate'
                          then (PC.severe_sepsis_onset - interval '6 hours'/*initial_lactate_lookback*/)
                          else (PC.severe_sepsis_onset - interval '6 hours'/*orders_lookback*/) end))
            as is_met
    from cf_and_hpf_cvalues PC
    where PC.name in ( 'crystalloid_fluid', 'initial_lactate' )
  ),

  indexed_cfhfp as (
    select PC.window_id,
           PC.pat_id,
           PC.name,
           (case when PC.is_met then PC.measurement_time else null end) as measurement_time,
           (case when PC.is_met then PC.value else null end) as value,
           PC.is_met,
           row_number() over (
             partition by PC.window_id, PC.pat_id, PC.name
             order by PC.is_met desc, PC.measurement_time nulls last
           ) as row
    from cfhfp PC
  )

  select NC.window_id,
         NC.pat_id,
         coalesce(C.name,             NC.name             ) as name,
         coalesce(C.measurement_time, NC.measurement_time ) as measurement_time,
         coalesce(C.value,            NC.value            ) as value,
         coalesce(C.is_met,           NC.is_met           ) as is_met,
         getdate()::timestamptz                             as update_date
  from null_cf_and_hpf NC
  left join indexed_cfhfp C
    on NC.window_id = C.window_id and NC.pat_id = C.pat_id and C.row = 1
  ;




create table %(prefix)shypotension_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with null_hypotension as (
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'systolic_bp'::varchar            as name,
           null::timestamptz                 as measurement_time,
           null::varchar                     as value,
           false                             as is_met
    from %(prefix)spat_partition_d%(dataset_id)s P
    union all
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'hypotension_map'::varchar        as name,
           null::timestamptz                 as measurement_time,
           null::varchar                     as value,
           false                             as is_met
    from %(prefix)spat_partition_d%(dataset_id)s P
    union all
    select P.tsp                             as window_id,
           P.pat_id                          as pat_id,
           'hypotension_dsbp'::varchar       as name,
           null::timestamptz                 as measurement_time,
           null::varchar                     as value,
           false                             as is_met
    from %(prefix)spat_partition_d%(dataset_id)s P
  ),

  -- TODO: could be optimized? The domain here is for every pat_id given
  -- the LHS of the join is criteria_meas
  pats_fluid_after_severe_sepsis as (
    select  SSPN.window_id,
            SSPN.pat_id,
            MFL.tsp,
            sum(MFL.value::numeric) as total_fluid,
            -- Fluids are met if they are overriden or if we have more than
            -- min(1.2L, 30 mL * weight) administered from 6 hours before severe sepsis
            coalesce(sum(MFL.value::numeric), 0) > least(1200, 30 * max(PW.weight)) as is_met

    from %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSPN

    left join %(prefix)spat_aggregates_d%(dataset_id)s PW
      on SSPN.window_id = PW.window_id and SSPN.pat_id = PW.pat_id

    left join criteria_meas MFL
      on SSPN.pat_id = MFL.pat_id
      and MFL.tsp between (SSPN.severe_sepsis_onset - interval '6 hours'/*orders_lookback*/) and SSPN.window_id

    where SSPN.severe_sepsis_is_met
    and MFL.dataset_id = %(dataset_id)s
    and isnumeric(MFL.value) and MFL.value <> 'nan'
    and MFL.fid = 'crystalloid_fluid'
    group by SSPN.window_id, SSPN.pat_id, MFL.tsp
  ),

  hypotension as (
    select PC.window_id,
           PC.pat_id,
           PC.name,
           PC.tsp as measurement_time,
           PC.value as value,
           (SSPN.severe_sepsis_is_met and PC.tsp >= SSPN.severe_sepsis_onset)
           and
           (case when PC.category = 'hypotension' then
                   (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                   and criteria_value_met(PC.value, PC.d_ovalue)
                   and criteria_value_met(NEXT.value, PC.d_ovalue)

                 when PC.category = 'hypotension_dsbp' then
                   (PFL.is_met and PFL.tsp < PC.tsp and NEXT.tsp < PFL.tsp + interval '1 hour')
                   and decrease_in_sbp_met(PBPSYS.bp_sys, PC.value, PC.d_ovalue)
                   and decrease_in_sbp_met(PBPSYS.bp_sys, NEXT.value, PC.d_ovalue)

                else false
            end) as is_met

    from %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSPN

    inner join %(prefix)spat_cvalues_d%(dataset_id)s PC
      on SSPN.window_id = PC.window_id and SSPN.pat_id = PC.pat_id and SSPN.severe_sepsis_onset <= PC.tsp

    left join %(prefix)spat_aggregates_d%(dataset_id)s PBPSYS
      on SSPN.window_id = PBPSYS.window_id and SSPN.pat_id = PBPSYS.pat_id

    left join pats_fluid_after_severe_sepsis PFL
      on SSPN.window_id = PFL.window_id and SSPN.pat_id = PFL.pat_id and PFL.tsp < PC.tsp

    -- TODO: LIMIT 1? Instead we use upper bound of interval '1 hour'
    left join criteria_meas NEXT
      on SSPN.pat_id = NEXT.pat_id and PC.fid = NEXT.fid
      and NEXT.tsp between PC.tsp and PC.window_id + interval '1 hour'

    where SSPN.severe_sepsis_is_met
    and PC.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
    and PFL.is_met
    and NEXT.dataset_id = %(dataset_id)s
  ),

  indexed_hypotension as (
    select PC.window_id,
           PC.pat_id,
           PC.name,
           (case when PC.is_met then PC.measurement_time else null end) as measurement_time,
           (case when PC.is_met then PC.value else null end) as value,
           PC.is_met,
           row_number() over (
             partition by PC.window_id, PC.pat_id, PC.name
             order by PC.is_met desc, PC.measurement_time nulls last
           ) as row
    from hypotension PC
  )

  select NC.window_id,
         NC.pat_id,
         coalesce(C.name,             NC.name             ) as name,
         coalesce(C.measurement_time, NC.measurement_time ) as measurement_time,
         coalesce(C.value,            NC.value            ) as value,
         coalesce(C.is_met,           NC.is_met           ) as is_met,
         getdate()::timestamptz                             as update_date
  from null_hypotension NC
  left join indexed_hypotension C
    on NC.window_id = C.window_id and NC.pat_id = C.pat_id and C.row = 1
  ;




create table %(prefix)sseptic_shock_onsets_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with septic_shock_onsets as (
    select stats.window_id,
           stats.pat_id,
           bool_or(stats.cnt > 0) as septic_shock_is_met,
           (case
              when not(bool_or(stats.cnt > 0)) then null
              else greatest(min(stats.onset), max(SSP.severe_sepsis_onset))
              end
            ) as septic_shock_onset
    from (
        -- Hypotension and hypoperfusion subqueries individually check
        -- that they occur after severe sepsis onset.
        (select H.window_id,
                H.pat_id,
                sum(case when H.is_met then 1 else 0 end) as cnt,
                min(H.measurement_time) as onset
         from %(prefix)shypotension_d%(dataset_id)s as H
         group by H.window_id, H.pat_id)
        union all
        (select HPF.window_id,
                HPF.pat_id,
                sum(case when HPF.is_met then 1 else 0 end) as cnt,
                min(HPF.measurement_time) as onset
         from %(prefix)scrystalloid_fluid_and_hypoperfusion_d%(dataset_id)s HPF
         where HPF.name = 'initial_lactate'
         group by HPF.window_id, HPF.pat_id)
    ) stats

    left join %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSP
      on stats.window_id = SSP.window_id and stats.pat_id = SSP.pat_id

    group by stats.window_id, stats.pat_id
  )
  select * from septic_shock_onsets
  ;




create table %(prefix)sorders_criteria_d%(dataset_id)s diststyle all sortkey(window_id, pat_id) as
  with orders_criteria as (
    -- Unlike previous labeling functions, this pulls out all orders fresh
    -- from criteria_meas, from their respective lookbacks to the end of the window.
    with orders_cvalues as (
      select PPRT.tsp as window_id,
             PPRT.pat_id,
             cd.name,
             meas.fid,
             cd.category,
             (case when cd.name = 'blood_culture_order' then json_extract_path_text(meas.value, 'collect_tsp')::timestamptz
                   when cd.name = 'initial_lactate_order' then json_extract_path_text(meas.value, 'result_tsp')::timestamptz
                   else meas.tsp
              end) as tsp,
             (case when cd.name = 'blood_culture_order' then json_extract_path_text(meas.value, 'status')
                   when cd.name = 'initial_lactate_order' then json_extract_path_text(meas.value, 'status')
                   when cd.name = 'vasopressors_order' and meas.value like '%%-%%' then regexp_substr(meas.value, '[0-9\.]*')
                   else meas.value
              end) as value,
             cd.override_value as d_ovalue
      from %(prefix)spat_partition_d%(dataset_id)s PPRT
      cross join criteria_default as cd
      left join %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSP
        on PPRT.tsp = SSP.window_id and PPRT.pat_id = SSP.pat_id
      left join criteria_meas meas
          on PPRT.pat_id = meas.pat_id
          and meas.fid = cd.fid
          and cd.dataset_id = meas.dataset_id
          and (meas.tsp is null
                or (case when cd.name = 'blood_culture_order'     then meas.value <> 'nan' and json_extract_path_text(meas.value, 'collect_tsp') <> 'NaT' and json_extract_path_text(meas.value, 'collect_tsp')::timestamptz between SSP.severe_sepsis_onset - interval '48 hours' /*blood_culture_order_lookback*/   and SSP.window_id else false end)
                or (case when cd.name = 'initial_lactate_order'   then meas.value <> 'nan' and json_extract_path_text(meas.value, 'result_tsp')  <> 'NaT' and json_extract_path_text(meas.value, 'result_tsp')::timestamptz  between SSP.severe_sepsis_onset - interval '6 hours' /*initial_lactate_lookback*/        and SSP.window_id else false end)
                or (cd.name = 'antibiotics_order'       and meas.tsp between SSP.severe_sepsis_onset - interval '24 hours' /*antibiotics_order_lookback*/     and SSP.window_id)
                or (cd.name = 'crystalloid_fluid_order' and meas.tsp between SSP.severe_sepsis_onset - interval '6 hours'  /*orders_lookback*/                and SSP.window_id)
                or (case
                    when cd.name not in ('blood_culture_order', 'initial_lactate_order', 'antibiotics_order', 'crystalloid_fluid_order')
                    then meas.tsp between SSP.severe_sepsis_onset and SSP.window_id
                    else false end) -- NOTE: this lookback is weaker than CMS criteria.
          )
      where cd.dataset_id = %(dataset_id)s
      and meas.dataset_id = %(dataset_id)s
      and meas.value <> 'nan'
      and cd.name in (
        'initial_lactate_order',
        'blood_culture_order',
        'antibiotics_order',
        'crystalloid_fluid_order',
        'vasopressors_order'
      )
      and meas.fid in (
        'lactate_order',
        'blood_culture_order',
        'cms_antibiotics_order',
        'cms_antibiotics',
        'crystalloid_fluid_order',
        'crystalloid_fluid',
        'lactate_order',
        'vasopressors_dose_order',
        'vasopressors_dose'
      )
    ),

    indexed_orders as (
      select  CV.window_id,
              CV.pat_id,
              CV.name,
              CV.tsp as measurement_time,
              (case when CV.category in ('after_severe_sepsis_dose', 'after_septic_shock_dose')
                      then dose_order_status(CV.fid)
                    else order_status(CV.fid, CV.value)
               end) as value,

              (case
                  when CV.category = 'after_severe_sepsis' then
                    coalesce(SSP.severe_sepsis_is_met
                              and CV.tsp > (case
                                              when CV.name = 'blood_culture_order'
                                              then SSP.severe_sepsis_onset - interval '48 hours' /*blood_culture_order_lookback*/
                                              when CV.name = 'initial_lactate_order'
                                              then SSP.severe_sepsis_onset - interval '6 hours'  /*initial_lactate_lookback*/
                                              else SSP.severe_sepsis_onset
                                              end)
                             , false
                    )
                    and ( order_met(CV.name, CV.value) )

                  when CV.category = 'after_severe_sepsis_dose' then
                    coalesce(SSP.severe_sepsis_is_met
                              and CV.tsp > (case
                                              when CV.name = 'antibiotics_order'
                                              then SSP.severe_sepsis_onset - interval '48 hours' /*antibiotics_order_lookback*/
                                              when CV.name = 'crystalloid_fluid_order'
                                              then SSP.severe_sepsis_onset - interval '6 hours'  /*orders_lookback*/
                                              else SSP.severe_sepsis_onset
                                              end)
                             , false
                    )
                    and ( dose_order_met(CV.fid,
                                         (case when isnumeric(CV.value) then CV.value::numeric else null::numeric end),
                                         (json_extract_path_text(CV.d_ovalue, 'lower')::numeric)) )

                  when CV.category = 'after_septic_shock' then
                    coalesce(SSH.septic_shock_is_met and CV.tsp > SSH.septic_shock_onset, false)
                    and ( order_met(CV.name, CV.value) )

                  when CV.category = 'after_septic_shock_dose' then
                    coalesce(SSH.septic_shock_is_met and CV.tsp > SSH.septic_shock_onset, false)
                    and ( dose_order_met(CV.fid,
                                         (case when isnumeric(CV.value) then CV.value::numeric else null::numeric end),
                                         (json_extract_path_text(CV.d_ovalue, 'lower')::numeric)) )

                  else criteria_value_met(CV.value, CV.d_ovalue)
                  end
              ) as is_met,

              row_number() over (
                partition by CV.window_id, CV.pat_id, CV.name
                order by CV.tsp nulls last
              ) as row

      from orders_cvalues CV

      left join %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSP
        on CV.window_id = SSP.window_id and CV.pat_id = SSP.pat_id

      left join %(prefix)sseptic_shock_onsets_d%(dataset_id)s SSH
        on CV.window_id = SSH.window_id and CV.pat_id = SSH.pat_id
    )
    select C.*, getdate()::timestamptz as update_date
    from indexed_orders C
    where C.row = 1
  ),

  repeat_lactate as (
    with indexed_orders as (
      select  CV.window_id,
              CV.pat_id,
              CV.name,
              json_extract_path_text(CV.value, 'result_tsp')::timestamptz as measurement_time,
              order_status(CV.fid, json_extract_path_text(CV.value, 'status')) as value,
              ((
                coalesce(initial_lactate_order.is_met and lactate_results.is_met, false)
                  and order_met(CV.name, json_extract_path_text(CV.value, 'status'))
                  and (coalesce(json_extract_path_text(CV.value, 'result_tsp')::timestamptz > initial_lactate_order.tsp, false)
                          and coalesce(lactate_results.tsp > initial_lactate_order.tsp, false))
              ) or
              (
                not( coalesce(initial_lactate_order.is_completed
                                and ( lactate_results.is_met or
                                      json_extract_path_text(CV.value, 'result_tsp')::timestamptz <= initial_lactate_order.tsp )
                              , false) )
              )) is_met,

              row_number() over (
                partition by CV.window_id, CV.pat_id, CV.name
                order by CV.tsp nulls last
              ) as row

      from %(prefix)spat_cvalues_d%(dataset_id)s CV

      left join (
          select oc.window_id,
                 oc.pat_id,
                 max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                 coalesce(bool_or(oc.is_met), false) as is_met,
                 coalesce(min(oc.value) = 'Completed', false) as is_completed
          from orders_criteria oc
          where oc.name = 'initial_lactate_order'
          group by oc.window_id, oc.pat_id
      ) initial_lactate_order
      on CV.window_id = initial_lactate_order.window_id and CV.pat_id = initial_lactate_order.pat_id

      left join (
          select p3.window_id,
                 p3.pat_id,
                 max(case when p3.value::numeric > 2.0 then p3.tsp else null end) tsp,
                 coalesce(bool_or(p3.value::numeric > 2.0), false) is_met
          from %(prefix)spat_cvalues_d%(dataset_id)s p3
          where p3.name = 'initial_lactate'
          group by p3.window_id, p3.pat_id
      ) lactate_results
      on CV.window_id = lactate_results.pat_id and CV.pat_id = lactate_results.pat_id

      where CV.name = 'repeat_lactate_order'
      and   json_extract_path_text(CV.value, 'result_tsp') <> 'NaT'
    )
    select C.*, getdate()::timestamptz as update_date
    from indexed_orders C
    where C.row = 1
  )

  select O.window_id, O.pat_id, O.name, O.measurement_time, O.value, O.is_met, O.update_date from orders_criteria O
  union all select R.window_id, R.pat_id, R.name, R.measurement_time, R.value, R.is_met, R.update_date from repeat_lactate R
  ;




create table %(prefix)sseptic_shock_outputs_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  select new_criteria.*,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         SSH.septic_shock_onset
  from (
      select * from %(prefix)ssevere_sepsis_criteria_d%(dataset_id)s SSP
      union all select * from %(prefix)scrystalloid_fluid_and_hypoperfusion_d%(dataset_id)s
      union all select * from %(prefix)shypotension_d%(dataset_id)s
      union all select * from %(prefix)sorders_criteria_d%(dataset_id)s
  ) new_criteria

  left join %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s SSP
    on new_criteria.window_id = SSP.window_id and new_criteria.pat_id = SSP.pat_id

  left join %(prefix)sseptic_shock_onsets_d%(dataset_id)s SSH
    on new_criteria.window_id = SSP.window_id and new_criteria.pat_id = SSH.pat_id;
  ;



create table %(prefix)scdm_shock_labels_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  select sw.pat_id, sw.window_id as tsp, 'cms state' as label_type, sw.state as label from (
    select stats.window_id, stats.pat_id,
        (
          case
          when ssp_present and ssh_present then (
            case
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
            when stats.window_id - stats.septic_shock_onset  >= '6 hours'::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
            when stats.window_id - stats.septic_shock_onset  >= '6 hours'::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com

            else 30
            end
          )

          when ssp_present then (
            case
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 1 then 23 -- sev_sep_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com

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
      select OCR.window_id, OCR.pat_id,
          bool_or(severe_sepsis_wo_infection_onset is not null) as sspwoi_present,
          bool_or(severe_sepsis_onset is not null)              as ssp_present,
          bool_or(septic_shock_onset is not null)               as ssh_present,

          max(severe_sepsis_wo_infection_onset)                 as severe_sepsis_wo_infection_onset,
          max(severe_sepsis_onset)                              as severe_sepsis_onset,
          max(septic_shock_onset)                               as septic_shock_onset,

          sum(case when name = 'suspicion_of_infection' and is_met then 1 else 0 end)     as sus_count,
          sum(case when name = 'suspicion_of_infection' and not is_met then 1 else 0 end) as sus_noinf_count,
          sum(case when name = 'suspicion_of_infection' and not is_met then 1 else 0 end) as sus_null_count,

          sum(case when name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met then 1 else 0 end)
            as sirs_count,

          sum(case when name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp',
                                 'respiratory_failure','creatinine','bilirubin','platelet','inr','lactate')
                   and is_met
              then 1
              else 0
              end)
            as organ_count,

          sum(case when name in ('initial_lactate_order','blood_culture_order',
                                 'antibiotics_order', 'crystalloid_fluid_order')
                   and is_met
              then 1
              else 0
              end)
            as sev_sep_3hr_count,

          sum(case when name = 'repeat_lactate_order' and is_met then 1 else 0 end)
            as sev_sep_6hr_count,

          sum(case when name = 'vasopressors_order' and is_met then 1 else 0 end)
            as sep_sho_6hr_count

      from %(prefix)sseptic_shock_outputs_d%(dataset_id)s OCR
      where OCR.pat_id = coalesce(null/*this_pat_id*/, OCR.pat_id)
      group by OCR.window_id, OCR.pat_id
    ) stats
  ) sw
  ;

-------------------------------------------
-- End of septic shock labeling
-------------------------------------------
'''


severe_sepsis_bundle_label_template = '''
'''

septic_shock_bundle_label_template = '''

-- Earliest occurrence of each state
create table earliest_occurrences_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(pat_id) as
  select L.pat_id, L.label, min(L.tsp) as tsp
  from cdm_labels_d%(dataset_id)s L
  where L.label_type = 'cms state'
  group by L.pat_id, L.label
  ;

create table onset_times_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(pat_id) as
  select I.pat_id,

         -- Onset times
         least(min(L10.sspwoi), min(L20.sspwoi), min(L30.sspwoi)) as severe_sepsis_wo_infection_onset,
         least(min(L20.ssp), min(L30.ssp)) as severe_sepsis_onset,
         min(L30.ssh) as septic_shock_onset,

         -- Window ends used to calculate onset times
         least(min(L10.w_sspwoi), min(L20.w_ssp), min(L30.w_ssh)) as w_severe_sepsis_wo_infection_onset,
         least(min(L20.w_ssp), min(L30.w_ssh)) as w_severe_sepsis_onset,
         min(L30.w_ssh) as w_septic_shock_onset
  from
  ( select distinct I.pat_id from earliest_occurrences_d%(dataset_id)s I ) I

  left join
  (
    -- Earliest occurrence of sspwoi.
    select WL10.pat_id,
           min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
           min(WL10.tsp) as w_sspwoi
    from (
      select L10.pat_id, min(L10.tsp) as tsp
      from earliest_occurrences_d%(dataset_id)s L10
      where L10.label >= 10 and L10.label < 20
      group by L10.pat_id
    ) WL10
    -- Retrieve exact timestamp of onset within the window.
    inner join septic_shock_outputs_d%(dataset_id)s LWindow
      on WL10.pat_id = LWindow.pat_id
      and WL10.tsp = LWindow.window_id
    group by WL10.pat_id
  ) L10
    on I.pat_id = L10.pat_id

  left join (
    -- Earliest occurrence of ssp.
    select WL20.pat_id,
           min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
           min(LWindow.severe_sepsis_onset) as ssp,
           min(WL20.tsp) as w_ssp
    from (
      select L20.pat_id, min(L20.tsp) as tsp
      from earliest_occurrences_d%(dataset_id)s L20
      where L20.label >= 20 and L20.label < 30
      group by L20.pat_id
    ) WL20
    -- Retrieve exact timestamp of onset within the window.
    inner join septic_shock_outputs_d%(dataset_id)s LWindow
      on WL20.pat_id = LWindow.pat_id
      and WL20.tsp = LWindow.window_id
    group by WL20.pat_id
  ) L20
    on I.pat_id = L20.pat_id

  left join (
    -- Earliest occurrence of ssh.
    select WL30.pat_id,
           min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
           min(LWindow.severe_sepsis_onset) as ssp,
           min(LWindow.septic_shock_onset) as ssh,
           min(WL30.tsp) as w_ssh
    from (
      select L30.pat_id, min(L30.tsp) as tsp
      from earliest_occurrences_d%(dataset_id)s L30
      where L30.label >= 30
      group by L30.pat_id
    ) WL30
    -- Retrieve exact timestamp of onset within the window.
    inner join septic_shock_outputs_d%(dataset_id)s LWindow
      on WL30.pat_id = LWindow.pat_id
      and WL30.tsp = LWindow.window_id
    group by WL30.pat_id
  ) L30
    on I.pat_id = L30.pat_id
  group by I.pat_id
  ;

--
-- Create timestamps for evaluating bundle criteria

--
-- Use template for bundle criteria calculation



create table %(prefix)sbundle_compliance_outputs_d%(dataset_id)s diststyle key distkey(pat_id) sortkey(window_id, pat_id) as
  with
  severe_sepsis_criteria_at_onset as (
    select SSP.*
    from onset_times T
    inner join septic_shock_outputs_d%(dataset_id)s SSP
      on T.w_severe_sepsis_onset = SSP.window_id and T.pat_id = SSP.pat_id
    where not(T.severe_sepsis_onset is null or T.w_severe_sepsis_onset is null)
    and (T.septic_shock_onset is null or T.severe_sepsis_onset <> T.septic_shock_onset)
  ),
  septic_shock_criteria_at_onset as (
    select SSH.*
    from onset_times T
    inner join septic_shock_outputs_d%(dataset_id)s SSH
      on T.w_septic_shock_onset = SSH.window_id and T.pat_id = SSH.pat_id
    where not(T.septic_shock_onset is null or T.w_septic_shock_onset is null)
  )
  severe_sepsis_6hr_bundle as (
    select T.severe_sepsis_onset + interval ''6 hours'' as ts, SSP.*
    from onset_times T
    inner join lateral %s(coalesce(%s, T.pat_id), T.severe_sepsis_onset, T.severe_sepsis_onset + interval ''6 hours'', %s, %s, %s, %s) SSP
    on SSP.pat_id = T.pat_id
    where T.severe_sepsis_onset is not null
    and (T.septic_shock_onset is null or T.severe_sepsis_onset <> T.septic_shock_onset)
  ),
  septic_shock_6hr_bundle as (
    select T.septic_shock_onset + interval ''6 hours'' as ts, SSH.*
    from onset_times T
    inner join lateral %s(coalesce(%s, T.pat_id), T.septic_shock_onset, T.septic_shock_onset + interval ''6 hours'', %s, %s, %s, %s) SSH
    on SSH.pat_id = T.pat_id
    where T.septic_shock_onset is not null
  )
  select SSP.window_id,
         SSP.pat_id,
         SSP.name,
         (case when SSP.name like ''%%_order'' then PB.measurement_time else SSP.measurement_time end) as measurement_time,
         (case when SSP.name like ''%%_order'' then PB.value            else SSP.value            end) as value,
         (case when SSP.name like ''%%_order'' then PB.is_met           else SSP.is_met           end) as is_met,
         (case when SSP.name like ''%%_order'' then PB.update_date      else SSP.update_date      end) as update_date,
         SSP.severe_sepsis_onset,
         SSP.severe_sepsis_wo_infection_onset,
         SSP.septic_shock_onset
  from severe_sepsis SSP
  inner join severe_sepsis_6hr_bundle PB on SSP.pat_id = PB.pat_id and SSP.name = PB.name
  union all
  select SSH.window_id,
         SSH.pat_id,
         SSH.name,
         (case when SSH.name like ''%%_order'' then HB.measurement_time else SSH.measurement_time end) as measurement_time,
         (case when SSH.name like ''%%_order'' then HB.value            else SSH.value            end) as value,
         (case when SSH.name like ''%%_order'' then HB.is_met           else SSH.is_met           end) as is_met,
         (case when SSH.name like ''%%_order'' then HB.update_date      else SSH.update_date      end) as update_date,
         SSH.severe_sepsis_onset,
         SSH.severe_sepsis_wo_infection_onset,
         SSH.septic_shock_onset
  from septic_shock SSH
  inner join septic_shock_6hr_bundle HB on SSH.pat_id = HB.pat_id and SSH.name = HB.name
  ;


create table %(prefix)sbundle_compliance_labels_d%(dataset_id)s diststyle all sortkey(pat_id, tsp) as
  select sw.pat_id, sw.window_id as tsp, 'bundle compliance state' as label_type, sw.state as label from (
    select stats.window_id, stats.pat_id,
        (
          case
          when ssp_present and ssh_present then (
            case
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
            when stats.window_id - stats.septic_shock_onset  >= '6 hours'::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
            when stats.window_id - stats.septic_shock_onset  >= '6 hours'::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com

            else 30
            end
          )

          when ssp_present then (
            case
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
            when stats.window_id - stats.severe_sepsis_onset >= '6 hours'::interval and sev_sep_6hr_count = 1 then 23 -- sev_sep_6hr_com
            when stats.window_id - stats.severe_sepsis_onset >= '3 hours'::interval and sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com

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
      select OCR.window_id, OCR.pat_id,
          bool_or(severe_sepsis_wo_infection_onset is not null) as sspwoi_present,
          bool_or(severe_sepsis_onset is not null)              as ssp_present,
          bool_or(septic_shock_onset is not null)               as ssh_present,

          max(severe_sepsis_wo_infection_onset)                 as severe_sepsis_wo_infection_onset,
          max(severe_sepsis_onset)                              as severe_sepsis_onset,
          max(septic_shock_onset)                               as septic_shock_onset,

          sum(case when name = 'suspicion_of_infection' and is_met then 1 else 0 end)     as sus_count,
          sum(case when name = 'suspicion_of_infection' and not is_met then 1 else 0 end) as sus_noinf_count,
          sum(case when name = 'suspicion_of_infection' and not is_met then 1 else 0 end) as sus_null_count,

          sum(case when name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met then 1 else 0 end)
            as sirs_count,

          sum(case when name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp',
                                 'respiratory_failure','creatinine','bilirubin','platelet','inr','lactate')
                   and is_met
              then 1
              else 0
              end)
            as organ_count,

          sum(case when name in ('initial_lactate_order','blood_culture_order',
                                 'antibiotics_order', 'crystalloid_fluid_order')
                   and is_met
              then 1
              else 0
              end)
            as sev_sep_3hr_count,

          sum(case when name = 'repeat_lactate_order' and is_met then 1 else 0 end)
            as sev_sep_6hr_count,

          sum(case when name = 'vasopressors_order' and is_met then 1 else 0 end)
            as sep_sho_6hr_count

      from %(prefix)sbundle_compliance_outputs_d%(dataset_id)s OCR
      where OCR.pat_id = coalesce(null/*this_pat_id*/, OCR.pat_id)
      group by OCR.window_id, OCR.pat_id
    ) stats
  ) sw
  ;
'''

create_report_template = '''
'''

tabulate_report_template = '''
'''

full_compliance_report_template = '''
'''

unit_compiance_report_template = '''
'''

delete_severe_sepsis_template = '''
drop table %(prefix)smeasurement_times_d%(dataset_id)s;
drop table %(prefix)spat_partition_d%(dataset_id)s;
drop table %(prefix)spat_cvalues_d%(dataset_id)s;
drop table %(prefix)spat_aggregates_d%(dataset_id)s;
drop table %(prefix)sall_sirs_org_d%(dataset_id)s;
drop table %(prefix)sall_sirs_org_triples_d%(dataset_id)s;
drop table %(prefix)snull_infections_d%(dataset_id)s;
drop table %(prefix)sinfections_d%(dataset_id)s;
drop table %(prefix)ssevere_sepsis_candidates_d%(dataset_id)s;
drop table %(prefix)ssevere_sepsis_onsets_d%(dataset_id)s;
drop table %(prefix)ssevere_sepsis_criteria_d%(dataset_id)s;
'''

delete_severe_sepsis_output_template = '''
drop table %(prefix)ssevere_sepsis_outputs_d%(dataset_id)s;
drop table %(prefix)scdm_labels_d%(dataset_id)s;
'''

delete_septic_shock_template = '''
drop table %(prefix)scrystalloid_fluid_and_hypoperfusion_d%(dataset_id)s;
drop table %(prefix)shypotension_d%(dataset_id)s;
drop table %(prefix)sseptic_shock_onsets_d%(dataset_id)s;
drop table %(prefix)sorders_criteria_d%(dataset_id)s;
drop table %(prefix)sseptic_shock_outputs_d%(dataset_id)s;
'''


unload_table_template = '''
unload ('select * from %(table_name)s%(dataset_id)s')
to 's3://opsdx-clarity-etl-stage/redshift-labels/%(dataset_name)s/%(table_name)s%(dataset_id)s_'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
manifest delimiter '\\t' parallel %(parallel)s;
'''


severe_sepsis_unload_tables = [
  ('measurement_times_d'        , 'off'),
  ('pat_partition_d'            , 'off'),
  ('pat_cvalues_d'              , 'on' ),
  ('pat_aggregates_d'           , 'off'),
  ('all_sirs_org_d'             , 'on' ),
  ('all_sirs_org_triples_d'     , 'on' ),
  ('null_infections_d'          , 'off'),
  ('infections_d'               , 'off'),
  ('severe_sepsis_candidates_d' , 'off'),
  ('severe_sepsis_onsets_d'     , 'off'),
  ('severe_sepsis_criteria_d'   , 'on' ),
  ('severe_sepsis_outputs_d'    , 'on' ),
  ('cdm_labels_d'               , 'off'),
]

septic_shock_unload_tables = [
  ('crystalloid_fluid_and_hypoperfusion_d' , 'off'),
  ('hypotension_d'                         , 'off'),
  ('orders_criteria_d'                     , 'off'),
  ('septic_shock_onsets_d'                 , 'off'),
  ('septic_shock_outputs_d'                , 'on' ),
  ('cdm_shock_labels_d'                    , 'off'),
]

################################
# Toplevel

# dataset_id = 1
# dataset_name = 'hcgh_1yr'

dataset_id = 3
dataset_name = 'hcgh_3yr'

# dataset_id = 12
# dataset_name = 'jhh_1yr'

# dataset_id = 13
# dataset_name = 'bmc_1yr'

bpa_template = window_template_measurements + bpa_template + severe_sepsis_output_template
ssp_template = window_template_meas_periodic + severe_sepsis_template + severe_sepsis_output_template
ssh_template = window_template_meas_periodic + septic_shock_template + severe_sepsis_output_template

with open('bpa_labels_d%s.sql' % dataset_id, 'w') as f:
  f.write(bpa_template % {'dataset_id': dataset_id})

with open('severe_sepsis_labels_d%s.sql' % dataset_id, 'w') as f:
  f.write(ssp_template % {'dataset_id': dataset_id, 'prefix': ''})

with open('septic_shock_labels_d%s.sql' % dataset_id, 'w') as f:
  f.write(ssh_template % {'dataset_id': dataset_id, 'prefix': ''})

# Drop/cleanup file.
# delete_template = delete_severe_sepsis_template + delete_severe_sepsis_output_template
delete_template = delete_severe_sepsis_template + delete_septic_shock_template

with open('drop_labels_d%s.sql' % dataset_id, 'w') as f:
 f.write(delete_template % {'dataset_id': dataset_id, 'prefix': ''})

# Unload file.
# unload_tables = severe_sepsis_unload_tables
unload_tables = severe_sepsis_unload_tables + septic_shock_unload_tables

with open('unload_labels_d%s.sql' % dataset_id, 'w') as f:
  f.write('\n'.join(map(lambda t: unload_table_template % {'dataset_name': dataset_name, 'dataset_id': dataset_id, 'table_name': t[0], 'parallel': t[1]}, unload_tables)))

# Truncate file.
# truncate_tables = severe_sepsis_unload_tables
truncate_tables = severe_sepsis_unload_tables + septic_shock_unload_tables

with open('truncate_labels_d%s.sql' % dataset_id, 'w') as f:
  f.write('\n'.join(map(lambda t: 'truncate table %s%s;' % (t[0], dataset_id), truncate_tables)))
