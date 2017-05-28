drop table if exists cdm_reports;
create table cdm_reports (
    dataset_id                         integer,
    label_id                           integer,
    w_max_state                        integer,
    w_start                            timestamptz,
    w_end                              timestamptz,
    pat_id                             varchar(50),
    name                               varchar(50),
    measurement_time                   timestamptz,
    value                              text,
    override_time                      timestamptz,
    override_user                      text,
    override_value                     json,
    is_met                             boolean,
    update_date                        timestamptz,
    severe_sepsis_onset                timestamptz,
    severe_sepsis_wo_infection_onset   timestamptz,
    septic_shock_onset                 timestamptz,
    w_severe_sepsis_onset              timestamptz,
    w_severe_sepsis_wo_infection_onset timestamptz,
    w_septic_shock_onset               timestamptz,
    primary key (dataset_id, label_id, w_max_state, w_start, w_end, pat_id, name)
);

---------------------------------------------------
-- State encoding helpers.

create or replace function encode_hosp_best_state(label integer)
  returns integer
language plpgsql
as $func$ begin
  return
    case when label = 35 then 37
         when label = 33 then 35
         when label = 31 then 33
         when label = 23 then 25
         when label = 21 then 23
         else label
    end;
end; $func$;


create or replace function decode_hosp_best_state(label integer)
  returns integer
language plpgsql
as $func$ begin
  return
    case when label = 37 then 35
         when label = 35 then 33
         when label = 33 then 31
         when label = 25 then 23
         when label = 23 then 21
         else label
    end;
end; $func$;


---------------------------------------------------
-- Pat ID - Enc ID matching for labels and reports.
---------------------------------------------------

--
-- Best effort matching of pat_ids and a labelling timestamp, to an enc_id
create or replace function match_label_encounters(_dataset_id       integer,
                                                  _label_id         integer)
  returns table ( pat_id      text,
                  enc_id      integer,
                  tsp         timestamptz,
                  label_type  text,
                  label       integer )
language plpgsql
as $func$ begin
  return query
    with los as (
      select C.dataset_id, C.enc_id, min(C.enter_time) as arrival, max(C.leave_time) as departure
      from care_unit C
      where C.dataset_id = coalesce(_dataset_id, C.dataset_id)
      group by C.dataset_id, C.enc_id
    ),
    label_encs as (
      select R.pat_id, P.enc_id, R.tsp, R.label_type, R.label
      from cdm_labels R
      inner join pat_enc P on R.pat_id = P.pat_id and R.dataset_id = P.dataset_id
      inner join los CU
        on P.enc_id = CU.enc_id
        and P.dataset_id = CU.dataset_id
        and R.tsp between CU.arrival and CU.departure
      where R.dataset_id  = coalesce(_dataset_id, R.dataset_id)
      and   R.label_id    = coalesce(_label_id,   R.label_id)
    ),
    missing_encs as (
      select R.pat_id, R.tsp, R.label_type, R.label
      from cdm_labels R
      where R.dataset_id  = coalesce(_dataset_id, R.dataset_id)
      and   R.label_id    = coalesce(_label_id,   R.label_id)
      except
      select D.pat_id, D.tsp, D.label_type, D.label from label_encs D
    )
    select R.pat_id,
           (case
              when R.enc_id is null
              then coalesce(
                      first_value(R.enc_id) over (partition by R.pat_id order by R.tsp rows unbounded preceding),
                      (select first(R.enc_id) as enc_id from (
                          select CU.enc_id, CU.departure from (
                            select P.enc_id from pat_enc P
                            where P.pat_id = R.pat_id
                            and P.dataset_id = coalesce(_dataset_id, P.dataset_id)
                          ) P
                          inner join los CU on P.enc_id = CU.enc_id
                          where CU.departure < R.tsp
                          order by CU.departure desc
                        ) R
                      ))
              else R.enc_id end) as enc_id,
           R.tsp,
           R.label_type,
           R.label
    from (
      select * from label_encs
      union all
      select M.pat_id, null as enc_id, M.tsp, M.label_type, M.label from missing_encs M
    ) R
    order by R.pat_id, enc_id, R.tsp;
end; $func$;


--
-- Best effort matching of pat_ids and a criteria window, to an enc_id.
create or replace function match_report_encounters(_pat_state        integer,
                                                   _dataset_id       integer,
                                                   _label_id         integer,
                                                   _data_delay_limit interval default '2 days')
  returns table ( pat_id    varchar(50),
                  enc_id    integer,
                  arrival   timestamptz,
                  departure timestamptz )
language plpgsql
as $func$ begin
  return query
    with los as (
      select C.dataset_id, C.enc_id, min(C.enter_time) as arrival, max(C.leave_time) as departure
      from care_unit C
      where C.dataset_id = coalesce(_dataset_id, C.dataset_id)
      group by C.dataset_id, C.enc_id
    ),
    report_care_units as (
      select distinct R.pat_id, P.enc_id, CU.arrival, CU.departure
      from cdm_reports R
      inner join pat_enc P on R.pat_id = P.pat_id and R.dataset_id = P.dataset_id
      inner join los CU
        on P.enc_id = CU.enc_id
        and P.dataset_id = CU.dataset_id
        and (R.w_start between CU.arrival and CU.departure or R.w_end between CU.arrival and CU.departure)
      where R.dataset_id  = coalesce(_dataset_id, R.dataset_id)
      and   R.label_id    = coalesce(_label_id,   R.label_id)
      and   R.w_max_state = coalesce(_pat_state,  R.w_max_state)
    ),
    delayed_patients as (
      select distinct R.pat_id from cdm_reports R
      where R.dataset_id  = coalesce(_dataset_id, R.dataset_id)
      and   R.label_id    = coalesce(_label_id,   R.label_id)
      and   R.w_max_state = coalesce(_pat_state,  R.w_max_state)
      except
      select distinct RCU.pat_id from report_care_units RCU
    ),
    delayed_care_units as (
      select distinct R.pat_id, P.enc_id, CU.arrival, CU.departure
      from cdm_reports R
      inner join pat_enc P on R.pat_id = P.pat_id and R.dataset_id = P.dataset_id
      left join los CU
        on P.enc_id = CU.enc_id
        and P.dataset_id = CU.dataset_id
      where R.dataset_id  = coalesce(_dataset_id, R.dataset_id)
      and   R.label_id    = coalesce(_label_id,   R.label_id)
      and   R.w_max_state = coalesce(_pat_state,  R.w_max_state)
      and   R.pat_id in ( select D.pat_id from delayed_patients D )
      and   R.w_start between CU.departure and CU.departure + _data_delay_limit
    )
    select * from report_care_units union all select * from delayed_care_units;
end; $func$;


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


--
-- Populates the care_unit table for a dataset.
create or replace function create_care_unit(_dataset_id integer) returns void language plpgsql
as $func$ begin
  delete from care_unit where dataset_id = _dataset_id;

  insert into care_unit (dataset_id, enc_id, enter_time, leave_time, care_unit)
    with raw_care_unit_tbl as (
      select R.dataset_id, R.enc_id,
             R.tsp as enter_time,
             lead(R.tsp,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp) as leave_time,
             (case when R.care_unit = 'Arrival' then R.next_unit else R.care_unit end) as care_unit
      from (
        select R.dataset_id, R.enc_id, R.tsp, R.care_unit,
               lead(R.tsp,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp) as next_tsp,
               lead(R.care_unit,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp) as next_unit,
               first_value(R.care_unit) over (PARTITION by R.enc_id order by R.tsp) as first_unit
        from (
          select cdm_s.dataset_id, cdm_s.enc_id, cdm_s.value::timestamptz as tsp, 'Arrival' as care_unit
          from cdm_s
          where cdm_s.fid = 'adt_arrival_time'
          and cdm_s.dataset_id = _dataset_id
          union all
          select cdm_t.dataset_id, cdm_t.enc_id, cdm_t.tsp, cdm_t.value as care_unit
          from cdm_t
          where cdm_t.fid = 'care_unit'
          and cdm_t.dataset_id = _dataset_id
        ) R
        order by R.enc_id, R.tsp
      ) R
      where not (R.care_unit = 'Arrival' and R.first_unit <> 'Arrival')
      and (R.next_tsp is null or R.tsp <> R.next_tsp)
      order by R.enc_id, enter_time
    ),
    discharge_fitered as (
      select raw_care_unit_tbl.*
      from
      raw_care_unit_tbl
      where care_unit != 'Discharge' and leave_time is not null
    )
    select dataset_id, enc_id, enter_time, leave_time, care_unit
    from discharge_fitered;

  return;
end; $func$;


--
-- Populates the cdm_reports table for any combination of dataset/label/patient state
create or replace function create_criteria_report(_pat_state  integer,
                                                  _dataset_id integer,
                                                  _label_id   integer)
  returns void
language plpgsql
as $func$ begin
    insert into cdm_reports (
        dataset_id,
        label_id,
        w_max_state,
        w_start,
        w_end,
        pat_id,
        name,
        measurement_time,
        value,
        override_time,
        override_user,
        override_value,
        is_met,
        update_date,
        severe_sepsis_onset,
        severe_sepsis_wo_infection_onset,
        septic_shock_onset,
        w_severe_sepsis_onset,
        w_severe_sepsis_wo_infection_onset,
        w_septic_shock_onset
      )
      -- Earliest occurrence of each state
      with earliest_occurrences as (
        select L.dataset_id, L.label_id, L.pat_id, L.label, min(L.tsp) as tsp
        from cdm_labels L
        where L.dataset_id = coalesce(_dataset_id, L.dataset_id)
        and   L.label_id   = coalesce(_label_id, L.label_id)
        group by L.dataset_id, L.label_id, L.pat_id, L.label
      ),
      state_onsets as (
        select I.dataset_id, I.label_id, I.pat_id,
               least(min(L10.sspwoi), min(L20.sspwoi), min(L30.sspwoi)) as severe_sepsis_wo_infection_onset,
               least(min(L20.ssp), min(L30.ssp)) as severe_sepsis_onset,
               min(L30.ssh) as septic_shock_onset
        from
        ( select distinct I.dataset_id, I.label_id, I.pat_id from earliest_occurrences I ) I

        left join
        (
          -- Earliest occurrence of sspwoi.
          /*
          select L10.dataset_id, L10.label_id, L10.pat_id, min(L10.tsp) as tsp
          from earliest_occurrences L10
          where L10.label >= 10 and L10.label < 20
          group by L10.pat_id, L10.dataset_id, L10.label_id
          */
          select WL10.dataset_id, WL10.label_id, WL10.pat_id,
                 min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi
          from (
            select L10.dataset_id, L10.label_id, L10.pat_id, min(L10.tsp) as tsp
            from earliest_occurrences L10
            where L10.label >= 10 and L10.label < 20
            group by L10.pat_id, L10.dataset_id, L10.label_id
          ) WL10
          inner join lateral get_cms_labels_for_window_v5(
            WL10.pat_id, WL10.tsp - interval '6 hours', WL10.tsp, WL10.dataset_id
          ) LWindow
            on WL10.pat_id = LWindow.pat_id
          group by WL10.pat_id, WL10.dataset_id, WL10.label_id
        ) L10
          on I.dataset_id = L10.dataset_id
          and I.label_id = L10.label_id
          and I.pat_id = L10.pat_id

        left join (
          -- Earliest occurrence of ssp.
          /*
          select L20.dataset_id, L20.label_id, L20.pat_id, min(L20.tsp) as tsp
          from earliest_occurrences L20
          where L20.label >= 20 and L20.label < 30
          group by L20.pat_id, L20.dataset_id, L20.label_id
          */
          select WL20.dataset_id, WL20.label_id, WL20.pat_id,
                 min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
                 min(LWindow.severe_sepsis_onset) as ssp
          from (
            select L20.dataset_id, L20.label_id, L20.pat_id, min(L20.tsp) as tsp
            from earliest_occurrences L20
            where L20.label >= 20 and L20.label < 30
            group by L20.pat_id, L20.dataset_id, L20.label_id
          ) WL20
          inner join lateral get_cms_labels_for_window_v5(
            WL20.pat_id, WL20.tsp - interval '6 hours', WL20.tsp, WL20.dataset_id
          ) LWindow
            on WL20.pat_id = LWindow.pat_id
          group by WL20.pat_id, WL20.dataset_id, WL20.label_id
        ) L20
          on I.dataset_id = L20.dataset_id
          and I.label_id = L20.label_id
          and I.pat_id = L20.pat_id

        left join (
          -- Earliest occurrence of ssh.
          /*
          select L30.dataset_id, L30.label_id, L30.pat_id, min(L30.tsp) as tsp
          from earliest_occurrences L30
          where L30.label >= 30
          group by L30.pat_id, L30.dataset_id, L30.label_id
          */
          select WL30.dataset_id, WL30.label_id, WL30.pat_id,
                 min(LWindow.severe_sepsis_wo_infection_onset) as sspwoi,
                 min(LWindow.severe_sepsis_onset) as ssp,
                 min(LWindow.septic_shock_onset) as ssh
          from (
            select L30.dataset_id, L30.label_id, L30.pat_id, min(L30.tsp) as tsp
            from earliest_occurrences L30
            where L30.label >= 30
            group by L30.pat_id, L30.dataset_id, L30.label_id
          ) WL30
          inner join lateral get_cms_labels_for_window_v5(
            WL30.pat_id, WL30.tsp - interval '6 hours', WL30.tsp, WL30.dataset_id
          ) LWindow
            on WL30.pat_id = LWindow.pat_id
          group by WL30.pat_id, WL30.dataset_id, WL30.label_id
        ) L30
          on I.dataset_id = L30.dataset_id
          and I.label_id = L30.label_id
          and I.pat_id = L30.pat_id

        group by I.dataset_id, I.label_id, I.pat_id
      )

      select WS.dataset_id                            as dataset_id,
             WS.label_id                              as label_id,
             WS.max_state                             as w_max_state,
             WS.window_ts - interval '6 hours'        as w_start,
             WS.window_ts                             as w_end,
             CWindow.pat_id                           as pat_id,
             CWindow.name                             as name,
             CWindow.measurement_time                 as measurement_time,
             CWindow.value                            as value,
             CWindow.override_time                    as override_time,
             CWindow.override_user                    as override_user,
             CWindow.override_value                   as override_value,
             CWindow.is_met                           as is_met,
             CWindow.update_date                      as update_date,
             S.severe_sepsis_onset                    as severe_sepsis_onset,
             S.severe_sepsis_wo_infection_onset       as severe_sepsis_wo_infection_onset,
             S.septic_shock_onset                     as septic_shock_onset,
             CWindow.severe_sepsis_onset              as w_severe_sepsis_onset,
             CWindow.severe_sepsis_wo_infection_onset as w_severe_sepsis_wo_infection_onset,
             CWindow.septic_shock_onset               as w_septic_shock_onset
      from (
        -- Earliest occurrence of the worst state.
        select L2.pat_id, L2.dataset_id, L2.label_id, min(L2.tsp) as window_ts, max(L2.label) as max_state
        from (
          -- Best hospital care state for each patient.
          select L.pat_id, decode_hosp_best_state(max(encode_hosp_best_state(L.label))) as label
          from cdm_labels L
          where L.dataset_id = coalesce(_dataset_id, L.dataset_id)
          and   L.label_id   = coalesce(_label_id, L.label_id)
          group by L.pat_id
        ) L1
        inner join cdm_labels L2
            on L1.pat_id = L2.pat_id
            and L1.label = L2.label
        where L1.label    = coalesce(_pat_state, L1.label)
        and L2.dataset_id = coalesce(_dataset_id, L2.dataset_id)
        and L2.label_id   = coalesce(_label_id, L2.label_id)
        group by L2.pat_id, L2.dataset_id, L2.label_id
      ) WS

      inner join lateral get_cms_labels_for_window_v5(
        WS.pat_id, WS.window_ts - interval '6 hours', WS.window_ts, WS.dataset_id
      ) CWindow
        on WS.pat_id = CWindow.pat_id

      left join state_onsets S
        on WS.dataset_id = S.dataset_id
        and WS.label_id = S.label_id
        and WS.pat_id = S.pat_id

      order by WS.max_state desc, CWindow.pat_id
    on conflict(dataset_id, label_id, w_max_state, w_start, w_end, pat_id, name)
      do update
        set measurement_time                   = excluded.measurement_time,
            value                              = excluded.value,
            override_time                      = excluded.override_time,
            override_user                      = excluded.override_user,
            override_value                     = excluded.override_value,
            is_met                             = excluded.is_met,
            update_date                        = excluded.update_date,
            severe_sepsis_onset                = excluded.severe_sepsis_onset,
            severe_sepsis_wo_infection_onset   = excluded.severe_sepsis_wo_infection_onset,
            septic_shock_onset                 = excluded.septic_shock_onset,
            w_severe_sepsis_onset              = excluded.w_severe_sepsis_onset,
            w_severe_sepsis_wo_infection_onset = excluded.w_severe_sepsis_wo_infection_onset,
            w_septic_shock_onset               = excluded.w_septic_shock_onset;
end; $func$;


----------------------------------------------------------------------
-- Per-patient criteria object construction.

create or replace function tabulate_criteria(_pat_state  integer,
                                             _dataset_id integer,
                                             _label_id   integer)
  returns table ( dataset_id                         integer,
                  label_id                           integer,
                  w_max_state                        integer,
                  pat_id                             varchar(50),
                  severe_sepsis_onset                timestamptz,
                  severe_sepsis_wo_infection_onset   timestamptz,
                  septic_shock_onset                 timestamptz,
                  w_severe_sepsis_onset              timestamptz,
                  w_severe_sepsis_wo_infection_onset timestamptz,
                  w_septic_shock_onset               timestamptz,
                  criteria                           json
  )
language plpgsql
as $func$ begin
  return query
    select  C.dataset_id, C.label_id, C.w_max_state, C.pat_id,
            C.severe_sepsis_onset, C.severe_sepsis_wo_infection_onset, C.septic_shock_onset,
            C.w_severe_sepsis_onset, C.w_severe_sepsis_wo_infection_onset, C.w_septic_shock_onset,
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
                        and C.name in (
                          'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
                          'respiratory_failure',
                          'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                        )
                        and coalesce(C.override_time, C.measurement_time) between C.w_severe_sepsis_wo_infection_onset - interval '6 hours' and C.w_severe_sepsis_wo_infection_onset
                    then json_object('{t, value}', ARRAY[
                           humanize_report_timestamp(C.severe_sepsis_onset, coalesce(C.override_time, C.measurement_time)),
                           coalesce(C.override_value::text, C.value)::text
                         ]::text[])

                   when C.is_met
                       and C.name not in (
                          'sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc',
                          'respiratory_failure',
                          'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
                        )
                    then json_object('{t, value}', ARRAY[
                           humanize_report_timestamp(C.severe_sepsis_onset, coalesce(C.override_time, C.measurement_time)),
                           coalesce(C.override_value::text, C.value)::text
                         ]::text[])

                   else null
              end
            ) as criteria
    from cdm_reports C
    where C.dataset_id  = coalesce(_dataset_id, C.dataset_id)
    and   C.label_id    = coalesce(_label_id,   C.label_id)
    and   C.w_max_state = coalesce(_pat_state,  C.w_max_state)
    group by C.dataset_id, C.label_id, C.w_max_state, C.pat_id,
             C.severe_sepsis_onset, C.severe_sepsis_wo_infection_onset, C.septic_shock_onset,
             C.w_severe_sepsis_onset, C.w_severe_sepsis_wo_infection_onset, C.w_septic_shock_onset;
end; $func$;


----------------------------------------------------------------------
-- Top-level human-readable report generation.

create or replace function criteria_report(_pat_state  integer,
                                           _dataset_id integer,
                                           _label_id   integer)
  returns table ( pat_worst_state                   integer,
                  pat_id                            varchar(50),
                  severe_sepsis_onset               text,
                  sirs_organ_dys_onset              text,
                  septic_shock_onset                text,
                  w_severe_sepsis_onset             text,
                  w_sirs_organ_dys_onset            text,
                  w_septic_shock_onset              text,
                  infection                         text,
                  sirs_criteria                     text,
                  org_df_criteria                   text,
                  septic_shock_criteria             text,
                  orders                            text
  )
language plpgsql
as $func$ begin
  return query
    select W.w_max_state as pat_worst_state,
           W.pat_id,
           W.severe_sepsis_onset,
           W.sirs_organ_dys_onset,
           W.septic_shock_onset,
           W.w_severe_sepsis_onset,
           W.w_sirs_organ_dys_onset,
           W.w_septic_shock_onset,

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
      select  R.w_max_state, R.pat_id,
              humanize_report_timestamp(null, R.severe_sepsis_onset)                                 as severe_sepsis_onset,
              humanize_report_timestamp(R.severe_sepsis_onset, R.severe_sepsis_wo_infection_onset)   as sirs_organ_dys_onset,
              humanize_report_timestamp(R.severe_sepsis_onset, R.septic_shock_onset)                 as septic_shock_onset,
              humanize_report_timestamp(R.severe_sepsis_onset, R.w_severe_sepsis_onset)              as w_severe_sepsis_onset,
              humanize_report_timestamp(R.severe_sepsis_onset, R.w_severe_sepsis_wo_infection_onset) as w_sirs_organ_dys_onset,
              humanize_report_timestamp(R.severe_sepsis_onset, R.w_septic_shock_onset)               as w_septic_shock_onset,
              R.criteria#>>'{antibiotics_order, met}'         as antibiotics_order_met,
              R.criteria#>>'{antibiotics_order, t}'           as antibiotics_order_t,
              R.criteria#>>'{antibiotics_order, value}'       as antibiotics_order_value,
              R.criteria#>>'{bilirubin, t}'                   as bilirubin_t,
              R.criteria#>>'{bilirubin, value}'               as bilirubin_value,
              R.criteria#>>'{blood_culture_order, met}'       as blood_culture_order_met,
              R.criteria#>>'{blood_culture_order, t}'         as blood_culture_order_t,
              R.criteria#>>'{blood_culture_order, value}'     as blood_culture_order_value,
              R.criteria#>>'{blood_pressure, t}'              as blood_pressure_t,
              R.criteria#>>'{blood_pressure, value}'          as blood_pressure_value,
              R.criteria#>>'{creatinine, t}'                  as creatinine_t,
              R.criteria#>>'{creatinine, value}'              as creatinine_value,
              R.criteria#>>'{crystalloid_fluid, t}'           as crystalloid_fluid_t,
              R.criteria#>>'{crystalloid_fluid, value}'       as crystalloid_fluid_value,
              R.criteria#>>'{crystalloid_fluid_order, met}'   as crystalloid_fluid_order_met,
              R.criteria#>>'{crystalloid_fluid_order, t}'     as crystalloid_fluid_order_t,
              R.criteria#>>'{crystalloid_fluid_order, value}' as crystalloid_fluid_order_value,
              R.criteria#>>'{decrease_in_sbp, t}'             as decrease_in_sbp_t,
              R.criteria#>>'{decrease_in_sbp, value}'         as decrease_in_sbp_value,
              R.criteria#>>'{heart_rate, t}'                  as heart_rate_t,
              R.criteria#>>'{heart_rate, value}'              as heart_rate_value,
              R.criteria#>>'{hypotension_dsbp, t}'            as hypotension_dsbp_t,
              R.criteria#>>'{hypotension_dsbp, value}'        as hypotension_dsbp_value,
              R.criteria#>>'{hypotension_map, t}'             as hypotension_map_t,
              R.criteria#>>'{hypotension_map, value}'         as hypotension_map_value,
              R.criteria#>>'{initial_lactate, t}'             as initial_lactate_t,
              R.criteria#>>'{initial_lactate, value}'         as initial_lactate_value,
              R.criteria#>>'{initial_lactate_order, met}'     as initial_lactate_order_met,
              R.criteria#>>'{initial_lactate_order, t}'       as initial_lactate_order_t,
              R.criteria#>>'{initial_lactate_order, value}'   as initial_lactate_order_value,
              R.criteria#>>'{inr, t}'                         as inr_t,
              R.criteria#>>'{inr, value}'                     as inr_value,
              R.criteria#>>'{lactate, t}'                     as lactate_t,
              R.criteria#>>'{lactate, value}'                 as lactate_value,
              R.criteria#>>'{mean_arterial_pressure, t}'      as mean_arterial_pressure_t,
              R.criteria#>>'{mean_arterial_pressure, value}'  as mean_arterial_pressure_value,
              R.criteria#>>'{platelet, t}'                    as platelet_t,
              R.criteria#>>'{platelet, value}'                as platelet_value,
              R.criteria#>>'{repeat_lactate_order, met}'      as repeat_lactate_order_met,
              R.criteria#>>'{repeat_lactate_order, t}'        as repeat_lactate_order_t,
              R.criteria#>>'{repeat_lactate_order, value}'    as repeat_lactate_order_value,
              R.criteria#>>'{respiratory_failure, t}'         as respiratory_failure_t,
              R.criteria#>>'{respiratory_failure, value}'     as respiratory_failure_value,
              R.criteria#>>'{respiratory_rate, t}'            as respiratory_rate_t,
              R.criteria#>>'{respiratory_rate, value}'        as respiratory_rate_value,
              R.criteria#>>'{sirs_temp, t}'                   as sirs_temp_t,
              R.criteria#>>'{sirs_temp, value}'               as sirs_temp_value,
              R.criteria#>>'{suspicion_of_infection, t}'      as suspicion_of_infection_t,
              R.criteria#>>'{suspicion_of_infection, value}'  as suspicion_of_infection_value,
              R.criteria#>>'{systolic_bp, t}'                 as systolic_bp_t,
              R.criteria#>>'{systolic_bp, value}'             as systolic_bp_value,
              R.criteria#>>'{vasopressors_order, met}'        as vasopressors_order_met,
              R.criteria#>>'{vasopressors_order, t}'          as vasopressors_order_t,
              R.criteria#>>'{vasopressors_order, value}'      as vasopressors_order_value,
              R.criteria#>>'{wbc, t}'                         as wbc_t,
              R.criteria#>>'{wbc, value}'                     as wbc_value
      from tabulate_criteria(_pat_state, _dataset_id, _label_id) R
    ) W
    order by W.w_max_state desc, W.pat_id;
end; $func$;


--------------------------------------------------
-- Compliance queries.

--
-- Returns raw compliance statistics.
create or replace function tabulate_compliance(_pat_state  integer,
                                               _dataset_id integer,
                                               _label_id   integer)
  returns table ( pat_worst_state                  integer,
                  pat_id                           varchar(50),
                  enc_id                           integer,
                  severe_sepsis_onset              timestamptz,
                  severe_sepsis_wo_infection_onset timestamptz,
                  septic_shock_onset               timestamptz,
                  age                              text,
                  gender                           text,
                  care_unit_entry                  timestamptz,
                  care_unit                        text,
                  antibiotics_met                  integer,
                  antibiotics_unmet                integer,
                  blood_culture_met                integer,
                  blood_culture_unmet              integer,
                  crystalloid_fluid_met            integer,
                  crystalloid_fluid_unmet          integer,
                  initial_lactate_met              integer,
                  initial_lactate_unmet            integer,
                  repeat_lactate_met               integer,
                  repeat_lactate_unmet             integer,
                  vasopressors_met                 integer,
                  vasopressors_unmet               integer,
                  length_of_stay_hrs               double precision
  )
language plpgsql
as $func$ begin
  return query
    select
           -- Dimensions
           R.w_max_state,
           R.pat_id,
           E.enc_id,
           R.severe_sepsis_onset,
           R.severe_sepsis_wo_infection_onset,
           R.septic_shock_onset,
           (case when S.fid = 'age' then S.value else null end) as age,
           (case when S.fid = 'gender' then S.value else null end) as gender,

           CU.enter_time as care_unit_entry,
           CU.care_unit as care_unit,
            -- Note: Care unit can be null on:
            -- i) no match for this care unit (which should be ignored by later functions)
            -- ii) data delays, and no matches across all patients (which should be handled by later functions)

           -- Metrics
           (case when (R.criteria#>>'{antibiotics_order, met}')::boolean       then 1 else 0 end) as antibiotics_met,
           (case when (R.criteria#>>'{antibiotics_order, met}')::boolean       then 0 else 1 end) as antibiotics_unmet,
           (case when (R.criteria#>>'{blood_culture_order, met}')::boolean     then 1 else 0 end) as blood_culture_met,
           (case when (R.criteria#>>'{blood_culture_order, met}')::boolean     then 0 else 1 end) as blood_culture_unmet,
           (case when (R.criteria#>>'{crystalloid_fluid_order, met}')::boolean then 1 else 0 end) as crystalloid_fluid_met,
           (case when (R.criteria#>>'{crystalloid_fluid_order, met}')::boolean then 0 else 1 end) as crystalloid_fluid_unmet,
           (case when (R.criteria#>>'{initial_lactate_order, met}')::boolean   then 1 else 0 end) as initial_lactate_met,
           (case when (R.criteria#>>'{initial_lactate_order, met}')::boolean   then 0 else 1 end) as initial_lactate_unmet,
           (case when (R.criteria#>>'{repeat_lactate_order, met}')::boolean    then 1 else 0 end) as repeat_lactate_met,
           (case when (R.criteria#>>'{repeat_lactate_order, met}')::boolean    then 0 else 1 end) as repeat_lactate_unmet,
           (case when (R.criteria#>>'{vasopressors_order, met}')::boolean      then 1 else 0 end) as vasopressors_met,
           (case when (R.criteria#>>'{vasopressors_order, met}')::boolean      then 0 else 1 end) as vasopressors_unmet,

           -- Length of stay
           (extract(epoch from E.departure) - extract(epoch from E.arrival))/3600.0 as length_of_stay_hrs

           -- TODO:
           -- Readmissions
    from tabulate_criteria(_pat_state, _dataset_id, _label_id) R
    inner join match_report_encounters(_pat_state, _dataset_id, _label_id) E
      on R.pat_id = E.pat_id
    left join care_unit CU
      on E.enc_id = CU.enc_id
      and R.dataset_id = CU.dataset_id
      and coalesce(R.severe_sepsis_onset, R.severe_sepsis_wo_infection_onset) between CU.enter_time and CU.leave_time
    left join cdm_s S
      on E.enc_id = S.enc_id and R.dataset_id = S.dataset_id
      and S.fid in ('age', 'gender')
    ;
end; $func$;


--
-- Returns compliance statistics by state and care unit across dataset.
create or replace function full_compliance_report(_pat_state  integer,
                                                  _dataset_id integer,
                                                  _label_id   integer)
  returns table ( pat_worst_state           integer,
                  age                       text,
                  gender                    text,
                  care_unit                 text,
                  hour_of_day               integer,
                  num_patients              numeric,
                  antibiotics_met           numeric,
                  antibiotics_unmet         numeric,
                  blood_culture_met         numeric,
                  blood_culture_unmet       numeric,
                  crystalloid_fluid_met     numeric,
                  crystalloid_fluid_unmet   numeric,
                  initial_lactate_met       numeric,
                  initial_lactate_unmet     numeric,
                  repeat_lactate_met        numeric,
                  repeat_lactate_unmet      numeric,
                  vasopressors_met          numeric,
                  vasopressors_unmet        numeric,
                  length_of_stay_hrs        numeric
  )
language plpgsql
as $func$ begin
  return query
    select ByPat.pat_worst_state,
           ByPat.age,
           ByPat.gender,
           ByPat.care_unit,
           ByPat.hour_of_day,
           count(*)::numeric                           as num_patients,
           sum(ByPat.antibiotics_met)::numeric         as antibiotics_met,
           sum(ByPat.antibiotics_unmet)::numeric       as antibiotics_unmet,
           sum(ByPat.blood_culture_met)::numeric       as blood_culture_met,
           sum(ByPat.blood_culture_unmet)::numeric     as blood_culture_unmet,
           sum(ByPat.crystalloid_fluid_met)::numeric   as crystalloid_fluid_met,
           sum(ByPat.crystalloid_fluid_unmet)::numeric as crystalloid_fluid_unmet,
           sum(ByPat.initial_lactate_met)::numeric     as initial_lactate_met,
           sum(ByPat.initial_lactate_unmet)::numeric   as initial_lactate_unmet,
           sum(ByPat.repeat_lactate_met)::numeric      as repeat_lactate_met,
           sum(ByPat.repeat_lactate_unmet)::numeric    as repeat_lactate_unmet,
           sum(ByPat.vasopressors_met)::numeric        as vasopressors_met,
           sum(ByPat.vasopressors_unmet)::numeric      as vasopressors_unmet,
           max(ByPat.length_of_stay_hrs)::numeric      as length_of_stay_hrs
    from (
      select ByPatEnc.pat_worst_state,
             ByPatEnc.pat_id,
             first(ByPatEnc.age)                   as age,
             first(ByPatEnc.gender)                as gender,
             first(ByPatEnc.care_unit)             as care_unit,
             first(ByPatEnc.hour_of_day)           as hour_of_day,
             sum(ByPatEnc.antibiotics_met)         as antibiotics_met,
             sum(ByPatEnc.antibiotics_unmet)       as antibiotics_unmet,
             sum(ByPatEnc.blood_culture_met)       as blood_culture_met,
             sum(ByPatEnc.blood_culture_unmet)     as blood_culture_unmet,
             sum(ByPatEnc.crystalloid_fluid_met)   as crystalloid_fluid_met,
             sum(ByPatEnc.crystalloid_fluid_unmet) as crystalloid_fluid_unmet,
             sum(ByPatEnc.initial_lactate_met)     as initial_lactate_met,
             sum(ByPatEnc.initial_lactate_unmet)   as initial_lactate_unmet,
             sum(ByPatEnc.repeat_lactate_met)      as repeat_lactate_met,
             sum(ByPatEnc.repeat_lactate_unmet)    as repeat_lactate_unmet,
             sum(ByPatEnc.vasopressors_met)        as vasopressors_met,
             sum(ByPatEnc.vasopressors_unmet)      as vasopressors_unmet,
             max(ByPatEnc.length_of_stay_hrs)      as length_of_stay_hrs
      from (
        select R.pat_worst_state,
               R.pat_id,
               R.enc_id,
               first(R.age) as age,
               first(R.gender) as gender,

               -- Handle when an encounter's ssp/sspwoi onset is outside any care unit.
               -- This occurs when data arrives after discharge, and currently we attribute
               -- the event to the last care unit.
               coalesce(
                  first(R.care_unit order by R.care_unit_entry desc),
                  (select first(CU.care_unit order by CU.enter_time desc)
                    from care_unit CU
                    where CU.dataset_id = _dataset_id and CU.enc_id = R.enc_id)
                ) as care_unit,

               first(date_part('hour', coalesce(R.severe_sepsis_onset, R.severe_sepsis_wo_infection_onset)))::int as hour_of_day,

               max(R.antibiotics_met)         as antibiotics_met,
               max(R.antibiotics_unmet)       as antibiotics_unmet,
               max(R.blood_culture_met)       as blood_culture_met,
               max(R.blood_culture_unmet)     as blood_culture_unmet,
               max(R.crystalloid_fluid_met)   as crystalloid_fluid_met,
               max(R.crystalloid_fluid_unmet) as crystalloid_fluid_unmet,
               max(R.initial_lactate_met)     as initial_lactate_met,
               max(R.initial_lactate_unmet)   as initial_lactate_unmet,
               max(R.repeat_lactate_met)      as repeat_lactate_met,
               max(R.repeat_lactate_unmet)    as repeat_lactate_unmet,
               max(R.vasopressors_met)        as vasopressors_met,
               max(R.vasopressors_unmet)      as vasopressors_unmet,
               max(R.length_of_stay_hrs)      as length_of_stay_hrs

        from tabulate_compliance(_pat_state, _dataset_id, _label_id) R
        group by R.pat_worst_state, R.pat_id, R.enc_id
      ) ByPatEnc
      group by ByPatEnc.pat_worst_state, ByPatEnc.pat_id
    ) ByPat
    group by ByPat.pat_worst_state, ByPat.age, ByPat.gender, ByPat.care_unit, ByPat.hour_of_day
    order by ByPat.pat_worst_state desc, ByPat.care_unit, ByPat.hour_of_day;
end; $func$;


--
-- Returns compliance statistics by state and time period across dataset.
create or replace function unit_compliance_report(_pat_state  integer,
                                                  _dataset_id integer,
                                                  _label_id   integer)
  returns table ( pat_worst_state           integer,
                  care_unit                 text,
                  num_patients              numeric,
                  antibiotics_met           numeric,
                  antibiotics_unmet         numeric,
                  blood_culture_met         numeric,
                  blood_culture_unmet       numeric,
                  crystalloid_fluid_met     numeric,
                  crystalloid_fluid_unmet   numeric,
                  initial_lactate_met       numeric,
                  initial_lactate_unmet     numeric,
                  repeat_lactate_met        numeric,
                  repeat_lactate_unmet      numeric,
                  vasopressors_met          numeric,
                  vasopressors_unmet        numeric
  )
language plpgsql
as $func$ begin
  return query
    select R.pat_worst_state,
           R.care_unit,
           sum(R.num_patients)            as num_patients,
           sum(R.antibiotics_met)         as antibiotics_met,
           sum(R.antibiotics_unmet)       as antibiotics_unmet,
           sum(R.blood_culture_met)       as blood_culture_met,
           sum(R.blood_culture_unmet)     as blood_culture_unmet,
           sum(R.crystalloid_fluid_met)   as crystalloid_fluid_met,
           sum(R.crystalloid_fluid_unmet) as crystalloid_fluid_unmet,
           sum(R.initial_lactate_met)     as initial_lactate_met,
           sum(R.initial_lactate_unmet)   as initial_lactate_unmet,
           sum(R.repeat_lactate_met)      as repeat_lactate_met,
           sum(R.repeat_lactate_unmet)    as repeat_lactate_unmet,
           sum(R.vasopressors_met)        as vasopressors_met,
           sum(R.vasopressors_unmet)      as vasopressors_unmet
    from full_compliance_report(_pat_state, _dataset_id, _label_id) R
    group by R.pat_worst_state, R.care_unit
    order by R.pat_worst_state desc, R.care_unit;
end; $func$;


--
-- Returns compliance statistics by state and time period across dataset.
create or replace function timing_compliance_report(_pat_state  integer,
                                                    _dataset_id integer,
                                                    _label_id   integer)
  returns table ( pat_worst_state           integer,
                  hour_of_day               integer,
                  num_patients              numeric,
                  antibiotics_met           numeric,
                  antibiotics_unmet         numeric,
                  blood_culture_met         numeric,
                  blood_culture_unmet       numeric,
                  crystalloid_fluid_met     numeric,
                  crystalloid_fluid_unmet   numeric,
                  initial_lactate_met       numeric,
                  initial_lactate_unmet     numeric,
                  repeat_lactate_met        numeric,
                  repeat_lactate_unmet      numeric,
                  vasopressors_met          numeric,
                  vasopressors_unmet        numeric
  )
language plpgsql
as $func$ begin
  return query
    select R.pat_worst_state,
           (R.hour_of_day % 6) * 4        as hour_of_day,
           sum(R.num_patients)            as num_patients,
           sum(R.antibiotics_met)         as antibiotics_met,
           sum(R.antibiotics_unmet)       as antibiotics_unmet,
           sum(R.blood_culture_met)       as blood_culture_met,
           sum(R.blood_culture_unmet)     as blood_culture_unmet,
           sum(R.crystalloid_fluid_met)   as crystalloid_fluid_met,
           sum(R.crystalloid_fluid_unmet) as crystalloid_fluid_unmet,
           sum(R.initial_lactate_met)     as initial_lactate_met,
           sum(R.initial_lactate_unmet)   as initial_lactate_unmet,
           sum(R.repeat_lactate_met)      as repeat_lactate_met,
           sum(R.repeat_lactate_unmet)    as repeat_lactate_unmet,
           sum(R.vasopressors_met)        as vasopressors_met,
           sum(R.vasopressors_unmet)      as vasopressors_unmet
    from full_compliance_report(_pat_state, _dataset_id, _label_id) R
    group by R.pat_worst_state, (R.hour_of_day % 6) * 4
    order by R.pat_worst_state desc, hour_of_day;
end; $func$;


create or replace function age_compliance_report(_pat_state  integer,
                                                 _dataset_id integer,
                                                 _label_id   integer)
  returns table ( pat_worst_state           integer,
                  age                       integer,
                  num_patients              numeric,
                  antibiotics_met           numeric,
                  antibiotics_unmet         numeric,
                  blood_culture_met         numeric,
                  blood_culture_unmet       numeric,
                  crystalloid_fluid_met     numeric,
                  crystalloid_fluid_unmet   numeric,
                  initial_lactate_met       numeric,
                  initial_lactate_unmet     numeric,
                  repeat_lactate_met        numeric,
                  repeat_lactate_unmet      numeric,
                  vasopressors_met          numeric,
                  vasopressors_unmet        numeric
  )
language plpgsql
as $func$ begin
  return query
    select R.pat_worst_state,
           (floor(R.age::int / 5) * 5)::int as age,
           sum(R.num_patients)              as num_patients,
           sum(R.antibiotics_met)           as antibiotics_met,
           sum(R.antibiotics_unmet)         as antibiotics_unmet,
           sum(R.blood_culture_met)         as blood_culture_met,
           sum(R.blood_culture_unmet)       as blood_culture_unmet,
           sum(R.crystalloid_fluid_met)     as crystalloid_fluid_met,
           sum(R.crystalloid_fluid_unmet)   as crystalloid_fluid_unmet,
           sum(R.initial_lactate_met)       as initial_lactate_met,
           sum(R.initial_lactate_unmet)     as initial_lactate_unmet,
           sum(R.repeat_lactate_met)        as repeat_lactate_met,
           sum(R.repeat_lactate_unmet)      as repeat_lactate_unmet,
           sum(R.vasopressors_met)          as vasopressors_met,
           sum(R.vasopressors_unmet)        as vasopressors_unmet
    from full_compliance_report(_pat_state, _dataset_id, _label_id) R
    group by R.pat_worst_state, floor(R.age::int / 5) * 5
    order by R.pat_worst_state desc, age;
end; $func$;


create or replace function gender_compliance_report(_pat_state  integer,
                                                    _dataset_id integer,
                                                    _label_id   integer)
  returns table ( pat_worst_state           integer,
                  gender                    text,
                  num_patients              numeric,
                  antibiotics_met           numeric,
                  antibiotics_unmet         numeric,
                  blood_culture_met         numeric,
                  blood_culture_unmet       numeric,
                  crystalloid_fluid_met     numeric,
                  crystalloid_fluid_unmet   numeric,
                  initial_lactate_met       numeric,
                  initial_lactate_unmet     numeric,
                  repeat_lactate_met        numeric,
                  repeat_lactate_unmet      numeric,
                  vasopressors_met          numeric,
                  vasopressors_unmet        numeric
  )
language plpgsql
as $func$ begin
  return query
    select R.pat_worst_state,
           R.gender                       as gender,
           sum(R.num_patients)            as num_patients,
           sum(R.antibiotics_met)         as antibiotics_met,
           sum(R.antibiotics_unmet)       as antibiotics_unmet,
           sum(R.blood_culture_met)       as blood_culture_met,
           sum(R.blood_culture_unmet)     as blood_culture_unmet,
           sum(R.crystalloid_fluid_met)   as crystalloid_fluid_met,
           sum(R.crystalloid_fluid_unmet) as crystalloid_fluid_unmet,
           sum(R.initial_lactate_met)     as initial_lactate_met,
           sum(R.initial_lactate_unmet)   as initial_lactate_unmet,
           sum(R.repeat_lactate_met)      as repeat_lactate_met,
           sum(R.repeat_lactate_unmet)    as repeat_lactate_unmet,
           sum(R.vasopressors_met)        as vasopressors_met,
           sum(R.vasopressors_unmet)      as vasopressors_unmet
    from full_compliance_report(_pat_state, _dataset_id, _label_id) R
    group by R.pat_worst_state, R.gender
    order by R.pat_worst_state desc, gender;
end; $func$;


create or replace function los_compliance_report(_pat_state  integer,
                                                 _dataset_id integer,
                                                 _label_id   integer)
  returns table ( pat_worst_state           integer,
                  los                       numeric,
                  num_patients              numeric,
                  antibiotics_met           numeric,
                  antibiotics_unmet         numeric,
                  blood_culture_met         numeric,
                  blood_culture_unmet       numeric,
                  crystalloid_fluid_met     numeric,
                  crystalloid_fluid_unmet   numeric,
                  initial_lactate_met       numeric,
                  initial_lactate_unmet     numeric,
                  repeat_lactate_met        numeric,
                  repeat_lactate_unmet      numeric,
                  vasopressors_met          numeric,
                  vasopressors_unmet        numeric
  )
language plpgsql
as $func$ begin
  return query
    select R.pat_worst_state,
           (floor(R.length_of_stay_hrs / 24) * 24) as los,

           sum(R.num_patients)            as num_patients,
           sum(R.antibiotics_met)         as antibiotics_met,
           sum(R.antibiotics_unmet)       as antibiotics_unmet,
           sum(R.blood_culture_met)       as blood_culture_met,
           sum(R.blood_culture_unmet)     as blood_culture_unmet,
           sum(R.crystalloid_fluid_met)   as crystalloid_fluid_met,
           sum(R.crystalloid_fluid_unmet) as crystalloid_fluid_unmet,
           sum(R.initial_lactate_met)     as initial_lactate_met,
           sum(R.initial_lactate_unmet)   as initial_lactate_unmet,
           sum(R.repeat_lactate_met)      as repeat_lactate_met,
           sum(R.repeat_lactate_unmet)    as repeat_lactate_unmet,
           sum(R.vasopressors_met)        as vasopressors_met,
           sum(R.vasopressors_unmet)      as vasopressors_unmet
    from full_compliance_report(_pat_state, _dataset_id, _label_id) R
    group by R.pat_worst_state, (floor(R.length_of_stay_hrs / 24) * 24)
    order by R.pat_worst_state desc, los;
end; $func$;


--------------------------------------------------
-- Histogram queries.

--
-- Returns a histogram of durations between sirs/orgdf onset
-- and severe sepsis onset by patient state.
create or replace function soi_onset_histogram(_pat_state        integer,
                                               _dataset_id       integer,
                                               _label_id         integer,
                                               _bucket_size_secs double precision)
  returns table ( pat_worst_state int, bucket double precision, n bigint )
language plpgsql
as $func$ begin
  return query
    select D.w_max_state, floor(D.secs / _bucket_size_secs) * _bucket_size_secs as bucket, count(*)
    from (
      select R.w_max_state,
             extract(epoch from R.severe_sepsis_onset)
                - extract(epoch from R.severe_sepsis_wo_infection_onset)
                as secs
      from tabulate_criteria(_pat_state, _dataset_id, _label_id) R
    ) D
    group by D.w_max_state, floor(D.secs / _bucket_size_secs)
    order by D.w_max_state desc, bucket;
end; $func$;

--
-- Returns a histogram of durations (positive and negative) between sirs/orgdf onset
-- and note times by patient state.
create or replace function note_gap_histogram(_pat_state        integer,
                                              _dataset_id       integer,
                                              _label_id         integer,
                                              _bucket_size_secs double precision)
  returns table ( pat_worst_state int,
                  bucket          double precision,
                  n               bigint,
                  state_total     numeric )
language plpgsql
as $func$ begin
  return query
    select Hist.*,
           sum(Hist.n) over (partition by Hist.w_max_state) as state_total
    from (
      select D.w_max_state, floor(D.secs / _bucket_size_secs) * _bucket_size_secs as bucket, count(*) as n
      from (
        select C.w_max_state,
               extract(epoch from coalesce(C.override_time, C.measurement_time))
                  - extract(epoch from C.severe_sepsis_wo_infection_onset)
                  as secs
        from cdm_reports C
        where C.dataset_id  = coalesce(_dataset_id, C.dataset_id)
        and   C.label_id    = coalesce(_label_id,   C.label_id)
        and   C.w_max_state = coalesce(_pat_state,  C.w_max_state)
        and   C.name = 'suspicion_of_infection'
      ) D
      group by D.w_max_state, floor(D.secs / _bucket_size_secs)
    ) Hist
    order by Hist.w_max_state desc, Hist.bucket;
end; $func$;
