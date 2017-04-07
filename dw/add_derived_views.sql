-- ===========================================================================
-- add_cdm_feats_2_criteria_meas
-- ===========================================================================
CREATE OR REPLACE FUNCTION add_cdm_feats_2_criteria_meas(_fid text, _category text)
  returns VOID
   LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN

    IF _category = 'T' THEN
      EXECUTE format(
        'insert into criteria_meas (dataset_id,         pat_id,         tsp,               fid,               value,    update_date)
          select              cdm_t.dataset_id, pat_enc.pat_id,   cdm_t.tsp,         cdm_t.fid,  first(cdm_t.value),    now()
          FROM
          cdm_t
          inner join
          pat_enc
          on cdm_t.enc_id = pat_enc.enc_id
          where fid = ''%s''::text
          group by cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp, cdm_t.fid
          ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
        ',_fid);
    ELSIF _category = 'TWF' THEN
        EXECUTE format(
        'insert into criteria_meas (dataset_id,         pat_id,         tsp,         fid,       value,      update_date)
          select            cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp,      ''%s''::text,  cdm_twf.%s,      now()
          FROM
          cdm_twf
          inner join
          pat_enc
          on cdm_twf.enc_id = pat_enc.enc_id
          where cdm_twf.%s_c <8
          ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
        ',_fid,_fid,_fid);
    else
       raise notice 'unknown feature type % for feature %', _category, _fid;
    END IF;

END; $function$;
-- ===========================================================================
-- generate_criteria_inputs
-- ===========================================================================
CREATE OR REPLACE FUNCTION generate_criteria_inputs()
 RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE
  _fid TEXT;
  _cat TEXT;
BEGIN
    -- ================================================
    -- Upsert Suspicion of infection proxy
    -- ================================================
    insert into criteria (dataset_id, pat_id, name,                  is_met, measurement_time,override_time,override_user, override_value, value, update_date)
    select cdm_t.dataset_id, pat_enc.pat_id, 'suspicion_of_infection', true, cdm_t.tsp, cdm_t.tsp,          'cdm_t'::text, '[{"text": "infection"}]'::json,'infection'::text,now()
    from
    cdm_t
    left join
    pat_enc
    on cdm_t.enc_id = pat_enc.enc_id
    where cdm_t.fid='suspicion_of_infection'
    group by cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp
    ON CONFLICT (dataset_id, pat_id, name, override_time) DO UPDATE SET
      is_met=EXCLUDED.is_met,              measurement_time=EXCLUDED.measurement_time,
      override_user=EXCLUDED.override_user,override_value=EXCLUDED.override_value,    value=EXCLUDED.value,  update_date=EXCLUDED.update_date;
    -- ================================================
    -- Upsert platelet from platelets
    -- ================================================
    insert into criteria_meas (dataset_id, pat_id, tsp, fid, value, update_date)
    select                     cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp, 'platelet'::text, cdm_twf.platelets, now()
    FROM
    cdm_twf
    inner join
    pat_enc
    on cdm_twf.enc_id = pat_enc.enc_id
    where cdm_twf.platelets_c <8
    ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
    -- ================================================
    -- Upsert normal cases
    -- ================================================
    --  Oneday, I'll replace will a lateral
    FOR _fid, _cat in
      select cd.fid, first(f.category) as category
      from
      criteria_default cd
      left join
      cdm_feature f
      on cd.fid = f.fid
      where cd.fid not in ('suspicion_of_infection','platelet')
      group by cd.fid
    LOOP
      PERFORM add_cdm_feats_2_criteria_meas(_fid, _cat);
    END LOOP;

    --   select cd.fid, first(f.category) as cat
    --   from
    --   criteria_default cd
    --   left join
    --   cdm_feature f
    --   on cd.fid = f.fid
    --   NATURAL join lateral add_cdm_feats_2_criteria_meas(cd.fid, cat)
    --   where cd.fid not in ('suspicion_of_infection','platelet')
    --   group by cd.fid;

END; $function$;
-- ===========================================================================
-- calculate historical_criteria
-- ===========================================================================
CREATE OR REPLACE FUNCTION calculate_historical_criteria(this_pat_id text)
 RETURNS table(window_ts                        timestamptz,
               pat_id                           varchar(50),
               pat_state                        INTEGER
               )
 LANGUAGE plpgsql
AS $function$
DECLARE
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    create temporary table new_criteria_windows as
        select window_ends.tsp as ts, new_criteria.*
        from (  select distinct meas.pat_id, meas.tsp from criteria_meas meas
                where meas.pat_id = coalesce(this_pat_id, meas.pat_id)
--                 and meas.tsp between ts_start and ts_end
        ) window_ends
        inner join lateral calculate_criteria(
            coalesce(this_pat_id, window_ends.pat_id), window_ends.tsp - window_size, window_ends.tsp
        ) new_criteria
        on window_ends.pat_id = new_criteria.pat_id;

    return query
            select sw.*
            from get_window_states('new_criteria_windows', this_pat_id) sw;
    drop table new_criteria_windows;
    return;
END; $function$;