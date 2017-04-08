----------------------------------------------------------------------------------------------
-- create_udf.sql
-- create all user defined functions
-- best practice: run this file every time when we deploy new version
----------------------------------------------------------------------------------------------
-- add_cdm_t for medication summation
CREATE OR REPLACE FUNCTION add_cdm_t(key1 INT, key2 timestamptz, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        IF new_value ~ '^[0-9\.]+$' THEN
            UPDATE cdm_t SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
        ELSE

            UPDATE cdm_t SET value = json_object_set_key(value::json, 'dose',
                (value::json->>'dose')::numeric
                    + (new_value::json->>'dose')::numeric)::text
                , confidence = confidence | confidence_flag
                WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
        END IF;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_t(enc_id,tsp,fid,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

-- add_cdm_t for medication summation
CREATE OR REPLACE FUNCTION add_cdm_t(dsid INT, key1 INT, key2 timestamptz, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        IF new_value ~ '^[0-9\.]+$' THEN
            UPDATE cdm_t SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND tsp = key2 AND fid = key3 AND dataset_id = dsid;
        ELSE
            UPDATE cdm_t SET value = json_object_set_key(value::json, 'dose',
                (value::json->>'dose')::numeric
                    + (new_value::json->>'dose')::numeric)::text
                , confidence = confidence | confidence_flag
                WHERE enc_id = key1 AND tsp = key2 AND fid = key3 AND dataset_id = dsid;
        END IF;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_t(dataset_id,enc_id,tsp,fid,value,confidence) VALUES (dsid,key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION based_on_popmean(confidence INT) RETURNS integer AS $$
    BEGIN
        IF confidence is null THEN RETURN 0;
        END IF;
        RETURN confidence & 16 >> 4;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION "json_object_del_key"(
  "json"          json,
  "key_to_del"    TEXT
)
  RETURNS json
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT CASE
  WHEN ("json" -> "key_to_del") IS NULL THEN "json"
  ELSE (SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')
          FROM (SELECT *
                  FROM json_each("json")
                 WHERE "key" <> "key_to_del"
               ) AS "fields")::json
END
$function$;

CREATE OR REPLACE FUNCTION "json_object_set_key"(
  "json"          json,
  "key_to_set"    TEXT,
  "value_to_set"  anyelement
)
  RETURNS json
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')::json
  FROM (SELECT *
          FROM json_each("json")
         WHERE "key" <> "key_to_set"
         UNION ALL
        SELECT "key_to_set", to_json("value_to_set")) AS "fields"
$function$;

-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;

-- And then wrap an aggregate around it
DROP AGGREGATE IF EXISTS public.FIRST(anyelement);
CREATE AGGREGATE public.FIRST (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);

-- Create a function that always returns the last non-NULL item
CREATE OR REPLACE FUNCTION public.last_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;

-- And then wrap an aggregate around it
DROP AGGREGATE IF EXISTS public.LAST(anyelement);
CREATE AGGREGATE public.LAST (
        sfunc    = public.last_agg,
        basetype = anyelement,
        stype    = anyelement
);

CREATE OR REPLACE FUNCTION calculate_popmean(target TEXT, fid TEXT, dataset_id int)
RETURNS real
AS $BODY$
DECLARE
    fid_c TEXT;
    fid_popmean TEXT;
    popmean TEXT;
BEGIN
    fid_c = fid || '_c';
    fid_popmean = fid || '_popmean';
    EXECUTE 'SELECT cast(avg('|| quote_ident(fid) ||
        ') AS text) FROM ' || target || '  WHERE '|| quote_ident(fid_c)
        ||' < 8 and dataset_id = ' || dataset_id INTO popmean;
    RAISE NOTICE '% = %', fid_popmean, popmean;
    EXECUTE 'SELECT INSERT INTO cdm_g (dataset_id,model_id,value,confidence) values (' || dataset_id || ',' || model_id || ',' ||quote_literal(fid_popmean)||', '||quote_literal(popmean)||', 24)';
    return popmean;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION last_value_in_window(fid TEXT, target TEXT, win_h real, recalculate_popmean boolean, dataset_id int, model_id int)
RETURNS VOID
AS $BODY$
DECLARE
    row record;
    block_enc_id INT;
    block_value real;
    block_c int;
    fid_c TEXT;
    block_start timestamptz;
    block_end timestamptz;
    block_rows int;
    popmean real;
    tsp_diff_secs int;
    win_secs int;
    fid_popmean TEXT;
BEGIN
    block_enc_id = -1;
    block_value = 0;
    block_c = 0;
    block_start = '-infinity';
    fid_c = fid || '_c';
    win_secs = round(win_h * 3600);
    -- clean filled values
    EXECUTE 'UPDATE ' || target || ' SET('
        || quote_ident(fid) ||', '|| quote_ident(fid_c) ||
        ') = (null, null) WHERE '|| quote_ident(fid_c) ||' >= 8';
    IF recalculate_popmean THEN
        -- calculate population mean
        SELECT INTO popmean calculate_popmean(target, fid, dataset_id, model_id);
    ELSE
        fid_popmean = fid || '_popmean';
        EXECUTE 'SELECT value from cdm_g where fid = ' || quote_literal(fid_popmean) || ' and dataset_id = ' || dataset_id || ' and model_id = ' || model_id INTO popmean;
    END IF;
    RAISE NOTICE 'popmean:%', popmean;
    FOR row IN EXECUTE 'SELECT enc_id, tsp, '
        || quote_ident(fid) ||' fv, '|| quote_ident(fid_c)
        ||' fc FROM ' || target || ' where dataset_id = ' || dataset_id || '  ORDER BY enc_id, tsp'
    LOOP
        -- RAISE NOTICE '%', row;
        IF block_enc_id <> row.enc_id THEN
            -- if this is a new enc_id
            block_end = 'infinity';
            IF block_rows > 0 THEN
                EXECUTE 'UPDATE ' || target || '  SET ('
                    || quote_ident(fid) ||', '|| quote_ident(fid_c) ||') = ('
                    || block_value || ', (8 |'||block_c||')) WHERE enc_id='
                    || block_enc_id || ' and tsp >= ' || quote_literal(block_start)
                    || ' AND '|| quote_ident(fid_c) ||' is null AND dataset_id = ' || dataset_id;
                -- raise notice 'execute update %', block_rows;
            END IF;

            -- create new block
            -- RAISE NOTICE 'new block for enc_id %', row.enc_id;
            block_enc_id = row.enc_id;
            block_value = popmean;
            block_c = 16;
            block_start = row.tsp;
            block_rows = 0;
        END IF;
        SELECT EXTRACT(EPOCH FROM row.tsp - block_start)::int INTO tsp_diff_secs;
        IF row.fv is not null and row.fc is not null THEN
            -- this row contain measured value
            block_end = row.tsp;
            IF block_rows > 0 THEN
                EXECUTE 'UPDATE ' || target || '  SET ('
                    || quote_ident(fid) ||', '|| quote_ident(fid_c) ||') = ('
                    || block_value || ',(8 | '||block_c||')) WHERE enc_id='
                    || block_enc_id || ' and tsp >= ' || quote_literal(block_start)
                    || ' and tsp <= ' || quote_literal(block_end)
                    || ' AND '|| quote_ident(fid_c) ||' is null AND dataset_id = ' || dataset_id;
                -- raise notice 'execute update %', block_rows;
            END IF;
            -- create new block
            -- RAISE NOTICE 'new block for enc_id %', row.enc_id;
            block_enc_id = row.enc_id;
            block_value = row.fv;
            block_c = row.fc;
            block_start = row.tsp;
            block_rows = 0;
        ELSIF tsp_diff_secs > win_secs THEN
            -- if current tsp is out of window, i.e., invalid
            block_end = row.tsp;
            IF block_rows > 0 THEN
                EXECUTE 'UPDATE ' || target || '  SET ('
                    || quote_ident(fid) ||', '|| quote_ident(fid_c) ||') = ('
                    || block_value || ', (8 | '||block_c||')) WHERE enc_id='
                    || block_enc_id || ' and tsp >= ' || quote_literal(block_start)
                    || ' and tsp < ' || quote_literal(block_end)
                    || ' AND '|| quote_ident(fid_c) ||' is null AND dataset_id = ' || dataset_id;
                -- raise notice 'execute update %', block_rows;
            END IF;
            -- create new block
            -- RAISE NOTICE 'new block for enc_id %', row.enc_id;
            block_enc_id = row.enc_id;
            block_value = popmean;
            block_c = 16;
            block_start = row.tsp;
            block_rows = 1;
        ELSE
            block_rows = block_rows + 1;
            -- RAISE NOTICE 'block_rows: %', block_rows;
        END IF;
    END LOOP;
    IF block_rows > 0 THEN
        EXECUTE 'UPDATE ' || target || '  SET ('
            || quote_ident(fid) ||', '|| quote_ident(fid_c) ||') = ('
            || block_value || ', (8 | '||block_c||')) WHERE enc_id='
            || block_enc_id || ' and tsp >= ' || quote_literal(block_start)
            || ' AND '|| quote_ident(fid_c) ||' is null AND dataset_id = ' || dataset_id;
        -- raise notice 'execute update %', block_rows;
    END IF;
END
$BODY$
LANGUAGE plpgsql;


----------------------------
-- UDFs for TREWS
----------------------------
CREATE OR REPLACE FUNCTION get_parameter(key text)
RETURNS text as
$$
select value from parameters where name = key;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION reset_parameter()
RETURNS void as
$$
BEGIN
update parameters
    set value = '6 hours' where name = 'lookbackhours';
update parameters
    set value = '12 hours' where name = 'notifications_expire_hours';
RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION isnumeric(text) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
    x = $1::NUMERIC;
    RETURN TRUE;
EXCEPTION WHEN others THEN
    RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION isnumeric(double precision) RETURNS BOOLEAN AS $$
BEGIN
    RETURN TRUE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

create or replace function max_order_status(s1 text, s2 text) returns text as $$
BEGIN
return  (
        case
        when s1 = s2 then s1
        when s1 = 'Completed' or s2 = 'Completed' then 'Completed'
        else s1
        end
        );
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_states_snapshot(this_pat_id text)
RETURNS table( pat_id                           varchar(50),
               state                            int,
               severe_sepsis_onset              timestamptz,
               septic_shock_onset               timestamptz,
               severe_sepsis_wo_infection_onset timestamptz,
               event_id                         int)
AS $func$ BEGIN RETURN QUERY
select pat_enc.pat_id,
       coalesce(ce.flag, 0),
       ce.severe_sepsis_onset,
       ce.septic_shock_onset,
       ce.severe_sepsis_wo_infection_onset,
       ce.event_id
from (select distinct pat_enc.pat_id from pat_enc) as pat_enc
left join
(select criteria_events.pat_id, max(flag) flag,
    GREATEST( max(case when name = 'suspicion_of_infection' then override_time else null end),
        (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[2],
        min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ))
    as severe_sepsis_onset,
    LEAST(
        min(measurement_time) filter (where name in ('systolic_bp','hypotension_map','hypotension_dsbp') and is_met ),
        min(measurement_time) filter (where name = 'initial_lactate' and is_met)
    ) as septic_shock_onset,
    GREATEST(
        (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[2],
        min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ))
    as severe_sepsis_wo_infection_onset,
    max(criteria_events.event_id) event_id
 from
 criteria_events
 where criteria_events.pat_id = coalesce(this_pat_id, criteria_events.pat_id) and
       criteria_events.event_id = (select max(ce2.event_id) from criteria_events ce2 where ce2.pat_id = criteria_events.pat_id and flag >= 0)
 group by criteria_events.pat_id
)
as ce on pat_enc.pat_id = ce.pat_id
where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
; END $func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_states(table_name text, this_pat_id text)
RETURNS table( pat_id varchar(50), state int) AS $func$ BEGIN RETURN QUERY EXECUTE
format('select stats.pat_id,
    (
    case
    when sus_count = 1 then
        (
        case when sirs_count > 1 and organ_count > 0 then (
            (
            case
            when (fluid_count = 1 and hypotension_count > 0) and hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when (fluid_count = 1 and hypotension_count > 0) then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 1 then 23 -- sev_sep_6hr_com
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com
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
    when sirs_count > 1 and organ_count > 0 and sus_null_count = 1 then 10 -- sev_sep w.o. sus
    when sirs_count > 1 and organ_count > 0 and sus_noinf_count = 1 then 12 -- sev_sep w.o. sus
    else 0 -- health case
    end) as state
from
(
select %I.pat_id,
    count(*) filter (where name = ''suspicion_of_infection'' and is_met) as sus_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value#>>''{0,text}'' = ''No Infection'') as sus_noinf_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value is null) as sus_null_count,
    count(*) filter (where name = ''crystalloid_fluid'' and is_met) as fluid_count,
    count(*) filter (where name = ''initial_lactate'' and is_met) as hypoperfusion_count,
    count(*) filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) as sirs_count,
    count(*) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met ) as organ_count,
    count(*) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met ) as hypotension_count,
    count(*) filter (where name in (''initial_lactate_order'',''blood_culture_order'',''antibiotics_order'', ''crystalloid_fluid_order'') and is_met ) as sev_sep_3hr_count,
    count(*) filter (where name = ''repeat_lactate_order'' and is_met ) as sev_sep_6hr_count,
    count(*) filter (where name = ''vasopressors_order'' and is_met ) as sep_sho_6hr_count,
    first(override_time) filter (where name = ''suspicion_of_infection'' and is_met) as sus_onset,
    (array_agg(measurement_time order by measurement_time)  filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) )[2]   as sirs_onset,
    min(measurement_time) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met ) as organ_onset,
    min(measurement_time) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met ) as hypotension_onset,
    min(measurement_time) filter (where name = ''initial_lactate'' and is_met) as hypoperfusion_onset
from %I
where %I.pat_id = coalesce($1, %I.pat_id)
group by %I.pat_id
) stats', table_name, table_name, table_name, table_name, table_name)
USING this_pat_id
; END $func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_window_states(table_name text, this_pat_id text)
RETURNS table( ts timestamptz, pat_id varchar(50), state int) AS $func$ BEGIN RETURN QUERY EXECUTE
format('select stats.ts, stats.pat_id,
    (
    case
    when sus_count = 1 then
        (
        case when sirs_count > 1 and organ_count > 0 then (
            (
            case
            when (fluid_count = 1 and hypotension_count > 0) and hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when now() - LEAST(hypotension_onset, hypoperfusion_onset) > ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset)  > ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when (fluid_count = 1 and hypotension_count > 0) then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when now() - hypotension_onset > ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when hypoperfusion_count = 1 then
                (case
                    -- septic shock
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 32 -- sev_sep_3hr_exp
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 34 -- sev_sep_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 0 then 36 -- sep_sho_6hr_exp
                    when now() - hypoperfusion_onset > ''6 hours''::interval and sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 1 then 33 -- sev_sep_6hr_com
                    when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count = 4 then 31 -- sev_sep_3hr_com
                    else
                    30 end)
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count < 4 then 22 -- sev_sep_3hr_exp
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 0 then 24 -- sev_sep_6hr_exp
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''6 hours''::interval and sev_sep_6hr_count = 1 then 23 -- sev_sep_6hr_com
            when now() - GREATEST(sus_onset, sirs_onset, organ_onset) > ''3 hours''::interval and sev_sep_3hr_count = 4 then 21 -- sev_sep_3hr_com
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
    when sirs_count > 1 and organ_count > 0 and sus_null_count = 1 then 10 -- sev_sep w.o. sus
    when sirs_count > 1 and organ_count > 0 and sus_noinf_count = 1 then 12 -- sev_sep w.o. sus
    else 0 -- health case
    end) as state
from
(
select %I.ts, %I.pat_id,
    count(*) filter (where name = ''suspicion_of_infection'' and is_met) as sus_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value#>>''{0,text}'' = ''No Infection'') as sus_noinf_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value is null) as sus_null_count,
    count(*) filter (where name = ''crystalloid_fluid'' and is_met) as fluid_count,
    count(*) filter (where name = ''initial_lactate'' and is_met) as hypoperfusion_count,
    count(*) filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) as sirs_count,
    count(*) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met ) as organ_count,
    count(*) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met ) as hypotension_count,
    count(*) filter (where name in (''initial_lactate_order'',''blood_culture_order'',''antibiotics_order'', ''crystalloid_fluid_order'') and is_met ) as sev_sep_3hr_count,
    count(*) filter (where name = ''repeat_lactate_order'' and is_met ) as sev_sep_6hr_count,
    count(*) filter (where name = ''vasopressors_order'' and is_met ) as sep_sho_6hr_count,
    first(override_time) filter (where name = ''suspicion_of_infection'' and is_met) as sus_onset,
    (array_agg(measurement_time order by measurement_time)  filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) )[2]   as sirs_onset,
    min(measurement_time) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met ) as organ_onset,
    min(measurement_time) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met ) as hypotension_onset,
    min(measurement_time) filter (where name = ''initial_lactate'' and is_met) as hypoperfusion_onset
from %I
where %I.pat_id = coalesce($1, %I.pat_id)
group by %I.ts, %I.pat_id
) stats', table_name, table_name, table_name, table_name, table_name, table_name, table_name)
USING this_pat_id
; END $func$ LANGUAGE plpgsql;

-------------------------------------------------
--  Criteria Management and Calculation.
-------------------------------------------------

CREATE OR REPLACE FUNCTION get_criteria(this_pat_id text)
RETURNS table(
    pat_id              varchar(50),
    event_id            int,
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date     timestamptz
) AS $func$ BEGIN RETURN QUERY
SELECT
    coalesce(e.pat_id, c.pat_id) pat_id,
    e.event_id,
    coalesce(e.name, c.name) as name,
    coalesce(e.is_met, c.is_met) is_met,
    coalesce(e.measurement_time, c.measurement_time) measurement_time,
    coalesce(e.override_time, c.override_time) override_time,
    coalesce(e.override_user, c.override_user) override_user,
    coalesce(e.override_value, c.override_value) override_value,
    coalesce(e.value, c.value) as value,
    coalesce(e.update_date, c.update_date) update_date
FROM criteria c
full JOIN
(
    select  ce.pat_id,
            ce.name,
            ce.event_id,
            ce.is_met,
            ce.measurement_time,
            ce.override_time,
            ce.override_user,
            ce.override_value,
            ce.value,
            ce.update_date
    from criteria_events ce
    where ce.pat_id = coalesce(this_pat_id, ce.pat_id)
    and ce.event_id = (
        select max(ce2.event_id) from criteria_events ce2
        where ce2.pat_id = coalesce(this_pat_id, ce2.pat_id) and ce2.flag > 0
    )
) as e
on c.pat_id = e.pat_id and c.name = e.name
where (c.pat_id = coalesce(this_pat_id, c.pat_id) or e.pat_id = coalesce(this_pat_id, e.pat_id) )
;
END $func$ LANGUAGE plpgsql;

create or replace function criteria_value_met(m_value text, c_ovalue json, d_ovalue json)
    returns boolean language plpgsql as $func$
BEGIN
    return coalesce(
        (c_ovalue is not null and c_ovalue#>>'{0,text}' = 'Not Indicated')
        or not (
            m_value::numeric
                between coalesce((c_ovalue#>>'{0,lower}')::numeric, (d_ovalue#>>'{lower}')::numeric, m_value::numeric)
                and coalesce((c_ovalue#>>'{0,upper}')::numeric, (d_ovalue#>>'{upper}')::numeric, m_value::numeric)
        ), false);
END; $func$;

create or replace function decrease_in_sbp_met(pat_sbp numeric, m_value text, c_ovalue json, d_ovalue json)
    returns boolean language plpgsql as $func$
BEGIN
    return coalesce( pat_sbp - m_value::numeric
            > (coalesce((c_ovalue#>>'{0,upper}')::numeric, (d_ovalue#>>'{upper}')::numeric, m_value::numeric)
    ), false);
END; $func$;

create or replace function urine_output_met(urine_output numeric, weight numeric)
    returns boolean language plpgsql as $func$
BEGIN
    return coalesce((
        urine_output / coalesce( weight, ( select value::numeric from cdm_g where fid = 'weight_popmean' ) )
            < 0.5
    ), false);
END; $func$;

create or replace function get_next_meas(
            this_pat_id varchar(50), this_fid varchar(50), tsp_prev timestamptz)
    returns table(pat_id varchar(50), fid varchar(50), tsp timestamptz, value text) language plpgsql as $func$
BEGIN
    return query select meas.pat_id, meas.fid, meas.tsp, meas.value from criteria_meas meas
        where meas.pat_id = this_pat_id and meas.fid = this_fid and meas.tsp > tsp_prev
        limit 1;
END; $func$;

create or replace function after_severe_sepsis_met(this_pat_id text, m_tsp timestamptz, sevsep_is_met boolean, sevsep_onset timestamptz)
    returns table(is_met boolean) language plpgsql as $func$
BEGIN
    return query select coalesce(bool_or(ss.is_met), false) from (
        select (sevsep_is_met and m_tsp > sevsep_onset) as is_met
        union all select bool_or(m_tsp > ss.severe_sepsis_onset) is_met
                  from get_states_snapshot(this_pat_id) ss
                  where ss.state >= 20
    ) ss;
END; $func$;

create or replace function after_septic_shock_met(this_pat_id text, m_tsp timestamptz, sepshk_is_met boolean, sepshk_onset timestamptz)
    returns table(is_met boolean) language plpgsql as $func$
BEGIN
    return query select coalesce(bool_or(ss.is_met), false) from (
        select (sepshk_is_met and m_tsp > sepshk_onset) as is_met
        union all select bool_or(m_tsp > ss.septic_shock_onset)
                  from get_states_snapshot(this_pat_id) ss
                  where ss.state >= 30
    ) ss;
END; $func$;

create or replace function hypotension_met(this_pat_id varchar, this_tsp timestamptz, next_tsp timestamptz, override boolean)
    returns table(is_met boolean) language plpgsql as $func$
BEGIN
    return query
        select bool_or(
            (case when override then c_fluid.value::numeric > 0
                else c_fluid.value::numeric > 30
                end)
            and this_tsp > c_fluid.tsp
            and next_tsp < c_fluid.tsp + '1 hour'::interval
        )
        from criteria_meas c_fluid
        where
            (case when override then c_fluid.fid in ('crystalloid_fluid', 'fluids_intake')
                else c_fluid.fid = 'crystalloid_fluid'
                end)
        and c_fluid.pat_id = this_pat_id
        and isnumeric(c_fluid.value);
END; $func$;

create or replace function order_met(order_name text, order_value text)
    returns boolean language plpgsql as $func$
BEGIN
    return case when order_name = 'blood_culture_order'
                    then order_value in ('In  process', 'Preliminary', 'Final', 'Completed', 'Not Indicated')
                when order_name = 'initial_lactate_order' or order_name = 'repeat_lactate_order'
                    then order_value in ('Preliminary', 'Sent', 'Final', 'Completed', 'Not Indicated')
                else false
            end;
END; $func$;

create or replace function dose_order_status(order_fid text, override_value_text text)
    returns text language plpgsql as $func$
BEGIN
    return case when override_value_text = 'Not Indicated' then 'Completed'
                when order_fid in ('cms_antibiotics_order', 'crystalloid_fluid_order', 'vasopressors_dose_order') then 'Ordered'
                when order_fid in ('cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose') then 'Completed'
                else null
            end;
END; $func$;

create or replace function order_status(order_fid text, value_text text, override_value_text text)
    returns text language plpgsql as $func$
BEGIN
    return case when override_value_text = 'Not Indicated' then 'Completed'
                when order_fid = 'lactate_order' and value_text in ('In  process', 'Sent', 'Preliminary', 'Final', 'Completed', 'Not Indicated') then 'Completed'
                when order_fid = 'lactate_order' and value_text = 'Signed' then 'Ordered'
                when order_fid = 'blood_culture_order' and value_text in ('In  process', 'Sent', 'Preliminary', 'Final', 'Completed', 'Not Indicated') then 'Completed'
                when order_fid = 'blood_culture_order' and value_text = 'Signed' then 'Ordered'
                else null
            end;
END; $func$;

create or replace function dose_order_met(order_fid text, override_value_text text, dose_value numeric, dose_limit numeric)
    returns boolean language plpgsql as $func$
DECLARE
    order_status text := dose_order_status(order_fid, override_value_text);
BEGIN
    return case when override_value_text = 'Not Indicated' then true
                when order_status = 'Completed' then dose_value > dose_limit
                else false
            end;
END; $func$;


CREATE OR REPLACE FUNCTION calculate_criteria(this_pat_id text, ts_start timestamptz, ts_end timestamptz)
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
AS $function$ BEGIN

return query
    with pat_ids as (
        select distinct pat_enc.pat_id from pat_enc
        where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
    ),
    pat_urine_output as (
        select pat_ids.pat_id, sum(uo.value::numeric) as value
        from pat_ids
        inner join criteria_meas uo on pat_ids.pat_id = uo.pat_id
        where uo.fid = 'urine_output'
        and isnumeric(uo.value)
        and ts_end - uo.tsp < interval '2 hours'
        group by pat_ids.pat_id
    ),
    pat_weights as (
        select ordered.pat_id, first(ordered.value) as value
        from (
            select pat_ids.pat_id, weights.value::numeric as value
            from pat_ids
            inner join criteria_meas weights on pat_ids.pat_id = weights.pat_id
            where weights.fid = 'weight'
            order by weights.tsp
        ) as ordered
        group by ordered.pat_id
    ),
    pat_nbp_sys as (
        select pat_ids.pat_id, avg(sbp_meas.value::numeric) as value
        from pat_ids
        inner join criteria_meas sbp_meas on pat_ids.pat_id = sbp_meas.pat_id
        where isnumeric(sbp_meas.value) and sbp_meas.fid = 'nbp_sys'
        group by pat_ids.pat_id
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
        left join criteria c on pat_ids.pat_id = c.pat_id and cd.name = c.name
        left join criteria_meas meas
            on pat_ids.pat_id = meas.pat_id and meas.fid = cd.fid
            and meas.fid = cd.fid
            and (meas.tsp is null or meas.tsp between ts_start and ts_end)
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
            select  pat_cvalues.pat_id,
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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    pat_cvalues.value as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue) as is_met
            from pat_cvalues
            where pat_cvalues.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
            order by pat_cvalues.tsp
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
                pat_cvalues.pat_id,
                pat_cvalues.name,
                cdm_twf.tsp,
                cdm_twf.pao2_to_fio2 as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                (   cdm_twf.tsp between ts_start and ts_end
                    and cdm_twf.pao2_to_fio2 < coalesce((c_ovalue#>>'{0,lower}')::numeric, (d_ovalue#>>'{lower}')::numeric)
                    and cdm_twf.pao2_to_fio2_c < 8
                ) as is_met
            from pat_cvalues
            inner join pat_enc on pat_cvalues.pat_id = pat_enc.pat_id
            left join cdm_twf on pat_enc.enc_id = cdm_twf.enc_id
            where pat_cvalues.category = 'cdm_twf'
            order by cdm_twf.tsp
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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    pat_cvalues.value as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    (case
                        when pat_cvalues.category = 'decrease_in_sbp' then
                            decrease_in_sbp_met(
                                (select max(pat_nbp_sys.value) from pat_nbp_sys where pat_nbp_sys.pat_id = pat_cvalues.pat_id),
                                pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)

                        when pat_cvalues.category = 'urine_output' then
                            urine_output_met(
                                (select max(pat_urine_output.value) from pat_urine_output where pat_urine_output.pat_id = pat_cvalues.pat_id),
                                (select max(pat_weights.value) from pat_weights where pat_weights.pat_id = pat_cvalues.pat_id)
                            )

                        else criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        end
                    ) as is_met
            from pat_cvalues
            where pat_cvalues.name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate')
            order by pat_cvalues.tsp
        ) as ordered
        group by ordered.pat_id, ordered.name
    ),
    severe_sepsis as (
        select * from infection
        union all select * from sirs
        union all select * from respiratory_failures
        union all select * from organ_dysfunction_except_rf
    ),
    severe_sepsis_now as (
        with organ_dysfunction as (
            select * from respiratory_failures
            union all select * from organ_dysfunction_except_rf
        )
        select stats.pat_id,
               coalesce(bool_or(stats.suspicion_of_infection and stats.sirs_cnt > 1 and stats.org_df_cnt > 0), false) as severe_sepsis_is_met,
               max(greatest(stats.inf_onset, stats.sirs_onset, stats.org_df_onset)) as severe_sepsis_onset,
               max(greatest(stats.sirs_onset, stats.org_df_onset)) as severe_sepsis_wo_infection_onset
        from (
            select infection.pat_id,
                   coalesce(bool_or(infection.is_met), false) as suspicion_of_infection,
                   sum(case when sirs.is_met then 1 else 0 end) as sirs_cnt,
                   sum(case when organ_dysfunction.is_met then 1 else 0 end) as org_df_cnt,
                   max(infection.override_time) as inf_onset,
                   (array_agg(sirs.measurement_time order by sirs.measurement_time))[2] as sirs_onset,
                   min(organ_dysfunction.measurement_time) as org_df_onset
            from infection
            left join sirs on infection.pat_id = sirs.pat_id
            left join organ_dysfunction on infection.pat_id = organ_dysfunction.pat_id
            group by infection.pat_id
        ) stats
        group by stats.pat_id
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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    pat_cvalues.value as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    (case when coalesce(pat_cvalues.c_ovalue#>>'{0,text}' = 'Not Indicated', false) then
                                    criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                                else pat_cvalues.fid = 'crystalloid_fluid' and criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                            end)
                    and (ssn.severe_sepsis_onset is not null
                            and coalesce(pat_cvalues.c_otime, pat_cvalues.tsp) >= ssn.severe_sepsis_onset)
                    as is_met
            from pat_cvalues
            left join severe_sepsis_now ssn on pat_cvalues.pat_id = ssn.pat_id
            where pat_cvalues.name = 'crystalloid_fluid'
            order by pat_cvalues.tsp
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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    pat_cvalues.value as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    (case
                        when pat_cvalues.category = 'hypotension' then
                            (select bool_or(hm.is_met) from
                                hypotension_met(pat_cvalues.pat_id, pat_cvalues.tsp,
                                                            (select next.tsp from get_next_meas(pat_cvalues.pat_id, pat_cvalues.fid, pat_cvalues.tsp) as next),
                                                            (select coalesce(pat_cvalues.c_ovalue#>>'{0,text}' = 'Not Indicated', false) from crystalloid_fluid where crystalloid_fluid.pat_id = pat_cvalues.pat_id)
                                ) as hm)
                                and criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                                -- and next consecutive value also met
                                and criteria_value_met(
                                    (select next.value from get_next_meas(pat_cvalues.pat_id, pat_cvalues.fid, pat_cvalues.tsp) as next)
                                    , pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        when pat_cvalues.category = 'hypotension_dsbp' then
                            (select bool_or(hm.is_met) from
                                hypotension_met(
                                    pat_cvalues.pat_id,
                                    pat_cvalues.tsp,
                                    (select next.tsp from get_next_meas(pat_cvalues.pat_id, pat_cvalues.fid, pat_cvalues.tsp) as next),
                                    (select coalesce(pat_cvalues.c_ovalue#>>'{0,text}' = 'Not Indicated', false)
                                        from crystalloid_fluid where crystalloid_fluid.pat_id = pat_cvalues.pat_id)
                                ) as hm)
                                and decrease_in_sbp_met(
                                        (select max(pat_nbp_sys.value) from pat_nbp_sys where pat_nbp_sys.pat_id = pat_cvalues.pat_id),
                                        pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                                -- and next consecutive value also met
                                and decrease_in_sbp_met(
                                        (select max(pat_nbp_sys.value) from pat_nbp_sys where pat_nbp_sys.pat_id = pat_cvalues.pat_id),
                                        (select next.value from get_next_meas(pat_cvalues.pat_id, pat_cvalues.fid, pat_cvalues.tsp) as next), pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        else false
                        end
                    )
                    and (ssn.severe_sepsis_onset is not null
                            and coalesce(pat_cvalues.c_otime, pat_cvalues.tsp) >= ssn.severe_sepsis_onset)
                    as is_met
            from pat_cvalues
            left join severe_sepsis_now ssn on pat_cvalues.pat_id = ssn.pat_id
            where pat_cvalues.name in ('systolic_bp', 'hypotension_map', 'hypotension_dsbp')
            order by pat_cvalues.tsp
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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    pat_cvalues.value as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        and (ssn.severe_sepsis_onset is not null
                                and coalesce(pat_cvalues.c_otime, pat_cvalues.tsp) >= ssn.severe_sepsis_onset)
                        as is_met
            from pat_cvalues
            left join severe_sepsis_now ssn on pat_cvalues.pat_id = ssn.pat_id
            where pat_cvalues.name = 'initial_lactate'
            order by pat_cvalues.tsp
        ) as ordered
        group by ordered.pat_id, ordered.name
    ),
    septic_shock as (
        select * from crystalloid_fluid
        union all select * from hypotension
        union all select * from hypoperfusion
    ),
    septic_shock_now as (
        select stats.pat_id,
               bool_or(stats.cnt > 0) as septic_shock_is_met,
               greatest(min(stats.onset), max(ssn.severe_sepsis_onset)) as septic_shock_onset
        from (
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
        left join severe_sepsis_now ssn on stats.pat_id = ssn.pat_id
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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    (case when pat_cvalues.category in ('after_severe_sepsis_dose', 'after_septic_shock_dose')
                            then dose_order_status(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}')
                          else order_status(pat_cvalues.fid, pat_cvalues.value, pat_cvalues.c_ovalue#>>'{0,text}')
                     end) as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    (case
                        when pat_cvalues.category = 'after_severe_sepsis' then
                            (select bool_or(assm.is_met)
                                from after_severe_sepsis_met(pat_cvalues.pat_id, greatest(pat_cvalues.c_otime, pat_cvalues.tsp),
                                    (select bool_or(severe_sepsis_now.severe_sepsis_is_met) from severe_sepsis_now),
                                    (select min(severe_sepsis_now.severe_sepsis_onset) from severe_sepsis_now)
                                ) assm)
                            and ( order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value)) )

                        when pat_cvalues.category = 'after_severe_sepsis_dose' then
                            (select bool_or(assm.is_met)
                                from after_severe_sepsis_met(pat_cvalues.pat_id, greatest(pat_cvalues.c_otime, pat_cvalues.tsp),
                                    (select bool_or(severe_sepsis_now.severe_sepsis_is_met) from severe_sepsis_now),
                                    (select min(severe_sepsis_now.severe_sepsis_onset) from severe_sepsis_now)
                                ) assm)
                            and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value::numeric,
                                    coalesce((pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                             (pat_cvalues.d_ovalue#>>'{lower}')::numeric)) )

                        when pat_cvalues.category = 'after_septic_shock' then
                            (select bool_or(assm.is_met)
                                from after_septic_shock_met(pat_cvalues.pat_id, greatest(pat_cvalues.c_otime, pat_cvalues.tsp),
                                    (select bool_or(septic_shock_now.septic_shock_is_met) from septic_shock_now),
                                    (select min(septic_shock_now.septic_shock_onset) from septic_shock_now)
                                ) assm)
                            and ( order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value)) )

                        when pat_cvalues.category = 'after_septic_shock_dose' then
                            (select bool_or(assm.is_met)
                                from after_septic_shock_met(pat_cvalues.pat_id, greatest(pat_cvalues.c_otime, pat_cvalues.tsp),
                                    (select bool_or(septic_shock_now.septic_shock_is_met) from septic_shock_now),
                                    (select min(septic_shock_now.septic_shock_onset) from septic_shock_now)
                                ) assm)
                            and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value::numeric,
                                    coalesce((pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                             (pat_cvalues.d_ovalue#>>'{lower}')::numeric)) )

                        else criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        end
                    ) as is_met
            from pat_cvalues
            where pat_cvalues.name in (
                'initial_lactate_order',
                'blood_culture_order',
                'antibiotics_order',
                'crystalloid_fluid_order',
                'vasopressors_order'
            )
            order by pat_cvalues.tsp
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
                    (not(coalesce(initial_lactate_order.is_met and lactate_results.is_met, false)
                            and order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value)))
                     and (lactate_results.tsp is null
                          or (pat_cvalues.tsp > initial_lactate_order.tsp and lactate_results.tsp > initial_lactate_order.tsp))
                    ) is_met
            from pat_cvalues
            left join (
                select oc.pat_id,
                       max(case when oc.is_met then oc.measurement_time else null end) as tsp,
                       bool_or(oc.is_met) as is_met
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
           severe_sepsis_now.severe_sepsis_onset,
           severe_sepsis_now.severe_sepsis_wo_infection_onset,
           septic_shock_now.septic_shock_onset
    from (
        select * from severe_sepsis
        union all select * from septic_shock
        union all select * from orders_criteria
        union all select * from repeat_lactate
    ) new_criteria
    left join severe_sepsis_now on new_criteria.pat_id = severe_sepsis_now.pat_id
    left join septic_shock_now on new_criteria.pat_id = septic_shock_now.pat_id;

return;
END; $function$;

-- Calculates criteria over windows, with each window based on the timestamps
-- at which a measurement is available. This could be replaced by regularly-spaced
-- window endpoints from a generated series.
--
CREATE OR REPLACE FUNCTION calculate_max_criteria(this_pat_id text)
 RETURNS table(window_ts                        timestamptz,
               pat_id                           varchar(50),
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
    ts_end timestamptz := now();
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    create temporary table new_criteria_windows as
        select window_ends.tsp as ts, new_criteria.*
        from (  select distinct meas.pat_id, meas.tsp from criteria_meas meas
                where meas.pat_id = coalesce(this_pat_id, meas.pat_id)
                and meas.tsp > ts_end - window_size
        ) window_ends
        inner join lateral calculate_criteria(
            coalesce(this_pat_id, window_ends.pat_id), window_ends.tsp - window_size, window_ends.tsp
        ) new_criteria
        on window_ends.pat_id = new_criteria.pat_id;

    return query
        with state_windows as (
            select sw.pat_id, sw.state, min(ts) as ts
            from get_window_states('new_criteria_windows', this_pat_id) sw
            group by sw.pat_id, sw.state
        )
        select new_criteria_windows.*
        from new_criteria_windows
        inner join (
            select state_windows.pat_id, min(state_windows.ts) as ts
            from state_windows
            where state_windows.state = (
                select max(sw2.state) from state_windows sw2 where state_windows.pat_id = sw2.pat_id
            )
            group by state_windows.pat_id
        ) max_windows
        on new_criteria_windows.pat_id = max_windows.pat_id
        and new_criteria_windows.ts = max_windows.ts;

    drop table new_criteria_windows;
    return;
END; $function$;

--------------------------------------------
-- Criteria snapshot utilities.
--------------------------------------------

CREATE OR REPLACE FUNCTION advance_criteria_snapshot(this_pat_id text default null)
RETURNS void AS $$
DECLARE
    ts_end timestamptz := now();
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    perform auto_deactivate(this_pat_id);
    create temporary table new_criteria as
        select * from calculate_criteria(this_pat_id, ts_end - window_size, ts_end);

    with criteria_inserts as
    (
        insert into criteria (pat_id, name, measurement_time, value, override_time, override_user, override_value, is_met, update_date)
        select pat_id, name, measurement_time, value, override_time, override_user, override_value, is_met, update_date
        from new_criteria
        on conflict (pat_id, name) do update
        set is_met              = excluded.is_met,
            measurement_time    = excluded.measurement_time,
            value               = excluded.value,
            override_time       = excluded.override_time,
            override_user       = excluded.override_user,
            override_value      = excluded.override_value,
            update_date         = excluded.update_date
        returning *
    ),
    state_change as
    (
        select snapshot.pat_id, snapshot.event_id as from_event_id, snapshot.state as state_from, live.state as state_to
        from get_states('new_criteria', this_pat_id) live
        left join get_states_snapshot(this_pat_id) snapshot on snapshot.pat_id = live.pat_id
        where snapshot.state < live.state
        or ( snapshot.state = 10 and snapshot.severe_sepsis_wo_infection_onset < now() - window_size)
    ),
    deactivate_old_snapshot as
    (
        update criteria_events
        set flag = -1
        from state_change
        where criteria_events.event_id = state_change.from_event_id
        and criteria_events.pat_id = state_change.pat_id
    ),
    notified_patients as (
        select distinct si.pat_id
        from state_change si
        inner join (
            select  new_criteria.pat_id,
                    first(new_criteria.severe_sepsis_onset) severe_sepsis_onset,
                    first(new_criteria.septic_shock_onset) septic_shock_onset,
                    first(new_criteria.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset
            from new_criteria
            group by new_criteria.pat_id
        ) nc on si.pat_id = nc.pat_id
        left join lateral update_notifications(si.pat_id, flag_to_alert_codes(si.state_to),
            nc.severe_sepsis_onset, nc.septic_shock_onset, nc.severe_sepsis_wo_infection_onset) n on si.pat_id = n.pat_id
    )
    insert into criteria_events (event_id, pat_id, name, measurement_time, value, override_time, override_user, override_value, is_met, update_date, flag)
    select s.event_id, c.pat_id, c.name, c.measurement_time, c.value, c.override_time, c.override_user, c.override_value, c.is_met, c.update_date, s.state_to as flag
    from ( select ssid.event_id, si.pat_id, si.state_to
           from state_change si
           cross join (select nextval('criteria_event_ids') event_id) ssid
    ) as s
    inner join new_criteria c on s.pat_id = c.pat_id
    left join notified_patients as np on s.pat_id = np.pat_id
    where not c.name like '%_order';

    drop table new_criteria;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION override_criteria_snapshot(this_pat_id text default null)
    RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
    ts_end timestamptz := now();
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    create temporary table new_criteria as
    select * from calculate_criteria(this_pat_id, ts_end - window_size, ts_end);

    -- TODO: test search over windows for worst patient state.
    -- create temporary table new_criteria as
    -- select * from calculate_max_criteria(this_pat_id);

    -- Deactivate old snapshots, and add a new snapshot.
    with pat_states as (
        select * from get_states('new_criteria', this_pat_id)
    ),
    criteria_inserts as (
        insert into criteria (pat_id, name, override_time, override_user, override_value, is_met, update_date)
        select pat_id, name, override_time, override_user, override_value, is_met, update_date
        from new_criteria
        where name in ( 'suspicion_of_infection', 'crystalloid_fluid' )
        on conflict (pat_id, name) do update
        set is_met              = excluded.is_met,
            measurement_time    = excluded.measurement_time,
            value               = excluded.value,
            override_time       = excluded.override_time,
            override_user       = excluded.override_user,
            override_value      = excluded.override_value,
            update_date         = excluded.update_date
        returning *
    ),
    deactivate_old_snapshot as (
        update criteria_events
        set flag = -1
        from new_criteria
        where criteria_events.event_id = (
            select max(event_id) from criteria_events ce
            where ce.pat_id = new_criteria.pat_id and ce.flag > 0
        )
        and criteria_events.pat_id = new_criteria.pat_id
    ),
    notified_patients as (
        select distinct pat_states.pat_id
        from pat_states
        inner join (
            select  new_criteria.pat_id,
                    first(new_criteria.severe_sepsis_onset) severe_sepsis_onset,
                    first(new_criteria.septic_shock_onset) septic_shock_onset,
                    first(new_criteria.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset
            from new_criteria
            group by new_criteria.pat_id
        ) nc on pat_states.pat_id = nc.pat_id
        left join lateral update_notifications(pat_states.pat_id, flag_to_alert_codes(pat_states.state),
            nc.severe_sepsis_onset, nc.septic_shock_onset, nc.severe_sepsis_wo_infection_onset) n
        on pat_states.pat_id = n.pat_id
    )
    insert into criteria_events (event_id, pat_id, name, measurement_time, value, override_time, override_user, override_value, is_met, update_date, flag)
    select ssid.event_id, new_criteria.pat_id, new_criteria.name, new_criteria.measurement_time, new_criteria.value, new_criteria.override_time, new_criteria.override_user, new_criteria.override_value, new_criteria.is_met, new_criteria.update_date, pat_states.state as flag
    from new_criteria
    cross join (select nextval('criteria_event_ids') event_id) ssid
    inner join pat_states on new_criteria.pat_id = pat_states.pat_id
    left join notified_patients np on new_criteria.pat_id = np.pat_id
    where not new_criteria.name like '%_order';

    drop table new_criteria;
    return;
END; $function$;


-----------------------------------------------
-- Notification management
-----------------------------------------------

-- '200','201','202','203','204','300','301','302','303','304','305','306'
CREATE OR REPLACE FUNCTION update_notifications(this_pat_id text, alert_codes text[],
    severe_sepsis_onset timestamptz, septic_shock_onset timestamptz, sirs_plus_organ_onset timestamptz)
RETURNS table(pat_id varchar(50), alert_code text) AS $$
BEGIN
-- clean notifications
delete from notifications
    where notifications.pat_id = this_pat_id and notifications.message#>>'{alert_code}' <> any(alert_codes);

-- add new notifications
return query
insert into notifications (pat_id, message)
select
    pat_enc.pat_id,
    json_build_object('alert_code', code, 'read', false,'timestamp',
        date_part('epoch',
            (case when code in ('201','204','303','306') then septic_shock_onset
                        when code in ('205', '300') then sirs_plus_organ_onset
                        else severe_sepsis_onset
                        end)::timestamptz
            +
                    (case
                        when code = '202' then '3 hours'
                        when code in ('203','204','205') then '6 hours'
                        when code = '304' then '2 hours'
                        when code in ('305','306') then '5 hours'
                        else '0 hours'
                        end)::interval
    )) message
from (select distinct pat_enc.pat_id from pat_enc) as pat_enc
cross join unnest(alert_codes) as code
left join notifications n2 on n2.pat_id = pat_enc.pat_id and n2.message#>>'{alert_code}' = code
where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id) and n2.message is null and code <> '0'
returning notifications.pat_id, message#>>'{alert_code}';

END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION flag_to_alert_codes(flag int) RETURNS text[] AS $$ DECLARE ret text[]; -- complete all mappings
BEGIN -- Note the CASTING being done for the 2nd and 3rd elements of the array
 CASE
     WHEN flag = 10 THEN ret := array['205', '300'];
     WHEN flag = 20 THEN ret := array['200',
                                '202',
                                '203',
                                '301',
                                '302',
                                '304',
                                '305'];
     WHEN flag = 21 THEN ret := array['200',
                                '203',
                                '302',
                                '305'];
     WHEN flag = 22 THEN ret := array['200','202'];
     WHEN flag = 23 THEN ret := array['200'];
     WHEN flag = 24 THEN ret := array['200','203'];
     WHEN flag = 30 THEN ret := array['200','201',
                                '202',
                                '203',
                                '204',
                                '301',
                                '302',
                                '303',
                                '304',
                                '305',
                                '306'];
     WHEN flag = 31 THEN ret := array['200','201',
                                '203',
                                '204',
                                '302',
                                '303',
                                '305',
                                '306'];
     WHEN flag = 32 THEN ret := array['200','201','202'];
     WHEN flag = 33 THEN ret := array['200','201',
                                '204',
                                '303',
                                '306'];
     WHEN flag = 34 THEN ret := array['200','201','203'];
     WHEN flag = 35 THEN ret := array['200','201'];
     WHEN flag = 36 THEN ret := array['200','201','204'];
     ELSE ret := array['0'];
 END CASE ; RETURN ret;
END;$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_notifications_for_epic(this_pat_id text default null)
RETURNS table(
    pat_id              varchar(50),
    visit_id            varchar(50),
    count               int
) AS $func$ BEGIN RETURN QUERY
select pat_enc.pat_id, pat_enc.visit_id,
    (case when deactivated is true then 0
      else coalesce(counts.count::int, 0)
    end)
    from pat_enc
    left join pat_status on pat_enc.pat_id = pat_status.pat_id
    left join
    (
    select notifications.pat_id,
        (
            case when count(*) > 5 then 5
            else count(*)
            end
        ) as count
    from
    notifications
    where not (message#>>'{read}')::bool
        and (message#>>'{timestamp}')::numeric < date_part('epoch', now())
    group by notifications.pat_id
    ) as counts on counts.pat_id = pat_enc.pat_id
    where pat_enc.pat_id like 'E%' and pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id);
END $func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_popmean(target TEXT, fid TEXT)
RETURNS real
AS $BODY$
DECLARE
    fid_c TEXT;
    fid_popmean TEXT;
    popmean TEXT;
BEGIN
    fid_c = fid || '_c';
    fid_popmean = fid || '_popmean';
    EXECUTE 'SELECT cast(avg('|| quote_ident(fid) ||
        ') AS text) FROM ' || target || '  WHERE '|| quote_ident(fid_c)
        ||' < 8 ' INTO popmean;
    RAISE NOTICE '% = %', fid_popmean, popmean;
    EXECUTE 'SELECT merge_cdm_g('||quote_literal(fid_popmean)||', '||quote_literal(popmean)||', 24)';
    return popmean;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_tables(IN _schema TEXT, IN name TEXT)
RETURNS void
LANGUAGE plpgsql
AS
$$
DECLARE
    row     record;
BEGIN
    FOR row IN
        SELECT
            table_schema,
            table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE'
        AND
            table_schema = _schema
        AND
            table_name ILIKE (name || '%')
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION drop_tables_pattern(IN _schema TEXT, IN pattern TEXT)
RETURNS void
LANGUAGE plpgsql
AS
$$
DECLARE
    row     record;
BEGIN
    FOR row IN
        SELECT
            table_schema,
            table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE'
        AND
            table_schema = _schema
        AND
            table_name ILIKE pattern || '%'
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION del_pat(this_pat_id text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
DELETE
FROM cdm_twf
WHERE enc_id IN
    (SELECT enc_id
     FROM pat_enc
     WHERE pat_id = this_pat_id);
  DELETE
  FROM cdm_t WHERE enc_id IN
    (SELECT enc_id
     FROM pat_enc
     WHERE pat_id = this_pat_id);
  DELETE
  FROM cdm_s WHERE enc_id IN
    (SELECT enc_id
     FROM pat_enc
     WHERE pat_id = this_pat_id);
  DELETE
  FROM criteria WHERE pat_id = this_pat_id;
  DELETE
  FROM criteria_meas WHERE pat_id = this_pat_id;
  DELETE
  FROM pat_enc WHERE pat_id = this_pat_id;
END;
$$;

create or replace function delete_test_scenarios() returns void language plpgsql as $$ begin
    delete from criteria where pat_id ~ E'^\\d+$' and pat_id::integer between 3000 and 3200;
    delete from criteria_events where pat_id ~ E'^\\d+$' and pat_id::integer between 3000 and 3200;
    delete from notifications where pat_id ~ E'^\\d+$' and pat_id::integer between 3000 and 3200;
end; $$;

----------------------------------------------------
--  deactivate functionality for patients
----------------------------------------------------

create or replace function deactivate(pid text, deactivated boolean) returns void language plpgsql as $$ begin
insert into pat_status (pat_id, deactivated, deactivated_tsp)
    values (
        pid, deactivated, now()
        )
    on conflict (pat_id) do update
    set deactivated = Excluded.deactivated, deactivated_tsp = now();
-- if false then reset patient
IF not deactivated THEN
    update criteria_events set flag = -1
    where pat_id = pid and flag <> -1;
    delete from notifications where pat_id = pid;
    perform advance_criteria_snapshot(pid);
END IF;
end; $$;

CREATE OR REPLACE FUNCTION auto_deactivate(pid text DEFAULT NULL) RETURNS void LANGUAGE plpgsql AS $$ BEGIN -- if criteria_events has been in an event for longer than deactivate_hours, then this patient should be deactivated automatically
perform deactivate(pat_id, TRUE)
FROM
  ( SELECT DISTINCT snapshot.pat_id
   FROM get_states_snapshot(pid) snapshot
   LEFT JOIN pat_status s ON s.pat_id = snapshot.pat_id
   WHERE STATE >= 20
     AND now() - severe_sepsis_onset > get_parameter('deactivate_hours')::interval
     AND (s.pat_id IS NULL
          OR NOT s.deactivated) ) AS sub ; END; $$;
------------------------------
-- garbage collection
------------------------------
create or replace function garbage_collection() returns void language plpgsql as $$ begin
perform reactivate();
end; $$;

create or replace function reactivate(this_pat_id text default null) returns void language plpgsql as $$ begin
perform deactivate(pat_id, false) from(
    select pat_id from pat_status
    where deactivated
    and now() - deactivated_tsp > get_parameter('deactivate_expire_hours')::interval
    and pat_id = coalesce(this_pat_id, pat_id)
) as sub;
end; $$;

----------------------------------------------------
-- deterioration feedback functions
----------------------------------------------------
CREATE OR REPLACE FUNCTION set_deterioration_feedback(pid text, tsp timestamptz, deterioration json, uid text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
INSERT INTO deterioration_feedback (pat_id, tsp, deterioration, uid)
VALUES (pid,
        tsp,
        deterioration,
        uid) ON conflict (pat_id) DO
UPDATE
SET tsp = Excluded.tsp,
    deterioration = Excluded.deterioration,
    uid = Excluded.uid;
INSERT INTO criteria_log (pat_id, tsp, event, update_date)
VALUES ( pid,
         now(),
         json_build_object('event_type', 'set_deterioration_feedback', 'uid', uid, 'value', deterioration),
         now() ); END; $$;


----------------------------------------------------
-- Feature Views Functions
----------------------------------------------------
create or replace function update_implemented_trainable_fids()
RETURNS void as
$BODY$
DECLARE
_fid TEXT;
_nRows integer;
_impFeatures TEXT[];
BEGIN
-- =========================
-- TWF Processing
-- =========================
	FOR _fid in -- Get the Columns which are actually features
		SELECT information_schema.columns.column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name   = 'cdm_twf'
			AND column_name not like '%\_c'
			AND column_name not in ('enc_id','tsp','meta_data')

	LOOP -- loop over the columns which are features in cdm_twf

    EXECUTE format('select count(%s) from cdm_twf where %s_c <> 16 and %s_c <> 24',_fid,_fid,_fid) into _nRows ;

    IF _nRows > 0 THEN
      _impFeatures = array_append(_impFeatures, _fid);
    ELSE
       raise notice 'TWF feature % not implemented', _fid;
    END IF;

	END LOOP;
-- =========================
-- S Processing
-- =========================
	FOR _fid in select distinct fid from cdm_s
  LOOP
    _impFeatures = array_append(_impFeatures, _fid);
  END LOOP;
-- =========================
--  Create Table and Wrap Up
-- =========================
  raise notice 'The following features are implemented %', _impFeatures;

  DROP TABLE IF EXISTS implemented_trainable_fids ;
  create table implemented_trainable_fids (fid text);

  FOREACH _fid IN ARRAY _impFeatures
  LOOP
    INSERT INTO implemented_trainable_fids VALUES (_fid);
  END LOOP;

--   \COPY implemented_trainable_fids to '/home/ubuntu/uData/impfeatsTrain.csv' DELIMITER ',' CSV HEADER;
RETURN;
END
$BODY$ Language plpgsql;

-- select * from update_implemented_trainable_fids();
----------------------------------------------------
-- Feature Views Functions
----------------------------------------------------

create or replace function update_implemented_meas_fids()
RETURNS void as
$BODY$
DECLARE
_fid TEXT;
_nRows integer;
_impFeatures TEXT[];
BEGIN
-- =========================
-- TWF Processing
-- =========================
	FOR _fid in -- Get the Columns which are actually features
		SELECT information_schema.columns.column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name   = 'cdm_twf'
			AND column_name not like '%\_c'
			AND column_name not in ('enc_id','tsp','meta_data')

	LOOP -- loop over the columns which are features in cdm_twf

    EXECUTE format('select count(%s) from cdm_twf where %s_c <8',_fid,_fid) into _nRows ;

    IF _nRows > 0 THEN
      _impFeatures = array_append(_impFeatures, _fid);
    ELSE
       raise notice 'TWF feature % not implemented', _fid;
    END IF;

	END LOOP;
-- =========================
-- T Processing
-- =========================
	FOR _fid in select distinct fid from cdm_t
  LOOP
    _impFeatures = array_append(_impFeatures, _fid);
  END LOOP;
-- =========================
-- S Processing
-- =========================
	FOR _fid in select distinct fid from cdm_s
  LOOP
    _impFeatures = array_append(_impFeatures, _fid);
  END LOOP;
-- =========================
--  Create Table and Wrap Up
-- =========================
  raise notice 'The following features are implemented %', _impFeatures;

  DROP TABLE IF EXISTS implemented_meas_fids;
  create table implemented_meas_fids (fid text);

  FOREACH _fid IN ARRAY _impFeatures
  LOOP
    INSERT INTO implemented_meas_fids VALUES (_fid);
  END LOOP;

--   \COPY implemented_fids to '/home/ubuntu/uData/impfeats.csv' DELIMITER ',' CSV HEADER;
RETURN;
END
$BODY$ Language plpgsql;

-- select * from update_implemented_meas_fids();

----------------------------------------------------------------------
-- calculate_trews_contributors
-- Returns a time-series of top 'rank_limit' features and values that
-- contribute to a patient's trewscore.
-- This query is used to generate the frontend chart.
--
-- TODO: use 'information_schema.columns' to dynamically
-- pull out columns in the 'trews' table for array unnesting.
-----------------------------------------------------------------------

create or replace function calculate_trews_contributors(this_pat_id text, rank_limit int)
returns table(enc_id int, tsp timestamptz, trewscore numeric, fid text, trews_value double precision, cdm_value text, rnk bigint)
as $func$
declare
    twf_fid_names text[];
    twf_fid_exprs text[];
    fid_query text;
begin
    create temporary table twf_rank as
    select *
    from
    (
        select KV.*,
                rank() over ( partition by KV.enc_id, KV.tsp order by KV.trews_value desc nulls last ) as rnk
        from (
            select R.enc_id, R.tsp, R.trewscore, S.* from (
                select trews.enc_id, trews.tsp, trews.trewscore,
                ARRAY[
                 'shock_idx',
                 'hemoglobin',
                 'spo2',
                 'platelets',
                 'sodium',
                 'fluids_intake_24hr',
                 'rass',
                 'urine_output_6hr',
                 'neurologic_sofa',
                 'bun_to_cr',
                 'heart_rate',
                 'lactate',
                 'minutes_since_any_organ_fail',
                 'sirs_raw',
                 'sirs_temperature_oor',
                 'sirs_resp_oor',
                 'hypotension_raw',
                 'hypotension_intp',
                 'age',
                 'chronic_pulmonary_hist',
                 'chronic_bronchitis_diag',
                 'esrd_diag',
                 'heart_arrhythmias_diag',
                 'heart_failure_diag',
                 'bun',
                 'cardio_sofa',
                 'creatinine',
                 'emphysema_hist',
                 'esrd_prob',
                 'gcs',
                 'gender',
                 'heart_arrhythmias_prob',
                 'heart_failure_hist',
                 'hematologic_sofa',
                 'lipase',
                 'mapm',
                 'paco2',
                 'resp_rate',
                 'resp_sofa',
                 'sirs_hr_oor',
                 'sirs_wbc_oor',
                 'temperature',
                 'wbc',
                 'amylase',
                 'nbp_dias',
                 'renal_sofa',
                 'urine_output_24hr',
                 'worst_sofa',
                 'pao2'
                ]::text[] as names,
                ARRAY[
                    trews.shock_idx,
                    trews.hemoglobin,
                    trews.spo2,
                    trews.platelets,
                    trews.sodium,
                    trews.fluids_intake_24hr,
                    trews.rass,
                    trews.urine_output_6hr,
                    trews.neurologic_sofa,
                    trews.bun_to_cr,
                    trews.heart_rate,
                    trews.lactate,
                    trews.minutes_since_any_organ_fail,
                    trews.sirs_raw,
                    trews.sirs_temperature_oor,
                    trews.sirs_resp_oor,
                    trews.hypotension_raw,
                    trews.hypotension_intp,
                    trews.age,
                    trews.chronic_pulmonary_hist,
                    trews.chronic_bronchitis_diag,
                    trews.esrd_diag,
                    trews.heart_arrhythmias_diag,
                    trews.heart_failure_diag,
                    trews.bun,
                    trews.cardio_sofa,
                    trews.creatinine,
                    trews.emphysema_hist,
                    trews.esrd_prob,
                    trews.gcs,
                    trews.gender,
                    trews.heart_arrhythmias_prob,
                    trews.heart_failure_hist,
                    trews.hematologic_sofa,
                    trews.lipase,
                    trews.mapm,
                    trews.paco2,
                    trews.resp_rate,
                    trews.resp_sofa,
                    trews.sirs_hr_oor,
                    trews.sirs_wbc_oor,
                    trews.temperature,
                    trews.wbc,
                    trews.amylase,
                    trews.nbp_dias,
                    trews.renal_sofa,
                    trews.urine_output_24hr,
                    trews.worst_sofa,
                    trews.pao2
                ]::double precision[] as trews_values
                from pat_enc
                inner join trews on pat_enc.enc_id = trews.enc_id
                where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
            ) R, lateral unnest(R.names, R.trews_values) S(fid, trews_value)
        ) KV
    ) RKV
    where RKV.rnk <= rank_limit;

    select array_agg(distinct 'TWF.' || twf_rank.fid), array_agg(distinct quote_literal(twf_rank.fid))
            into twf_fid_exprs, twf_fid_names
    from twf_rank
    where twf_rank.fid not in (
        'age', 'chronic_bronchitis_diag', 'chronic_pulmonary_hist', 'emphysema_hist',
        'esrd_diag', 'esrd_prob', 'gender', 'heart_arrhythmias_diag', 'heart_arrhythmias_prob', 'heart_failure_diag', 'heart_failure_hist'
    );

    fid_query := format(
        'select R.enc_id, R.tsp, R.trewscore, R.fid, R.trews_value, S.cdm_value, R.rnk'
        || ' from ('
        || ' select T.enc_id, T.tsp, T.trewscore, T.fid, T.trews_value, T.rnk,'
        || ' array_cat(T.cdm_s_names, ARRAY[%s]::text[]) as cdm_names,'
        || ' array_cat(T.cdm_s_values, ARRAY[%s]::text[]) as cdm_values'
        || ' from ('
        ||   ' select T1.enc_id, T1.tsp, T1.trewscore, T1.fid, T1.trews_value, T1.rnk,'
        ||          ' array_agg(S.fid)::text[] as cdm_s_names,'
        ||          ' array_agg(S.value)::text[] as cdm_s_values'
        ||   ' from twf_rank T1 inner join cdm_s S on T1.enc_id = S.enc_id'
        ||   ' group by T1.enc_id, T1.tsp, T1.trewscore, T1.fid, T1.trews_value, T1.rnk'
        || ') T'
        || ' inner join cdm_twf TWF on T.enc_id = TWF.enc_id and T.tsp = TWF.tsp'
        || ') R'
        || ' inner join lateral unnest(R.cdm_names, R.cdm_values) as S(fid, cdm_value)'
        || ' on R.fid = S.fid'
        , array_to_string(twf_fid_names, ','), array_to_string(twf_fid_exprs, ',') );

    return query execute fid_query;

    drop table twf_rank;
    return;
end $func$ LANGUAGE plpgsql;
