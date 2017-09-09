----------------------------------------------------------------------------------------------
-- create_udf.sql
-- create all user defined functions
-- best practice: run this file every time when we deploy new version
----------------------------------------------------------------------------------------------

CREATE or replace FUNCTION _final_median(anyarray) RETURNS float8 AS $$
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)
  ) q2;
$$ LANGUAGE SQL IMMUTABLE;

DROP AGGREGATE median(anyelement);
CREATE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);

create or replace function ol_pat_enc()
RETURNS
table(enc_id integer,
      pat_id varchar(50),
      visit_id varchar(50))
AS $func$ BEGIN RETURN QUERY
select p.enc_id, p.pat_id, p.visit_id
FROM pat_enc p
WHERE p.pat_id ~ '^E'
  AND p.enc_id NOT IN
    ( SELECT distinct cdm_t.enc_id
     FROM cdm_t
     WHERE fid = 'discharge' )
AND p.enc_id NOT IN
    ( SELECT distinct cdm_t.enc_id
     FROM cdm_t
     WHERE fid = 'care_unit' and value = 'Discharge')
; END $func$ LANGUAGE plpgsql;
/*
 * UDF used in CDM
 * predefined functions
 */
CREATE OR REPLACE FUNCTION merge_cdm_g(key2 TEXT, new_value TEXT, confidence_flag INT) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_g SET value = new_value, confidence = confidence_flag WHERE fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_g(fid,value,confidence) VALUES (key2,new_value, confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_s(key1 INT, key2 TEXT, new_value TEXT, confidence_flag Int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_s SET value = new_value, confidence = confidence_flag WHERE enc_id = key1 AND fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_s(enc_id,fid,value,confidence) VALUES (key1,key2,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_m(key1 INT, key2 TEXT, key3 INT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_m SET value = new_value, confidence = confidence_flag WHERE enc_id = key1 AND fid = key2 AND line = key3;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_m(enc_id,fid,line,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_t(key1 INT, key2 timestamptz, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_t SET value = new_value, confidence=confidence_flag WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
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

CREATE OR REPLACE FUNCTION merge_cdm_twf(update_set_cols TEXT, update_set_values TEXT, update_where TEXT, insert_cols TEXT, insert_values TEXT) RETURNS VOID AS
$$
DECLARE
    tmpint  INTEGER := 0;
BEGIN
    LOOP
        -- first try to update the key
        EXECUTE 'UPDATE cdm_twf SET (' || update_set_cols || ') = ('
            || update_set_values || ') WHERE ' || update_where ;
        GET DIAGNOSTICS tmpint = ROW_COUNT;
        IF tmpint THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            EXECUTE 'INSERT INTO cdm_twf(' || insert_cols
                || ') VALUES (' || insert_values || ')';
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_twf_workspace(update_set_cols TEXT, update_set_values TEXT, update_where TEXT, insert_cols TEXT, insert_values TEXT) RETURNS VOID AS
$$
DECLARE
    tmpint  INTEGER := 0;
BEGIN
    LOOP
        -- first try to update the key
        EXECUTE 'UPDATE cdm_twf_workspace SET (' || update_set_cols || ') = ('
            || update_set_values || ') WHERE ' || update_where ;
        GET DIAGNOSTICS tmpint = ROW_COUNT;
        IF tmpint THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            EXECUTE 'INSERT INTO cdm_twf_workspace(' || insert_cols
                || ') VALUES (' || insert_values || ')';
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_cdm_g(key2 TEXT, new_value TEXT, confidence_flag INT) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_g SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_g(fid,value,confidence) VALUES (key2,new_value, confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_cdm_s(key1 INT, key2 TEXT, new_value TEXT, confidence_flag Int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_s SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_s(enc_id,fid,value,confidence) VALUES (key1,key2,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_cdm_m(key1 INT, key2 TEXT, key3 INT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_m SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND fid = key2 AND line = key3;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_m(enc_id,fid,line,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

-- add_cdm_t for medication summation
CREATE OR REPLACE FUNCTION add_cdm_t(key1 INT, key2 timestamptz, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        IF isnumeric(new_value) THEN
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

CREATE OR REPLACE FUNCTION last_value_in_window(twf_fids TEXT[], twf_table TEXT, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    raise notice 'Fillin talbe % for fids: %', twf_table, twf_fids;
    with fid_win as (
        select fid, window_size_in_hours from unnest(twf_fids) inner join cdm_feature on unnest = fid where category = 'TWF' and is_measured
    ),
    select_enc_id_array as (
        select '(' || string_agg(enc_id::text, ', ') || ')' as enc_id_array from unnest(enc_ids) as enc_id
        ),
    select_insert_col as (
        select
            string_agg(fid || ', ' || fid || '_c', ',' || E'\n') as insert_col from fid_win
    ),
    select_u_col as (
        select
            string_agg(fid || ' = excluded.' || fid || ', ' || fid || '_c = excluded.' || fid || '_c', ',' || E'\n') as u_col from fid_win
    ),
    select_r_col as (
        select
            string_agg('(case when ' || fid || '_c < 8 then ' || fid || ' else null end) as ' || fid || ', (case when ' || fid || '_c < 8 then ' || fid || '_c else null end) as ' || fid || '_c', ',' || E'\n') as r_col from fid_win
    ),
    select_s_col as(
        SELECT
            string_agg(fid || ', ' || fid || '_c, last(case when ' || fid || ' is null then null else json_build_object(''val'', ' || fid || ', ''ts'', tsp,  ''conf'', '|| fid || '_c) end) over (partition by enc_id order by tsp rows between unbounded preceding and current row) as prev_' || fid || ', (select value::numeric from cdm_g where fid = ''' || fid || '_popmean'') as ' || fid || '_popmean', ',' || E'\n') as s_col
                    from fid_win
    ),
    select_col as (
        select string_agg('(case when ' || fid || ' is not null then ' || fid || ' when (tsp - (prev_' || fid || '->>''ts'')::timestamptz) <= ''' || window_size_in_hours || 'hours''::interval then (prev_' || fid || '->>''val'')::numeric else ' || fid || '_popmean end ) as ' || fid || ',' || E'\n' || '(case when ' || fid || ' is not null then ' || fid || '_c when (tsp - (prev_' || fid || '->>''ts'')::timestamptz) <= ''' || window_size_in_hours || 'hours''::interval then ((prev_' || fid || '->>''conf'')::int | 8) else 24 end ) as ' || fid || '_c', ',' || E'\n') as col
            from fid_win
    )
    select
    'INSERT INTO ' || twf_table || '(
    enc_id, tsp, ' || insert_col || '
    )
    (
        select enc_id, tsp,
           ' || col || '
        from (
            select enc_id, tsp,
            ' || s_col || '
            from (
                select enc_id, tsp,
                ' || r_col || '
                from ' || twf_table ||
                (case
                    when start_tsp is not null or end_tsp is not null or enc_ids is not null then ' where '
                 else ' ' end)
                ||
                (case
                    when start_tsp is not null
                        then ' and tsp >= ''' || start_tsp || '''::timestamptz'
                    else '' end) ||
                (case
                    when end_tsp is not null
                        then ' and tsp <= ''' || end_tsp || '''::timestamptz'
                    else '' end) ||
                (case
                    when enc_ids is not null
                        then ' and enc_id in ' || enc_id_array
                    else '' end) || '
                order by enc_id, tsp
            ) R
        ) S
    ) ON CONFLICT (enc_id, tsp) DO UPDATE SET
    ' || u_col || ';'
        into query_str from select_r_col cross join select_s_col cross join select_col cross join select_u_col cross join select_enc_id_array cross join select_insert_col;
    raise notice '%', query_str;
    IF is_exec THEN
        execute query_str;
    END IF;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION last_value_in_window(fid TEXT, target TEXT, win_h real, recalculate_popmean boolean)
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
        SELECT INTO popmean calculate_popmean(target, fid);
    ELSE
        fid_popmean = fid || '_popmean';
        EXECUTE 'SELECT value from cdm_g where fid = ' || quote_literal(fid_popmean) INTO popmean;
    END IF;
    RAISE NOTICE 'popmean:%', popmean;
    FOR row IN EXECUTE 'SELECT enc_id, tsp, '
        || quote_ident(fid) ||' fv, '|| quote_ident(fid_c)
        ||' fc FROM ' || target || '  ORDER BY enc_id, tsp'
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
                    || ' AND '|| quote_ident(fid_c) ||' is null';
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
                    || ' AND '|| quote_ident(fid_c) ||' is null';
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
                    || ' AND '|| quote_ident(fid_c) ||' is null';
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
            || ' AND '|| quote_ident(fid_c) ||' is null';
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
RETURNS table( pat_id                               varchar(50),
               event_id                             int,
               state                                int,
               severe_sepsis_onset                  timestamptz,
               septic_shock_onset                   timestamptz,
               severe_sepsis_wo_infection_onset     timestamptz,
               severe_sepsis_wo_infection_initial   timestamptz,
               severe_sepsis_lead_time              timestamptz
             )
AS $func$ BEGIN

  RETURN QUERY
  with max_events_by_pat as (
    select    PE.pat_id, max(CE.event_id) as event_id
    from      pat_enc PE
    left join criteria_events CE on PE.pat_id = CE.pat_id
    where     PE.pat_id = coalesce(this_pat_id, PE.pat_id)
    and       ( CE.flag is null or CE.flag >= 0 )
    group by  PE.pat_id
  )
  select MEV.pat_id,
         MEV.event_id,
         coalesce(CE.flag, 0) as state,
         CE.severe_sepsis_onset,
         CE.septic_shock_onset,
         CE.severe_sepsis_wo_infection_onset,
         CE.severe_sepsis_wo_infection_initial,
         CE.severe_sepsis_lead_time
  from max_events_by_pat MEV
  left join lateral (
    select
      ICE.pat_id,
      max(flag) flag,
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

      LEAST(
          (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[1],
          min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ))
      as severe_sepsis_wo_infection_initial,

      LEAST( max(case when name = 'suspicion_of_infection' then override_time else null end),
             (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[2],
             min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ))
      as severe_sepsis_lead_time
    from
    criteria_events ICE
    where ICE.pat_id   = MEV.pat_id
    and   ICE.event_id = MEV.event_id
    and   ICE.flag >= 0
    group by ICE.pat_id
  )
  as CE on MEV.pat_id = CE.pat_id;

END $func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_states(table_name text, this_pat_id text, where_clause text default '')
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
                    when sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
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
                    when sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
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
                    when sep_sho_6hr_count = 1 then 35 -- sep_sho_6hr_com
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
%s
group by %I.pat_id
) stats', table_name, table_name, table_name, table_name, where_clause, table_name)
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
FROM (
    select * from criteria c2 where c2.pat_id = coalesce(this_pat_id, c2.pat_id)
) c
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
;
END $func$ LANGUAGE plpgsql;

create or replace function criteria_value_met(m_value text, c_ovalue json, d_ovalue json)
    returns boolean language plpgsql as $func$
BEGIN
    return coalesce(
        (c_ovalue is not null and c_ovalue#>>'{0,text}' = 'Not Indicated')
        or (c_ovalue is not null and c_ovalue#>>'{0,text}' ~* 'Clinically Inappropriate')
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

-- REVIEW: (Yanif)->(Andong): additional statuses in DW version:
-- 'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
-- 'Final', 'Final result', 'Edited Result - FINAL',
-- 'Completed', 'Corrected', 'Not Indicated'
create or replace function order_met(order_name text, order_value text)
    returns boolean language plpgsql as $func$
BEGIN
    return case when order_name = 'blood_culture_order'
                    then order_value in ('In  process', 'Preliminary', 'Final', 'Completed', 'Not Indicated') or order_value ~* 'Clinically Inappropriate'

                -- REVIEW: (Yanif)->(Andong): why is there no 'In  process' below analogously to order_status?
                when order_name = 'initial_lactate_order' or order_name = 'repeat_lactate_order'
                    then order_value in ('Preliminary', 'Sent', 'Final', 'Completed', 'Not Indicated') or order_value ~* 'Clinically Inappropriate'
                else false
            end;
END; $func$;

create or replace function dose_order_status(order_fid text, override_value_text text)
    returns text language plpgsql as $func$
BEGIN
    return case when override_value_text = 'Not Indicated' then 'Completed'
                when override_value_text ~* 'Clinically Inappropriate' then 'Completed'
                when order_fid in ('cms_antibiotics_order', 'crystalloid_fluid_order', 'vasopressors_dose_order') then 'Ordered'
                when order_fid in ('cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose') then 'Completed'
                else null
            end;
END; $func$;

-- REVIEW: (Yanif)->(Andong): additional statuses in DW version:
-- 'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
-- 'Final', 'Final result', 'Edited Result - FINAL',
-- 'Completed', 'Corrected', 'Not Indicated'
create or replace function order_status(order_fid text, value_text text, override_value_text text)
    returns text language plpgsql as $func$
BEGIN
    return case when override_value_text = 'Not Indicated' then 'Completed'
                when override_value_text = 'Clinically Inappropriate' then 'Completed'
                when order_fid = 'lactate_order' and (value_text in ('In  process', 'Sent', 'Preliminary', 'Final', 'Completed', 'Corrected', 'Not Indicated') or value_text ~* 'Clinically Inappropriate') then 'Completed'
                when order_fid = 'lactate_order' and value_text = 'Signed' then 'Ordered'
                when order_fid = 'blood_culture_order' and (value_text in ('In  process', 'Sent', 'Preliminary', 'Final', 'Completed', 'Corrected', 'Not Indicated') or value_text ~* 'Clinically Inappropriate') then 'Completed'
                when order_fid = 'blood_culture_order' and value_text = 'Signed' then 'Ordered'
                else null
            end;
END; $func$;

create or replace function dose_order_met(order_fid text, override_value_text text, dose_value numeric, dose_limit numeric)
    returns boolean language plpgsql as $func$
DECLARE
    order_status text := dose_order_status(order_fid, override_value_text);
BEGIN
    return case when override_value_text = 'Not Indicated' or override_value_text ~* 'Clinically Inappropriate' then true
                when order_status = 'Completed' then dose_value > dose_limit
                else false
            end;
END; $func$;



CREATE OR REPLACE FUNCTION calculate_criteria(this_pat_id text, ts_start timestamptz, ts_end timestamptz)
 RETURNS table(pat_id                               varchar(50),
               name                                 varchar(50),
               measurement_time                     timestamptz,
               value                                text,
               override_time                        timestamptz,
               override_user                        text,
               override_value                       json,
               is_met                               boolean,
               update_date                          timestamptz,
               severe_sepsis_onset                  timestamptz,
               severe_sepsis_wo_infection_onset     timestamptz,
               severe_sepsis_wo_infection_initial   timestamptz,
               septic_shock_onset                   timestamptz
               )
 LANGUAGE plpgsql
AS $function$
DECLARE
  orders_lookback interval := interval '6 hours';
BEGIN
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
    pat_bp_sys as (
        select pat_ids.pat_id, avg(sbp_meas.value::numeric) as value
        from pat_ids
        inner join criteria_meas sbp_meas on pat_ids.pat_id = sbp_meas.pat_id
        where isnumeric(sbp_meas.value) and sbp_meas.fid = 'bp_sys'
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
                pat_cvalues.tsp,
                (coalesce(pat_cvalues.c_ovalue#>>'{0,text}', (pat_cvalues.fid ||': '|| pat_cvalues.value))) as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                (coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value) is not null) as is_met
            from pat_cvalues
            inner join pat_enc on pat_cvalues.pat_id = pat_enc.pat_id
            where pat_cvalues.category = 'respiratory_failure'
            order by pat_cvalues.tsp
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
                                (select max(pat_bp_sys.value) from pat_bp_sys where pat_bp_sys.pat_id = pat_cvalues.pat_id),
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
               max(SC.onset) as sirs_onset,
               min(SC.initial) as sirs_initial,
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
                 (array_agg(sirs.measurement_time order by sirs.measurement_time))[2] as onset,
                 (array_agg(sirs.measurement_time order by sirs.measurement_time))[1] as initial
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
        group by IC.pat_id
    ),
    severe_sepsis_now as (
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
             sspm.severe_sepsis_wo_infection_initial,
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
               min(least(stats.sirs_initial, stats.org_df_onset)
                   ) as severe_sepsis_wo_infection_initial,
               min(least(stats.inf_onset, stats.sirs_onset, stats.org_df_onset))
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
            select  pat_cvalues.pat_id,
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
                    and (ssn.severe_sepsis_is_met
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
            with pat_fluid_overrides as (
              select CFL.pat_id, coalesce(bool_or(CFL.override_value#>>'{0,text}' = 'Not Indicated' or CFL.override_value#>>'{0,text}' ~* 'Clinically Inappropriate'), false) as override
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
              left join severe_sepsis_now SSPN on MFL.pat_id = SSPN.pat_id
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
            left join severe_sepsis_now SSPN on PC.pat_id = SSPN.pat_id

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
            select  pat_cvalues.pat_id,
                    pat_cvalues.name,
                    pat_cvalues.tsp as measurement_time,
                    pat_cvalues.value as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        and (ssn.severe_sepsis_is_met
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
    pats_that_have_orders as (
        select distinct pat_cvalues.pat_id
        from pat_cvalues
        where pat_cvalues.name in (
            'initial_lactate_order',
            'blood_culture_order',
            'antibiotics_order',
            'crystalloid_fluid_order',
            'vasopressors_order'
        )
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
            with past_criteria as (
                select  PO.pat_id, SNP.state,
                        min(SNP.severe_sepsis_lead_time) as severe_sepsis_lead_time,
                        min(SNP.septic_shock_onset) as septic_shock_onset
                from pats_that_have_orders PO
                inner join lateral get_states_snapshot(PO.pat_id) SNP on PO.pat_id = SNP.pat_id
                where SNP.state >= 20
                group by PO.pat_id, SNP.state
            ),
            orders_severe_sepsis_lead_times as (
                select LT.pat_id, min(LT.severe_sepsis_lead_time) - orders_lookback as severe_sepsis_lead_time
                from (
                    select  SSPN.pat_id,
                            min(case when SSPN.severe_sepsis_is_met then SSPN.severe_sepsis_lead_time else null end)
                                as severe_sepsis_lead_time
                        from severe_sepsis_now SSPN
                        group by SSPN.pat_id
                    union all
                    select PC.pat_id, min(PC.severe_sepsis_lead_time) as severe_sepsis_lead_time
                        from past_criteria PC
                        where PC.state >= 20
                        group by PC.pat_id
                ) LT
                group by LT.pat_id
            ),
            orders_septic_shock_onsets as (
                select ONST.pat_id, min(ONST.septic_shock_onset) as septic_shock_onset
                from (
                    select  SSHN.pat_id,
                            min(case when SSHN.septic_shock_is_met then SSHN.septic_shock_onset else null end)
                                as septic_shock_onset
                        from septic_shock_now SSHN
                        group by SSHN.pat_id
                    union all
                    select PC.pat_id, min(PC.septic_shock_onset) as septic_shock_onset
                        from past_criteria PC
                        where PC.state >= 30
                        group by PC.pat_id
                ) ONST
                group by ONST.pat_id
            )
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
                            ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OLT.severe_sepsis_lead_time, false) )
                            and ( order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value)) )

                        when pat_cvalues.category = 'after_severe_sepsis_dose' then
                            ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OLT.severe_sepsis_lead_time, false) )
                            and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value::numeric,
                                    coalesce((pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                             (pat_cvalues.d_ovalue#>>'{lower}')::numeric)) )

                        when pat_cvalues.category = 'after_septic_shock' then
                            ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OST.septic_shock_onset, false) )
                            and ( order_met(pat_cvalues.name, coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value)) )

                        when pat_cvalues.category = 'after_septic_shock_dose' then
                            ( coalesce(greatest(pat_cvalues.c_otime, pat_cvalues.tsp) > OST.septic_shock_onset, false) )
                            and ( dose_order_met(pat_cvalues.fid, pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value::numeric,
                                    coalesce((pat_cvalues.c_ovalue#>>'{0,lower}')::numeric,
                                             (pat_cvalues.d_ovalue#>>'{lower}')::numeric)) )

                        else criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        end
                    ) as is_met
            from pat_cvalues
              left join orders_severe_sepsis_lead_times OLT
                on pat_cvalues.pat_id = OLT.pat_id
              left join orders_septic_shock_onsets OST
                on pat_cvalues.pat_id = OST.pat_id
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
                       bool_or(oc.is_met) as is_met,
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
           severe_sepsis_now.severe_sepsis_onset,
           severe_sepsis_now.severe_sepsis_wo_infection_onset,
           severe_sepsis_now.severe_sepsis_wo_infection_initial,
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
CREATE OR REPLACE FUNCTION order_event_update(this_pat_id text)
RETURNS void AS $$
begin
    insert into criteria_events
    select gss.event_id,
           gss.pat_id,
           c.name,
           c.is_met,
           c.measurement_time,
           c.override_time,
           c.override_user,
           c.override_value,
           c.value,
           c.update_date,
           gss.state
    from get_states_snapshot(this_pat_id) gss
    inner join criteria c on gss.pat_id = c.pat_id and c.name ~ '_order'
    left join criteria_events e on e.pat_id = gss.pat_id and e.event_id = gss.event_id and e.name = c.name
    where gss.state in (23,24,35,36) and c.is_met and not coalesce(e.is_met, false)
    -- (
    --     -- (
    --     --     -- normal sepsis states: update met orders from criteria
    --     --     gss.state in (20,30) and c.is_met and not coalesce(e.is_met, false)
    --     --     )
    --     -- or
    --     (
    --         -- expired sepsis states: fixed unmet orders in criteria_events
    --         gss.states in (24,36) --and not coalesce(e.is_met, false)
    --         )
    --     or
    --     (
    --         -- completed sepsis states: fix all met orders from criteria
    --         gss.states in (23,35) --and c.is_met and not coalesce(e.is_met, false)
    --         )
    --     -- or
    --     -- (
    --     --     -- 3hr completed or expired
    --     --     gss.states in (21,22,31,32) and c.is_met and not coalesce(e.is_met, false)
    --     --     )
    -- )
    on conflict (event_id, pat_id, name) do update
    set is_met              = excluded.is_met,
        measurement_time    = excluded.measurement_time,
        override_time       = excluded.override_time,
        override_user       = excluded.override_user,
        override_value      = excluded.override_value,
        value               = excluded.value,
        update_date         = excluded.update_date,
        flag                = excluded.flag;
end;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION advance_criteria_snapshot(this_pat_id text default null, func_mode text default 'advance')
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
        select coalesce(snapshot.pat_id, live.pat_id) as pat_id,
               coalesce(snapshot.event_id, 0) as from_event_id,
               coalesce(snapshot.state, 0) as state_from,
               live.state as state_to
        from get_states('new_criteria', this_pat_id) live
        left join get_states_snapshot(this_pat_id) snapshot on snapshot.pat_id = live.pat_id
        where snapshot.state is null
        or snapshot.state < live.state
        or ( snapshot.state = 10 and snapshot.severe_sepsis_wo_infection_onset < now() - window_size)
    ),
    deactivate_old_snapshot as
    (
        update criteria_events
        set flag = flag - 1000
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
                    first(new_criteria.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset,
                    first(new_criteria.severe_sepsis_wo_infection_initial) severe_sepsis_wo_infection_initial
            from new_criteria
            group by new_criteria.pat_id
        ) nc on si.pat_id = nc.pat_id
        left join lateral update_notifications(si.pat_id, flag_to_alert_codes(si.state_to),
                                               nc.severe_sepsis_onset,
                                               nc.septic_shock_onset,
                                               nc.severe_sepsis_wo_infection_onset,
                                               nc.severe_sepsis_wo_infection_initial,
                                               func_mode
                                               ) n
        on si.pat_id = n.pat_id
    )
    insert into criteria_events (event_id, pat_id, name, measurement_time, value,
                                 override_time, override_user, override_value, is_met, update_date, flag)
    select s.event_id, c.pat_id, c.name, c.measurement_time, c.value,
           c.override_time, c.override_user, c.override_value, c.is_met, c.update_date,
           s.state_to as flag
    from ( select ssid.event_id, si.pat_id, si.state_to
           from state_change si
           cross join (select nextval('criteria_event_ids') event_id) ssid
    ) as s
    inner join new_criteria c on s.pat_id = c.pat_id
    left join notified_patients as np on s.pat_id = np.pat_id
    where not c.name like '%_order';
    drop table new_criteria;
    perform order_event_update(this_pat_id);
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
        set flag = flag - 1000
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
                    first(new_criteria.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset,
                    first(new_criteria.severe_sepsis_wo_infection_initial) severe_sepsis_wo_infection_initial
            from new_criteria
            group by new_criteria.pat_id
        ) nc on pat_states.pat_id = nc.pat_id
        left join lateral update_notifications(pat_states.pat_id, flag_to_alert_codes(pat_states.state),
            nc.severe_sepsis_onset, nc.septic_shock_onset, nc.severe_sepsis_wo_infection_onset, nc.severe_sepsis_wo_infection_initial, 'override') n
        on pat_states.pat_id = n.pat_id
    )
    insert into criteria_events (event_id, pat_id, name, measurement_time, value,
                                 override_time, override_user, override_value, is_met, update_date, flag)
    select ssid.event_id, NC.pat_id, NC.name, NC.measurement_time, NC.value,
           NC.override_time, NC.override_user, NC.override_value, NC.is_met, NC.update_date,
           pat_states.state as flag
    from new_criteria NC
    cross join (select nextval('criteria_event_ids') event_id) ssid
    inner join pat_states on NC.pat_id = pat_states.pat_id
    left join notified_patients np on NC.pat_id = np.pat_id
    where not NC.name like '%_order';
    drop table new_criteria;
    perform order_event_update(this_pat_id);
    return;
END; $function$;


-----------------------------------------------
-- Notification management
-----------------------------------------------
CREATE OR REPLACE FUNCTION lmcscore_alert_on(this_pat_id text, timeout text default '15 minutes')
RETURNS table(
    alert_on boolean,
    tsp timestamptz,
    score real
    )
AS $$
declare
    threshold numeric;
begin
    select value::numeric from trews_parameters where name = 'lmc_threshold' into threshold;
    return query
    select coalesce(s.score::numeric > threshold, false),
           s.tsp::timestamptz,
           s.score::real
    from (select enc_id, lmcscore.tsp, lmcscore.score, rank() over (partition by enc_id, lmcscore.tsp order by model_id desc) from lmcscore)
     s inner join pat_enc p on s.enc_id = p.enc_id
    where p.pat_id = this_pat_id and now() - s.tsp < timeout::interval and s.rank = 1
    order by s.tsp desc limit 1;
end;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION trewscore_alert_on(this_pat_id text, timeout text, model text)
RETURNS table(
    alert_on boolean,
    tsp timestamptz,
    score real
    )
AS $$
declare
    threshold numeric;
begin
    if model = 'trews' then
        select value::numeric from trews_parameters where name = 'trews_threshold' into threshold;
        return query
            select coalesce(s.trewscore::numeric > threshold, false),
                   s.tsp::timestamptz,
                   s.trewscore::real
            from trews s inner join pat_enc p on s.enc_id = p.enc_id
            where p.pat_id = this_pat_id and now() - s.tsp < timeout::interval
            order by s.tsp desc limit 1;
    else
        return query select * from lmcscore_alert_on(this_pat_id, timeout);
    end if;

end;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION pat_lmcscore_timeout(this_pat_id text)
RETURNS boolean as $$
declare
    last_tsp timestamptz;
    timeout  interval;
begin
    select coalesce(value::interval, '6 hours'::interval) from parameters where name = 'suppression_timeout' into timeout;
    select max(tsp) from lmcscore s inner join pat_enc p on s.enc_id = p.enc_id where p.pat_id = this_pat_id into last_tsp;
 return now() - last_tsp > timeout;
end;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION suppression_on(this_pat_id text)
RETURNS boolean as $$
begin
    return
    (
        select coalesce(
            (select value from parameters where name = 'suppression' limit 1)
            ~ hospital, false)
        from pat_hosp(this_pat_id));
end;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION update_suppression_alert(this_pat_id text, channel text, model text, notify boolean default false)
RETURNS void AS $$
DECLARE
    trews_alert_on boolean;
    trews_tsp timestamptz;
    trewscore real;
    curr_state int;
    tsp timestamptz;
BEGIN
    select * from trewscore_alert_on(this_pat_id, (select coalesce(value, '6 hours') from parameters where name = 'suppression_timeout'), model) into trews_alert_on, trews_tsp, trewscore;
    select state, severe_sepsis_wo_infection_initial from get_states_snapshot(this_pat_id) into curr_state, tsp;
    delete from notifications where pat_id = this_pat_id
        and notifications.message#>>'{alert_code}' ~ '205|300|206|307'
        and (notifications.message#>>'{model}' = model or not notifications.message::jsonb ? 'model');
    if curr_state = 10 then
        if coalesce(trews_alert_on, false) then
            insert into notifications (pat_id, message) values
            (
                this_pat_id,
                json_build_object('alert_code', '300', 'read', false, 'timestamp', date_part('epoch',tsp), 'suppression', 'true', 'trews_tsp', date_part('epoch', trews_tsp), 'trewscore', trewscore, 'model', model)
            );
        else
            insert into notifications (pat_id, message) values
            (
                this_pat_id,
                json_build_object('alert_code', '307', 'read', false, 'timestamp', date_part('epoch',tsp), 'suppression', 'true', 'trews_tsp', date_part('epoch', trews_tsp), 'trewscore', trewscore, 'model', model)
            );
        end if;
    end if;
    if notify then
        perform pg_notify(channel, 'invalidate_cache:' || this_pat_id || ':' || model);
        perform * from notify_future_notification(channel, this_pat_id);
    end if;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

-- '200','201','202','203','204','300','301','302','303','304','305','306'
-- update notifications when state changed
CREATE OR REPLACE FUNCTION update_notifications(this_pat_id text, alert_codes text[],
                                                severe_sepsis_onset timestamptz,
                                                septic_shock_onset timestamptz,
                                                sirs_plus_organ_onset timestamptz,
                                                sirs_plus_organ_initial timestamptz,
                                                mode text)
RETURNS table(pat_id varchar(50), alert_code text) AS $$
BEGIN
    -- clean notifications
    delete from notifications
        where notifications.pat_id = this_pat_id;
    if suppression_on(this_pat_id) and mode in ('override', 'reset') then
        -- suppression alerts
        insert into notifications (pat_id, message)
        select
            pat_enc.pat_id,
            json_build_object('alert_code',
                (case when not alert_on and tsp is not null
                    then '307'
                    else code
                end)
                , 'read', false,'timestamp',
                date_part('epoch', sirs_plus_organ_onset::timestamptz),
                'suppression', 'true',
                'model', 'trews',
                'trews_tsp', tsp,
                'trewscore', score) message
        from (select distinct pat_enc.pat_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        cross join trewscore_alert_on(pat_enc.pat_id,(select coalesce(value, '6 hours') from parameters where name = 'suppression_timeout'), 'trews') as trews
        where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
            and code = '300';
        insert into notifications (pat_id, message)
        select
            pat_enc.pat_id,
            json_build_object('alert_code',
                (case when not alert_on and tsp is not null
                    then '307'
                    else code
                end)
                , 'read', false,'timestamp',
                date_part('epoch', sirs_plus_organ_onset::timestamptz),
                'suppression', 'true',
                'model', 'lmc',
                'trews_tsp', tsp,
                'trewscore', score) message
        from (select distinct pat_enc.pat_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        cross join trewscore_alert_on(pat_enc.pat_id,(select coalesce(value, '6 hours') from parameters where name = 'suppression_timeout'),  'lmc') as trews
        where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
            and code = '300';
        -- normal notifications
        return query
        insert into notifications (pat_id, message)
        select
            pat_enc.pat_id,
            json_build_object('alert_code', code, 'read', false,'timestamp',
                date_part('epoch',
                    (case when code in ('201','204','303','306') then septic_shock_onset
                          when code = '300' then sirs_plus_organ_onset
                          else severe_sepsis_onset
                          end)::timestamptz
                    +
                    (case
                        when code = '202' then '3 hours'
                        when code in ('203','204') then '6 hours'
                        when code = '304' then '2 hours'
                        when code in ('305','306') then '5 hours'
                        else '0 hours'
                        end)::interval),
                'suppression', 'true'
            ) message
        from (select distinct pat_enc.pat_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
        and code <> '0' and code <> '300'
        returning notifications.pat_id, message#>>'{alert_code}';
    else
        -- normal notifications
        return query
        insert into notifications (pat_id, message)
        select
            pat_enc.pat_id,
            json_build_object('alert_code', code, 'read', false,'timestamp',
                date_part('epoch',
                    (case when code in ('201','204','303','306') then septic_shock_onset
                          when code = '300' then sirs_plus_organ_onset
                          else severe_sepsis_onset
                          end)::timestamptz
                    +
                    (case
                        when code = '202' then '3 hours'
                        when code in ('203','204') then '6 hours'
                        when code = '304' then '2 hours'
                        when code in ('305','306') then '5 hours'
                        else '0 hours'
                        end)::interval),
                'suppression', 'false'
            ) message
        from (select distinct pat_enc.pat_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
        and code <> '0'
        returning notifications.pat_id, message#>>'{alert_code}';
    end if;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION flag_to_alert_codes(flag int) RETURNS text[] AS $$ DECLARE ret text[]; -- complete all mappings
BEGIN -- Note the CASTING being done for the 2nd and 3rd elements of the array
 CASE
     WHEN flag = 10 THEN ret := array['300'];
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


CREATE OR REPLACE FUNCTION get_notifications_for_epic(this_pat_id text default null, model text default 'trews')
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
            (case when count(*) > 5 then 5
                  else count(*)
                  end
            ) as count
      from
      notifications
      where not (message#>>'{read}')::bool
      and (message#>>'{timestamp}')::numeric < date_part('epoch', now())
      and (not message::jsonb ? 'model' or message#>>'{model}' = model)
      group by notifications.pat_id
  )
  counts on counts.pat_id = pat_enc.pat_id

  left join (
    select cdm_t.enc_id, last(cdm_t.value order by cdm_t.tsp) as unit
    from cdm_t
    where cdm_t.fid = 'care_unit'
    group by cdm_t.enc_id
  ) pat_unit on pat_enc.enc_id = pat_unit.enc_id

  where pat_enc.pat_id like 'E%'
  and   pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
  and   pat_unit.unit similar to (select min(p.value) from parameters p where p.name = 'notifications_whitelist');
END $func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION notify_future_notification(channel text, this_pat_id text default null)
RETURNS void AS $func$
declare
  payload text;
BEGIN
  select 'future_epic_sync:' || string_agg(pat_tsp, '|') from
  (select pat_id, pat_id || ',' || string_agg(tsp, ',') pat_tsp from
      (select pat_id, ((message#>>'{timestamp}')::numeric::int)::text tsp
      from notifications
      where (message#>>'{alert_code}')::text in ('202','203','204','205','206')
      and (message#>>'{timestamp}')::numeric > date_part('epoch', now())
      and pat_id like 'E%' and pat_id = coalesce(this_pat_id, pat_id)
      order by pat_id, tsp) O
  group by pat_id) G group by pat_id into payload;
  raise notice '%', payload;
  perform pg_notify(channel, payload);
END $func$ LANGUAGE plpgsql;
----------------------------------------------------
--  deactivate functionality for patients
----------------------------------------------------

create or replace function deactivate(pid text, deactivated boolean, event_type text default null) returns void language plpgsql
as $$ begin
    insert into pat_status (pat_id, deactivated, deactivated_tsp)
        values (
            pid, deactivated, now()
        )
    on conflict (pat_id) do update
    set deactivated = excluded.deactivated, deactivated_tsp = now();
    if event_type is not null then
        insert into criteria_log (pat_id, tsp, event, update_date)
        values (pid,
                now(),
                json_build_object('event_type', event_type, 'uid', 'dba', 'deactivated', deactivated),
                now()
               );
    end if;
    -- if false then reset patient automatically
    if not deactivated then
        perform reset_patient(pid);
    end IF;
end; $$;

-- if care is completed (severe sepsis/septic shock bundle completed or expired), then deactivate the patient; after 72 hours, the patient will be reactivate
-- else if patient was in state 10 and expired, then we reset the patient

CREATE OR REPLACE FUNCTION auto_deactivate(pid text DEFAULT NULL) RETURNS void LANGUAGE plpgsql
-- deactivate patients who is active and had positive state for more than deactivate_hours'
AS $func$
declare
    res text;
BEGIN
    -- check if current snapshot (20 or 30) was expired or complished
    with pats as (
        select gss.pat_id from get_states_snapshot(pid) gss
        left join pat_status s on s.pat_id = gss.pat_id
        where (state = 23 or state = 35)
        and (s.pat_id IS NULL or not s.deactivated)
    )
    -- if criteria_events has been in an event for longer than deactivate_hours,
    -- then this patient should be deactivated automatically
    select into res deactivate(pat_id, TRUE, 'auto_deactivate') from pats;
    return;
END; $func$;


------------------------------
-- garbage collection
------------------------------

-- REVIEW: (Yanif)->(Andong): refactor. Having both garbage_collection *and* reactivate is unnecessary.
-- Andong: We may need more functions in the future
create or replace function garbage_collection(this_pat_id text default null) returns void language plpgsql as $$ begin
    perform reactivate(this_pat_id);
    perform reset_soi_pats(this_pat_id);
    perform reset_bundle_expired_pats(this_pat_id);
    -- other garbage collection functions
end; $$;

create or replace function reactivate(this_pat_id text default null) returns void language plpgsql
-- turn deactivated patients longer than deactivate_expire_hours to active
as $$ begin
    perform deactivate(s.pat_id, false, 'auto_deactivate') from pat_status s
    inner join lateral get_states_snapshot(s.pat_id) SNP on s.pat_id = SNP.pat_id
    where deactivated
    and now() - SNP.severe_sepsis_onset > get_parameter('deactivate_expire_hours')::interval
    and s.pat_id = coalesce(this_pat_id, s.pat_id);
end; $$;

create or replace function reset_soi_pats(this_pat_id text default null)
returns void language plpgsql as $$
-- reset patients who are in state 10 and expired for lookbackhours
begin
    perform reset_patient(pat_id) from
    (select distinct e.pat_id
    from criteria_events e
    inner join criteria_meas m on e.pat_id = m.pat_id
    inner join lateral get_states_snapshot(e.pat_id) SNP on e.pat_id = SNP.pat_id
    where flag = 10 and now() - SNP.severe_sepsis_wo_infection_initial > (select value from parameters where name = 'lookbackhours')::interval
    and e.pat_id = coalesce(this_pat_id, e.pat_id)) p;
end; $$;


create or replace function reset_bundle_expired_pats(this_pat_id text default null)
returns void language plpgsql as $$
declare res text;
-- reset patients who are in state 10 and expired for lookbackhours
begin
    with pats as (
    select distinct e.pat_id
        from criteria_events e
        inner join lateral get_states_snapshot(e.pat_id) SNP on e.pat_id = SNP.pat_id
        where flag in (22,24,32,34,36) and now() - SNP.severe_sepsis_onset > get_parameter('deactivate_expire_hours')::interval
        and e.pat_id = coalesce(this_pat_id, e.pat_id)
    ),
    logging as (
        insert into criteria_log (pat_id, tsp, event, update_date)
        values (
              this_pat_id,
              now(),
              '{"event_type": "reset_bundle_expired_pats", "uid":"dba"}',
              now()
          )
    )
    select reset_patient(pat_id) from pats into res;
    return;
end; $$;

create or replace function reset_patient(this_pat_id text, _event_id int default null)
returns void language plpgsql as $$
begin
    -- reset user input
    delete from criteria where pat_id = this_pat_id and override_value is not null;
    if _event_id is null then
        update criteria_events set flag = flag - 1000
        where pat_id = this_pat_id and flag > 0;
    else
        update criteria_events set flag = flag - 1000
        where pat_id = this_pat_id and event_id = _event_id;
    end if;
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
          this_pat_id,
          now(),
          '{"event_type": "reset", "uid":"dba"}',
          now()
      );
    delete from notifications where pat_id = this_pat_id;
    perform advance_criteria_snapshot(this_pat_id, 'reset');
end; $$;

----------------------------------------------------
-- deterioration feedback functions
----------------------------------------------------
CREATE OR REPLACE FUNCTION set_deterioration_feedback(pid text, tsp timestamptz, deterioration json, uid text)
    RETURNS void LANGUAGE plpgsql
AS $$ BEGIN
    INSERT INTO deterioration_feedback (pat_id, tsp, deterioration, uid)
    VALUES (pid,
            tsp,
            deterioration,
            uid)
    ON conflict (pat_id) DO UPDATE
    SET tsp = Excluded.tsp,
        deterioration = Excluded.deterioration,
        uid = Excluded.uid;
    INSERT INTO criteria_log (pat_id, tsp, event, update_date)
    VALUES ( pid,
             now(),
             json_build_object('event_type', 'set_deterioration_feedback', 'uid', uid, 'value', deterioration),
             now() );
END; $$;


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


create or replace function calculate_lmc_contributors(this_pat_id text, rank_limit int, add_tz boolean default false)
  returns table(enc_id      int,
                tsp         timestamptz,
                trewscore   numeric,
                fid         text,
                trews_value double precision,
                cdm_value   text,
                rnk         bigint)
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
                rank() over ( partition by KV.enc_id, KV.tsp order by KV.model_id, KV.score_part desc nulls last ) as rnk
        from (
            select R.model_id, R.enc_id, R.tsp, R.score, S.fid, S.score_part
            from (
                select SCORE.model_id,
                       SCORE.enc_id,
                       (case when add_tz then SCORE.tsp at time zone 'UTC' else SCORE.tsp end) as tsp,
                       SCORE.score::numeric as score,
                ARRAY[
                 'shock_idx',
                 'hemoglobin',
                 'spo2',
                 'sodium',
                 'fluids_intake_24hr',
                 'rass',
                 'urine_output_6hr',
                 'neurologic_sofa',
                 'bun_to_cr',
                 'heart_rate',
                 'minutes_since_any_organ_fail',
                 'sirs_raw',
                 'sirs_temperature_oor',
                 'sirs_resp_oor',
                 'hypotension_raw',
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
                 'pao2',
                 'fluids_intake_1hr',
                 'arterial_ph',
                 'qsofa',
                 'organ_insufficiency_hist',
                 'organ_insufficiency_diag',
                 'chronic_kidney_hist',
                 'liver_disease_hist',
                 'diabetes_hist',
                 'renal_insufficiency_diag',
                 'diabetes_diag',
                 'liver_disease_diag',
                 'renal_insufficiency_hist',
                 'minutes_since_any_antibiotics',
                 'acute_liver_failure',
                 'acute_organ_failure',
                 'any_organ_failure',
                 'weight'
                ]::text[] as names,
                ARRAY[
                    SCORE.shock_idx,
                    SCORE.hemoglobin,
                    SCORE.spo2,
                    SCORE.sodium,
                    SCORE.fluids_intake_24hr,
                    SCORE.rass,
                    SCORE.urine_output_6hr,
                    SCORE.neurologic_sofa,
                    SCORE.bun_to_cr,
                    SCORE.heart_rate,
                    SCORE.minutes_since_any_organ_fail,
                    SCORE.sirs_raw,
                    SCORE.sirs_temperature_oor,
                    SCORE.sirs_resp_oor,
                    SCORE.hypotension_raw,
                    SCORE.age,
                    SCORE.chronic_pulmonary_hist,
                    SCORE.chronic_bronchitis_diag,
                    SCORE.esrd_diag,
                    SCORE.heart_arrhythmias_diag,
                    SCORE.heart_failure_diag,
                    SCORE.bun,
                    SCORE.cardio_sofa,
                    SCORE.creatinine,
                    SCORE.emphysema_hist,
                    SCORE.esrd_prob,
                    SCORE.gcs,
                    SCORE.gender,
                    SCORE.heart_arrhythmias_prob,
                    SCORE.heart_failure_hist,
                    SCORE.hematologic_sofa,
                    SCORE.lipase,
                    SCORE.mapm,
                    SCORE.paco2,
                    SCORE.resp_rate,
                    SCORE.resp_sofa,
                    SCORE.sirs_hr_oor,
                    SCORE.sirs_wbc_oor,
                    SCORE.temperature,
                    SCORE.wbc,
                    SCORE.amylase,
                    SCORE.nbp_dias,
                    SCORE.renal_sofa,
                    SCORE.urine_output_24hr,
                    SCORE.worst_sofa,
                    SCORE.pao2,
                    SCORE.fluids_intake_1hr,
                    SCORE.arterial_ph,
                    SCORE.qsofa,
                    SCORE.organ_insufficiency_hist,
                    SCORE.organ_insufficiency_diag,
                    SCORE.chronic_kidney_hist,
                    SCORE.liver_disease_hist,
                    SCORE.diabetes_hist,
                    SCORE.renal_insufficiency_diag,
                    SCORE.diabetes_diag,
                    SCORE.liver_disease_diag,
                    SCORE.renal_insufficiency_hist,
                    SCORE.minutes_since_any_antibiotics,
                    SCORE.acute_liver_failure,
                    SCORE.acute_organ_failure,
                    SCORE.any_organ_failure,
                    SCORE.weight
                ]::double precision[] as score_parts
                from pat_enc
                inner join lmcscore SCORE on pat_enc.enc_id = SCORE.enc_id
                where pat_enc.pat_id = coalesce(this_pat_id, pat_enc.pat_id)
            ) R, lateral unnest(R.names, R.score_parts) S(fid, score_part)
        ) KV
    ) RKV
    where RKV.rnk <= rank_limit;

    select array_agg(distinct 'TWF.' || twf_rank.fid), array_agg(distinct quote_literal(twf_rank.fid))
            into twf_fid_exprs, twf_fid_names
    from twf_rank
    where twf_rank.fid not in (
        'age', 'gender',
        'chronic_pulmonary_hist',
        'emphysema_hist',
        'heart_failure_hist',
        'organ_insufficiency_hist',
        'chronic_kidney_hist',
        'liver_disease_hist',
        'diabetes_hist',
        'renal_insufficiency_hist',
        'chronic_bronchitis_diag',
        'esrd_diag',
        'heart_arrhythmias_diag',
        'heart_failure_diag',
        'organ_insufficiency_diag',
        'renal_insufficiency_diag',
        'diabetes_diag',
        'liver_disease_diag',
        'esrd_prob',
        'heart_arrhythmias_prob'
    );

    fid_query := format(
        'select R.enc_id, R.tsp, R.score, R.fid, R.score_part, S.cdm_value, R.rnk'
        || ' from ('
        || ' select T.enc_id, T.tsp, T.score, T.fid, T.score_part, T.rnk,'
        || ' array_cat(T.cdm_s_names, ARRAY[%s]::text[]) as cdm_names,'
        || ' array_cat(T.cdm_s_values, ARRAY[%s]::text[]) as cdm_values'
        || ' from ('
        ||   ' select T1.enc_id, T1.tsp, T1.score, T1.fid, T1.score_part, T1.rnk,'
        ||          ' array_agg(S.fid)::text[] as cdm_s_names,'
        ||          ' array_agg(S.value)::text[] as cdm_s_values'
        ||   ' from twf_rank T1 inner join cdm_s S on T1.enc_id = S.enc_id'
        ||   ' group by T1.enc_id, T1.tsp, T1.score, T1.fid, T1.score_part, T1.rnk'
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


------------------------------
-- LMCScore analysis.
--
create or replace function get_short_long_lmcscore_jump_distribution(bucket_size numeric, short_model_id int default 2408)
returns table(bucket_id bigint, freq bigint)
as $func$
begin
  -- Return a histogram of score difference buckets.
  return query
    select R.bucket, count(distinct R.enc_id)
    from (
      -- Join for pairs of short and long model scores, where the long model score is
      -- the first one after the last short model score.
      -- Then, compute buckets for score differences.
      select R.enc_id,
             floor(abs(min(R.score) - first(S.score order by S.tsp)) / bucket_size)::bigint as bucket
      from
      (
        -- Find the last short model timestamp and score
        select S.enc_id, max(S.tsp) as tsp, last(S.score order by S.tsp) as score
        from (
          -- Find enc_ids with multiple models
          select enc_id, count(*) as c
          from (select distinct enc_id, model_id from lmcscore) R
          group by enc_id having count(*) > 1
        ) R
        inner join (
          -- Compute transformed scores
          select enc_id, tsp, model_id, score from lmcscore
        ) S
        on R.enc_id = S.enc_id
        where S.model_id = short_model_id
        group by S.enc_id
      ) R
      left join lmcscore S
        on R.enc_id = S.enc_id and R.tsp < S.tsp
      group by R.enc_id
      having min(S.tsp) is not null
    ) R
    group by R.bucket;
end $func$ LANGUAGE plpgsql;


----------------------------------------
-- Utility methods.
----------------------------------------
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

create or replace function cdm_stats_count(_dataset_id int, _table text) returns jsonb language plpgsql as $$
declare
result jsonb;
begin
  execute 'select jsonb_build_object(''count'',
    (select count(*) from ' || _table || ' where dataset_id = '|| _dataset_id ||')::text)' into result;
return result;
end; $$;

create or replace function distribute_advance_criteria_snapshot(server text, lookback_hours int, hospital text, nprocs int default 2)
returns void language plpgsql as $$
declare
begin
  execute 'with pats as
  (select distinct m.pat_id from criteria_meas m
    inner join pat_hosp() h on h.pat_id = m.pat_id
    where now() - tsp < interval ''' || lookback_hours || ' hours'' and h.hospital = '''||hospital||'''),
  pats_group as
  (select pats.*, row_number() over () % ' || nprocs || ' g from pats),
  queries as (
    select string_agg((''select advance_criteria_snapshot(''''''||pat_id||'''''')'')::text, '';'') q from pats_group group by g
  ),
  query_arrays as (
    select array_agg(q) arr from queries
  )
  select distribute('''||server||''', arr, '|| nprocs ||') from query_arrays';
end;
$$;

create or replace function pat_hosp(this_pat_id text default null)
RETURNS
table(pat_id text, hospital text)
AS $func$ BEGIN RETURN QUERY
SELECT u.pat_id::text, (CASE WHEN unit ~* 'hc' THEN 'HCGH' WHEN unit ~* 'jh' THEN 'JHH' WHEN unit ~* 'bmc|bv' THEN 'BMC' WHEN unit ~* 'smh' THEN 'SMH' WHEN unit ~* 'sh' THEN 'SH' ELSE unit END) hospital
   FROM
     (SELECT c.pat_id,
             first(value) unit
      FROM
        (SELECT p.pat_id, t.tsp, t.value
         FROM pat_enc p
         INNER JOIN cdm_t t ON t.enc_id = p.enc_id
         WHERE fid = 'care_unit' and value <> 'Discharge'
            and p.pat_id = coalesce(this_pat_id, p.pat_id)
         ORDER BY p.pat_id,
               t.tsp DESC) c
   GROUP BY c.pat_id) u
; END $func$ LANGUAGE plpgsql;


