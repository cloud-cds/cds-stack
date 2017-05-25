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
    EXECUTE 'SELECT INSERT INTO cdm_g (dataset_id,value,confidence) values (' || dataset_id || ',' ||quote_literal(fid_popmean)||', '||quote_literal(popmean)||', 24)';
    return popmean;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_cdm_twf_from_cdm_t(twf_fids TEXT[], twf_table TEXT, this_dataset_id int, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    with fid_date_type as (
        select fid, data_type from unnest(twf_fids) inner join cdm_feature on unnest = fid where category = 'TWF' and is_measured and dataset_id = this_dataset_id
        ),
    select_fid_array as (
        select '(' || string_agg('''' || fid || '''' , ', ') || ')' as fid_array from fid_date_type
        ),
    select_enc_id_array as (
        select '(' || string_agg(enc_id::text, ', ') || ')' as enc_id_array from unnest(enc_ids) as enc_id
        ),
    select_insert_cols as (
        select string_agg(fid || ', ' || fid || '_c' , ', ') as insert_cols from fid_date_type
        ),
    select_from_cols as (
        select string_agg(
                '((rec->>''' || fid || ''')::json->>''value'')::' || data_type || ' as ' || fid ||
                ', ((rec->>''' || fid || ''')::json->>''confidence'')::int as ' || fid || '_c'
            , ',') from_cols from fid_date_type
        ) ,
    select_set_cols as (
        select string_agg(
            fid || ' = excluded.' || fid || ', ' || fid || '_c = excluded.' || fid || '_c', ', '
            ) as set_cols from fid_date_type
        )
    select
        'insert into cdm_twf (dataset_id, enc_id, tsp, ' || insert_cols || ')
        (
          select dataset_id, enc_id, tsp, ' || from_cols || '
          from
          (
            select dataset_id, enc_id, tsp, json_object_agg(fid, json_build_object(''value'', value, ''confidence'', confidence)) as rec
            from cdm_t where dataset_id = ' || this_dataset_id || ' and fid in ' || fid_array || (case when enc_ids is not null then ' and enc_id in ' ||enc_id_array else '' end) || ' ' ||
            (case
                    when start_tsp is not null
                        then ' and tsp >= ''' || start_tsp || '''::timestamptz'
                    else '' end) ||
                (case
                    when end_tsp is not null
                        then ' and tsp <= ''' || end_tsp || '''::timestamptz'
                    else '' end)
            || '
            group by dataset_id, enc_id, tsp
          ) as T
        ) on conflict (dataset_id, enc_id, tsp) do update set ' || set_cols
    into query_str
    from select_insert_cols cross join select_from_cols cross join select_set_cols cross join select_fid_array cross join select_enc_id_array;
    raise notice '%', query_str;
    IF is_exec THEN
        execute query_str;
    END IF;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION last_value_in_window(twf_fids TEXT[], twf_table TEXT, this_dataset_id int, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    raise notice 'Fillin talbe % for fids: %', twf_table, twf_fids;
    with fid_win as (
        select fid, window_size_in_hours from unnest(twf_fids) inner join cdm_feature on unnest = fid where category = 'TWF' and is_measured and dataset_id = this_dataset_id
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
            string_agg(fid || ', ' || fid || '_c, last(case when ' || fid || ' is null then null else json_build_object(''val'', ' || fid || ', ''ts'', tsp,  ''conf'', '|| fid || '_c) end) over (partition by enc_id order by tsp rows between unbounded preceding and current row) as prev_' || fid || ', (select value::numeric from cdm_g where fid = ''' || fid || '_popmean'' and dataset_id = ' || this_dataset_id || ') as ' || fid || '_popmean', ',' || E'\n') as s_col
                    from fid_win
    ),
    select_col as (
        select string_agg('(case when ' || fid || ' is not null then ' || fid || ' when (tsp - (prev_' || fid || '->>''ts'')::timestamptz) <= ''' || window_size_in_hours || 'hours''::interval then (prev_' || fid || '->>''val'')::numeric else ' || fid || '_popmean end ) as ' || fid || ',' || E'\n' || '(case when ' || fid || ' is not null then ' || fid || '_c when (tsp - (prev_' || fid || '->>''ts'')::timestamptz) <= ''' || window_size_in_hours || 'hours''::interval then ((prev_' || fid || '->>''conf'')::int | 8) else 24 end ) as ' || fid || '_c', ',' || E'\n') as col
            from fid_win
    )
    select
    'INSERT INTO ' || twf_table || '(
    dataset_id, enc_id, tsp, ' || insert_col || '
    )
    (
        select dataset_id, enc_id, tsp,
           ' || col || '
        from (
            select dataset_id, enc_id, tsp,
            ' || s_col || '
            from (
                select dataset_id, enc_id, tsp,
                ' || r_col || '
                from ' || twf_table || '
                where dataset_id = ' || this_dataset_id  ||
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
    ) ON CONFLICT (dataset_id, enc_id, tsp) DO UPDATE SET
    ' || u_col || ';'
        into query_str from select_r_col cross join select_s_col cross join select_col cross join select_u_col cross join select_enc_id_array cross join select_insert_col;
    raise notice '%', query_str;
    IF is_exec THEN
        execute query_str;
    END IF;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION last_value_in_window(fid TEXT, target TEXT, win_h real, recalculate_popmean boolean, dataset_id int)
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
        ') = (null, null) WHERE '|| quote_ident(fid_c) ||' >= 8 and dataset_id = ' || dataset_id ;
    IF recalculate_popmean THEN
        -- calculate population mean
        SELECT INTO popmean calculate_popmean(target, fid, dataset_id );
    ELSE
        fid_popmean = fid || '_popmean';
        EXECUTE 'SELECT value from cdm_g where fid = ' || quote_literal(fid_popmean) || ' and dataset_id = ' || dataset_id INTO popmean;
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
--   does this need to depend on dataset ID?
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
    count(*) filter (where name = ''suspicion_of_infection'' and is_met)                                                            as sus_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value#>>''{0,text}'' = ''No Infection'') as sus_noinf_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value is null)                           as sus_null_count,
    count(*) filter (where name = ''crystalloid_fluid'' and is_met)                                                                 as fluid_count,
    count(*) filter (where name = ''initial_lactate'' and is_met)                                                                   as hypoperfusion_count,
    count(*) filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'')
                        and is_met
                        and severe_sepsis_wo_infection_onset is not null )                                                          as sirs_count,
    count(*) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',
                                    ''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'')
                        and is_met
                        and severe_sepsis_wo_infection_onset is not null )                                                          as organ_count,
    count(*) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met )                          as hypotension_count,
    count(*) filter (where name in (''initial_lactate_order'',''blood_culture_order'',
                                    ''antibiotics_order'', ''crystalloid_fluid_order'') and is_met )                                as sev_sep_3hr_count,
    count(*) filter (where name = ''repeat_lactate_order'' and is_met )                                                             as sev_sep_6hr_count,
    count(*) filter (where name = ''vasopressors_order'' and is_met )                                                               as sep_sho_6hr_count,
    first(override_time) filter (where name = ''suspicion_of_infection'' and is_met)                                                as sus_onset,
    (array_agg(measurement_time order by measurement_time)
       filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) )[2]                          as sirs_onset,
    min(measurement_time)
       filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',
                              ''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met )                           as organ_onset,
    min(measurement_time) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met )             as hypotension_onset,
    min(measurement_time) filter (where name = ''initial_lactate'' and is_met)                                                      as hypoperfusion_onset
from %I
where %I.pat_id = coalesce($1, %I.pat_id)
group by %I.ts, %I.pat_id
) stats', table_name, table_name, table_name, table_name, table_name, table_name, table_name)
USING this_pat_id
; END $func$ LANGUAGE plpgsql;

-------------------------------------------------
--  Criteria Management and Calculation.
-------------------------------------------------

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

create or replace function urine_output_met(urine_output numeric, weight numeric, _dataset_id integer)
    returns boolean language plpgsql as $func$
BEGIN
    return coalesce((
        urine_output / coalesce( weight, ( select value::numeric from cdm_g where fid = 'weight_popmean' and dataset_id = _dataset_id ) )
            < 0.5
    ), false);
END; $func$;

create or replace function order_met(order_name text, order_value text)
    returns boolean language plpgsql as $func$
BEGIN
    return case when order_name = 'blood_culture_order'
                    then order_value in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    )

                when order_name = 'initial_lactate_order' or order_name = 'repeat_lactate_order'
                    then order_value in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    )
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
                when order_fid = 'lactate_order' and value_text in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    ) then 'Completed'

                when order_fid = 'lactate_order' and value_text in ('None', 'Signed') then 'Ordered'

                when order_fid = 'blood_culture_order' and value_text in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    ) then 'Completed'

                when order_fid = 'blood_culture_order' and value_text in ('None', 'Signed') then 'Ordered'
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


CREATE OR REPLACE FUNCTION calculate_criteria(
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


-- ===========================================================================
-- calculate historical_criteria
-- ===========================================================================
CREATE OR REPLACE FUNCTION calculate_historical_criteria(
        this_pat_id text,
        _dataset_id INTEGER DEFAULT NULL,
        ts_start timestamptz DEFAULT '-infinity'::timestamptz,
        ts_end timestamptz DEFAULT 'infinity'::timestamptz,
        window_limit text default 'all',
        use_clarity_notes boolean default false
  )
--   @Peter, positive inf time and negative inf time?
--   passing in a null value will calculate historical criteria over all patientis
--  RETURNS table(window_ts                        timestamptz,
--                pat_id                           varchar(50),
--                pat_state                        INTEGER
--                )
  returns void
  LANGUAGE plpgsql
AS $function$
DECLARE
    window_size interval := get_parameter('lookbackhours')::interval;
    pat_id_str text;
    use_clarity_notes_str text;
BEGIN

    select coalesce(_dataset_id, max(dataset_id)) into _dataset_id from dw_version;
    raise notice 'Running calculate historical criteria on dataset_id %', _dataset_id;


    IF this_pat_id is NULL THEN
      pat_id_str = 'NULL';
    ELSE
      pat_id_str = format('''%s''',this_pat_id);
    END IF;


    if use_clarity_notes THEN
      use_clarity_notes_str = 'True';
    ELSE
      use_clarity_notes_str = 'False';
    END IF;

    drop table if exists new_criteria_windows;

     EXECUTE format(
         '
        create temporary table new_criteria_windows as
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
          calculate_criteria(coalesce(%s, window_ends.pat_id),
                             window_ends.tsp - ''%s''::interval,
                             window_ends.tsp,
                             %s, %s) new_criteria
        on window_ends.pat_id = new_criteria.pat_id;'
        , _dataset_id,  pat_id_str, _dataset_id, ts_start, ts_end, window_limit
        , pat_id_str, window_size, _dataset_id, use_clarity_notes_str);


    insert into historical_criteria (pat_id, dataset_id, pat_state, window_ts)
    select sw.pat_id, _dataset_id, sw.state, sw.ts
    from get_window_states('new_criteria_windows', this_pat_id) sw
    ON CONFLICT (pat_id, dataset_id, window_ts) DO UPDATE SET pat_state = excluded.pat_state;


    with pat_events as (
      select pat_id, ts, row_number() over () as event_id
      from ( select distinct pat_id, ts from new_criteria_windows ) PW
    )
    insert into criteria_events (
      dataset_id, event_id, pat_id, name, is_met, measurement_time,
      override_time, override_user, override_value, value, update_date, flag
    )
    select _dataset_id, pat_events.event_id, cw.pat_id, name, is_met, measurement_time,
            override_time, override_user, override_value, value, hc.window_ts, hc.pat_state-1000
    from new_criteria_windows cw
    inner join pat_events on cw.pat_id = pat_events.pat_id and cw.ts = pat_events.ts
    inner join historical_criteria hc on cw.pat_id = hc.pat_id and cw.ts = hc.window_ts
    where hc.dataset_id = _dataset_id
    on conflict (dataset_id, event_id, pat_id, name) do update
      set is_met = excluded.is_met,
          measurement_time = excluded.measurement_time,
          override_time = excluded.override_time,
          override_user = excluded.override_user,
          override_value = excluded.override_value,
          value = excluded.value,
          update_date = excluded.update_date,
          flag = excluded.flag;


    -- clean notifications
    delete from historical_notifications HN
      where HN.dataset_id = _dataset_id
      and HN.pat_id in (select distinct pat_id from new_criteria_windows);

    -- add notifications
    insert into historical_notifications (dataset_id, pat_id, message)
    select
        _dataset_id,
        NC.pat_id,
        json_build_object('alert_code', code, 'read', false,'timestamp',
            date_part('epoch',
                (case when code in ('201','204','303','306') then NC.septic_shock_onset
                    when code in ('205', '300') then NC.severe_sepsis_wo_infection_onset
                    else NC.severe_sepsis_onset
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
    from (
      select  pat_id,
              max(ts) as ts,
              severe_sepsis_onset,
              max(severe_sepsis_wo_infection_onset) as severe_sepsis_wo_infection_onset,
              septic_shock_onset
      from new_criteria_windows
      group by pat_id, severe_sepsis_onset, septic_shock_onset
    ) NC
    inner join historical_criteria HC on NC.pat_id = HC.pat_id and NC.ts = HC.window_ts
    cross join lateral unnest(flag_to_alert_codes(HC.pat_state)) code
    where HC.dataset_id = _dataset_id
    and code <> '0';


    drop table new_criteria_windows;
    return;
END; $function$;


-----------------------------------------------
-- Notification management
-----------------------------------------------

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
-- ===========================================================================
-- load_cdm_twf_to_criteria_meas
-- ===========================================================================
CREATE OR REPLACE FUNCTION load_cdm_twf_to_criteria_meas(_fid text, _dataset_id integer, incremental boolean)
  returns VOID
   LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
IF incremental THEN
  EXECUTE format(
  'insert into criteria_meas (dataset_id,         pat_id,         tsp,         fid,           value,      update_date)
    select            cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp,         ''%s''::text,  last(cdm_twf.%s),      now()
    FROM
    cdm_twf
    inner join
    pat_enc
    on cdm_twf.enc_id = pat_enc.enc_id
    and cdm_twf.dataset_id = pat_enc.dataset_id
    where cdm_twf.%s_c <8 and cdm_twf.dataset_id = %s and pat_enc.dataset_id = %s
    and (pat_enc.meta_data->>''pending'')::boolean
    group by cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp
    ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
  ',_fid,_fid,_fid,_dataset_id,_dataset_id);
ELSE
  EXECUTE format(
  'insert into criteria_meas (dataset_id,         pat_id,         tsp,         fid,           value,      update_date)
    select            cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp,         ''%s''::text,  last(cdm_twf.%s),      now()
    FROM
    cdm_twf
    inner join
    pat_enc
    on cdm_twf.enc_id = pat_enc.enc_id
    and cdm_twf.dataset_id = pat_enc.dataset_id
    where cdm_twf.%s_c <8 and cdm_twf.dataset_id = %s and pat_enc.dataset_id = %s
    group by cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp
    ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
  ',_fid,_fid,_fid,_dataset_id,_dataset_id);
END IF;
END; $function$;

-- ===========================================================================
-- load_cdm_to_criteria_meas
-- ===========================================================================
CREATE OR REPLACE FUNCTION load_cdm_to_criteria_meas(_dataset_id integer, incremental boolean default false)
 RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE
  _fid TEXT;
BEGIN
    -- ================================================
    -- Upsert Suspicion of infection proxy
    -- ================================================
    insert into suspicion_of_infection_hist (dataset_id, pat_id, name, is_met, measurement_time,override_time,override_user, override_value, value, update_date)
    select cdm_t.dataset_id, pat_enc.pat_id, 'suspicion_of_infection', true, cdm_t.tsp, cdm_t.tsp, 'cdm_t'::text, '[{"text": "infection"}]'::json,'infection'::text,now()
    from
    cdm_t
    left join
    pat_enc
    on cdm_t.enc_id = pat_enc.enc_id
    and cdm_t.dataset_id = pat_enc.dataset_id
    where cdm_t.fid='suspicion_of_infection' and cdm_t.dataset_id = _dataset_id
    and (not incremental or (pat_enc.meta_data->>'pending')::boolean)
    group by cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp
    ON CONFLICT (dataset_id, pat_id, name, override_time) DO UPDATE SET
      is_met=EXCLUDED.is_met,              measurement_time=EXCLUDED.measurement_time,
      override_user=EXCLUDED.override_user,override_value=EXCLUDED.override_value,    value=EXCLUDED.value,  update_date=EXCLUDED.update_date;
    -- ================================================
    -- Upsert cdm_t features
    -- ================================================
    insert into criteria_meas (dataset_id, pat_id, tsp, fid, value, update_date)
    select cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp, cdm_t.fid, first(cdm_t.value), now()
    FROM
    cdm_t
    inner join
    pat_enc
    on cdm_t.enc_id = pat_enc.enc_id
    and cdm_t.dataset_id = pat_enc.dataset_id
    inner JOIN
    criteria_default
    on cdm_t.fid = criteria_default.fid
    and cdm_t.dataset_id = criteria_default.dataset_id
    where cdm_t.dataset_id = _dataset_id and not(cdm_t.fid = 'suspicion_of_infection')
    and (not incremental or (pat_enc.meta_data->>'pending')::boolean)
    group by cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp, cdm_t.fid
    ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
    -- ================================================
    -- Upsert cdm_twf
    -- ================================================
    FOR _fid in
      select cd.fid
      from
      criteria_default cd
      left join
      cdm_feature f
      on cd.fid = f.fid and cd.dataset_id = f.dataset_id
      where f.category = 'TWF' and f.dataset_id = _dataset_id
      group by cd.fid
    LOOP
      PERFORM load_cdm_twf_to_criteria_meas(_fid,_dataset_id, incremental);
    END LOOP;
    -- ================================================
    -- Handle bp_sys as a special case
    -- ================================================
    raise notice 'handling bp_sys as a special case';

    insert into criteria_meas (dataset_id,         pat_id,         tsp,               fid,               value,    update_date)
    select            cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp,         'bp_sys'::text,  last(cdm_twf.nbp_sys),      now()
    FROM
    cdm_twf
    inner join
    pat_enc
    on cdm_twf.enc_id = pat_enc.enc_id
    and cdm_twf.dataset_id = pat_enc.dataset_id
    where cdm_twf.nbp_sys_c <8 and cdm_twf.dataset_id = _dataset_id
    and (not incremental or (pat_enc.meta_data->>'pending')::boolean)
    group by cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp
    ON CONFLICT (dataset_id,   pat_id,               tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;

    insert into criteria_meas (dataset_id,         pat_id,         tsp,               fid,               value,    update_date)
    select            cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp,         'bp_sys'::text,  last(cdm_twf.abp_sys),      now()
    FROM
    cdm_twf
    inner join
    pat_enc
    on cdm_twf.enc_id = pat_enc.enc_id
    and cdm_twf.dataset_id = pat_enc.dataset_id
    where cdm_twf.abp_sys_c <8 and cdm_twf.dataset_id = _dataset_id
    and (not incremental or (pat_enc.meta_data->>'pending')::boolean)
    group by cdm_twf.dataset_id, pat_enc.pat_id, cdm_twf.tsp
    ON CONFLICT (dataset_id,   pat_id,               tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;


END; $function$;


----------------------------------------------------------------------
-- calculate_trews_contributors
-- Returns a time-series of top 'rank_limit' features and values that
-- contribute to a patient's trewscore.
-- This query is used to generate the frontend chart.
--
-- TODO: use 'information_schema.columns' to dynamically
-- pull out columns in the 'trews' table for array unnesting.
-----------------------------------------------------------------------

-- TODO: remove when LMC is deployed.
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


--
-- Generalized function to compute score contributors for both TREWS and LMC.
create or replace function calculate_score_contributors(score_table text, score_attr text, this_pat_id text, rank_limit int, add_tz boolean default false)
  returns table(enc_id      int,
                tsp         timestamptz,
                score       numeric,
                fid         text,
                score_part  double precision,
                cdm_value   text,
                rnk         bigint)
as $func$
declare
    twf_fid_names text[];
    twf_fid_exprs text[];
    fid_query text;
begin
    execute format ('
    create temporary table twf_rank as
    select *
    from
    (
        select KV.*,
                rank() over ( partition by KV.enc_id, KV.tsp order by KV.score_part desc nulls last ) as rnk
        from (
            select R.enc_id, R.tsp, R.score, S.* from (
                select SCORE.enc_id,
                       (case when $3 then SCORE.tsp at time zone ''UTC'' else SCORE.tsp end) as tsp,
                       (SCORE.%I)::numeric as score,
                ARRAY[
                 ''shock_idx'',
                 ''hemoglobin'',
                 ''spo2'',
                 ''platelets'',
                 ''sodium'',
                 ''fluids_intake_24hr'',
                 ''rass'',
                 ''urine_output_6hr'',
                 ''neurologic_sofa'',
                 ''bun_to_cr'',
                 ''heart_rate'',
                 ''lactate'',
                 ''minutes_since_any_organ_fail'',
                 ''sirs_raw'',
                 ''sirs_temperature_oor'',
                 ''sirs_resp_oor'',
                 ''hypotension_raw'',
                 ''hypotension_intp'',
                 ''age'',
                 ''chronic_pulmonary_hist'',
                 ''chronic_bronchitis_diag'',
                 ''esrd_diag'',
                 ''heart_arrhythmias_diag'',
                 ''heart_failure_diag'',
                 ''bun'',
                 ''cardio_sofa'',
                 ''creatinine'',
                 ''emphysema_hist'',
                 ''esrd_prob'',
                 ''gcs'',
                 ''gender'',
                 ''heart_arrhythmias_prob'',
                 ''heart_failure_hist'',
                 ''hematologic_sofa'',
                 ''lipase'',
                 ''mapm'',
                 ''paco2'',
                 ''resp_rate'',
                 ''resp_sofa'',
                 ''sirs_hr_oor'',
                 ''sirs_wbc_oor'',
                 ''temperature'',
                 ''wbc'',
                 ''amylase'',
                 ''nbp_dias'',
                 ''renal_sofa'',
                 ''urine_output_24hr'',
                 ''worst_sofa'',
                 ''pao2''
                ]::text[] as names,
                ARRAY[
                    SCORE.shock_idx,
                    SCORE.hemoglobin,
                    SCORE.spo2,
                    SCORE.platelets,
                    SCORE.sodium,
                    SCORE.fluids_intake_24hr,
                    SCORE.rass,
                    SCORE.urine_output_6hr,
                    SCORE.neurologic_sofa,
                    SCORE.bun_to_cr,
                    SCORE.heart_rate,
                    SCORE.lactate,
                    SCORE.minutes_since_any_organ_fail,
                    SCORE.sirs_raw,
                    SCORE.sirs_temperature_oor,
                    SCORE.sirs_resp_oor,
                    SCORE.hypotension_raw,
                    SCORE.hypotension_intp,
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
                    SCORE.pao2
                ]::double precision[] as score_parts
                from pat_enc
                inner join %I SCORE on pat_enc.enc_id = SCORE.enc_id
                where pat_enc.pat_id = coalesce($1, pat_enc.pat_id)
            ) R, lateral unnest(R.names, R.score_parts) S(fid, score_part)
        ) KV
    ) RKV
    where RKV.rnk <= $2'
    , score_attr, score_table)
    using this_pat_id, rank_limit, add_tz;

    select array_agg(distinct 'TWF.' || twf_rank.fid), array_agg(distinct quote_literal(twf_rank.fid))
            into twf_fid_exprs, twf_fid_names
    from twf_rank
    where twf_rank.fid not in (
        'age', 'chronic_bronchitis_diag', 'chronic_pulmonary_hist', 'emphysema_hist',
        'esrd_diag', 'esrd_prob', 'gender', 'heart_arrhythmias_diag', 'heart_arrhythmias_prob', 'heart_failure_diag', 'heart_failure_hist'
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

-- TODO: replace calculate_trews_contributors
create or replace function calculate_trews_contributors_v2(this_pat_id text, rank_limit int)
returns table(enc_id int, tsp timestamptz, trewscore numeric, fid text, trews_value double precision, cdm_value text, rnk bigint)
as $func$
begin
  return query select * from calculate_score_contributors('trews', 'trewscore', this_pat_id, rank_limit);
end $func$ LANGUAGE plpgsql;

create or replace function calculate_lmc_contributors(this_pat_id text, rank_limit int)
returns table(enc_id int, tsp timestamptz, trewscore numeric, fid text, trews_value double precision, cdm_value text, rnk bigint)
as $func$
begin
  return query select * from calculate_score_contributors('lmcscore', 'score', this_pat_id, rank_limit, true);
end $func$ LANGUAGE plpgsql;
