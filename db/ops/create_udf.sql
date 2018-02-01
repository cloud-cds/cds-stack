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



create or replace function pat_id_to_enc_id(_pat_id text) RETURNS int AS $func$
DECLARE _enc_id int;
BEGIN
select max(enc_id) from pat_enc where pat_id = _pat_id into _enc_id;
return _enc_id;
END $func$ LANGUAGE plpgsql;

create or replace function pat_id_to_visit_id(_pat_id text) RETURNS varchar(50) AS $func$
DECLARE _visit_id varchar(50);
BEGIN
select visit_id into _visit_id from pat_enc where pat_id = _pat_id order by enc_id desc limit 1;
return _visit_id;
END $func$ LANGUAGE plpgsql;


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

create or replace function workspace_fillin_delta(twf_fids text[], twf_table text, t_table text, job_id text, workspace text default 'workspace')
returns int
as $BODY$
declare
    enc_ids int[];
    num_rows int;
begin
    execute '
    --create_job_cdm_twf_table
    drop table if exists ' || workspace || '.' || job_id || '_cdm_twf;
    create unlogged table IF NOT EXISTS ' || workspace || '.' || job_id || '_cdm_twf as
    select * from cdm_twf with no data;
    alter table ' || workspace || '.' || job_id || '_cdm_twf add primary key (enc_id, tsp);
    ';

    perform load_delta_cdm_twf_from_cdm_t(twf_fids, twf_table, t_table, job_id, workspace);
    perform last_value_delta(twf_fids, twf_table);
    execute 'select count(*) from ' || workspace || '.' || job_id || '_cdm_twf;' into num_rows;
    return num_rows;
end
$BODY$
LANGUAGE plpgsql;

create or replace function workspace_submit_delta(twf_table text)
returns void
as $BODY$
declare
    cols text;
begin
    SELECT string_agg(column_name || ' = Excluded.' || column_name, ',') into cols
    FROM information_schema.columns
    WHERE table_name = 'cdm_twf' and column_name <> 'enc_id' and column_name <> 'tsp';
    execute '
    INSERT INTO cdm_twf
      SELECT * FROM ' || twf_table || '
    ON conflict (enc_id, tsp) do UPDATE SET ' || cols;
end
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_delta_cdm_twf_from_cdm_t(twf_fids TEXT[], twf_table TEXT, t_table TEXT, job_id text, workspace text, is_exec boolean default true)
-- also delete entries in cdm_twf after min_tsp
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    with fid_date_type as (
        select fid, data_type from unnest(twf_fids) inner join cdm_feature on unnest = fid where category = 'TWF' and is_measured
        ),
    select_fid_array as (
        select '(' || string_agg('''' || fid || '''' , ', ') || ')' as fid_array from fid_date_type
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
        'with idx as (
            select enc_id, min(tsp) as min_tsp
            from ' || workspace || '.cdm_t where job_id = ''' || job_id || '''
            group by enc_id
        )
        insert into ' || twf_table || ' (enc_id, tsp, ' || insert_cols || ')
        (
          select enc_id, tsp, ' || from_cols || '
          from
          (
            select cdm_t.enc_id, cdm_t.tsp, json_object_agg(fid, json_build_object(''value'', value, ''confidence'', confidence)) as rec
            from ' || t_table || ' as cdm_t inner join idx on cdm_t.enc_id = idx.enc_id and cdm_t.tsp >= idx.min_tsp'
            || ' where fid in ' || fid_array ||
            ' group by cdm_t.enc_id, cdm_t.tsp'
          ||
          ') as T
        ) on conflict (enc_id, tsp) do update set ' || set_cols || ';
        drop table if exists ' || workspace || '.' || job_id || '_cdm_twf_del;
        create unlogged table ' || workspace || '.' || job_id || '_cdm_twf_del as
            select twf.enc_id, twf.tsp from cdm_twf twf
            inner join (select enc_id, min(tsp) as tsp from ' || workspace ||'.cdm_t
                where job_id = ''' || job_id || ''' and fid in ' || fid_array || ' group by enc_id
            ) as min_tsp on twf.enc_id = min_tsp.enc_id and twf.tsp >= min_tsp.tsp;
        Delete from cdm_twf
        using ' || workspace || '.' || job_id || '_cdm_twf_del as del
        where cdm_twf.enc_id = del.enc_id and cdm_twf.tsp = del.tsp;
        '
    into query_str
    from select_insert_cols cross join select_from_cols cross join select_set_cols cross join select_fid_array;
    raise notice '%', query_str;
    IF is_exec THEN
        execute query_str;
    END IF;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION last_value_delta(twf_fids TEXT[], twf_table TEXT, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
    twf_fids_str text;
    oft_col text;
BEGIN
    raise notice 'Fillin table % for fids: %', twf_table, twf_fids;
    select string_agg('''' || unnest || '''', ',') from unnest(twf_fids) into twf_fids_str;
    select string_agg('last(value order by tsp) filter (where fid = ''' || unnest || ''') as ' || unnest || '_last,
            last(confidence order by tsp) filter (where fid = ''' || unnest || ''') as ' || unnest || '_last_c', ',') from unnest(twf_fids)
    into oft_col;
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
            string_agg(fid || ', ' || fid || '_c, coalesce(last(case when ' || fid || ' is null then null else json_build_object(''val'', ' || fid || ', ''ts'', tsp,  ''conf'', '|| fid || '_c) end) over (partition by R.enc_id order by tsp rows between unbounded preceding and current row), last(case when oft.'|| fid || '_last is null then null else json_build_object(''val'', oft.'|| fid || '_last, ''ts'', tsp, ''conf'', oft.'|| fid || '_last_c) end) over (partition by oft.enc_id) ) as prev_' || fid || ', (select value::numeric from cdm_g where fid = ''' || fid || '_popmean'' ) as ' || fid || '_popmean', ',' || E'\n') as s_col
                    from fid_win
    ),
    select_col as (
        select string_agg('(case when ' || fid || ' is not null then ' || fid || ' when prev_' || fid || ' is not null then (prev_' || fid || '->>''val'')::numeric else ' || fid || '_popmean end ) as ' || fid || ',' || E'\n' || '(case when ' || fid || ' is not null then ' || fid || '_c when prev_' || fid || ' is not null then ((prev_' || fid || '->>''conf'')::int | 8) else 24 end ) as ' || fid || '_c', ',' || E'\n') as col
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
            with oft as (
                select cdm_t.enc_id, ' || oft_col || '
                from cdm_t inner join (select enc_id, min(tsp) as min_tsp from ' || twf_table ||
                ' group by enc_id) as twf on cdm_t.enc_id = twf.enc_id and cdm_t.tsp < twf.min_tsp
                where fid in ('|| twf_fids_str ||')
                group by cdm_t.enc_id
            )
            select R.enc_id, R.tsp,
            ' || s_col || '
            from (
                select enc_id, tsp,
                ' || r_col || '
                from ' || twf_table ||
                (case when start_tsp is not null or end_tsp is not null or enc_ids is not null then ' where ' else '' end) ||
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
            ) R left join oft on R.enc_id = oft.enc_id
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

create or replace function workspace_fillin(twf_fids text[], twf_table text, t_table text, job_id text, workspace text default 'workspace')
returns void
as $BODY$
declare
    enc_ids int[];
begin
    execute 'select array_agg(enc_id) from pat_enc p
    inner join ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
    on p.visit_id = bp.visit_id' into enc_ids;
    execute '
    --create_job_cdm_twf_table
    create table IF NOT EXISTS ' || workspace || '.' || job_id || '_cdm_twf as
    select * from cdm_twf with no data;
    alter table ' || workspace || '.' || job_id || '_cdm_twf add primary key (enc_id, tsp);
    ';
    perform load_cdm_twf_from_cdm_t(twf_fids, twf_table, t_table, enc_ids, now() - (select value::interval from parameters where name = 'etl_workspace_lookbackhours'));
    perform last_value(twf_fids, twf_table);
end
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_cdm_twf_from_cdm_t(twf_fids TEXT[], twf_table TEXT, t_table TEXT, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    with fid_date_type as (
        select fid, data_type from unnest(twf_fids) inner join cdm_feature on unnest = fid where category = 'TWF' and is_measured
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
        'insert into ' || twf_table || ' (enc_id, tsp, ' || insert_cols || ')
        (
          select enc_id, tsp, ' || from_cols || '
          from
          (
            select enc_id, tsp, json_object_agg(fid, json_build_object(''value'', value, ''confidence'', confidence)) as rec
            from ' || t_table || ' where fid in ' || fid_array || (case when enc_ids is not null then ' and enc_id in ' ||enc_id_array else '' end) || ' ' ||
            (case
                    when start_tsp is not null
                        then ' and tsp >= ''' || start_tsp || '''::timestamptz'
                    else '' end) ||
                (case
                    when end_tsp is not null
                        then ' and tsp <= ''' || end_tsp || '''::timestamptz'
                    else '' end)
            || '
            group by enc_id, tsp' ||
            (case when start_tsp is not null
              then
              ' union all
              select enc_id, tsp, json_object_agg(fid, json_build_object(''value'', value, ''confidence'', confidence)) as rec
            from
            (select enc_id, fid, last(tsp order by tsp) as tsp, last(value order by tsp) as value, last(confidence order by tsp) as confidence from cdm_t where fid in ' || fid_array || (case when enc_ids is not null then ' and enc_id in ' ||enc_id_array else '' end) || ' and tsp < ''' || start_tsp || '''::timestamptz
            group by enc_id, fid) carry_on
            group by enc_id, tsp'
              else '' end)
          ||
          ') as T
        ) on conflict (enc_id, tsp) do update set ' || set_cols
    into query_str
    from select_insert_cols cross join select_from_cols cross join select_set_cols cross join select_fid_array cross join select_enc_id_array;
    raise notice '%', query_str;
    IF is_exec THEN
        execute query_str;
    END IF;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION last_value(twf_fids TEXT[], twf_table TEXT, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    raise notice 'Fillin table % for fids: %', twf_table, twf_fids;
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
            string_agg(fid || ', ' || fid || '_c, last(case when ' || fid || ' is null then null else json_build_object(''val'', ' || fid || ', ''ts'', tsp,  ''conf'', '|| fid || '_c) end) over (partition by enc_id order by tsp rows between unbounded preceding and current row) as prev_' || fid || ', (select value::numeric from cdm_g where fid = ''' || fid || '_popmean'' ) as ' || fid || '_popmean', ',' || E'\n') as s_col
                    from fid_win
    ),
    select_col as (
        select string_agg('(case when ' || fid || ' is not null then ' || fid || ' when prev_' || fid || ' is not null then (prev_' || fid || '->>''val'')::numeric else ' || fid || '_popmean end ) as ' || fid || ',' || E'\n' || '(case when ' || fid || ' is not null then ' || fid || '_c when prev_' || fid || ' is not null then ((prev_' || fid || '->>''conf'')::int | 8) else 24 end ) as ' || fid || '_c', ',' || E'\n') as col
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
                (case when start_tsp is not null or end_tsp is not null or enc_ids is not null then ' where ' else '' end) ||
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

CREATE OR REPLACE FUNCTION get_trews_parameter(key text)
RETURNS real as
$$
select value from trews_parameters where name = key;
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


CREATE OR REPLACE FUNCTION get_states_snapshot(this_enc_id int)
RETURNS table( enc_id                               int,
               event_id                             int,
               state                                int,
               severe_sepsis_onset                  timestamptz,
               septic_shock_onset                   timestamptz,
               severe_sepsis_wo_infection_onset     timestamptz,
               severe_sepsis_wo_infection_initial   timestamptz,
               severe_sepsis_lead_time              timestamptz,
               trewscore                            text,
               trewscore_threshold                  text,
               alert_flag                            int,
               septic_shock_hypotension_is_met      boolean
             )
AS $func$ BEGIN

  RETURN QUERY
  with max_events_by_pat as (
    select    PE.enc_id, max(CE.event_id) as event_id
    from      pat_enc PE
    left join criteria_events CE on PE.enc_id = CE.enc_id
    where     PE.enc_id = coalesce(this_enc_id, PE.enc_id)
    and       ( CE.flag is null or CE.flag >= 0 )
    group by  PE.enc_id
  )
  select MEV.enc_id,
         MEV.event_id,
         coalesce(CE.flag, 0) as state,
         (case when ui_severe_sepsis_onset is not null then ui_severe_sepsis_onset
            when coalesce(CE.flag, 0) in (11,14,15,25,26,27,28,29) or coalesce(CE.flag, 0) >= 40 then CE.trews_severe_sepsis_onset
            else CE.severe_sepsis_onset end) as severe_sepsis_onset,
         greatest((case when ui_septic_shock_onset is not null then ui_septic_shock_onset
            when coalesce(CE.flag, 0) in (11,14,15,25,26,27,28,29) or coalesce(CE.flag, 0) >= 40 then CE.septic_shock_onset
            else CE.septic_shock_onset end),
            (case when ui_severe_sepsis_onset is not null then ui_severe_sepsis_onset
                when coalesce(CE.flag, 0) in (11,14,15,25,26,27,28,29) or coalesce(CE.flag, 0) >= 40 then CE.trews_severe_sepsis_onset
                else CE.severe_sepsis_onset end)
         ) as septic_shock_onset,
         (case when coalesce(CE.flag, 0) in (11,14,15,25,26,27,28,29) or coalesce(CE.flag, 0) >= 40 then CE.trews_severe_sepsis_wo_infection_onset
            else CE.severe_sepsis_wo_infection_onset end) as severe_sepsis_wo_infection_onset,
         (case when coalesce(CE.flag, 0) in (11,14,15,25,26,27,28,29) or coalesce(CE.flag, 0) >= 40 then CE.trews_severe_sepsis_wo_infection_initial
            else CE.severe_sepsis_wo_infection_initial end) as severe_sepsis_wo_infection_initial,
         (case when coalesce(CE.flag, 0) in (11,14,15,25,26,27,28,29) or coalesce(CE.flag, 0) >= 40 then CE.trews_severe_sepsis_lead_time
            else CE.severe_sepsis_lead_time end) as severe_sepsis_lead_time,
         CE.trewscore,
         (case when CE.trewscore is null then null else CE.trewscore_threshold end) as trewscore_threshold,
         (case when CE.trewscore is null then null else trews_subalert_met::int end) alert_flag,
         CE.septic_shock_hypotension_cnt > 0 as septic_shock_hypotension_is_met
  from max_events_by_pat MEV
  left join lateral (
    select
      ICE.enc_id,
      max(flag) flag,
      GREATEST( max(case when name = 'suspicion_of_infection' then override_time else null end),
                (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[2],
                min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ))
      as severe_sepsis_onset,
      max(override_time) filter (where name = 'ui_severe_sepsis') ui_severe_sepsis_onset,
      max(override_time) filter (where name = 'ui_septic_shock') ui_septic_shock_onset,
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
      as severe_sepsis_lead_time,
      -- new trews timestamp
      GREATEST( max(case when name = 'suspicion_of_infection' then override_time else null end),
                min(measurement_time) filter (where name = 'trews_subalert' and is_met),
                min(measurement_time) filter (where name in ('trews_bilirubin','trews_creatinine','trews_gcs','trews_inr','trews_lactate','trews_platelet','trews_vent') and is_met ))
      as trews_severe_sepsis_onset,

      GREATEST(
          min(measurement_time) filter (where name = 'trews_subalert' and is_met),
          min(measurement_time) filter (where name in ('trews_bilirubin','trews_creatinine','trews_gcs','trews_inr','trews_lactate','trews_platelet','trews_vent') and is_met ))
      as trews_severe_sepsis_wo_infection_onset,

      LEAST(
          min(measurement_time) filter (where name = 'trews_subalert' and is_met),
          min(measurement_time) filter (where name in ('trews_bilirubin','trews_creatinine','trews_gcs','trews_inr','trews_lactate','trews_platelet','trews_vent') and is_met ))
      as trews_severe_sepsis_wo_infection_initial,

      LEAST( max(case when name = 'suspicion_of_infection' then override_time else null end),
             min(measurement_time) filter (where name = 'trews_subalert' and is_met),
             min(measurement_time) filter (where name in ('trews_bilirubin','trews_creatinine','trews_gcs','trews_inr','trews_lactate','trews_platelet','trews_vent') and is_met ))
      as trews_severe_sepsis_lead_time,
      first((value::json)#>>'{score}') filter (where name = 'trews_subalert') trewscore,
      first((value::json)#>>'{threshold}') filter (where name = 'trews_subalert') trewscore_threshold,
      count(*) filter (where name = 'trews_subalert' and is_met) trews_subalert_met,
      count(*) filter (where name in ('systolic_bp','hypotension_map','hypotension_dsbp') and is_met) septic_shock_hypotension_cnt
    from
    criteria_events ICE
    where ICE.enc_id   = MEV.enc_id
    and   ICE.event_id = MEV.event_id
    and   ICE.flag >= 0
    group by ICE.enc_id
  )
  as CE on MEV.enc_id = CE.enc_id;

END $func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_states(table_name text, this_enc_id int, where_clause text default '')
RETURNS table( enc_id int, state int) AS $func$ BEGIN RETURN QUERY EXECUTE
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
    end) as state
from
(
select %I.enc_id,
    count(*) filter (where name = ''suspicion_of_infection'' and is_met) as sus_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value#>>''{0,text}'' = ''No Infection'') as sus_noinf_count,
    count(*) filter (where name = ''suspicion_of_infection'' and (not is_met) and override_value is null) as sus_null_count,
    count(*) filter (where name = ''crystalloid_fluid'' and is_met) as fluid_count,
    count(*) filter (where name = ''initial_lactate'' and is_met) as hypoperfusion_count,
    count(*) filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) as sirs_count,
    count(*) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met ) as organ_count,
    count(*) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met ) as hypotension_count,
    count(*) filter (where name in (''initial_lactate_order'',''blood_culture_order'',''antibiotics_order'', ''crystalloid_fluid_order'') and is_met ) as sev_sep_3hr_count,
    count(*) filter (where name = ''repeat_lactate_order'' and is_met) as sev_sep_6hr_count,
    count(*) filter (where name = ''vasopressors_order'' and is_met ) as sep_sho_6hr_count,
    first(override_time) filter (where name = ''suspicion_of_infection'' and is_met) as sus_onset,
    (array_agg(measurement_time order by measurement_time)  filter (where name in (''sirs_temp'',''heart_rate'',''respiratory_rate'',''wbc'') and is_met ) )[2]   as sirs_onset,
    min(measurement_time) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and is_met ) as organ_onset,
    count(*) filter (where name in (''blood_pressure'',''mean_arterial_pressure'',''decrease_in_sbp'',''respiratory_failure'',''creatinine'',''bilirubin'',''platelet'',''inr'',''lactate'') and not is_met and override_value#>>''{0,text}'' = ''No Infection'') as orgdf_override,
    min(measurement_time) filter (where name in (''systolic_bp'',''hypotension_map'',''hypotension_dsbp'') and is_met ) as hypotension_onset,
    min(measurement_time) filter (where name = ''initial_lactate'' and is_met) as hypoperfusion_onset,
    count(*) filter (where name = ''trews_subalert'' and is_met) as trews_subalert_met,
    count(*) filter (where name ~ ''trews_'' and name <> ''trews_subalert'' and is_met) as trews_orgdf_met,
    count(*) filter (where name ~ ''trews_'' and name <> ''trews_subalert'' and not is_met and override_value#>>''{0,text}'' = ''No Infection'') as trews_orgdf_override,
    min(measurement_time) filter (where name = ''trews_subalert'' and is_met) as trews_subalert_onset,
    count(*) filter (where name = ''ui_severe_sepsis'' and is_met) as ui_severe_sepsis_cnt,
    min(override_time) filter (where name = ''ui_severe_sepsis'' and is_met) as ui_severe_sepsis_onset,
    count(*) filter (where name = ''ui_septic_shock'' and is_met) as ui_septic_shock_cnt,
    min(override_time) filter (where name = ''ui_septic_shock'' and is_met) as ui_septic_shock_onset,
    count(*) filter (where name = ''ui_deactivate'' and is_met) as ui_deactivate_cnt,
    first(GSS.state) state,
    first(GSS.severe_sepsis_onset) severe_sepsis_onset,
    first(GSS.septic_shock_onset) septic_shock_onset,
    first(GSS.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset,
    first(GSS.severe_sepsis_wo_infection_initial) severe_sepsis_wo_infection_initial,
    first(GSS.severe_sepsis_lead_time) severe_sepsis_lead_time
from %I
left join get_states_snapshot(%I.enc_id) GSS on GSS.enc_id = %I.enc_id
where %I.enc_id = coalesce($1, %I.enc_id)
%s
group by %I.enc_id
) stats', table_name, table_name, table_name, table_name, table_name, table_name, where_clause, table_name)
USING this_enc_id
; END $func$ LANGUAGE plpgsql;

-------------------------------------------------
--  Criteria Management and Calculation.
-------------------------------------------------

CREATE OR REPLACE FUNCTION get_criteria(this_enc_id int)
RETURNS table(
    enc_id              int,
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
    coalesce(e.enc_id, c.enc_id) enc_id,
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
    select * from criteria c2 where c2.enc_id = coalesce(this_enc_id, c2.enc_id)
) c
full JOIN
(
    with max_event_id_by_enc as (
        select ce2.enc_id, max(ce2.event_id) as event_id from criteria_events ce2
        where ce2.enc_id = coalesce(this_enc_id, ce2.enc_id) and ce2.flag > 0
        group by ce2.enc_id
    )
    select  ce.enc_id,
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
    inner join max_event_id_by_enc m
      on ce.enc_id = m.enc_id and ce.event_id = m.event_id
    where ce.enc_id = coalesce(this_enc_id, ce.enc_id)
) as e
on c.enc_id = e.enc_id and c.name = e.name
;
END $func$ LANGUAGE plpgsql;


-- Criteria retrieval with recent values.
-- TODO: replace get_criteria when API server has been updated.
CREATE OR REPLACE FUNCTION get_criteria_v2(this_enc_id int)
RETURNS table(
    enc_id                   int,
    event_id                 int,
    name                     varchar(50),
    is_met                   boolean,
    measurement_time         timestamptz,
    override_time            timestamptz,
    override_user            text,
    override_value           json,
    value                    text,
    recent_measurement_time  timestamptz,
    recent_value             text,
    recent_baseline_time     timestamptz,
    recent_baseline_value    text,
    trigger_baseline_time    timestamptz,
    trigger_baseline_value   text,
    update_date              timestamptz
) AS $func$ BEGIN RETURN QUERY
SELECT
    coalesce(e.enc_id, c.enc_id)                     as enc_id,
    e.event_id                                       as event_id,
    coalesce(e.name, c.name)                         as name,
    coalesce(e.is_met, c.is_met)                     as is_met,
    coalesce(e.measurement_time, c.measurement_time) as measurement_time,
    coalesce(e.override_time, c.override_time)       as override_time,
    coalesce(e.override_user, c.override_user)       as override_user,
    coalesce(e.override_value, c.override_value)     as override_value,
    coalesce(e.value, c.value)                       as value,
    recent.tsp                                       as recent_measurement_time,
    recent.value                                     as recent_value,
    recent.recent_baseline_time                      as recent_baseline_time,
    recent.recent_baseline_value                     as recent_baseline_value,

    (case
      when coalesce(e.name, c.name) in ('trews_bilirubin', 'trews_creatinine', 'trews_platelet')
      then
        -- first occurrence of largest value, excluding latest observation
        (select min(R.t order by R.v desc)
          from unnest(recent.tsp_by_tsp, recent.value_by_tsp) R(t,v)
          where R.t < coalesce(e.measurement_time, c.measurement_time))

      else
        (select first(R.t order by R.t desc)
          from unnest(recent.tsp_by_tsp) as R(t)
          where R.t < coalesce(e.measurement_time, c.measurement_time))
     end
    ) as trigger_baseline_time,

    (case
      when coalesce(e.name, c.name) in ('trews_bilirubin', 'trews_creatinine', 'trews_platelet')
      then
        -- max value before triggering value
        (select max(R.v)
          from unnest(recent.tsp_by_tsp, recent.value_by_tsp) R(t,v)
          where R.t < coalesce(e.measurement_time, c.measurement_time))

      else
        (select first(R.v order by R.t desc)
          from unnest(recent.tsp_by_tsp, recent.value_by_tsp) as R(t,v)
          where R.t < coalesce(e.measurement_time, c.measurement_time))
     end
    ) as trigger_baseline_value,

    coalesce(e.update_date, c.update_date)           as update_date
FROM (
    select * from criteria c2 where c2.enc_id = coalesce(this_enc_id, c2.enc_id)
) c
full JOIN
(
    with max_event_id_by_enc as (
        select ce2.enc_id, max(ce2.event_id) as event_id from criteria_events ce2
        where ce2.enc_id = coalesce(this_enc_id, ce2.enc_id) and ce2.flag > 0
        group by ce2.enc_id
    )
    select  ce.enc_id,
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
    inner join max_event_id_by_enc m
      on ce.enc_id = m.enc_id and ce.event_id = m.event_id
    where ce.enc_id = coalesce(this_enc_id, ce.enc_id)
) as e
on c.enc_id = e.enc_id and c.name = e.name
left join
(
  -- Retrieve most recent value, even if it falls outside the 6hr window
  -- of criteria calculation.
  select enc_ids.enc_id,
         cd.name,
         first(cdm_t.tsp order by cdm_t.tsp desc) as tsp,
         first(cdm_t.value order by cdm_t.tsp desc) as value,
         (case
            when cd.name in ('trews_bilirubin', 'trews_creatinine', 'trews_platelet')
              then
                -- first occurrence of largest value, excluding latest observation
                (select min(R.t order by R.v desc)
                  from unnest(array_agg(cdm_t.tsp order by cdm_t.value desc nulls last),
                              array_agg(cdm_t.value order by cdm_t.value desc nulls last)) R(t,v)
                  where R.t <> max(cdm_t.tsp))

            else (array_agg(cdm_t.tsp order by cdm_t.tsp desc nulls last))[2]
          end)
          as recent_baseline_time,

         (case
            when cd.name in ('trews_bilirubin', 'trews_creatinine', 'trews_platelet')
              then
              -- max value excluding latest observation
              (select max(x) from unnest((array_agg(cdm_t.value order by cdm_t.tsp desc nulls last))[2:]) R(x))

            else (array_agg(cdm_t.value order by cdm_t.tsp desc nulls last))[2]
          end)
          as recent_baseline_value,

          array_agg(cdm_t.tsp order by cdm_t.tsp desc nulls last) as tsp_by_tsp,
          array_agg(cdm_t.value order by cdm_t.tsp desc nulls last) as value_by_tsp

  from criteria_default cd
  cross join ( select distinct cdm_t.enc_id from cdm_t where cdm_t.enc_id = coalesce(this_enc_id, cdm_t.enc_id) ) enc_ids
  left join cdm_t
    on cd.fid = cdm_t.fid and enc_ids.enc_id = cdm_t.enc_id
  where cd.name not like '%_order'
  group by cd.name, enc_ids.enc_id
) as recent
on coalesce(c.enc_id, e.enc_id) = recent.enc_id and coalesce(e.name, c.name) = recent.name
;
END $func$ LANGUAGE plpgsql;


----------------------------------------------
-- Criteria calculation helpers.
--
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

-- 'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
-- 'Final', 'Final result', 'Edited Result - FINAL',
-- 'Completed', 'Corrected', 'Not Indicated'
create or replace function order_met(order_name text, order_value text, override_value text)
    returns boolean language plpgsql as $func$
BEGIN
    return case when order_name = 'blood_culture_order'
                    then
                      (override_value is not null and
                            (override_value ~* 'Clinically Inappropriate'
                                or override_value ~* 'Not Indicated')
                        )
                      or
                      (
                          order_value in (
                            'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                            'Final', 'Final result', 'Edited Result - FINAL',
                            'Completed', 'Corrected', 'Not Indicated'
                          )
                          or (order_value ~ 'status' and
                            (order_value::json)#>>'{status}' in (
                                'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                                'Final', 'Final result', 'Edited Result - FINAL',
                                'Completed', 'Corrected', 'Not Indicated'
                              )
                            )
                      )

                when order_name = 'initial_lactate_order' or order_name = 'repeat_lactate_order'
                    then
                    (override_value is not null and
                            (override_value ~* 'Clinically Inappropriate'
                                or override_value ~* 'Not Indicated')
                        )
                    or (
                      order_value in (
                        'Preliminary', 'Preliminary result',
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                      )
                      or (order_value ~ 'status' and
                            (order_value::json)#>>'{status}' in (
                                'Preliminary', 'Preliminary result',
                                'Final', 'Final result', 'Edited Result - FINAL',
                                'Completed', 'Corrected', 'Not Indicated'
                              )
                      )
                    )
                else false
            end;
END; $func$;

create or replace function dose_order_status(order_fid text, order_value text, override_value_text text)
    returns text language plpgsql as $func$
BEGIN
    return case when override_value_text = 'Not Indicated' then 'Completed'
                when override_value_text ~* 'Clinically Inappropriate' then 'Completed'
                when order_fid in ('cms_antibiotics_order', 'crystalloid_fluid_order', 'vasopressors_dose_order') and (order_value ~ 'discontinue_tsp' and (order_value::json)#>>'{discontinue_tsp}' is not null) then 'Discontinued'
                when order_fid in ('cms_antibiotics_order', 'crystalloid_fluid_order', 'vasopressors_dose_order') and (order_value ~ 'end_tsp' and (order_value::json)#>>'{end_tsp}' is not null) then 'Ended'
                when order_fid in ('cms_antibiotics_order', 'crystalloid_fluid_order', 'vasopressors_dose_order',
                                   'aminoglycosides_dose_order', 'aztreonam_dose_order', 'ciprofloxacin_dose_order',
                                   'cephalosporins_1st_gen_dose_order',
                                   'cephalosporins_2nd_gen_dose_order',
                                   'clindamycin_dose_order',
                                   'daptomycin_dose_order',
                                   'glycopeptides_dose_order',
                                   'linezolid_dose_order',
                                   'macrolides_dose_order',
                                   'penicillin_dose_order') then 'Ordered'
                when order_fid in ('cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose',
                                   'aminoglycosides_dose', 'aztreonam_dose', 'ciprofloxacin_dose',
                                   'cephalosporins_1st_gen_dose',
                                   'cephalosporins_2nd_gen_dose',
                                   'clindamycin_dose',
                                   'daptomycin_dose',
                                   'glycopeptides_dose',
                                   'linezolid_dose',
                                   'macrolides_dose',
                                   'penicillin_dose')
                        and (
                                (order_fid ~ '_dose$'
                                and (order_value::json)#>>'{action}' !~* 'cancel|stop'
                                and (order_value::json)#>>'{dose}' <> 'NaN'
                                and ((order_value::json)#>>'{dose}')::numeric > 0)
                            or
                                (order_fid !~ '_dose$' and order_value <> 'NaN')
                            )
                        then 'Completed'
                else null
            end;
END; $func$;

-- 'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result'
-- 'Final', 'Final result', 'Edited Result - FINAL',
-- 'Completed', 'Corrected', 'Not Indicated'
create or replace function order_status(order_fid text, value_text text, override_value_text text)
    returns text language plpgsql as $func$
BEGIN
    return case when override_value_text = 'Not Indicated' then 'Completed'
                when override_value_text ~* 'Clinically Inappropriate' then 'Completed'
                when order_fid = 'lactate_order' and (
                    value_text ~* 'Clinically Inappropriate'
                    or
                        value_text in (
                          'Preliminary', 'Preliminary result',
                          'Final', 'Final result', 'Edited Result - FINAL',
                          'Completed', 'Corrected', 'Not Indicated'
                        )
                    or (value_text ~ 'status' and (value_text::json)#>>'{status}' in (
                          'Preliminary', 'Preliminary result',
                          'Final', 'Final result', 'Edited Result - FINAL',
                          'Completed', 'Corrected', 'Not Indicated'
                        ))
                  )
                  then 'Completed'
                when order_fid = 'lactate_order' and
                    (
                        value_text in ('None', 'Signed', 'In process', 'In  process', 'Sent')
                        or
                        (
                            value_text ~ 'status'
                            and (
                                (value_text::json)#>>'{status}' in ('None', 'Signed', 'In process', 'In  process', 'Sent')
                                or (value_text::json)#>>'{status}' is null
                                )
                        )
                    )
                then 'Ordered'
                when order_fid = 'blood_culture_order' and (
                    value_text ~* 'Clinically Inappropriate'
                    or
                        value_text in (
                          'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                          'Final', 'Final result', 'Edited Result - FINAL',
                          'Completed', 'Corrected', 'Not Indicated'
                        )
                    or
                        (value_text ~ 'status' and (value_text::json)#>>'{status}' in (
                          'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                          'Final', 'Final result', 'Edited Result - FINAL',
                          'Completed', 'Corrected', 'Not Indicated'
                        ))
                  )
                  then 'Completed'
                when order_fid = 'blood_culture_order' and (value_text ~ 'status' and (value_text::json)#>>'{status}' in ('None', 'Signed') or (value_text::json)#>>'{status}' is null) then 'Ordered'
                else null
            end;
END; $func$;

create or replace function dose_order_met(order_fid text, override_value_text text, value text, dose_limit numeric)
    returns boolean language plpgsql as $func$
DECLARE
    order_status text := dose_order_status(order_fid, value, override_value_text);
    dose_value numeric := (case when value ~ 'dose' then ((value::json)#>>'{dose}')::numeric else value::numeric end);
BEGIN
    return case when override_value_text = 'Not Indicated' or override_value_text ~* 'Clinically Inappropriate' then true
                when order_status = 'Completed' then dose_value > dose_limit and dose_value <> 'NaN'::numeric
                else false
            end;
END; $func$;

CREATE or replace FUNCTION date_round(base_date timestamptz, round_interval INTERVAL) RETURNS timestamptz AS $BODY$
SELECT TO_TIMESTAMP((EXTRACT(epoch FROM $1)::INTEGER + EXTRACT(epoch FROM $2)::INTEGER / 2)
                / EXTRACT(epoch FROM $2)::INTEGER * EXTRACT(epoch FROM $2)::INTEGER)
$BODY$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION calculate_criteria(this_enc_id int, ts_start timestamptz, ts_end timestamptz)
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
  -- Criteria Lookbacks.
  initial_lactate_order_lookback       interval := get_parameter('initial_lactate_order_lookback');
  orders_lookback                      interval := get_parameter('orders_lookback');
  blood_culture_order_lookback         interval := get_parameter('blood_culture_order_lookback');
  antibiotics_order_lookback           interval := get_parameter('antibiotics_order_lookback');
  cms_on                               boolean  := get_parameter('cms_on');
BEGIN
raise notice 'enc_id:%', this_enc_id;
return query
    with enc_ids as (
        select distinct pat_enc.enc_id from pat_enc
        where pat_enc.enc_id = coalesce(this_enc_id, pat_enc.enc_id)
    ),
    esrd as (
        select e.enc_id
        from enc_ids e inner join cdm_s s on e.enc_id = s.enc_id
        where fid ~ 'esrd'
    ),
    seizure as (
        select e.enc_id
        from enc_ids e inner join cdm_s s on e.enc_id = s.enc_id
        where s.fid = 'chief_complaint' and s.value ~* 'seizure'
    ),
    pat_urine_output as (
        select enc_ids.enc_id, sum(uo.value::numeric) as value
        from enc_ids
        inner join cdm_t uo on enc_ids.enc_id = uo.enc_id
        where uo.fid = 'urine_output'
        and isnumeric(uo.value)
        and ts_end - uo.tsp < interval '2 hours'
        group by enc_ids.enc_id
    ),
    pat_weights as (
        select ordered.enc_id, first(ordered.value) as value
        from (
            select enc_ids.enc_id, weights.value::numeric as value
            from enc_ids
            inner join cdm_t weights on enc_ids.enc_id = weights.enc_id
            where weights.fid = 'weight'
            order by weights.tsp
        ) as ordered
        group by ordered.enc_id
    ),
    pat_bp_sys as (
        select enc_ids.enc_id, avg(t.value::numeric) as value
        from enc_ids
        inner join cdm_t t on enc_ids.enc_id = t.enc_id
        where isnumeric(t.value) and (t.fid = 'abp_sys' or t.fid = 'nbp_sys')
        group by enc_ids.enc_id
    ),
    pat_cvalues as (
        select enc_ids.enc_id,
               cd.name,
               t.fid,
               cd.category,
               t.tsp,
               t.value,
               c.override_time as c_otime,
               c.override_user as c_ouser,
               c.override_value as c_ovalue,
               cd.override_value as d_ovalue,
               c.is_met as c_ois_met
        from enc_ids
        cross join criteria_default as cd
        left join criteria c on enc_ids.enc_id = c.enc_id and cd.name = c.name
        left join cdm_t t
            on enc_ids.enc_id = t.enc_id and t.fid = cd.fid
            and (
                t.tsp is null
                or (cd.name = 'vasopressors_dose_order' and t.tsp between ts_start - orders_lookback and ts_end)
                or (cd.name in ('initial_lactate', 'initial_lactate_order', 'repeat_lactate_order')
                    and t.tsp between ts_start - initial_lactate_order_lookback and ts_end)
                or (cd.name = 'blood_culture_order' and t.tsp between ts_start - blood_culture_order_lookback and ts_end)
                or (cd.name = 'antibiotics_order' and t.tsp between ts_start - antibiotics_order_lookback and ts_end)
                or (cd.name ~ '_order' and t.tsp between ts_start - orders_lookback and ts_end)
                or (cd.name !~ '_order' and t.tsp between ts_start and ts_end)
                )
    ),
    infection as (
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
    ),
    trews_orgdf as (
        select
            ordered.enc_id,
            ordered.name,
            (case
                when ordered.name = 'trews_bilirubin' then last(((ordered.orgdf_details::json)#>>'{bilirubin_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_creatinine' then last(((ordered.orgdf_details::json)#>>'{creatinine_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_dsbp' then last(((ordered.orgdf_details::json)#>>'{delta_hypotetrigger,tsp}' )::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_gcs' then last(((ordered.orgdf_details::json)#>>'{gcs_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_inr' then last(((ordered.orgdf_details::json)#>>'{inr_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_lactate' then last(((ordered.orgdf_details::json)#>>'{lactate_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_map' then last(((ordered.orgdf_details::json)#>>'{map_hypotetrigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_platelet' then last(((ordered.orgdf_details::json)#>>'{platelets_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_sbpm' then last(((ordered.orgdf_details::json)#>>'{sbpm_hypotetrigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_vasopressors' then last(((ordered.orgdf_details::json)#>>'{vasopressors_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_vent' then last(((ordered.orgdf_details::json)#>>'{vent_trigger,tsp}')::timestamptz ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                else null end
            )::timestamptz as measurement_time,
            (case when ordered.name = 'trews_bilirubin' then last(((ordered.orgdf_details::jsonb)#>'{bilirubin_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_creatinine' then last(((ordered.orgdf_details::jsonb)#>'{creatinine_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_dsbp' then last(((ordered.orgdf_details::jsonb)#>'{delta_hypotetrigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_gcs' then last(((ordered.orgdf_details::jsonb)#>'{gcs_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_inr' then last(((ordered.orgdf_details::jsonb)#>'{inr_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_lactate' then last(((ordered.orgdf_details::jsonb)#>'{lactate_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_map' then last(((ordered.orgdf_details::jsonb)#>'{map_hypotetrigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_platelet' then last(((ordered.orgdf_details::jsonb)#>'{platelets_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_sbpm' then last(((ordered.orgdf_details::jsonb)#>'{sbpm_hypotetrigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_vasopressors' then last(((ordered.orgdf_details::jsonb)#>'{vasopressors_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_vent' then last(((ordered.orgdf_details::jsonb)#>'{vent_trigger}' #- '{tsp}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
            else null end) as value,
            (last(ordered.c_otime ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)) as override_time,
            (last(ordered.c_ouser ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)) as override_user,
            (last(ordered.c_ovalue ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)) as override_value,
            coalesce((case when last(ordered.c_ovalue#>>'{0,text}' ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz) = 'No Infection' then false
                when ordered.name = 'trews_bilirubin' then last(ordered.bilirubin_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_creatinine' then last(ordered.creatinine_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_dsbp' then last(ordered.delta_hypotension::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_gcs' then last(ordered.gcs_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_inr' then last(ordered.inr_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_lactate' then last(ordered.lactate_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_map' then last(ordered.map_hypotension::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_platelet' then last(ordered.platelets_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_sbpm' then last(ordered.sbpm_hypotension::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_vasopressors' then last(ordered.vasopressors_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                when ordered.name = 'trews_vent' then last(ordered.vent_orgdf::numeric = 1 ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
            else false end), false) as is_met,
            now() as update_date,
            false as is_acute -- NOTE: not implemented
        from (
            select pc.enc_id, pc.name,
            pc.c_otime, pc.c_ouser, pc.c_ovalue,
            ts.tsp, ts.score, ts.odds_ratio, ts.creatinine_orgdf,
            ts.bilirubin_orgdf, ts.platelets_orgdf, ts.gcs_orgdf, ts.inr_orgdf, ts.sbpm_hypotension, ts.map_hypotension, ts.delta_hypotension, ts.vasopressors_orgdf, ts.lactate_orgdf, ts.vent_orgdf,ts.orgdf_details
            from pat_cvalues pc
            left join trews_jit_score ts on pc.enc_id = ts.enc_id
            and ts.model_id = get_trews_parameter('trews_jit_model_id') and orgdf_details !~ '"tsp":"null"'
            where pc.name ~* 'trews_' and pc.name <> 'trews_subalert'
        ) ordered
        group by ordered.enc_id, ordered.name
    ),
    trews_subalert as (
        select
            ordered.enc_id,
            ordered.name,
            last(ordered.tsp ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                as measurement_time,
            last(json_build_object('score', ordered.score,
                                   'threshold', (ordered.orgdf_details::jsonb)#>'{th}',
                                   'alert', (ordered.orgdf_details::jsonb)#>'{alert}',
                                   'pct_mortality', (ordered.orgdf_details::jsonb)#>'{percent_mortality}',
                                   'pct_sevsep', (ordered.orgdf_details::jsonb)#>'{percent_sevsep}',
                                   'heart_rate', (ordered.orgdf_details::jsonb)#>'{heart_rate}',
                                   'lactate', (ordered.orgdf_details::jsonb)#>'{lactate}',
                                   'no_lab', (ordered.orgdf_details::jsonb)#>'{no_lab}',
                                   'sbpm', (ordered.orgdf_details::jsonb)#>'{sbpm}')::text ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)
                as value,
            (last(ordered.c_otime ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)) as override_time,
            (last(ordered.c_ouser ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)) as override_user,
            (last(ordered.c_ovalue ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz)) as override_value,
            coalesce(
                (case when last(ordered.c_ovalue#>>'{0,text}' ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz) = 'No Infection' then false
                    else last(((ordered.orgdf_details::jsonb)#>>'{alert}')::boolean ORDER BY tsp, ((orgdf_details::jsonb)#>>'{pred_time}')::timestamptz) and bool_or(trews_is_met)
                end)
            , false) as is_met,
            now() as update_date,
            false as is_acute -- NOTE: not implemented
        from (
            select pc.enc_id, pc.name,
            pc.c_otime, pc.c_ouser, pc.c_ovalue,
            ts.tsp, ts.score, ts.odds_ratio, ts.creatinine_orgdf,
            ts.bilirubin_orgdf, ts.platelets_orgdf, ts.gcs_orgdf, ts.inr_orgdf, ts.sbpm_hypotension, ts.map_hypotension, ts.delta_hypotension, ts.vasopressors_orgdf, ts.lactate_orgdf, ts.vent_orgdf,ts.orgdf_details,
            trews_orgdf.is_met trews_is_met
            from pat_cvalues pc
            left join trews_orgdf on pc.enc_id = trews_orgdf.enc_id
            left join trews_jit_score ts on pc.enc_id = ts.enc_id
            and ts.model_id = get_trews_parameter('trews_jit_model_id') and orgdf_details !~ '"tsp":"null"'
            where pc.name = 'trews_subalert'
        ) ordered
        group by ordered.enc_id, ordered.name
    ),
    trews_live as (
        select * from trews_subalert
        union all
        select * from trews_orgdf
    ),
    trews as (
        select trews_live.enc_id, trews_live.name,
        coalesce(ce.measurement_time, trews_live.measurement_time) measurement_time,
        coalesce(ce.value, trews_live.value) as value,
        trews_live.override_time,
        trews_live.override_user,
        trews_live.override_value,
        coalesce(ce.is_met, trews_live.is_met) is_met,
        trews_live.update_date,
        trews_live.is_acute
        from trews_live left join criteria_events ce on ce.enc_id = trews_live.enc_id and ce.name = trews_live.name
        and ce.flag in (25,26,27,28,29,40,41,42,43,44,45,46) and not (
            -- not all trews_orgdf is overrided with No Infection
            select (count(*) filter (where TOR.is_met)) = 0
                and (count(*) filter (where TOR.override_value#>>'{0,text}' = 'No Infection')) > 0
             from trews_orgdf TOR
        )
        and ce.event_id = (select max(ce2.event_id) from criteria_events ce2 where ce2.enc_id = trews_live.enc_id)
    ),
    sirs as (
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
                    criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue) and cms_on as is_met
            from pat_cvalues
            where pat_cvalues.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc')
            order by pat_cvalues.tsp
        ) as ordered
        group by ordered.enc_id, ordered.name
    ),
    respiratory_failures as (
        select
            ordered.enc_id,
            ordered.name,
            (first(ordered.tsp order by ordered.tsp) filter (where ordered.is_met)) as measurement_time,
            (first(ordered.value order by ordered.tsp) filter (where ordered.is_met))::text as value,
            (first(ordered.c_otime order by ordered.tsp) filter (where ordered.is_met)) as override_time,
            (first(ordered.c_ouser order by ordered.tsp) filter (where ordered.is_met)) as override_user,
            (first(ordered.c_ovalue order by ordered.tsp) filter (where ordered.is_met)) as override_value,
            coalesce(bool_or(ordered.is_met), false) as is_met,
            now() as update_date
        from (
            select
                pat_cvalues.enc_id,
                pat_cvalues.name,
                pat_cvalues.tsp,
                (coalesce(pat_cvalues.c_ovalue#>>'{0,text}', (pat_cvalues.fid ||': '|| pat_cvalues.value))) as value,
                pat_cvalues.c_otime,
                pat_cvalues.c_ouser,
                pat_cvalues.c_ovalue,
                (case
                    when pat_cvalues.c_ovalue#>>'{0,text}' = 'No Infection' then false
                    else coalesce(pat_cvalues.c_ovalue#>>'{0,text}', pat_cvalues.value) is not null
                  end) and cms_on as is_met
            from pat_cvalues
            inner join pat_enc on pat_cvalues.enc_id = pat_enc.enc_id
            where pat_cvalues.category = 'respiratory_failure'
            order by pat_cvalues.tsp
        ) as ordered
        group by ordered.enc_id, ordered.name
    ),
    organ_dysfunction_except_rf as (
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
                    (case when pat_cvalues.name = 'creatinine'
                            then pat_cvalues.fid || ':' || pat_cvalues.value
                          else pat_cvalues.value
                      end) as value,
                    pat_cvalues.c_otime,
                    pat_cvalues.c_ouser,
                    pat_cvalues.c_ovalue,
                    (case
                        when pat_cvalues.c_ovalue#>>'{0,text}' = 'No Infection' then false

                        when pat_cvalues.category = 'decrease_in_sbp' then
                            decrease_in_sbp_met(
                                (select max(pat_bp_sys.value::numeric) from pat_bp_sys where pat_bp_sys.enc_id = pat_cvalues.enc_id),
                                pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)

                        when pat_cvalues.category = 'urine_output' then
                            urine_output_met(
                                (select max(pat_urine_output.value) from pat_urine_output where pat_urine_output.enc_id = pat_cvalues.enc_id),
                                (select max(pat_weights.value) from pat_weights where pat_weights.enc_id = pat_cvalues.enc_id)
                            )
                        when pat_cvalues.name = 'creatinine' then
                            criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue) and esrd.enc_id is null -- excluded esrd enc_ids
                        when pat_cvalues.name = 'lactate' then
                            criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue) and seizure.enc_id is null -- exclued seizure enc_ids
                        else criteria_value_met(pat_cvalues.value, pat_cvalues.c_ovalue, pat_cvalues.d_ovalue)
                        end
                    ) and cms_on as is_met
            from pat_cvalues
            left join esrd on pat_cvalues.enc_id = esrd.enc_id
            left join seizure on pat_cvalues.enc_id = seizure.enc_id
            where pat_cvalues.name in (
              'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp',
              'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate'
            )
            order by pat_cvalues.tsp
        ) as ordered
        group by ordered.enc_id, ordered.name
    ),
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
    ),
    ui_severe_sepsis as (
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
    ),
    ui_septic_shock as (
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
    ),
    severe_sepsis as (
        select *, null::boolean as is_acute from infection
        union all select *, null::boolean as is_acute from sirs
        union all select *, null::boolean as is_acute from respiratory_failures
        union all select *, null::boolean as is_acute from organ_dysfunction_except_rf
        union all select * from trews
        union all select *, null::boolean as is_acute from ui_severe_sepsis
        union all select *, null::boolean as is_acute from ui_deactivate
    ),
    severe_sepsis_criteria as (
        with organ_dysfunction as (
            select * from respiratory_failures
            union all select * from organ_dysfunction_except_rf
        )
        select IC.enc_id,
               sum(IC.cnt) > 0 as suspicion_of_infection,
               sum(TA.cnt) > 0 as trews_subalert,
               sum(SC.cnt) as sirs_cnt,
               sum(OC.cnt) as org_df_cnt,
               max(IC.onset) as inf_onset,
               max(TA.onset) as trews_subalert_onset,
               max(SC.onset) as sirs_onset,
               min(SC.initial) as sirs_initial,
               max(OC.onset) as org_df_onset,
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
        left join
        (
          select sirs.enc_id,
                 sum(case when sirs.is_met then 1 else 0 end) as cnt,
                 (array_agg(sirs.measurement_time order by sirs.measurement_time))[2] as onset,
                 (array_agg(sirs.measurement_time order by sirs.measurement_time))[1] as initial
          from sirs
          group by sirs.enc_id
        ) SC on IC.enc_id = SC.enc_id
        left join
        (
          select organ_dysfunction.enc_id,
                 sum(case when organ_dysfunction.is_met then 1 else 0 end) as cnt,
                 min(organ_dysfunction.measurement_time) as onset
          from organ_dysfunction
          group by organ_dysfunction.enc_id
        ) OC on IC.enc_id = OC.enc_id
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
    ),
    severe_sepsis_now as (
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
    ),
    crystalloid_fluid as (
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
    ),
    hypotension as (
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
    ),
    hypoperfusion as (
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
    ),
    septic_shock as (
        select * from crystalloid_fluid
        union all select * from hypotension
        union all select * from hypoperfusion
        union all select * from ui_septic_shock
    ),
    septic_shock_now as (
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
    ),
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
                            (case when pat_cvalues.fid in ('aminoglycosides_dose','aminoglycosides_dose_order',
                                                           'aztreonam_dose','aztreonam_dose_order',
                                                           'ciprofloxacin_dose','ciprofloxacin_dos_ordere'
                                                           ) then 'comb1'
                                  when pat_cvalues.fid in ('cephalosporins_1st_gen_dose','cephalosporins_1st_gen_dose_order',
                                                           'cephalosporins_2nd_gen_dose','cephalosporins_2nd_gen_dose_order',
                                                           'clindamycin_dose','clindamycin_dose_order',
                                                           'daptomycin_dose','daptomycin_dose_order',
                                                           'glycopeptides_dose','glycopeptides_dose_order',
                                                           'linezolid_dose','linezolid_dose_order',
                                                           'macrolides_dose','macrolides_dose_order',
                                                           'penicillin_d_orderose','penicillin_dose'
                                                           ) then 'comb2'
                             else null end)::text as name,
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
                            or (pat_cvalues.name = 'blood_culture_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_blood_culture_order)
                            or (pat_cvalues.name = 'antibiotics_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_antibiotics_order)
                            or (pat_cvalues.name = 'crystalloid_fluid_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_order)
                            or (pat_cvalues.name = 'vasopressors_order' and pat_cvalues.tsp > OST.septic_shock_onset)
                                then pat_cvalues.tsp
                            else null end) as measurement_time,
                        (case when (pat_cvalues.name = 'initial_lactate_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_initial_lactate_order)
                            or (pat_cvalues.name = 'blood_culture_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_blood_culture_order)
                            or (pat_cvalues.name = 'antibiotics_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_antibiotics_order)
                            or (pat_cvalues.name = 'crystalloid_fluid_order' and pat_cvalues.tsp > OLT.severe_sepsis_onset_for_order)
                            or (pat_cvalues.name = 'vasopressors_order' and pat_cvalues.tsp > OST.septic_shock_onset)
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
                where pat_cvalues.name in (
                    'initial_lactate_order',
                    'blood_culture_order',
                    'crystalloid_fluid_order',
                    'vasopressors_order'
                ) or (pat_cvalues.name = 'antibiotics_order' and pat_cvalues.category = 'after_severe_sepsis_dose')
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
    left join septic_shock_now on new_criteria.enc_id = septic_shock_now.enc_id;

return;
END; $function$;

--------------------------------------------
-- Criteria snapshot utilities.
--------------------------------------------
CREATE OR REPLACE FUNCTION order_event_update(this_enc_id int)
RETURNS void AS $$
begin
    insert into criteria_events (event_id, enc_id, name, is_met, measurement_time, override_time, override_user, override_value, value, update_date, flag)
    select gss.event_id,
           gss.enc_id,
           c.name,
           c.is_met,
           c.measurement_time,
           (case when c.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else c.override_time end),
           (case when c.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else c.override_user end),
           (case when c.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else c.override_value end),
           c.value,
           c.update_date,
           gss.state
    from get_states_snapshot(this_enc_id) gss
    inner join criteria c on gss.enc_id = c.enc_id and c.name ~ '_order'
    left join criteria_events e on e.enc_id = gss.enc_id and e.event_id = gss.event_id and e.name = c.name
    where gss.state in (23,24,28,29,35,36,45,46,53,54,65,66) and c.is_met and not coalesce(e.is_met, false)
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
    on conflict (event_id, enc_id, name) do update
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

create or replace function advance_criteria_snapshot_enc_ids(enc_ids int[]) returns void language plpgsql as $$
begin
    perform advance_criteria_snapshot(enc_id) from unnest(enc_ids) as enc_id;
end; $$;

CREATE OR REPLACE FUNCTION advance_criteria_snapshot(this_enc_id int default null, func_mode text default 'advance')
RETURNS void AS $$
DECLARE
    ts_end timestamptz := now();
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    perform auto_deactivate(this_enc_id);

    create temporary table new_criteria as
        select * from calculate_criteria(this_enc_id, ts_end - window_size, ts_end);

    with criteria_inserts as
    (
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
        returning *
    ),
    state_change as
    (
        select coalesce(snapshot.enc_id, live.enc_id) as enc_id,
               coalesce(snapshot.event_id, 0) as from_event_id,
               coalesce(snapshot.state, 0) as state_from,
               live.state as state_to
        from get_states('new_criteria', this_enc_id) live
        left join get_states_snapshot(this_enc_id) snapshot on snapshot.enc_id = live.enc_id
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
    ),
    deactivate_old_snapshot as
    (
        update criteria_events
        set flag = flag - 1000
        from state_change
        where criteria_events.event_id = state_change.from_event_id
        and criteria_events.enc_id = state_change.enc_id
        and state_change.state_from >= 0
    ),
    notified_patients as (
        select distinct si.enc_id
        from state_change si
        inner join (
            select  new_criteria.enc_id,
                    first(new_criteria.severe_sepsis_onset) severe_sepsis_onset,
                    first(new_criteria.septic_shock_onset) septic_shock_onset,
                    first(new_criteria.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset,
                    first(new_criteria.severe_sepsis_wo_infection_initial) severe_sepsis_wo_infection_initial
            from new_criteria
            group by new_criteria.enc_id
        ) nc on si.enc_id = nc.enc_id
        left join lateral update_notifications(si.enc_id,
            flag_to_alert_codes(si.state_to),
            nc.severe_sepsis_onset,
            nc.septic_shock_onset,
            nc.severe_sepsis_wo_infection_onset,
            nc.severe_sepsis_wo_infection_initial,
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
    drop table new_criteria;
    perform order_event_update(this_enc_id);
    RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION override_criteria_snapshot(this_enc_id int default null)
    RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
    ts_end timestamptz := now();
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    create temporary table new_criteria as
    select * from calculate_criteria(this_enc_id, ts_end - window_size, ts_end);

    -- Deactivate old snapshots, and add a new snapshot.
    with pat_states as (
        select * from get_states('new_criteria', this_enc_id)
    ),
    criteria_inserts as (
        insert into criteria (enc_id, name, is_met, measurement_time, value, override_time, override_user, override_value, update_date, is_acute)
        select enc_id, name, is_met, measurement_time, value, override_time, override_user, override_value, update_date, is_acute
        from new_criteria
        -- NOTE: currently allow all criteria being updated
        -- name in ( 'suspicion_of_infection', 'crystalloid_fluid')
        on conflict (enc_id, name) do update
        set is_met              = excluded.is_met,
            measurement_time    = excluded.measurement_time,
            value               = excluded.value,
            update_date         = excluded.update_date,
            is_acute            = excluded.is_acute
        returning *
    ),
    deactivate_old_snapshot as (
        update criteria_events
        set flag = flag - 1000
        from new_criteria
        where criteria_events.event_id = (
            select max(event_id) from criteria_events ce
            where ce.enc_id = new_criteria.enc_id and ce.flag >= 0
        )
        and criteria_events.enc_id = new_criteria.enc_id
    ),
    notified_patients as (
        select distinct pat_states.enc_id
        from pat_states
        inner join (
            select  new_criteria.enc_id,
                    first(new_criteria.severe_sepsis_onset) severe_sepsis_onset,
                    first(new_criteria.septic_shock_onset) septic_shock_onset,
                    first(new_criteria.severe_sepsis_wo_infection_onset) severe_sepsis_wo_infection_onset,
                    first(new_criteria.severe_sepsis_wo_infection_initial) severe_sepsis_wo_infection_initial
            from new_criteria
            group by new_criteria.enc_id
        ) nc on pat_states.enc_id = nc.enc_id
        left join lateral update_notifications(pat_states.enc_id, flag_to_alert_codes(pat_states.state),
            nc.severe_sepsis_onset,
            nc.septic_shock_onset,
            nc.severe_sepsis_wo_infection_onset,
            nc.severe_sepsis_wo_infection_initial,
            'override') n
        on pat_states.enc_id = n.enc_id
    )
    insert into criteria_events (event_id, enc_id, name, measurement_time, value,
                                 override_time, override_user, override_value, is_met, update_date, is_acute, flag)
    select ssid.event_id, NC.enc_id, NC.name, NC.measurement_time, NC.value,
           (case when NC.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else NC.override_time end),
           (case when NC.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else NC.override_user end),
           (case when NC.override_value#>>'{0,text}' in ('Ordering', 'Ordered') then null else NC.override_value end),
           NC.is_met, NC.update_date, NC.is_acute,
           pat_states.state as flag
    from new_criteria NC
    cross join (select nextval('criteria_event_ids') event_id) ssid
    inner join pat_states on NC.enc_id = pat_states.enc_id
    left join notified_patients np on NC.enc_id = np.enc_id
    where not NC.name like '%_order';
    drop table new_criteria;
    perform order_event_update(this_enc_id);
    return;
END; $function$;


-----------------------------------------------
-- Notification management
-----------------------------------------------
CREATE OR REPLACE FUNCTION lmcscore_alert_on(this_enc_id int, timeout text default '15 minutes')
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
     s where s.enc_id = this_enc_id and now() - s.tsp < timeout::interval and s.rank = 1
    order by s.tsp desc limit 1;
end;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION trewscore_alert_on(this_enc_id int, timeout text, model text)
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
            from trews s
            where s.enc_id = this_enc_id and now() - s.tsp < timeout::interval
            order by s.tsp desc limit 1;
    else
        return query select * from lmcscore_alert_on(this_enc_id, timeout);
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


CREATE OR REPLACE FUNCTION suppression_on(this_enc_id int)
RETURNS boolean as $$
begin
    return
    (
        select coalesce(
            (select value from parameters where name = 'suppression' limit 1)
            ~ hospital, false)
        from enc_hosp(this_enc_id));
end;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION update_suppression_alert(this_enc_id int, channel text, model text, notify boolean default false)
RETURNS void AS $$
DECLARE
    trews_alert_on boolean;
    trews_tsp timestamptz;
    trewscore real;
    curr_state int;
    tsp timestamptz;
BEGIN
    select * from trewscore_alert_on(this_enc_id, (select coalesce(value, '6 hours') from parameters where name = 'suppression_timeout'), model) into trews_alert_on, trews_tsp, trewscore;
    select state, severe_sepsis_wo_infection_initial from get_states_snapshot(this_enc_id) into curr_state, tsp;
    delete from notifications where enc_id = this_enc_id
        and notifications.message#>>'{alert_code}' ~ '205|300|206|307'
        and (notifications.message#>>'{model}' = model or not notifications.message::jsonb ? 'model');
    if curr_state = 10 then
        if coalesce(trews_alert_on, false) then
            insert into notifications (enc_id, message) values
            (
                this_enc_id,
                json_build_object('alert_code', '300', 'read', false, 'timestamp', date_part('epoch',tsp), 'suppression', 'true', 'trews_tsp', date_part('epoch', trews_tsp), 'trewscore', trewscore, 'model', model)
            );
        else
            insert into notifications (enc_id, message) values
            (
                this_enc_id,
                json_build_object('alert_code', '307', 'read', false, 'timestamp', date_part('epoch',tsp), 'suppression', 'true', 'trews_tsp', date_part('epoch', trews_tsp), 'trewscore', trewscore, 'model', model)
            );
        end if;
    end if;
    if notify then
        perform pg_notify(channel, 'invalidate_cache:' || this_enc_id || ':' || model);
        perform * from notify_future_notification(channel, this_enc_id);
    end if;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

-- update notifications when state changed
CREATE OR REPLACE FUNCTION update_notifications(
    this_enc_id int,
    alert_codes text[],
    severe_sepsis_onset timestamptz,
    septic_shock_onset timestamptz,
    severe_sepsis_wo_inf_onset timestamptz,
    sirs_plus_organ_initial timestamptz,
    mode text)
RETURNS table(enc_id int, alert_code text) AS $$
BEGIN
    -- clean notifications
    delete from notifications
        where notifications.enc_id = this_enc_id;
    if suppression_on(this_enc_id) and mode in ('override', 'reset') then
        -- suppression alerts (DEPRECATED when TREWS_ETL_SUPPRESSION = 2)
        insert into notifications (enc_id, message)
        select
            pat_enc.enc_id,
            json_build_object('alert_code',
                (case when not alert_on and tsp is not null
                    then '307'
                    else code
                end)
                , 'read', false,'timestamp',
                date_part('epoch', severe_sepsis_wo_inf_onset::timestamptz),
                'suppression', 'true',
                'model', 'trews',
                'trews_tsp', tsp,
                'trewscore', score) message
        from (select distinct pat_enc.enc_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        cross join trewscore_alert_on(pat_enc.enc_id,(select coalesce(value, '6 hours') from parameters where name = 'suppression_timeout'), 'trews') as trews
        where pat_enc.enc_id = coalesce(this_enc_id, pat_enc.enc_id)
            and code = '300';
        insert into notifications (enc_id, message)
        select
            pat_enc.enc_id,
            json_build_object('alert_code',
                (case when not alert_on and tsp is not null
                    then '307'
                    else code
                end)
                , 'read', false,'timestamp',
                date_part('epoch', severe_sepsis_wo_inf_onset::timestamptz),
                'suppression', 'true',
                'model', 'lmc',
                'trews_tsp', tsp,
                'trewscore', score) message
        from (select distinct pat_enc.enc_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        cross join trewscore_alert_on(pat_enc.enc_id,(select coalesce(value, '6 hours') from parameters where name = 'suppression_timeout'),  'lmc') as trews
        where pat_enc.enc_id = coalesce(this_enc_id, pat_enc.enc_id)
            and code = '300';
        -- normal notifications
        return query
        insert into notifications (enc_id, message)
        select
            pat_enc.enc_id,
            json_build_object('alert_code', code, 'read', false,'timestamp',
                date_part('epoch',
                    (case when code in ('201','204','303','306') then septic_shock_onset
                          when code in ('300', '500') then severe_sepsis_wo_inf_onset
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
        from (select distinct pat_enc.enc_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        where pat_enc.enc_id = coalesce(this_enc_id, pat_enc.enc_id)
        and code <> '0' and code <> '300'
        returning notifications.enc_id, message#>>'{alert_code}';
    else
        -- normal notifications
        return query
        insert into notifications (enc_id, message)
        select
            pat_enc.enc_id,
            json_build_object('alert_code', code, 'read', false,'timestamp',
                date_part('epoch',
                    (case when code in ('201','204','303','306','401','404','503','506','601','604','703','706') then septic_shock_onset
                          when code in ('300','500') then severe_sepsis_wo_inf_onset
                          else severe_sepsis_onset
                          end)::timestamptz
                    +
                    (case
                        when code in ('202','402','602') then '3 hours'
                        when code in ('203','204','403','404','603','604') then '6 hours'
                        when code in ('304','504','704') then '2 hours'
                        when code in ('305','306','505','506','506','705','706') then '5 hours'
                        else '0 hours'
                        end)::interval),
                'suppression', 'false'
            ) message
        from (select distinct pat_enc.enc_id from pat_enc) as pat_enc
        cross join unnest(alert_codes) as code
        where pat_enc.enc_id = coalesce(this_enc_id, pat_enc.enc_id)
        and code <> '0'
        returning notifications.enc_id, message#>>'{alert_code}';
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
     -- trews:
     WHEN flag = 11 THEN ret := array['500'];
     WHEN flag = 25 THEN ret := array['400',
                                '402',
                                '403',
                                '501',
                                '502',
                                '504',
                                '505'];
     WHEN flag = 26 THEN ret := array['400',
                                '403',
                                '502',
                                '505'];
     WHEN flag = 27 THEN ret := array['400','402'];
     WHEN flag = 28 THEN ret := array['400'];
     WHEN flag = 29 THEN ret := array['400','403'];
     WHEN flag = 40 THEN ret := array['400','401',
                                '402',
                                '403',
                                '404',
                                '501',
                                '502',
                                '503',
                                '504',
                                '505',
                                '506'];
     WHEN flag = 41 THEN ret := array['400','401',
                                '403',
                                '404',
                                '502',
                                '503',
                                '505',
                                '506'];
     WHEN flag = 42 THEN ret := array['400','401','402'];
     WHEN flag = 43 THEN ret := array['400','401',
                                '404',
                                '503',
                                '506'];
     WHEN flag = 44 THEN ret := array['400','401','403'];
     WHEN flag = 45 THEN ret := array['400','401'];
     WHEN flag = 46 THEN ret := array['400','401','404'];
     -- ui:
     WHEN flag = 50 THEN ret := array['600',
                                '602',
                                '603',
                                '701',
                                '702',
                                '704',
                                '705'];
     WHEN flag = 51 THEN ret := array['600',
                                '603',
                                '702',
                                '705'];
     WHEN flag = 52 THEN ret := array['600','602'];
     WHEN flag = 53 THEN ret := array['600'];
     WHEN flag = 54 THEN ret := array['600','603'];
     WHEN flag = 60 THEN ret := array['600','601',
                                '602',
                                '603',
                                '604',
                                '701',
                                '702',
                                '703',
                                '704',
                                '705',
                                '706'];
     WHEN flag = 61 THEN ret := array['600','601',
                                '603',
                                '604',
                                '702',
                                '703',
                                '705',
                                '706'];
     WHEN flag = 62 THEN ret := array['600','601','602'];
     WHEN flag = 63 THEN ret := array['600','601',
                                '604',
                                '703',
                                '706'];
     WHEN flag = 64 THEN ret := array['600','601','603'];
     WHEN flag = 65 THEN ret := array['600','601'];
     WHEN flag = 66 THEN ret := array['600','601','604'];
     ELSE ret := array['0'];
 END CASE ; RETURN ret;
END;$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_trewscores_for_epic(this_enc_id int default null)
RETURNS table(
    pat_id              varchar(50),
    visit_id            varchar(50),
    enc_id              int,
    tsp                 timestamptz,
    trewscore           real
) AS $func$ #variable_conflict use_column
BEGIN RETURN QUERY
  with prev as (
    select p.pat_id, p.visit_id, p.enc_id, coalesce(
        (last(h.trewscore order by h.tsp)),
        0) trewscore_prev
    from pat_enc p
    inner join get_latest_enc_ids_within_notification_whitelist() wl
        on p.enc_id = wl.enc_id
    left join epic_trewscores_history h on h.enc_id = p.enc_id
    where p.enc_id = coalesce(this_enc_id, p.enc_id)
    and p.pat_id like 'E%'
    group by p.enc_id, p.visit_id, p.enc_id
  ),
  compare as
  (
      select tjs.enc_id, max(tjs.tsp) tsp,
            last(tjs.score order by tjs.tsp) trewscore,
            prev.trewscore_prev
      from prev
      inner join trews_jit_score tjs on tjs.enc_id = prev.enc_id and tjs.model_id = get_trews_parameter('trews_jit_model_id')
      group by tjs.enc_id, prev.trewscore_prev
  ),
  update_history as (
    insert into epic_trewscores_history (tsp, enc_id, trewscore)
    select c.tsp, c.enc_id, c.trewscore from compare c
    where round(c.trewscore::numeric,4) <> round(c.trewscore_prev::numeric, 4)
    on conflict (tsp, enc_id) do update set trewscore = Excluded.trewscore
    returning *
  )
  select prev.pat_id, prev.visit_id, prev.enc_id, compare.tsp, compare.trewscore::real
  from prev inner join compare on prev.enc_id = compare.enc_id inner join update_history on compare.enc_id = update_history.enc_id;
END $func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_notifications_for_epic(this_pat_id text default null, model text default 'trews')
RETURNS table(
    pat_id              varchar(50),
    visit_id            varchar(50),
    enc_id              int,
    count               int,
    score               text,
    threshold           text,
    flag                int
) AS $func$ #variable_conflict use_column
BEGIN
  RETURN QUERY
  with prev as (
    select p.pat_id, p.visit_id, p.enc_id, coalesce(
        last(h.count order by h.tsp),
        0) count_prev
    from pat_enc p
    inner join get_latest_enc_ids_within_notification_whitelist() wl
        on p.enc_id = wl.enc_id
    left join epic_notifications_history h on h.enc_id = p.enc_id
    where p.pat_id = coalesce(this_pat_id, p.pat_id)
    and p.pat_id like 'E%'
    group by p.pat_id, p.visit_id, p.enc_id
  ),
  compare as
  (
      select prev.pat_id, prev.visit_id, prev.enc_id, prev.count_prev,
            (case
                when state in (12,13) then 2
                when state = 16 then 3
                when state = 11 then 4
                when state = 10 then 5
                when state in (20,21,22,24,25,26,27,29,50,51,52,54) then 6
                when state in (30,31,32,33,34,36,40,41,42,43,44,46,60,61,62,63,64,66) then 8
                when state in (23,28,53) then 7
                when state in (35,45,65) then 9
              else 1
            end) count, gss.trewscore, gss.trewscore_threshold, gss.alert_flag
      from prev
      left join lateral get_states_snapshot(prev.enc_id) gss on gss.enc_id = prev.enc_id
  ),
  update_history as (
    insert into epic_notifications_history (tsp, enc_id, count, trewscore, threshold, flag)
    select now() tsp, c.enc_id, c.count, c.trewscore, c.trewscore_threshold, c.alert_flag from compare c
    where c.count <> c.count_prev
    on conflict (tsp, enc_id) do update set count = Excluded.count
    returning *
  )
  select c.pat_id, c.visit_id, c.enc_id, c.count, c.trewscore, c.trewscore_threshold, c.alert_flag from compare c inner join update_history uh on c.enc_id = uh.enc_id;
END $func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_notifications_for_refreshed_pats(serial_id int, model text default 'trews')
RETURNS table(
    pat_id              varchar(50),
    visit_id            varchar(50),
    enc_id              int,
    count               int,
    score               text,
    threshold           text,
    flag                int
) AS $func$ #variable_conflict use_column
BEGIN
  RETURN QUERY
  with prev as (
    select p.pat_id, p.visit_id, p.enc_id, coalesce(
        last(h.count order by h.tsp),
        0) count_prev
    from pat_enc p
    inner join get_latest_enc_ids_within_notification_whitelist() wl
        on p.enc_id = wl.enc_id
    inner join (select jsonb_array_elements_text(pats) as pat_id from refreshed_pats where id = serial_id)
        as rp on p.pat_id = rp.pat_id
    left join epic_notifications_history h on h.enc_id = p.enc_id
    where p.pat_id like 'E%'
    group by p.pat_id, p.visit_id, p.enc_id
  ),
  compare as
  (
      select prev.pat_id, prev.visit_id, prev.enc_id, prev.count_prev,
            (case
                when state in (12,13) then 2
                when state = 16 then 3
                when state = 11 then 4
                when state = 10 then 5
                when state in (20,21,22,24,25,26,27,29,50,51,52,54) then 6
                when state in (30,31,32,33,34,36,40,41,42,43,44,46,60,61,62,63,64,66) then 8
                when state in (23,28,53) then 7
                when state in (35,45,65) then 9
              else 1
            end) count, gss.trewscore, gss.trewscore_threshold, gss.alert_flag
      from prev
      left join lateral get_states_snapshot(prev.enc_id) gss on gss.enc_id = prev.enc_id
  ),
  update_history as (
    insert into epic_notifications_history (tsp, enc_id, count, trewscore, threshold, flag)
    select now() tsp, c.enc_id, c.count, c.trewscore, c.trewscore_threshold, c.alert_flag from compare c
    where c.count <> c.count_prev
    on conflict (tsp, enc_id) do update set count = Excluded.count
    returning *
  )
  select c.pat_id, c.visit_id, c.enc_id, c.count, c.trewscore, c.trewscore_threshold, c.alert_flag from compare c inner join update_history uh on c.enc_id = uh.enc_id;
END $func$ LANGUAGE plpgsql;

-- DEPRECATED
CREATE OR REPLACE FUNCTION get_notifications_count_for_epic(this_pat_id text default null, model text default 'trews')
RETURNS table(
    pat_id              varchar(50),
    visit_id            varchar(50),
    enc_id              int,
    count               int
) AS $func$ BEGIN RETURN QUERY
  with prev as (
    select p.pat_id, p.visit_id, p.enc_id, coalesce(
        (last(h.count order by h.id) filter (where h.id is not null)),
        0) count_prev
    from pat_enc p
    inner join get_latest_enc_ids_within_notification_whitelist() wl
        on p.enc_id = wl.enc_id
    left join epic_notifications_history h on h.enc_id = p.enc_id
    where p.pat_id = coalesce(this_pat_id, p.pat_id)
    and p.pat_id like 'E%'
    group by p.enc_id, p.visit_id, p.enc_id
  ),
  compare as
  (
      select prev.pat_id, prev.visit_id, prev.enc_id, prev.count_prev,
            (case when deactivated is true then 0
              else coalesce(counts.count::int, 0)
            end) count
      from prev
      left join pat_status on prev.enc_id = pat_status.enc_id
      left join
      (
          select notifications.enc_id,
                (case when count(*) > 5 then 5
                      else count(*)
                      end
                ) as count
          from
          notifications
          where not (message#>>'{read}')::bool
          and (message#>>'{timestamp}')::numeric < date_part('epoch', now())
          and (not message::jsonb ? 'model' or message#>>'{model}' = model)
          group by notifications.enc_id
      )
      counts on counts.enc_id = prev.enc_id
  )
  select compare.pat_id, compare.visit_id, compare.enc_id, compare.count from compare where compare.count <> compare.count_prev;
END $func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notify_future_notification(channel text, _pat_id text default null)
RETURNS void AS $func$
declare
  payload text;
BEGIN
  select 'future_epic_sync:' || string_agg(pat_tsp, '|') from
  (select enc_id, enc_id || ',' || string_agg(tsp, ',') pat_tsp from
      (select enc_id, ((message#>>'{timestamp}')::numeric::int)::text tsp
      from notifications
      where (message#>>'{alert_code}')::text in ('202','203','204','205','206',
        '402','403','404','405','406',
        '602','603','604')
      and (message#>>'{timestamp}')::numeric > date_part('epoch', now())
      and enc_id = coalesce(pat_id_to_enc_id(_pat_id), enc_id)
      order by enc_id, tsp) O
  group by enc_id) G into payload;
  if payload is not null then
      raise notice '%', payload;
      perform pg_notify(channel, payload);
  end if;
END $func$ LANGUAGE plpgsql;
----------------------------------------------------
--  deactivate functionality for patients
----------------------------------------------------

create or replace function deactivate(this_enc_id int, deactivated boolean, event_type text default null) returns void language plpgsql
as $$ begin
    insert into pat_status (enc_id, deactivated, deactivated_tsp)
        values (
            this_enc_id, deactivated, now()
        )
    on conflict (enc_id) do update
    set deactivated = excluded.deactivated, deactivated_tsp = now();
    if event_type is not null then
        insert into criteria_log (enc_id, tsp, event, update_date)
        values (this_enc_id,
                now(),
                json_build_object('event_type', event_type, 'uid', 'dba', 'deactivated', deactivated),
                now()
               );
    end if;
    -- if false then reset patient automatically
    if not deactivated then
        perform reset_patient(this_enc_id);
        insert into criteria_log (enc_id, tsp, event, update_date)
        values (this_enc_id,
                now(),
                json_build_object('event_type', 'reset', 'uid', 'dba', 'deactivated', deactivated),
                now()
               );
    end IF;
end; $$;

-- if care is completed (severe sepsis/septic shock bundle completed or expired), then deactivate the patient; after 72 hours, the patient will be reactivate
-- else if patient was in state 10 and expired, then we reset the patient

CREATE OR REPLACE FUNCTION auto_deactivate(this_enc_id int DEFAULT NULL) RETURNS void LANGUAGE plpgsql
-- deactivate patients who is active and had positive state for more than deactivate_hours'
AS $func$
declare
    res text;
BEGIN
    -- check if current snapshot (20 or 30) was expired or complished
    with pats as (
        select gss.enc_id from get_states_snapshot(this_enc_id) gss
        left join pat_status s on s.enc_id = gss.enc_id
        where state in (23,35,28,45,53,65)
        and (s.enc_id IS NULL or not s.deactivated)
    )
    -- if criteria_events has been in an event for longer than deactivate_hours,
    -- then this patient should be deactivated automatically
    select into res deactivate(enc_id, TRUE, 'auto_deactivate') from pats;
    return;
END; $func$;


------------------------------
-- garbage collection
------------------------------

create or replace function garbage_collection_enc_ids(enc_ids int[], workspace text default 'workspace') returns void language plpgsql as $$
begin
    perform garbage_collection(enc_id, workspace) from unnest(enc_ids) as enc_id;
end; $$;

create or replace function garbage_collection(this_enc_id int default null, workspace text default 'workspace') returns void language plpgsql as $$
declare
  gc_workspace boolean;
begin
    perform reactivate(this_enc_id);
    perform reset_soi_pats(this_enc_id);
    perform reset_bundle_expired_pats(this_enc_id);
    perform reset_noinf_expired_pats(this_enc_id);

    gc_workspace := (now() - (select max(tsp) from etl_job)) > (select max(value)::interval from parameters where name = 'gc_workspace_interval');
    if gc_workspace then
      perform del_old_refreshed_pats();
      perform drop_tables_pattern(workspace, '_' || to_char((now() - interval '2 days')::date, 'MMDD'));
    end if;
end; $$;


create or replace function garbage_collection(hospital text, workspace text) returns void language plpgsql as $$
declare
  gc_workspace boolean;
begin
    perform reactivate(enc_id),
        reset_soi_pats(enc_id),
        reset_bundle_expired_pats(enc_id),
        reset_noinf_expired_pats(enc_id),
        reset_orgdf_expired_pats(enc_id)
    from get_latest_enc_ids(hospital);

    gc_workspace := (now() - (select max(tsp) from etl_job)) > (select max(value)::interval from parameters where name = 'gc_workspace_interval');
    if gc_workspace then
      perform del_old_refreshed_pats();
      perform drop_tables_pattern(workspace, '_' || to_char((now() - interval '2 days')::date, 'MMDD'));
    end if;
end; $$;

create or replace function del_old_refreshed_pats() returns void language plpgsql
-- turn deactivated patients longer than deactivate_expire_hours to active
as $$ begin
    delete from refreshed_pats where now() - refreshed_tsp > '24 hours';
end; $$;

create or replace function reactivate(this_enc_id int default null) returns void language plpgsql
-- turn deactivated patients longer than deactivate_expire_hours to active
as $$
declare res text;
begin
    with pats as (
        select distinct s.enc_id
        from pat_status s
        inner join lateral get_states_snapshot(s.enc_id) SNP on s.enc_id = SNP.enc_id
        where deactivated
        and now() - SNP.severe_sepsis_onset > get_parameter('deactivate_expire_hours')::interval
        and s.enc_id = coalesce(this_enc_id, s.enc_id)
    ),
    logging as (
        insert into criteria_log (enc_id, tsp, event, update_date)
        select
              enc_id,
              now(),
              '{"event_type": "reactivate", "uid":"dba"}',
              now()
        from pats
    )
    select deactivate(enc_id, false, 'auto_deactivate') from pats into res;
    return;
end; $$;

create or replace function reset_soi_pats(this_enc_id int default null)
returns void language plpgsql as $$
declare res text;
-- reset patients who are in state 10 and expired for lookbackhours
begin
    with pats as (select distinct e.enc_id
        from criteria_events e
        inner join lateral get_states_snapshot(e.enc_id) SNP on e.enc_id = SNP.enc_id
        where flag = 10 and now() - SNP.severe_sepsis_wo_infection_initial > (select value from parameters where name = 'lookbackhours')::interval
        and e.enc_id = coalesce(this_enc_id, e.enc_id)
    ),
    logging as (
        insert into criteria_log (enc_id, tsp, event, update_date)
        select
              enc_id,
              now(),
              '{"event_type": "reset_soi_pats", "uid":"dba"}',
              now()
        from pats
    )
    select reset_patient(enc_id) from pats into res;
    return;
end; $$;


create or replace function reset_bundle_expired_pats(this_enc_id int default null)
returns void language plpgsql as $$
declare res text;
-- reset patients who are in state 10 and expired for lookbackhours
begin
    with pats as (
    select distinct e.enc_id
        from criteria_events e
        inner join lateral get_states_snapshot(e.enc_id) SNP on e.enc_id = SNP.enc_id
        where flag in (22,24,32,34,36,27,29,42,44,46,52,54,62,64,66) and now() - SNP.severe_sepsis_onset > get_parameter('deactivate_expire_hours')::interval
        and e.enc_id = coalesce(this_enc_id, e.enc_id)
    ),
    logging as (
        insert into criteria_log (enc_id, tsp, event, update_date)
        select
              enc_id,
              now(),
              '{"event_type": "reset_bundle_expired_pats", "uid":"dba"}',
              now()
        from pats
    )
    select reset_patient(enc_id) from pats into res;
    return;
end; $$;

create or replace function reset_noinf_expired_pats(this_enc_id int default null)
returns void language plpgsql as $$
declare res text;
-- reset patients who are in state 12,13,14 and expired for lookbackhours
begin
    with pats as (
    select distinct e.enc_id
        from criteria_events e
        inner join criteria c on e.enc_id = c.enc_id
        where flag in (12,13,14) and c.name = 'suspicion_of_infection'
        and now() - c.override_time::timestamptz
            > get_parameter('deactivate_expire_hours')::interval
        and e.enc_id = coalesce(this_enc_id, e.enc_id)
    ),
    logging as (
        insert into criteria_log (enc_id, tsp, event, update_date)
        select
              enc_id,
              now(),
              '{"event_type": "reset_noinf_expired_pats", "uid":"dba"}',
              now()
        from pats
    )
    select reset_patient(enc_id) from pats into res;
    return;
end; $$;

create or replace function reset_orgdf_expired_pats(this_enc_id int default null)
returns void language plpgsql as $$
declare res text;
-- Add 72 hr reset when all orgdf have been marked as not acute (issue #128)
begin
    with pats as (
        select enc_id
        from
        (
            select enc_id,
            coalesce(bool_or(c.is_met) filter (where c.name ~ 'trews'), false) is_any_trews_orgdf_met,
            coalesce(bool_or(c.is_met) filter (where c.name !~ 'trews'), false) is_any_cms_orgdf_met,
            count(*) filter (where c.override_value#>>'{0,text}' = 'No Infection') override_cnt,
            coalesce(now() - min(c.override_time::timestamptz) > get_parameter('deactivate_expire_hours')::interval, false) expired
            from criteria c
            where c.name in (
                'blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp',
                'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate', 'respiratory_failure'
                'trews_bilirubin', 'trews_creatinine', 'trews_dsbp', 'trews_gcs', 'trews_inr',
                'trews_lactate', 'trews_map', 'trews_platelet', 'trews_sbpm', 'trews_vasopressors', 'trews_vent')
            and c.enc_id = coalesce(this_enc_id, c.enc_id)
            group by enc_id
        ) S where (not is_any_trews_orgdf_met or not is_any_cms_orgdf_met) and expired and override_cnt > 0
    ),
    logging as (
        insert into criteria_log (enc_id, tsp, event, update_date)
        select
              enc_id,
              now(),
              '{"event_type": "reset_orgdf_expired_pats", "uid":"dba"}',
              now()
        from pats
    )
    select reset_patient(enc_id) from pats into res;
    return;
end; $$;

create or replace function reset_patient(this_enc_id int, _event_id int default null)
returns void language plpgsql as $$
begin
    -- reset user input
    delete from criteria where enc_id = this_enc_id and override_value is not null;
    if _event_id is null then
        update criteria_events set flag = flag - 1000
        where enc_id = this_enc_id and flag >= 0;
    else
        update criteria_events set flag = flag - 1000
        where enc_id = this_enc_id and event_id = _event_id and flag >= 0;
    end if;
    insert into pat_status (enc_id, deactivated, deactivated_tsp)
    select enc_id, 'f', now() from pat_status where enc_id = this_enc_id and deactivated
    on conflict (enc_id) do update
    set deactivated = excluded.deactivated, deactivated_tsp = now();
    delete from notifications where enc_id = this_enc_id;
    perform advance_criteria_snapshot(this_enc_id, 'reset');
end; $$;

----------------------------------------------------
-- deterioration feedback functions
----------------------------------------------------
CREATE OR REPLACE FUNCTION set_deterioration_feedback(this_enc_id int, tsp timestamptz, deterioration json, uid text)
    RETURNS void LANGUAGE plpgsql
AS $$ BEGIN
    INSERT INTO deterioration_feedback (enc_id, tsp, deterioration, uid)
    VALUES (this_enc_id,
            tsp,
            deterioration,
            uid)
    ON conflict (enc_id) DO UPDATE
    SET tsp = Excluded.tsp,
        deterioration = Excluded.deterioration,
        uid = Excluded.uid;
    INSERT INTO criteria_log (enc_id, tsp, event, update_date)
    VALUES ( this_enc_id,
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

CREATE OR REPLACE FUNCTION get_most_recent_job_tsp(IN regex TEXT)
RETURNS timestamptz
LANGUAGE plpgsql
AS
$$
DECLARE
    tsp     timestamptz;
BEGIN
    SELECT
        to_timestamp(split_part(table_name, '_', 4), 'YYYYMMDDHH24MISS')
        INTO tsp
    FROM
        information_schema.tables
    WHERE
        table_type = 'BASE TABLE'
    AND
        table_schema = 'workspace'
    AND
        table_name ~* regex
    order by table_name desc
    LIMIT 1;

    RETURN tsp;
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
    EXECUTE 'delete from ' || _schema || '.cdm_s where job_id ~* ' || quote_literal(pattern);
    EXECUTE 'delete from ' || _schema || '.cdm_t where job_id ~* ' || quote_literal(pattern);
END;
$$;

CREATE OR REPLACE FUNCTION compare_with_prod_cdm_t(start_tsp text, end_tsp text)
RETURNS table (type text, visit_id varchar, tsp timestamptz, fid varchar, value text, confidence int)
LANGUAGE plpgsql
AS
$$
declare
    local_exprs text := 'visit_id, tsp, fid, value, confidence';
    local_table text := '(select '|| local_exprs ||' from cdm_t inner join pat_enc pe on cdm_t.enc_id = pe.enc_id where cdm_t.enc_id in (select enc_id from get_latest_enc_ids(''HCGH''))) as cdm_t';
    with_dst_extension text := ' where tsp between ''' || start_tsp || '''::timestamptz and ''' || end_tsp || '''::timestamptz';
    query text := 'select ' || local_exprs || ' from ' || local_table || with_dst_extension;
    finalizer text := 'select * from A_DIFF_B union select * from B_DIFF_A';
    remote_query text := 'select * from dblink(''opsdx_prod_srv'', ' || quote_literal(query) || ') as remote_fields (visit_id varchar, tsp timestamptz, fid varchar, value text, confidence int)';
begin
return query
execute '
  WITH A_DIFF_B AS (
    SELECT ''dev_only'', '|| local_exprs || ' FROM ' || local_table || with_dst_extension || '
    EXCEPT
    SELECT ''dev_only'', ' || local_exprs || '
    FROM (
      '||remote_query||'
    ) AS tab_compare
  ), B_DIFF_A AS (
    SELECT ''prod_only'', ' || local_exprs || '
    FROM (
      '||remote_query||'
    ) AS tab_compare
    EXCEPT
    SELECT ''prod_only'',' || local_exprs || ' FROM ' || local_table || with_dst_extension || '
  )
  '|| finalizer;
end;
$$;

CREATE OR REPLACE FUNCTION compare_with_prod_visit_id(start_tsp text, end_tsp text)
RETURNS table (type text, visit_id varchar)
LANGUAGE plpgsql
AS
$$
declare
    local_exprs text := 'visit_id';
    with_dst_extension text := ' tsp between ''' || start_tsp || '''::timestamptz and ''' || end_tsp || '''::timestamptz';
    local_table text := '(select '|| local_exprs ||' from cdm_t inner join pat_enc pe on cdm_t.enc_id = pe.enc_id where cdm_t.enc_id in (select enc_id from get_latest_enc_ids(''HCGH'')) and'|| with_dst_extension || ' ) as cdm_t';
    query text := 'select ' || local_exprs || ' from ' || local_table;
    finalizer text := 'select * from A_DIFF_B union select * from B_DIFF_A';
    remote_query text := 'select * from dblink(''opsdx_prod_srv'', ' || quote_literal(query) || ') as remote_fields (visit_id varchar, tsp timestamptz, fid varchar, value text, confidence int)';
begin
return query
execute '
  WITH A_DIFF_B AS (
    SELECT distinct ''dev_only'', '|| local_exprs || ' FROM ' || local_table || '
    EXCEPT
    SELECT distinct ''dev_only'', ' || local_exprs || '
    FROM (
      '||remote_query||'
    ) AS tab_compare
  ), B_DIFF_A AS (
    SELECT distinct ''prod_only'', ' || local_exprs || '
    FROM (
      '||remote_query||'
    ) AS tab_compare
    EXCEPT
    SELECT distinct ''prod_only'',' || local_exprs || ' FROM ' || local_table || '
  )
  '|| finalizer;
end;
$$;

CREATE OR REPLACE FUNCTION get_latest_enc_ids(hospital text, max_tsp text default null)
RETURNS table (enc_id int)
LANGUAGE plpgsql
AS
$$
DECLARE
    bedded_patients     text;
begin
select table_name from information_schema.tables
where table_type = 'BASE TABLE'
    and table_schema = 'workspace'
    and table_name ilike 'job_etl_' || hospital || '%bedded_patients_transformed'
    and (max_tsp is null or substring(table_name from 10 + char_length(hospital) for 14) < to_char(max_tsp::timestamptz, 'YYYYMMDDHH24MISS'))
order by table_name desc limit 1 into bedded_patients;
return query
execute 'select enc_id from pat_enc p inner join workspace.' || bedded_patients || ' bp on bp.visit_id = p.visit_id';
END;
$$;

CREATE OR REPLACE FUNCTION get_latest_enc_ids_within_notification_whitelist()
RETURNS table (enc_id int)
LANGUAGE plpgsql
AS
$$
DECLARE
    bedded_patients     text;
begin
select table_name from information_schema.tables where table_type = 'BASE TABLE' and table_schema = 'workspace' and table_name ilike 'job_etl_' || (get_parameter('notifications_whitelist')) || 'bedded_patients_transformed' order by table_name desc limit 1 into bedded_patients;
return query
execute 'select enc_id from pat_enc p inner join workspace.' || bedded_patients || ' bp on bp.visit_id = p.visit_id';
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
  FROM pat_enc WHERE pat_id = this_pat_id;
END;
$$;

create or replace function delete_test_scenarios() returns void language plpgsql as $$ begin
    delete from criteria where pat_id ~ E'^\\d+$' and pat_id::integer between 3000 and 3200;
    delete from criteria_events where pat_id ~ E'^\\d+$' and pat_id::integer between 3000 and 3200;
    delete from notifications where pat_id ~ E'^\\d+$' and pat_id::integer between 3000 and 3200;
end; $$;

create or replace function cdm_stats_count(_table text) returns jsonb language plpgsql as $$
declare
result jsonb;
begin
  execute 'select jsonb_build_object(''count'',
    (select count(*) from ' || _table into result;
return result;
end; $$;

create or replace function distribute_advance_criteria_snapshot_for_hospital(server text, lookback_hours int, hospital text, nprocs int default 2)
returns void language plpgsql as $$
declare
begin
  execute 'with pats as
  (select distinct t.enc_id from cdm_t t
    inner join enc_hosp() h on h.enc_id = t.enc_id
    where now() - tsp < interval ''' || lookback_hours || ' hours'' and h.hospital = '''||hospital||'''),
  pats_group as
  (select pats.*, row_number() over () % ' || nprocs || ' g from pats),
  queries as (
    select string_agg((''select advance_criteria_snapshot(''||enc_id||'')'')::text, '';'') q from pats_group group by g
  ),
  query_arrays as (
    select array_agg(q) arr from queries
  )
  select distribute('''||server||''', arr, '|| nprocs ||') from query_arrays';
  return;
end;
$$;

create or replace function distribute_advance_criteria_snapshot_for_online_hospital(server text, hospital text, nprocs int default 2)
returns void language plpgsql as $$
declare
begin
  execute 'with pats as
  (select enc_id from get_latest_enc_ids('''||hospital||''')),
  pats_group as
  (select pats.*, row_number() over () % ' || nprocs || ' g from pats),
  queries as (
    select string_agg((''select advance_criteria_snapshot(''||enc_id||'')'')::text, '';'') q from pats_group group by g
  ),
  query_arrays as (
    select array_agg(q) arr from queries
  )
  select distribute('''||server||''', arr, '|| nprocs ||') from query_arrays';
  return;
end;
$$;

create or replace function distribute_advance_criteria_snapshot_for_job(server text, lookback_hours int, job_id text, nprocs int default 2)
returns void language plpgsql as $$
declare
begin
  execute 'with pats as
  (select distinct t.enc_id from cdm_t t
    inner join pat_enc p on t.enc_id = p.enc_id
    inner join workspace.' || job_id || '_bedded_patients_transformed bp on p.visit_id = bp.visit_id
    where now() - tsp < interval ''' || lookback_hours || ' hours''),
  pats_group as
  (select pats.*, row_number() over () % ' || nprocs || ' g from pats),
  queries as (
    select string_agg((''select advance_criteria_snapshot(''||enc_id||'')'')::text, '';'') q from pats_group group by g
  ),
  query_arrays as (
    select array_agg(q) arr from queries
  )
  select distribute('''||server||''', arr, '|| nprocs ||') from query_arrays';
end;
$$;

create or replace function distribute_advance_criteria_snapshot_for_enc(server text, lookback_hours int, enc_ids text, nprocs int default 2)
returns void language plpgsql as $$
declare
begin
  execute 'with pats as
  (select distinct p.enc_id from pat_enc p
    where p.enc_id in (' || enc_ids ||')
  ),
  pats_group as
  (select pats.*, row_number() over () % ' || nprocs || ' g from pats),
  queries as (
    select string_agg((''select advance_criteria_snapshot(''||enc_id||'')'')::text, '';'') q from pats_group group by g
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
             first(value order by c.tsp) unit
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

create or replace function enc_hosp(this_enc_id int default null)
RETURNS
table(enc_id int, hospital text)
AS $func$ BEGIN RETURN QUERY
SELECT u.enc_id, (CASE WHEN unit ~* 'hc' THEN 'HCGH' WHEN unit ~* 'jh' THEN 'JHH' WHEN unit ~* 'bmc|bv' THEN 'BMC' WHEN unit ~* 'smh' THEN 'SMH' WHEN unit ~* 'sh' THEN 'SH' ELSE unit END) hospital
   FROM
     (SELECT c.enc_id,
             first(value order by c.tsp) unit
      FROM
        (SELECT t.enc_id, t.tsp, t.value
         FROM cdm_t t
         WHERE fid = 'care_unit' and value <> 'Discharge'
            and t.enc_id = coalesce(this_enc_id, t.enc_id)
         ORDER BY t.enc_id,
               t.tsp DESC) c
   GROUP BY c.enc_id) u
; END $func$ LANGUAGE plpgsql;


create or replace function enc_last_etl_tsp(this_enc_id int default null)
RETURNS
table(enc_id int, hospital text)
AS $func$ BEGIN RETURN QUERY
SELECT
FROM enc_hosp(this_enc_id) h
; END $func$ LANGUAGE plpgsql;

create or replace function get_recent_admit_pats(max_duration text)
RETURNS
table(pat_id varchar(50), LOS text)
AS $func$ BEGIN RETURN QUERY
SELECT p.pat_id,
       (now() - value::timestamptz)::interval::text LOS
FROM cdm_s s
INNER JOIN pat_enc p ON s.enc_id = p.enc_id
WHERE fid = 'admittime'
  AND now() - value::timestamptz < max_duration::interval
ORDER BY now() - value::timestamptz
; END $func$ LANGUAGE plpgsql;

create or replace function workspace_to_cdm(job_id text, workspace text default 'workspace', keep_delta_table boolean default true)
returns integer as $func$
declare
    num_delta integer;
BEGIN
execute
'--insert new jobs
insert into etl_job (job_id, tsp, hospital, workspace)
    values (''' || job_id ||''',
            to_timestamp(''' || split_part(job_id, '_', 4) || ''', ''YYYYMMDDHH24MISS''),
            ''' || split_part(job_id, '_', 3) || ''',
            ''' || workspace || ''')
on conflict (job_id) do update set tsp = excluded.tsp, hospital = excluded.hospital, workspace = excluded.workspace;';

if to_regclass('' || workspace || '.' || job_id || '_bedded_patients_transformed') is not null then
    if job_id ~* 'push' then
        execute '
        --insert_new_patients
        insert into pat_enc (pat_id, visit_id, zid)
        select bp.pat_id, bp.visit_id, bp.zid
        from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        left join pat_enc pe on bp.visit_id = pe.visit_id and bp.zid = pe.zid
        where pe.enc_id is null
        on conflict (visit_id, pat_id)
        do update set zid = excluded.zid;';
    else
        execute '
        --insert_new_patients
        insert into pat_enc (pat_id, visit_id)
        select bp.pat_id, bp.visit_id
        from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        left join pat_enc pe on bp.visit_id = pe.visit_id
        where pe.enc_id is null
        on conflict (visit_id, pat_id)
        do nothing;';
    end if;
    execute '
    -- age
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, ''age'', bp.age, 1 as c from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
        where bp.age is not null and bp.age <> ''nan''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- gender
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, ''gender'', bp.gender::numeric::int, 1 as c from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
        where bp.gender is not null and isnumeric(bp.gender) and lower(bp.gender) <> ''nan''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- diagnosis
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, json_object_keys(diagnosis::json), ''True'', 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    where diagnosis is not null and diagnosis <> ''nan''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- problem
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', * from
    (select pe.enc_id, json_object_keys(problem_all::json) fid, ''True'', 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    where problem_all is not null and problem_all <> ''nan'') PL
    where not fid in (''gi_bleed_inhosp'',''stroke_inhosp'')
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- gi_bleed_inhosp and stroke_inhosp
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', * from
    (select pe.enc_id, admittime::timestamptz tsp, json_object_keys(problem_all::json) fid, ''True'', 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
        where bp.admittime is not null and bp.admittime <> ''nan'') PL
    where fid in (''gi_bleed_inhosp'',''stroke_inhosp'')
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- history
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, json_object_keys(history::json), ''True'', 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    where history is not null and history <> ''nan''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- hospital
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, ''hospital'', hospital, 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    where hospital is not null and hospital <> ''nan'' and hospital <> ''None''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- admittime
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, ''admittime'', admittime, 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
        where admittime is not null and admittime <> ''nan''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- patient class
    INSERT INTO ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, ''patient_class'', patient_class, 1
    from ' || workspace || '.' || job_id || '_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    where patient_class is not null and patient_class <> ''nan''
    ON CONFLICT (job_id, enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    ';
end if;

if to_regclass('' || workspace || '.' || job_id || '_location_history_transformed') is not null then
    execute '
    -- workspace_location_history_2_cdm_t
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, lr.value, 0 from ' || workspace || '.' || job_id || '_location_history_transformed lr
        inner join pat_enc on pat_enc.visit_id = lr.visit_id
    where lr.tsp <> ''NaT'' and lr.tsp::timestamptz < now()
    ON CONFLICT (job_id, enc_id, tsp, fid)
       DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    ';
end if;

if to_regclass('' || workspace || '.' || job_id || '_discharged') is not null then
    execute '
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select distinct '''|| job_id ||''', pat_enc.enc_id, disc.tsp::timestamptz, disc.fid, disc.value, 0 from ' || workspace || '.' || job_id || '_discharged disc
        inner join pat_enc on pat_enc.visit_id = disc.visit_id
    where disc.tsp <> ''NaT'' and disc.tsp::timestamptz < now()
    ON CONFLICT (job_id, enc_id, tsp, fid)
       DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    ';
end if;

-- chief_complaint
if to_regclass('' || workspace || '.' || job_id || '_chiefcomplaint_transformed') is not null then
    execute
    'insert into ' || workspace || '.cdm_s (job_id, enc_id, fid, value, confidence)
    select '''|| job_id ||''', pe.enc_id, ''chief_complaint'', cc.value, 1
    from ' || workspace || '.' || job_id || '_chiefcomplaint_transformed cc
    inner join pat_enc pe on pe.visit_id = cc.visit_id
    on conflict (job_id, enc_id, fid)
    do update set value = Excluded.value, confidence = excluded.confidence';
end if;

-- treatment team
if to_regclass('workspace.' || job_id || '_treatmentteam_transformed') is not null then
    execute
    'with treatment_team_raw as (
        select pe.enc_id, json_array_elements(tt.value::json) tt_json, tt.value
        from workspace.' || job_id || '_treatmentteam_transformed tt
        inner join pat_enc pe on pe.visit_id = tt.visit_id
        where tt.value <> ''[]''
    ),
    treatment_team as (
        select enc_id, greatest(max((tt_json->>''start'')::timestamptz) filter (where tt_json->>''start'' <> ''''),
                                max((tt_json->>''end'')::timestamptz) filter (where tt_json->>''end'' <> '''')) tsp,
            first(value) as value
        from treatment_team_raw ttr
        group by enc_id
    )
    insert into ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', enc_id, tsp, ''treatment_team'', value, 1
    from treatment_team where tsp is not null
    on conflict (job_id, enc_id, tsp, fid)
    do update set value = Excluded.value, confidence = Excluded.confidence';
end if;

-- med_orders
if to_regclass('' || workspace || '.' || job_id || '_med_orders_transformed') is not null then
    execute
    'INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', enc_id, tsp::timestamptz, fid,
    json_build_object(
        ''dose'', last(mo.dose order by mo.tsp),
        ''discontinue_tsp'', last((case when mo.discontinue_tsp = ''None'' then null else mo.discontinue_tsp end) order by mo.tsp),
        ''end_tsp'', last((case when mo.end_tsp = ''None'' then null else mo.end_tsp end) order by mo.tsp),
        ''order_mode'', last(mo.order_mode order by mo.tsp)
    ),
    0
    from ' || workspace || '.' || job_id || '_med_orders_transformed mo
    inner join pat_enc p on mo.visit_id = p.visit_id
    where tsp <> ''NaT'' and tsp::timestamptz < now() and mo.dose is not null and mo.dose <> ''''
    group by enc_id, tsp, fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence=0';
end if;

-- active_procedures
-- modify existing but inactive ones
if to_regclass('' || workspace || '.' || job_id || '_active_procedures_transformed') is not null then
    execute
    'INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', t.enc_id, t.tsp, t.fid,
        jsonb_build_object(''status'',
            last(case when t.value ~ ''status'' then (t.value::json)#>>''{status}'' else t.value end), ''discontinue_tsp'', now(), ''end_tsp'', now()),
    0
    from cdm_t t inner join pat_enc p on t.enc_id = p.enc_id
    inner join ' || workspace || '.' || job_id || '_active_procedures_transformed p2
        on p.visit_id = p2.visit_id
    left join ' || workspace || '.' || job_id || '_active_procedures_transformed lo
        on lo.visit_id = p.visit_id and lo.fid = t.fid and lo.tsp::timestamptz = t.tsp
    where lo.tsp is null and (t.value !~ ''end_tsp'' or (t.value::json)#>>''{end_tsp}'' is null)
    and t.fid in (''lactate_order'', ''blood_culture_order'')
    group by t.enc_id, t.tsp, t.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence=0;

    -- insert currently acitve ones
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', enc_id, tsp::timestamptz, fid, jsonb_build_object(''status'', last(lo.status order by lo.tsp)), 0
    from ' || workspace || '.' || job_id || '_active_procedures_transformed lo
    inner join pat_enc p on lo.visit_id = p.visit_id
    where tsp <> ''NaT'' and tsp::timestamptz < now() and fid in (''lactate_order'', ''blood_culture_order'')
    group by enc_id, tsp, fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence=0;

    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', enc_id, tsp::timestamptz, fid, last(lo.status order by lo.tsp), 0
    from ' || workspace || '.' || job_id || '_active_procedures_transformed lo
    inner join pat_enc p on lo.visit_id = p.visit_id
    where tsp <> ''NaT'' and tsp::timestamptz < now() and fid not in (''lactate_order'', ''blood_culture_order'')
    group by enc_id, tsp, fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence=0';
end if;


-- workspace_medication_administration_2_cdm_t
-- workspace_fluids_intake_2_cdm_t
if to_regclass('' || workspace || '.' || job_id || '_med_admin_transformed') is not null then
    execute
    'INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
        json_build_object(''dose'',SUM(mar.dose_value::numeric),''action'',last(mar.action))
        , 0
    from ' || workspace || '.' || job_id || '_med_admin_transformed mar
        inner join pat_enc on pat_enc.visit_id = mar.visit_id
    where isnumeric(mar.dose_value) and mar.tsp <> ''NaT'' and mar.tsp::timestamptz < now() and mar.fid ~ ''dose''
    group by pat_enc.enc_id, tsp, mar.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    -- others excluded fluids
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
        max(mar.dose_value::numeric), 0
    from ' || workspace || '.' || job_id || '_med_admin_transformed mar
        inner join pat_enc on pat_enc.visit_id = mar.visit_id
    where isnumeric(mar.dose_value) and mar.tsp <> ''NaT'' and mar.tsp::timestamptz < now() and mar.fid not ilike ''%_dose'' and mar.fid <> ''fluids_intake''
    group by pat_enc.enc_id, tsp, mar.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    -- workspace_fluids_intake_2_cdm_t
    with u as (
    select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid, mar.dose_value as value
        from ' || workspace || '.' || job_id || '_med_admin_transformed mar
            inner join pat_enc on pat_enc.visit_id = mar.visit_id
        where isnumeric(mar.dose_value) and mar.tsp <> ''NaT'' and mar.tsp::timestamptz < now() and mar.fid = ''fluids_intake''
                    and mar.dose_value::numeric > 0
    )
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', u.enc_id, u.tsp, u.fid,
            sum(u.value::numeric), 0
    from u
    group by u.enc_id, u.tsp, u.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;';
end if;


-- workspace_flowsheets_2_cdm_t
if to_regclass('' || workspace || '.' || job_id || '_flowsheets_transformed') is not null then
    execute
    'INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, last(fs.value), 0 from ' || workspace || '.' || job_id || '_flowsheets_transformed fs
        inner join pat_enc on pat_enc.visit_id = fs.visit_id
        where fs.tsp <> ''NaT'' and fs.tsp::timestamptz < now()
        and fs.fid <> ''fluids_intake'' and fs.value <> ''''
    group by pat_enc.enc_id, tsp, fs.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- workspace_fluids_intake_2_cdm_t
    with u as (
    select pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, fs.value::text
        from ' || workspace || '.' || job_id || '_flowsheets_transformed fs
            inner join pat_enc on pat_enc.visit_id = fs.visit_id
            where fs.tsp <> ''NaT'' and fs.tsp::timestamptz < now()
            and fs.fid = ''fluids_intake'' and fs.value <> ''''
            and fs.value::numeric > 0
    )
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', u.enc_id, u.tsp, u.fid,
            sum(u.value::numeric), 0
    from u
    group by u.enc_id, u.tsp, u.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    -- deleted entries
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, last(''DELETE''::text), 0 from ' || workspace || '.' || job_id || '_flowsheets_transformed fs
        inner join pat_enc on pat_enc.visit_id = fs.visit_id
        where fs.tsp <> ''NaT'' and fs.tsp::timestamptz < now()
        and fs.value = ''''
    group by pat_enc.enc_id, tsp, fs.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
';
end if;

if to_regclass('' || workspace || '.' || job_id || '_lab_orders_transformed') is not null then
    execute
    '-- lab_orders
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', enc_id, tsp::timestamptz, fid, last(lo.status), 0
    from ' || workspace || '.' || job_id || '_lab_orders_transformed lo
    inner join pat_enc p on lo.visit_id = p.visit_id
    where tsp <> ''NaT'' and tsp::timestamptz < now()
    group by enc_id, tsp, fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence=0;';
end if;

-- workspace_notes_2_cdm_notes
if to_regclass('' || workspace || '.' || job_id || '_notes_transformed') is not null
and to_regclass('' || workspace || '.' || job_id || '_note_texts_transformed') is not null
then
    execute
    'insert into ' || workspace || '.cdm_notes
    select ''' || job_id || ''', enc_id, N.note_id, N.note_type, N.note_status, first(NT.note_body) note_body, first(N.dates::json) dates, first(N.providers::json) providers
    from ' || workspace || '.' || job_id || '_notes_transformed N
    inner join pat_enc p on p.visit_id = N.visit_id
    left join ' || workspace || '.' || job_id || '_note_texts_transformed NT on N.note_id = NT.note_id
    group by enc_id, N.note_id, N.note_type, N.note_status
    on conflict (job_id, enc_id, note_id, note_type, note_status) do update
    set note_body = excluded.note_body,
        dates = excluded.dates,
        providers = excluded.providers;';
end if;

-- workspace_lab_results_2_cdm_t
if to_regclass('' || workspace || '.' || job_id || '_lab_results_transformed') is not null then
    execute '
    INSERT INTO ' || workspace || '.cdm_t (job_id, enc_id, tsp, fid, value, confidence)
    select '''|| job_id ||''', pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, first(lr.value order by lr.tsp::timestamptz), 0 from ' || workspace || '.' || job_id || '_lab_results_transformed lr
        inner join pat_enc on pat_enc.visit_id = lr.visit_id
    where lr.tsp <> ''NaT'' and lr.tsp::timestamptz < now()
    group by pat_enc.enc_id, lr.tsp, lr.fid
    ON CONFLICT (job_id, enc_id, tsp, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;';
end if;

if keep_delta_table then
    -- make sure all rows in delta cdm_t tables are real delta rows, i.e., remove existing rows
    -- update main tables from workspace delta tables
    execute
    '
    insert into cdm_s (enc_id, fid, value, confidence)
        select enc_id, fid, value, confidence from ' || workspace || '.cdm_s
        where job_id = ''' || job_id || ''' and value <> ''DELETE''
    on conflict (enc_id, fid) do update set value = excluded.value, confidence = excluded.confidence;
    delete from cdm_s using (select * from ' || workspace || '.cdm_s
        where value = ''DELETE'') as del
    where cdm_s.enc_id = del.enc_id and cdm_s.fid = del.fid ;

    delete from ' || workspace || '.cdm_t t
    using (
        select tt.enc_id, tt.tsp, tt.fid
        from ' || workspace || '.cdm_t tt
        inner join cdm_t ttt on tt.enc_id = ttt.enc_id and tt.tsp = ttt.tsp and tt.fid = ttt.fid and tt.value = ttt.value
        where tt.job_id = ''' || job_id || '''
    ) as e
    where t.job_id = ''' || job_id || ''' and t.enc_id = e.enc_id and t.tsp = e.tsp and t.fid = e.fid;

    insert into cdm_t (enc_id, tsp, fid, value, confidence)
        select enc_id, tsp, fid, value, confidence from ' || workspace || '.cdm_t
        where job_id = ''' || job_id || ''' and value <> ''DELETE''
    on conflict (enc_id, tsp, fid) do update set value = excluded.value, confidence = excluded.confidence ;
    delete from cdm_t using (select * from ' || workspace || '.cdm_t
        where job_id = ''' || job_id || ''' and  value = ''DELETE'') as del
    where cdm_t.enc_id = del.enc_id and cdm_t.tsp = del.tsp and cdm_t.fid = del.fid ;

    insert into cdm_notes (enc_id, note_id, note_type, note_status, note_body, dates, providers)
        select enc_id, note_id, note_type, note_status, note_body, dates, providers from ' || workspace || '.cdm_notes
        where job_id = ''' || job_id || ''' and note_body <> ''DELETE''
    on conflict (enc_id, note_id, note_type, note_status) do update set note_body = excluded.note_body, dates = excluded.dates, providers = excluded.providers ;
    delete from cdm_notes using (select * from ' || workspace || '.cdm_notes
        where job_id = ''' || job_id || ''' and note_body = ''DELETE'') as del
    where cdm_notes.enc_id = del.enc_id and cdm_notes.note_id = del.note_id and cdm_notes.note_type = del.note_type and cdm_notes.note_status = del.note_status ;
    ';
else
    execute
    'delete from '|| workspace || '.cdm_s where job_id = ''' || job_id || ''';
    delete from '|| workspace || '.cdm_t where job_id = ''' || job_id || ''';
    delete from '|| workspace || '.cdm_t where job_id = ''' || job_id || ''';';
end if;

execute 'select count(*) from '|| workspace || '.cdm_t where job_id = ''' || job_id || '''' into num_delta;

if num_delta > 0 then
    -- update orgdf baselines
    execute 'select * from update_orgdf_baselines(''' || job_id || ''', ''' || workspace || ''')';
end if;
return num_delta;
END $func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_orgdf_baselines(job_id text, workspace text)
RETURNS void AS $func$ #variable_conflict use_column
BEGIN
if to_regclass('' || workspace || '.' || job_id || '_bedded_patients_transformed') is not null then
    execute
    'with discharged as (
      select p.pat_id,
      last(t.enc_id order by t.tsp) enc_id
      from cdm_t t inner join pat_enc p on t.enc_id = p.enc_id
      inner join ' || workspace || '.' || job_id || '_bedded_patients_transformed bp on bp.pat_id = p.pat_id
      where fid = ''discharge''
      and tsp between now() - ''4 months''::interval and now()
      group by p.pat_id
    ),
    baseline as (
      select d.pat_id, d.enc_id,
      first(t.value::numeric order by t.value::numeric) filter (where t.fid = ''bilirubin'') bilirubin,
      first(t.tsp order by t.value::numeric) filter (where t.fid = ''bilirubin'') bilirubin_tsp,
      first(t.value::numeric order by t.value::numeric) filter (where t.fid = ''creatinine'') creatinine,
      first(t.tsp order by t.value::numeric) filter (where t.fid = ''creatinine'') creatinine_tsp,
      first(t.value::numeric order by t.value::numeric) filter (where t.fid = ''inr'') inr,
      first(t.tsp order by t.value::numeric) filter (where t.fid = ''inr'') inr_tsp,
      last(t.value::numeric order by t.value::numeric) filter (where t.fid = ''platelets'') platelets,
      last(t.tsp order by t.value::numeric) filter (where t.fid = ''platelets'') platelets_tsp
      from discharged d
      left join cdm_t t on d.enc_id = t.enc_id
      where t.fid in (''creatinine'', ''bilirubin'', ''inr'', ''platelets'')
      group by d.pat_id, d.enc_id
    )
    insert into orgdf_baselines (pat_id, bilirubin, bilirubin_tsp, creatinine, creatinine_tsp, inr, inr_tsp, platelets, platelets_tsp)
    select b.pat_id, b.bilirubin, b.bilirubin_tsp, b.creatinine, b.creatinine_tsp, b.inr, b.inr_tsp, b.platelets, b.platelets_tsp
    from baseline b
    on conflict (pat_id) do update set
    bilirubin = (case when Excluded.bilirubin is null then orgdf_baselines.bilirubin else Excluded.bilirubin end),
    bilirubin_tsp = (case when Excluded.bilirubin_tsp is null then orgdf_baselines.bilirubin_tsp else Excluded.bilirubin_tsp end),
    creatinine = (case when Excluded.creatinine is null then orgdf_baselines.creatinine else Excluded.creatinine end),
    creatinine_tsp = (case when Excluded.creatinine_tsp is null then orgdf_baselines.creatinine_tsp else Excluded.creatinine_tsp end),
    inr = (case when Excluded.inr is null then orgdf_baselines.inr else Excluded.inr end),
    inr_tsp = (case when Excluded.inr_tsp is null then orgdf_baselines.inr_tsp else Excluded.inr_tsp end),
    platelets = (case when Excluded.platelets is null then orgdf_baselines.platelets else Excluded.platelets end),
    platelets_tsp = (case when Excluded.platelets_tsp is null then orgdf_baselines.platelets_tsp else Excluded.platelets_tsp end)';
end if;
END $func$ LANGUAGE plpgsql;



create or replace function post_prediction(hospital text,
    server text, nprocs int, channel text default null, model text default null)
returns void language plpgsql as $$
-- reset patients who are in state 10 and expired for lookbackhours
begin
    perform garbage_collection();
    perform distribute_advance_criteria_snapshot_for_online_hospital(server, hospital, nprocs);
    if channel is not null then
        execute
        'with pats as (
          select p.enc_id, p.pat_id from pat_enc p
          inner join get_latest_enc_ids(''' || hospital || ''') e on p.enc_id = e.enc_id
        ),
        refreshed as (
          insert into refreshed_pats (refreshed_tsp, pats)
          select now(), jsonb_agg(pat_id) from pats
          returning id
        )
        select pg_notify('''|| channel || ''', ''invalidate_cache_batch:'' || id || '':'' || ''' || model || ''') from refreshed;';
    end if;
    return;
end; $$;

-----------------------------------
-- Patient cloning.
create or replace function delete_enc(this_enc_id int)
returns void language plpgsql as $$
declare
begin
  delete from cdm_twf where enc_id = this_enc_id;
  delete from cdm_t where enc_id = this_enc_id;
  delete from cdm_s where enc_id = this_enc_id;
  delete from cdm_notes where enc_id = this_enc_id;
  delete from cdm_labels where enc_id = this_enc_id;
  delete from criteria where enc_id = this_enc_id;
  delete from criteria_events where enc_id = this_enc_id;
  delete from criteria_log where enc_id = this_enc_id;
  delete from trews where enc_id = this_enc_id;
  delete from trews_jit_score where enc_id = this_enc_id;
  delete from lmcscore where enc_id = this_enc_id;
  delete from deterioration_feedback where enc_id = this_enc_id;
  delete from epic_notifications_history where enc_id = this_enc_id;
  delete from epic_trewscores_history where enc_id = this_enc_id;
  delete from feedback_log where enc_id = this_enc_id;
  delete from notifications where enc_id = this_enc_id;
  delete from pat_status where enc_id = this_enc_id;
  delete from user_interactions where enc_id = this_enc_id;
  delete from orgdf_baselines where pat_id = (select pat_id from pat_enc where enc_id = this_enc_id);
  delete from pat_enc where enc_id = this_enc_id;
end;
$$;

CREATE OR REPLACE FUNCTION get_ofd_enc_ids(win text default '1 month')
RETURNS table (enc_id int)
LANGUAGE plpgsql
AS
$$
begin
return query with ofd as
(select pe.enc_id, max(tsp) tsp from pat_enc pe
inner join cdm_t t on pe.enc_id = t.enc_id
group by pe.enc_id)
select ofd.enc_id from ofd where now() - tsp > win::interval
except select * from get_latest_enc_ids('HCGH')
except select * from get_latest_enc_ids('JHH')
except select * from get_latest_enc_ids('BMC');
END;
$$;

create or replace function clone_enc(from_enc int, to_pat_id text, to_visit_id text)
returns int language plpgsql as $$
declare to_enc int;
begin
  perform delete_enc(enc_id) from pat_enc where pat_id = to_pat_id and visit_id = to_visit_id;
  insert into pat_enc (pat_id, visit_id)
    values (to_pat_id, to_visit_id);
  select enc_id from pat_enc where pat_id = to_pat_id and visit_id = to_visit_id into to_enc;
  create temp table clone_temp as
    select * from cdm_twf where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into cdm_twf select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from cdm_t where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into cdm_t select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from cdm_s where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into cdm_s select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from cdm_notes where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into cdm_notes select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from cdm_labels where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into cdm_labels select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from criteria where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into criteria select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from criteria_events where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into criteria_events select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from orgdf_baselines where pat_id = (select pat_id from pat_enc where enc_id = from_enc);
  update clone_temp set pat_id = to_pat_id;
  insert into orgdf_baselines select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select * from trews_jit_score where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into trews_jit_score select * from clone_temp;
  drop table clone_temp;
  create temp table clone_temp as select enc_id, message from notifications where enc_id = from_enc;
  update clone_temp set enc_id = to_enc;
  insert into notifications (enc_id, message) select * from clone_temp;
  drop table clone_temp;
  perform pg_notify('on_opsdx_dev_etl', 'invalidate_cache:' || pat_id || ':trews-jit')
    from pat_enc where enc_id = to_enc;
  return to_enc;
end;
$$;

create or replace function shift_to_now(this_enc_id int, now_tsp timestamptz default now())
returns void language plpgsql as $$
declare shift_interval interval;
begin
select now_tsp - greatest(max(t.tsp), coalesce(max(c.override_time), max(c.measurement_time), max(t.tsp) )) from cdm_t t left join criteria c on t.enc_id = c.enc_id and c.override_user is not null where t.enc_id = this_enc_id into shift_interval;
update cdm_t set tsp = tsp + shift_interval where enc_id = this_enc_id;
update cdm_twf set tsp = tsp + shift_interval where enc_id = this_enc_id;
update criteria set override_time = override_time + shift_interval where enc_id = this_enc_id and override_user is not null;
update criteria_events set override_time = override_time + shift_interval,
    measurement_time = measurement_time + shift_interval,
    update_date = update_date + shift_interval
    where enc_id = this_enc_id;
update orgdf_baselines set
    creatinine_tsp = creatinine_tsp + shift_interval,
    inr_tsp = inr_tsp + shift_interval,
    bilirubin_tsp = bilirubin_tsp + shift_interval,
    platelets_tsp = platelets_tsp + shift_interval
    where pat_id = (select pat_id from pat_enc where enc_id = this_enc_id);
update notifications set
    message = message::jsonb #- '{timestamp}' || jsonb_build_object('timestamp',((message#>>'{timestamp}')::numeric + EXTRACT(EPOCH FROM shift_interval)));
end;
$$;

create or replace function refresh_enc(this_enc_id int, offset_interval text default '0')
returns void language plpgsql as $$
begin
    perform shift_to_now(this_enc_id, now() - offset_interval::interval);
    perform pg_notify('on_opsdx_dev_etl', 'invalidate_cache:' || pat_id || ':trews-jit')
    from pat_enc where enc_id = this_enc_id;
end;
$$;
------------------------------------------------
-- Alert Statistics


-- For each pat/enc/unit, count trews/cms snapshots that fired within the stay in that unit.
-- Also returns earliest time of trews/cms firing within that unit.
--
CREATE OR REPLACE FUNCTION get_alert_stats_by_enc(ts_start timestamptz, ts_end timestamptz)
RETURNS table(
    pat_id              character varying(50),
    enc_id              integer,
    care_unit           text,
    enter_time          timestamp with time zone,
    leave_time          timestamp with time zone,
    trews_no_cms        bigint,
    cms_no_trews        bigint,
    trews_and_cms       bigint,
    any_trews           bigint,
    any_cms             bigint,
    earliest_trews      timestamp with time zone,
    earliest_cms        timestamp with time zone
) AS $func$ BEGIN RETURN QUERY

with raw_care_unit_tbl as (
  select R.enc_id, R.enter_time, (case when R.leave_time is null then date_trunc('second', now()) else R.leave_time end) as leave_time, R.care_unit
  from (
    select R.enc_id,
           R.tsp as enter_time,
           lead(R.tsp,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp) as leave_time,
           (case when R.care_unit = 'Arrival' then R.next_unit else R.care_unit end) as care_unit
    from (
      select R.enc_id, R.tsp, R.care_unit,
             lead(R.tsp,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp,
                (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
             ) as next_tsp,
             lead(R.care_unit,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp,
                (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
             ) as next_unit,
             first_value(R.care_unit) over (PARTITION by R.enc_id order by R.tsp,
                (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
             ) as first_unit
      from (
        select cdm_s.enc_id, cdm_s.value::timestamptz as tsp, 'Arrival' as care_unit
        from cdm_s
        where cdm_s.fid = 'admittime'
        union all
        select cdm_t.enc_id, cdm_t.tsp, (case when cdm_t.fid = 'discharge' then 'Discharge' else cdm_t.value end) as care_unit
        from cdm_t
        where ( cdm_t.fid = 'care_unit' or cdm_t.fid = 'discharge' )
      ) R
      order by
        R.enc_id, R.tsp,
        (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
    ) R
    where not (R.care_unit = 'Arrival' and R.first_unit <> 'Arrival')
    and (R.next_tsp is null or R.tsp <> R.next_tsp)
    order by R.enc_id, enter_time
  ) R
),
discharge_filtered as (
  select R.*
  from raw_care_unit_tbl R
  where R.care_unit != 'Discharge' and R.leave_time is not null
),
care_unit as (
  select D.enc_id, D.enter_time, D.leave_time, D.care_unit
  from discharge_filtered D
),
snapshots as (
  select R.pat_id, R.enc_id, R.event_id,
         max(R.update_date) as update_date,
         count(*) filter (where R.trews_subalert > 0 and ( R.sirs < 2 or R.orgdf < 1 )) as trews_no_cms,
         count(*) filter (where R.sirs > 1 and R.orgdf > 0 and R.trews_subalert = 0) as cms_no_trews,
         count(*) filter (where R.trews_subalert > 0 and R.sirs > 1 and R.orgdf > 0) as trews_and_cms,
         count(*) filter (where R.trews_subalert > 0) as any_trews,
         count(*) filter (where R.sirs > 1 and R.orgdf > 0) as any_cms,
         min(R.trews_subalert_onset) filter (where R.trews_subalert > 0) as trews_subalert_onset,
         min(greatest(R.sirs_onset, R.organ_onset)) filter (where R.sirs > 1 and R.orgdf > 0) as cms_onset
  from (
    select p.pat_id, C.enc_id, C.event_id, C.flag,
           max(C.update_date) as update_date,
           count(*) filter (where C.name like 'trews_subalert' and C.is_met) as trews_subalert,
           count(*) filter (where C.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and C.is_met) as sirs,
           count(*) filter (where C.name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and C.is_met) as orgdf,
           (array_agg(C.measurement_time order by C.measurement_time)  filter (where C.name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and C.is_met ) )[2]   as sirs_onset,
           min(C.measurement_time) filter (where C.name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and C.is_met ) as organ_onset,
           min(C.measurement_time) filter (where C.name = 'trews_subalert' and C.is_met) as trews_subalert_onset
    from criteria_events C
    inner join (
      select distinct cdm_t.enc_id
      from cdm_t where cdm_t.fid =  'care_unit' and cdm_t.value like '%HCGH%'
      and cdm_t.enc_id not in ( select distinct R.enc_id from get_latest_enc_ids('HCGH') R  )
      union
      select distinct BP.enc_id from get_latest_enc_ids('HCGH') BP
    ) R on C.enc_id = R.enc_id
    inner join pat_enc p on c.enc_id = p.enc_id
    group by p.pat_id, C.enc_id, C.event_id, C.flag
    having max(C.update_date) between ts_start and ts_end
  ) R
  group by R.pat_id, R.enc_id, R.event_id
)
select R.pat_id, R.enc_id, care_unit.care_unit,
       min(care_unit.enter_time) as enter_time,
       max(care_unit.leave_time) as leave_time,
       count(distinct R.event_id) filter (where R.trews_no_cms > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time) as trews_no_cms,
       count(distinct R.event_id) filter (where R.cms_no_trews > 0 and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as cms_no_trews,
       count(distinct R.event_id) filter (where R.trews_and_cms > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as trews_and_cms,
       count(distinct R.event_id) filter (where R.any_trews > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time) as any_trews,
       count(distinct R.event_id) filter (where R.any_cms > 0 and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as any_cms,
       min(R.trews_subalert_onset) filter (where R.any_trews > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time) as earliest_trews,
       min(R.cms_onset) filter (where R.any_cms > 0 and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as earliest_cms
from snapshots R
inner join care_unit
on R.enc_id = care_unit.enc_id
and (
   (R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time)
or (R.cms_onset between care_unit.enter_time and care_unit.leave_time)
)
group by R.pat_id, R.enc_id, care_unit.care_unit
;
END $func$ LANGUAGE plpgsql;


-- For each unit, count # of pats/encs with trews/cms alerts that fired within the stay in that unit.
--
CREATE OR REPLACE FUNCTION get_alert_stats_by_unit(ts_start timestamptz, ts_end timestamptz)
RETURNS table(
    care_unit                 text,
    total_encs_with_alerts    bigint,
    trews_no_cms              bigint,
    cms_no_trews              bigint,
    trews_and_cms             bigint,
    any_trews                 bigint,
    any_cms                   bigint
) AS $func$ BEGIN RETURN QUERY
with raw_care_unit_tbl as (
  select R.enc_id, R.enter_time, (case when R.leave_time is null then date_trunc('second', now()) else R.leave_time end) as leave_time, R.care_unit
  from (
    select R.enc_id,
           R.tsp as enter_time,
           lead(R.tsp,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp) as leave_time,
           (case when R.care_unit = 'Arrival' then R.next_unit else R.care_unit end) as care_unit
    from (
      select R.enc_id, R.tsp, R.care_unit,
             lead(R.tsp,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp,
                (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
             ) as next_tsp,
             lead(R.care_unit,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp,
                (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
             ) as next_unit,
             first_value(R.care_unit) over (PARTITION by R.enc_id order by R.tsp,
                (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
             ) as first_unit
      from (
        select cdm_s.enc_id, cdm_s.value::timestamptz as tsp, 'Arrival' as care_unit
        from cdm_s
        where cdm_s.fid = 'admittime'
        union all
        select cdm_t.enc_id, cdm_t.tsp, (case when cdm_t.fid = 'discharge' then 'Discharge' else cdm_t.value end) as care_unit
        from cdm_t
        where ( cdm_t.fid = 'care_unit' or cdm_t.fid = 'discharge' )
      ) R
      order by
        R.enc_id, R.tsp,
        (case when R.care_unit = 'Arrival' then 0 when R.care_unit = 'Discharge' then 2 else 1 end)
    ) R
    where not (R.care_unit = 'Arrival' and R.first_unit <> 'Arrival')
    and (R.next_tsp is null or R.tsp <> R.next_tsp)
    order by R.enc_id, enter_time
  ) R
),
discharge_filtered as (
  select R.*
  from raw_care_unit_tbl R
  where R.care_unit != 'Discharge' and R.leave_time is not null
),
care_unit as (
  select D.enc_id, D.enter_time, D.leave_time, D.care_unit
  from discharge_filtered D
),
bp_included as (
  select distinct BP.enc_id
  from get_latest_enc_ids('HCGH') BP
  inner join cdm_s on cdm_s.enc_id = BP.enc_id and cdm_s.fid = 'age'
  inner join cdm_t on cdm_t.enc_id = BP.enc_id and cdm_t.fid = 'care_unit'
  group by BP.enc_id
  having count(*) filter (where cdm_s.value::numeric <= 18) = 0
  and count(*) filter(where cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')) = 0
),
hcgh_discharged as (
  select distinct cdm_t.enc_id
  from cdm_t
  where cdm_t.fid =  'care_unit' and cdm_t.value like '%HCGH%'
  and cdm_t.enc_id not in (
    select distinct enc_id
    from (
      select enc_id from bp_included
      union all
      select cdm_t.enc_id
      from cdm_t
      where cdm_t.fid = 'care_unit'
      and cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')
      group by cdm_t.enc_id
      union all
      select cdm_s.enc_id
      from cdm_s
      where cdm_s.fid = 'age' and cdm_s.value::numeric <= 18
    ) R
  )
),
enc_included as (
  select distinct enc_id from hcgh_discharged
  union
  select distinct enc_id from bp_included
),
snapshots as (
  select R.pat_id, R.enc_id, R.event_id,
         max(R.update_date) as update_date,
         count(*) filter (where R.trews_subalert > 0 and ( R.sirs < 2 or R.orgdf < 1 )) as trews_no_cms,
         count(*) filter (where R.sirs > 1 and R.orgdf > 0 and R.trews_subalert = 0) as cms_no_trews,
         count(*) filter (where R.trews_subalert > 0 and R.sirs > 1 and R.orgdf > 0) as trews_and_cms,
         count(*) filter (where R.trews_subalert > 0) as any_trews,
         count(*) filter (where R.sirs > 1 and R.orgdf > 0) as any_cms,
         min(R.trews_subalert_onset) filter (where R.trews_subalert > 0) as trews_subalert_onset,
         min(greatest(R.sirs_onset, R.organ_onset)) filter (where R.sirs > 1 and R.orgdf > 0) as cms_onset
  from (
    select p.pat_id, C.enc_id, C.event_id, C.flag,
           max(C.update_date) as update_date,
           count(*) filter (where C.name like 'trews_subalert' and C.is_met) as trews_subalert,
           count(*) filter (where C.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and C.is_met) as sirs,
           count(*) filter (where C.name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and C.is_met) as orgdf,
           (array_agg(C.measurement_time order by C.measurement_time)  filter (where C.name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and C.is_met ) )[2]   as sirs_onset,
           min(C.measurement_time) filter (where C.name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and C.is_met ) as organ_onset,
           min(C.measurement_time) filter (where C.name = 'trews_subalert' and C.is_met) as trews_subalert_onset
    from criteria_events C
    inner join enc_included R on C.enc_id = R.enc_id
    inner join pat_enc p on c.enc_id = p.enc_id
    group by p.pat_id, C.enc_id, C.event_id, C.flag
    having max(C.update_date) between ts_start and ts_end
  ) R
  group by R.pat_id, R.enc_id, R.event_id
)
select care_unit.care_unit,
       count(distinct R.enc_id) as total_encs,
       count(distinct R.enc_id) filter (where R.trews_no_cms > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time) as trews_no_cms,
       count(distinct R.enc_id) filter (where R.cms_no_trews > 0 and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as cms_no_trews,
       count(distinct R.enc_id) filter (where R.trews_and_cms > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as trews_and_cms,
       count(distinct R.enc_id) filter (where R.any_trews > 0 and R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time) as any_trews,
       count(distinct R.enc_id) filter (where R.any_cms > 0 and R.cms_onset between care_unit.enter_time and care_unit.leave_time) as any_cms
from snapshots R
inner join care_unit
on R.enc_id = care_unit.enc_id
and (
   (R.trews_subalert_onset between care_unit.enter_time and care_unit.leave_time)
or (R.cms_onset between care_unit.enter_time and care_unit.leave_time)
)
group by care_unit.care_unit
;
END $func$ LANGUAGE plpgsql;



------------------------------------------------
-- Timeline helpers.

-- TREWS alert and organ dysfunction interval construction
CREATE OR REPLACE FUNCTION get_trews_orgdf_intervals(this_enc_id integer)
RETURNS table(
    name      text,
    intervals json
) AS $func$ BEGIN RETURN QUERY
with enc_states as (
  select S.name, R.tsp, S.state
  from (
    select tsp,
           ARRAY[
             'trews_creatinine',
             'trews_bilirubin',
             'trews_platelet',
             'trews_gcs',
             'trews_inr',
             'trews_lactate',
             'trews_sbpm',
             'trews_map',
             'trews_dsbp',
             'trews_vasopressors',
             'trews_vent',
             'trews_subalert'
           ]::text[] as names,
           ARRAY[
              creatinine_orgdf::int,
              bilirubin_orgdf::int,
              platelets_orgdf::int,
              gcs_orgdf::int,
              inr_orgdf::int,
              lactate_orgdf::int,
              sbpm_hypotension::int,
              map_hypotension::int,
              delta_hypotension::int,
              vasopressors_orgdf::int,
              vent_orgdf::int,
              ((orgdf_details::jsonb)#>>'{alert}')::bool::int
           ]::int[] as states
    from trews_jit_score s
    where s.enc_id = this_enc_id
    and s.model_id = (select max(P.value) from trews_parameters P where P.name = 'trews_jit_model_id')
  ) R, lateral unnest(R.names, R.states) S(name, state)
),
state_nb as (
  select C.name,
         C.tsp,
         C.state,
         lead(C.state, 1) over (partition by C.name order by C.tsp rows between current row and 1 following) as next,
         lead(C.tsp, 1)   over (partition by C.name order by C.tsp rows between current row and 1 following) as next_tsp,
         lag(C.state, 1)  over (partition by C.name order by C.tsp rows 1 preceding) as previous,
         lag(C.tsp, 1)    over (partition by C.name order by C.tsp rows 1 preceding) as previous_tsp
  from enc_states C
),
edges as (
  select NB.name,
         NB.tsp,
         (case
            when NB.next_tsp is null then now()
            when NB.state <> NB.next then NB.next_tsp
            else NB.tsp
          end) as cf_tsp,
         NB.state,
         NB.next,
         NB.next_tsp
  from state_nb NB
  where NB.state <> NB.next or NB.next is null
  or NB.state <> NB.previous or NB.previous is null
  order by NB.name, NB.tsp
),
edge_pairs as (
  select E.*,
         lead(E.state, 1)  over (partition by E.name order by E.tsp rows between current row and 1 following) as next_edge_state,
         lead(E.tsp, 1)    over (partition by E.name order by E.tsp rows between current row and 1 following) as next_edge_tsp,
         lead(E.cf_tsp, 1) over (partition by E.name order by E.tsp rows between current row and 1 following) as next_edge_cf_tsp,
         lag(E.state, 1)   over (partition by E.name order by E.tsp rows 1 preceding) as prev_edge_label,
         lag(E.tsp, 1)     over (partition by E.name order by E.tsp rows 1 preceding) as prev_edge_tsp
  from edges E
),
intervals as (
  select E.name, E.state,
         E.tsp as ts_start,
         (case
            when E.next_edge_tsp is null and (E.prev_edge_tsp is null or E.state <> E.prev_edge_label) then E.cf_tsp
            when E.state = E.next_edge_state then E.next_edge_cf_tsp
            else E.next_edge_tsp
          end) as ts_end
  from edge_pairs E
  where (E.prev_edge_tsp is null or E.state <> E.prev_edge_label)
  order by E.name, E.tsp
)
select I.name,
       json_agg(json_build_object('value', I.state, 'ts_start', I.ts_start, 'ts_end', I.ts_end) order by I.ts_start) filter (where I.state = 1)
       as last_interval
from intervals I
group by I.name
;
END $func$ LANGUAGE plpgsql;