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

create or replace function ol_pat_enc(_dataset_id integer)
RETURNS
table(enc_id integer,
      pat_id varchar(50),
      visit_id varchar(50))
AS $func$ BEGIN RETURN QUERY
select p.enc_id, p.pat_id, p.visit_id
FROM pat_enc p
WHERE p.dataset_id = _dataset_id and p.pat_id ~ '^E'
  AND p.enc_id NOT IN
    ( SELECT distinct cdm_t.enc_id
     FROM cdm_t
     WHERE dataset_id = _dataset_id
       AND fid = 'discharge' )
AND p.enc_id NOT IN
    ( SELECT distinct cdm_t.enc_id
     FROM cdm_t
     WHERE dataset_id = _dataset_id
       AND fid = 'care_unit' and value = 'Discharge')
; END $func$ LANGUAGE plpgsql;


create or replace function enc_hosp(_dataset_id integer)
RETURNS
table(enc_id integer, hospital text)
AS $func$ BEGIN RETURN QUERY
SELECT u.enc_id, (CASE WHEN unit ~* 'hc' THEN 'HCGH' WHEN unit ~* 'jh' THEN 'JHH' WHEN unit ~* 'bmc|bv' THEN 'BMC' WHEN unit ~* 'smh' THEN 'SMH' WHEN unit ~* 'sh' THEN 'SH' ELSE unit END) hospital
   FROM
     (SELECT c.enc_id,
             first(value order by c.tsp) unit
      FROM
        (SELECT p.enc_id, t.tsp, t.value
         FROM pat_enc p
         INNER JOIN cdm_t t ON t.enc_id = p.enc_id and p.dataset_id = t.dataset_id
         WHERE t.dataset_id = _dataset_id
           AND fid = 'care_unit' and value <> 'Discharge'
      ORDER BY p.enc_id,
               t.tsp DESC) c
   GROUP BY c.enc_id) u
; END $func$ LANGUAGE plpgsql;

-- add_cdm_t for medication summation
CREATE OR REPLACE FUNCTION add_cdm_t(dsid INT, key1 INT, key2 timestamptz, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        IF isnumeric(new_value) THEN
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
    EXECUTE 'INSERT INTO cdm_g (dataset_id,fid,value,confidence) values (' || dataset_id || ',' ||quote_literal(fid_popmean)||', '||quote_literal(popmean)||', 24) on conflict(dataset_id,fid) do update set value=excluded.value, confidence=excluded.confidence';
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

CREATE OR REPLACE FUNCTION last_value(twf_fids TEXT[], twf_table TEXT, this_dataset_id int, enc_ids int[] default null, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    raise notice 'Fillin table % for fids: %', twf_table, twf_fids;
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
        select string_agg('(case when ' || fid || ' is not null then ' || fid || ' when prev_' || fid || ' is not null then (prev_' || fid || '->>''val'')::numeric else ' || fid || '_popmean end ) as ' || fid || ',' || E'\n' || '(case when ' || fid || ' is not null then ' || fid || '_c when prev_' || fid || ' is not null then ((prev_' || fid || '->>''conf'')::int | 8) else 24 end ) as ' || fid || '_c', ',' || E'\n') as col
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

CREATE OR REPLACE FUNCTION last_value(twf_fids TEXT[], twf_table TEXT, this_dataset_id int, proc_id int, nprocs int, start_tsp timestamptz default null, end_tsp timestamptz default null, is_exec boolean default true)
RETURNS VOID
AS $BODY$
DECLARE
    query_str text;
BEGIN
    raise notice 'Fillin table % for fids: %', twf_table, twf_fids;
    with fid_win as (
        select fid, window_size_in_hours from unnest(twf_fids) inner join cdm_feature on unnest = fid where category = 'TWF' and is_measured and dataset_id = this_dataset_id
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
        select string_agg('(case when ' || fid || ' is not null then ' || fid || ' when prev_' || fid || ' is not null then (prev_' || fid || '->>''val'')::numeric else ' || fid || '_popmean end ) as ' || fid || ',' || E'\n' || '(case when ' || fid || ' is not null then ' || fid || '_c when prev_' || fid || ' is not null then ((prev_' || fid || '->>''conf'')::int | 8) else 24 end ) as ' || fid || '_c', ',' || E'\n') as col
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
                ' and enc_id % ' || nprocs || ' = '|| proc_id ||'
                order by enc_id, tsp
            ) R
        ) S
    ) ON CONFLICT (dataset_id, enc_id, tsp) DO UPDATE SET
    ' || u_col || ';'
        into query_str from select_r_col cross join select_s_col cross join select_col cross join select_u_col cross join select_insert_col;
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
  return (case
    when isnumeric(m_value)
    then coalesce(
          (c_ovalue is not null and c_ovalue#>>'{0,text}' = 'Not Indicated')
          or not (
              m_value::numeric
                  between coalesce((c_ovalue#>>'{0,lower}')::numeric, (d_ovalue#>>'{lower}')::numeric, m_value::numeric)
                  and coalesce((c_ovalue#>>'{0,upper}')::numeric, (d_ovalue#>>'{upper}')::numeric, m_value::numeric)
          ), false)
    else false
    end
  );
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
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    )

                when order_name = 'initial_lactate_order' or order_name = 'repeat_lactate_order'
                    then order_value in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
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
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    ) then 'Completed'

                when order_fid = 'lactate_order' and value_text in ('None', 'Signed') then 'Ordered'

                when order_fid = 'blood_culture_order' and value_text in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
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
                when order_status = 'Completed' and dose_limit = 0 then true
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
    select ordered.pat_id, first(ordered.value order by ordered.tsp) as value
    from (
        select P.pat_id, weights.value::numeric as value, weights.tsp
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
          first(ordered.measurement_time order by ordered.measurement_time) as measurement_time,
          (first(ordered.value order by ordered.measurement_time))::text as value,
          first(ordered.c_otime order by ordered.measurement_time) as override_time,
          first(ordered.c_ouser order by ordered.measurement_time) as override_user,
          first(ordered.c_ovalue order by ordered.measurement_time) as override_value,
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
          first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met) as measurement_time,
          first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met)::text as value,
          first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met) as override_time,
          first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met) as override_user,
          first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met) as override_value,
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
        first(ordered.tsp ordered by ordered.tsp) filter (where ordered.is_met) as measurement_time,
        (first(ordered.value ordered by ordered.tsp) filter (where ordered.is_met))::text as value,
        first(ordered.c_otime ordered by ordered.tsp) filter (where ordered.is_met) as override_time,
        first(ordered.c_ouser ordered by ordered.tsp) filter (where ordered.is_met) as override_user,
        first(ordered.c_ovalue ordered by ordered.tsp) filter (where ordered.is_met) as override_value,
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
        first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met) as measurement_time,
        (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
        first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met) as override_time,
        first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met) as override_user,
        first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met) as override_value,
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
        (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
        (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
        (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
        (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
        (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
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
          (first(ordered.measurement_time order by ordered.is_met) filter (where ordered.is_met)) as measurement_time,
          (first(ordered.value order by ordered.is_met) filter (where ordered.is_met))::text as value,
          (first(ordered.c_otime order by ordered.is_met) filter (where ordered.is_met)) as override_time,
          (first(ordered.c_ouser order by ordered.is_met) filter (where ordered.is_met)) as override_user,
          (first(ordered.c_ovalue order by ordered.is_met) filter (where ordered.is_met)) as override_value,
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
          (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
          (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
          (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
          (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
          (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
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
        (first(ordered.measurement_time order by ordered.measurement_time) filter (where ordered.is_met)) as measurement_time,
        (first(ordered.value order by ordered.measurement_time) filter (where ordered.is_met))::text as value,
        (first(ordered.c_otime order by ordered.measurement_time) filter (where ordered.is_met)) as override_time,
        (first(ordered.c_ouser order by ordered.measurement_time) filter (where ordered.is_met)) as override_user,
        (first(ordered.c_ovalue order by ordered.measurement_time) filter (where ordered.is_met)) as override_value,
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
    select cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp, cdm_t.fid, first(cdm_t.value order by cdm_t.tsp), now()
    FROM
    cdm_t
    inner join
    pat_enc
    on cdm_t.enc_id = pat_enc.enc_id
    and cdm_t.dataset_id = pat_enc.dataset_id
    where cdm_t.dataset_id = _dataset_id and not(cdm_t.fid = 'suspicion_of_infection')
    and (not incremental or (pat_enc.meta_data->>'pending')::boolean)
    and (
            cdm_t.fid in (
                select distinct fid from criteria_default
                where dataset_id = _dataset_id
            ) or cdm_t.fid in (
                'culture_order','cbc_order','metabolic_panel_order'
            )
        )
    group by cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp, cdm_t.fid
    ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
    -- ================================================
    -- Upsert cdm_twf derived features
    -- ================================================
    FOR _fid in
      select cd.fid
      from
      criteria_default cd
      left join
      cdm_feature f
      on cd.fid = f.fid and cd.dataset_id = f.dataset_id
      where f.category = 'TWF' and f.dataset_id = _dataset_id
      and not f.is_measured
      group by cd.fid
    LOOP
      PERFORM load_cdm_twf_to_criteria_meas(_fid,_dataset_id, incremental);
    END LOOP;
    -- ================================================
    -- Handle bp_sys as a special case
    -- ================================================
    raise notice 'handling bp_sys as a special case';
    insert into criteria_meas (dataset_id, pat_id, tsp, fid, value, update_date)
    select cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp, 'bp_sys', first(cdm_t.value order by cdm_t.tsp), now()
    FROM
    cdm_t
    inner join
    pat_enc
    on cdm_t.enc_id = pat_enc.enc_id
    and cdm_t.dataset_id = pat_enc.dataset_id
    where cdm_t.dataset_id = _dataset_id
    and (not incremental or (pat_enc.meta_data->>'pending')::boolean)
    and (
            cdm_t.fid in ('abp_sys', 'nbp_sys')
        )
    group by cdm_t.dataset_id, pat_enc.pat_id, cdm_t.tsp
    ON CONFLICT (dataset_id, pat_id, tsp, fid) DO UPDATE SET value = excluded.value, update_date=excluded.update_date;
END; $function$;



---------------------------------
-- cdm healtch check functions --
---------------------------------
create or replace function dataset_config_compare(dataset_id_left int, dataset_id_right int)
  returns table(
    dataset_id              integer,
    config_table              text,
    content                 text)
as $func$
begin
return query
select * from(
  select dataset_id_left dataset_id, 'cdm_feature', left_only.* from
  (select concat_ws(', ', l.fid, l.category, l.data_type, l.is_measured, l.is_deprecated, l.fillin_func_id, l.window_size_in_hours, l.derive_func_id, l.derive_func_input, l.description, l.version, l.unit) from cdm_feature l where l.dataset_id = dataset_id_left
  except
  select concat_ws(', ', r.fid, r.category, r.data_type, r.is_measured, r.is_deprecated, r.fillin_func_id, r.window_size_in_hours, r.derive_func_id, r.derive_func_input, r.description, r.version, r.unit) from cdm_feature r where r.dataset_id = dataset_id_right) as left_only
  union
  select dataset_id_right dataset_id, 'cdm_feature', right_only.* from
  (select concat_ws(', ', r.fid, r.category, r.data_type, r.is_measured, r.is_deprecated, r.fillin_func_id, r.window_size_in_hours, r.derive_func_id, r.derive_func_input, r.description, r.version, r.unit) from cdm_feature r where r.dataset_id = dataset_id_right
  except
  select concat_ws(', ', l.fid, l.category, l.data_type, l.is_measured, l.is_deprecated, l.fillin_func_id, l.window_size_in_hours, l.derive_func_id, l.derive_func_input, l.description, l.version, l.unit) from cdm_feature l where l.dataset_id = dataset_id_left) as right_only
union
  select dataset_id_left dataset_id, 'cdm_function', left_only.* from
  (select concat_ws(', ', l.FUNC_ID,l.FUNC_TYPE,l.DESCRIPTION) from cdm_function l where l.dataset_id = dataset_id_left
  except
  select concat_ws(', ', r.FUNC_ID,r.FUNC_TYPE,r.DESCRIPTION) from cdm_function r where r.dataset_id = dataset_id_right) as left_only
  union
  select dataset_id_right dataset_id, 'cdm_function', right_only.* from
  (select concat_ws(', ', r.FUNC_ID,r.FUNC_TYPE,r.DESCRIPTION) from cdm_function r where r.dataset_id = dataset_id_right
  except
  select concat_ws(', ', l.FUNC_ID,l.FUNC_TYPE,l.DESCRIPTION) from cdm_function l where l.dataset_id = dataset_id_left) as right_only
union
  select dataset_id_left dataset_id, 'parameters', left_only.* from
  (select concat_ws(', ', l.name, l.value) from parameters l where l.dataset_id = dataset_id_left
  except
  select concat_ws(', ', r.name, r.value) from parameters r where r.dataset_id = dataset_id_right) as left_only
  union
  select dataset_id_right dataset_id, 'parameters', right_only.* from
  (select concat_ws(', ', r.name, r.value) from parameters r where r.dataset_id = dataset_id_right
  except
  select concat_ws(', ', l.name, l.value) from parameters l where l.dataset_id = dataset_id_left) as right_only
union
  select dataset_id_left dataset_id, 'criteria_default', left_only.* from
  (select concat_ws(', ', l.name, l.fid, l.override_value, l.category) from criteria_default l where l.dataset_id = dataset_id_left
  except
  select concat_ws(', ', r.name, r.fid, r.override_value, r.category) from criteria_default r where r.dataset_id = dataset_id_right) as left_only
  union
  select dataset_id_right dataset_id, 'criteria_default', right_only.* from
  (select concat_ws(', ', r.name, r.fid, r.override_value, r.category) from criteria_default r where r.dataset_id = dataset_id_right
  except
  select concat_ws(', ', l.name, l.fid, l.override_value, l.category) from criteria_default l where l.dataset_id = dataset_id_left) as right_only
) U;
end $func$ LANGUAGE plpgsql;

create or replace function run_cdm_label_and_report(_dataset_id int, _label_des text, server text, nprocs int)
returns void as
$func$
declare
  _label_id int;
begin
  -- TODO: delete previous labels
  select * from get_cms_label_series(
      _label_des, 1, null, _dataset_id, server, nprocs,
      now() - interval '1 week', now(), interval '1 week',
      interval '1 week', 'all', false, false, true, true) into _label_id;
  perform create_care_unit(_dataset_id);
  perform create_criteria_report(null, _dataset_id, _label_id);
end
$func$ language plpgsql;

-------------------------
-- cdm stats functions --
-------------------------
create or replace function run_cdm_stats(
  _dataset_id int, server text default 'dev_dw', nprocs int default 2,
  start_tsp timestamptz default '2000-01-01'::timestamptz,
  end_tsp timestamptz default '2100-01-01'::timestamptz)
RETURNS void AS
$func$
begin
  perform run_cdm_stats_p(_dataset_id, 'pat_enc', start_tsp, end_tsp);
  perform run_cdm_stats_p(_dataset_id, 'cdm_s', start_tsp, end_tsp);
  perform run_cdm_stats_p(_dataset_id, 'cdm_t', start_tsp, end_tsp);
  perform run_cdm_stats_p(_dataset_id, 'cdm_twf', start_tsp, end_tsp);
  perform run_cdm_stats_p(_dataset_id, 'criteria_meas', start_tsp, end_tsp);
  perform run_cdm_stats_t(_dataset_id, 'cdm_t', start_tsp, end_tsp);
  perform run_cdm_stats_t(_dataset_id, 'cdm_twf', start_tsp, end_tsp);
  perform run_cdm_stats_t(_dataset_id, 'criteria_meas', start_tsp, end_tsp);
  perform run_cdm_stats_f(_dataset_id, 'cdm_s', server, nprocs, start_tsp, end_tsp);
  perform run_cdm_stats_f(_dataset_id, 'cdm_t', server, nprocs, start_tsp, end_tsp);
  perform run_cdm_stats_f(_dataset_id, 'criteria_meas', server, nprocs, start_tsp, end_tsp);
  perform run_cdm_stats_f(_dataset_id, 'cdm_twf', server, nprocs, start_tsp, end_tsp);
end $func$ language plpgsql;

create or replace function run_cdm_stats_p(
  _dataset_id int, _table text,
  start_tsp timestamptz default '2000-01-01'::timestamptz,
  end_tsp timestamptz  default '2100-01-01'::timestamptz)
RETURNS void AS
$func$
declare
  T text;
  T2 text;
begin
T2 = '';
if _table = 'pat_enc' then
  T = 'pat_enc p';
elsif _table = 'criteria_meas' then
  T = 'criteria_meas t inner join pat_enc p on t.pat_id = p.pat_id
    and t.dataset_id = p.dataset_id';
else
  T = _table || ' t inner join pat_enc p on t.enc_id = p.enc_id
    and t.dataset_id = p.dataset_id';
end if;
if _table in ('cdm_t', 'cdm_twf', 'criteria_meas') then
  T2 = ' and t.tsp between ''' || start_tsp || '''::timestamptz and '''
    || end_tsp || '''::timestamptz';
end if;
execute
  'insert into cdm_stats select ' ||
  _dataset_id || ' dataset_id,
  '||quote_literal(_table)||' id,
  ''p'' id_type,
  '||quote_literal(_table)||' cdm_table,
  jsonb_build_object(
  ''cnt_enc_id'', count(distinct p.enc_id),
  ''cnt_visit_id'', count(distinct p.visit_id),
  ''cnt_pat_id'', count(distinct p.pat_id)) stats
  from ' || T || ' where p.dataset_id = ' || _dataset_id || T2
  || ' on conflict(dataset_id, id, id_type, cdm_table) do update set
    stats = excluded.stats'
  ;
end;
$func$ LANGUAGE plpgsql;

create or replace function run_cdm_stats_t(_dataset_id int, _table text,
  start_tsp timestamptz default '2000-01-01'::timestamptz,
  end_tsp timestamptz  default '2100-01-01'::timestamptz)
RETURNS void AS
$func$
declare
begin
execute
'with day_cnt as (
  select tsp::date date,
  count(*)
  from '||_table||'
  where dataset_id = '|| _dataset_id ||'
   and tsp between '''|| start_tsp ||'''::timestamptz
   and '''||end_tsp||'''::timestamptz
  group by 1
  order by 1
),
hour_cnt as (
  select extract(hour from tsp) hr,
  count(*)
  from '||_table||'
  where dataset_id = '|| _dataset_id ||'
    and tsp between '''|| start_tsp ||'''::timestamptz
    and '''||end_tsp||'''::timestamptz
  group by 1
  order by 1
),
day_row_cnt as (
  select jsonb_object_agg(date, count) day_rows from day_cnt
),
hour_row_cnt as (
  select jsonb_object_agg(hr, count) hour_rows from hour_cnt
),
stats as (
    select min(count) as min,
           max(count) as max
      from day_cnt
),
histogram as (
  select width_bucket(count, min, max, 99) as bucket,
        numrange(min(count)::numeric, max(count)::numeric, ''[]'') as range,
        count(*) as freq
  from day_cnt, stats
  group by bucket
  order by bucket
),
histogram_json as(
  select
  jsonb_build_object(''bucket'', bucket,''range'', range, ''freq'', freq, ''bar'', repeat(''*'', ((freq)::float / max(freq) over() * 30)::int)
  ) v
  from histogram
),
day_rows_histogram as (
  select jsonb_agg(v) hist
  from histogram_json
)
insert into cdm_stats
select t.dataset_id dataset_id,
  '||quote_literal(_table)||' id,
  ''t'' id_type,
  '||quote_literal(_table)||' cdm_table,
  jsonb_build_object(''tsp'', t.stats_tsp,
    ''day_row_cnt'', day_rows::jsonb,
    ''day_rows_histogram'', hist::jsonb,
    ''hour_row_cnt'', hour_rows::jsonb) as stats
from
(select
  '||_dataset_id||' dataset_id,
  jsonb_build_object(
    ''tsp_min'', min(tsp),
    ''tsp_max'', max(tsp),
    ''tsp_range'', age(max(tsp), min(tsp)),
    ''tsp_mean'', to_timestamp(avg(extract(''epoch'' from tsp))),
    ''tsp_5%'', percentile_disc(0.05) within group (order by tsp),
    ''tsp_25%'', percentile_disc(0.25) within group (order by tsp),
    ''tsp_50%'', percentile_disc(0.5) within group (order by tsp),
    ''tsp_75%'', percentile_disc(0.75) within group (order by tsp),
    ''tsp_95%'', percentile_disc(0.95) within group (order by tsp),
    ''cnt_date'', count(distinct tsp::date)
  ) stats_tsp
  from ' || _table || '
  where '||_table||'.dataset_id = '||_dataset_id||'
  and tsp between '''|| start_tsp ||'''::timestamptz
   and '''||end_tsp||'''::timestamptz
) t, day_row_cnt, day_rows_histogram, hour_row_cnt
on conflict(dataset_id, id, id_type, cdm_table) do update set stats = excluded.stats';
end;
$func$ LANGUAGE plpgsql;

create or replace function delete_dataset(_dataset_id int)
RETURNS void AS
$func$
begin
delete from cdm_s where dataset_id = _dataset_id;
delete from cdm_t where dataset_id = _dataset_id;
delete from cdm_twf where dataset_id = _dataset_id;
delete from cdm_notes where dataset_id = _dataset_id;
delete from criteria_meas where dataset_id = _dataset_id;
delete from trews where dataset_id = _dataset_id;
delete from pat_enc where dataset_id = _dataset_id;
perform setval('pat_enc_enc_id_seq', 1);
end;
$func$ language plpgsql;

create or replace function run_cdm_stats_f(_dataset_id int, _table text,
  server text default 'dev_dw', nprocs int default 2,
  start_tsp timestamptz default '2000-01-01'::timestamptz,
  end_tsp timestamptz  default '2100-01-01'::timestamptz
  )
RETURNS void AS
$func$
declare
queries text[];
q text;
use_hist boolean;
rec record;
T text;
begin
if _table = 'cdm_twf' then
  for rec in select * from cdm_feature f where f.category = 'TWF'
    and f.dataset_id = _dataset_id
  loop
    if rec.data_type ~* 'real|int' then
      execute 'select min(' || rec.fid || ') <> max(' || rec.fid || ')
        from cdm_twf where dataset_id = ' || _dataset_id || '
        and tsp between '''|| start_tsp ||'''::timestamptz
        and '''||end_tsp||'''::timestamptz'
        into use_hist;
    else use_hist = false;
    end if;
    if use_hist then
      q = '
      with s as (
        select
               min('||rec.fid||') as min,
               max('||rec.fid||') as max
        from '||_table||' t
        where t.dataset_id = '||_dataset_id||' and '||rec.fid||' is not null
          and tsp between '''|| start_tsp ||'''::timestamptz
          and '''||end_tsp||'''::timestamptz
      ),
      histogram as (
        select
           width_bucket('||rec.fid||', min, max, 99) as bucket,
           numrange(min('||rec.fid||')::numeric, max('||rec.fid||')::numeric, ''[]'') as range,
           count(*) as freq
        from s cross join '||_table||' t
        where t.dataset_id = '|| _dataset_id ||' and '||rec.fid||' is not null
        group by bucket
        order by bucket
      ),
      histogram_json as(
      select
      jsonb_build_object(''bucket'', bucket,''range'', range, ''freq'', freq) v
      from histogram
      ),
      histogram_agg as(
        select jsonb_agg(v) hist from histogram_json
      )';
    else
      q = '';
    end if;
    q = q || '
      insert into cdm_stats
      select M.dataset_id dataset_id,
      M.fid id,
      ''f'' id_type,
      ''cdm_twf'' cdm_table,
      cnt || stats || jsonb_build_object(
        ''is_measured'', is_measured,
        ''data_type'', data_type,
        ''histogram'', ';
    if use_hist then
      q = q || 'hist';
    else
      q = q || '''{}''::jsonb';
    end if;
    q = q || ') stats from
      (select
        '||_dataset_id||' dataset_id
        , '''||rec.fid||'''::text fid
        , jsonb_build_object(
          ''cnt'', count(*),
          ''cnt_meas'', count(*) filter (where '||rec.fid||'_c < 8),
          ''cnt_fill_last'', count(*) filter (where '||rec.fid||'_c between 8 and 23),
          ''cnt_fill_pop'', count(*) filter (where '||rec.fid||'_c = 24),
          ''cnt_null'', count(*) filter (where '||rec.fid||' is null),
          ''cnt_c_null'', count(*) filter (where '||rec.fid||'_c is null)
          ) cnt
        , last(f.is_measured) is_measured
        , last(f.data_type) data_type
        , ';
    if use_hist then
      q = q || '
      coalesce(jsonb_build_object(
                ''min'' , min('||rec.fid||') filter (where f.data_type ~* ''real|int'')
              , ''max'' , max('||rec.fid||') filter (where f.data_type ~* ''real|int'')
              , ''mean'', avg('||rec.fid||') filter (where f.data_type ~* ''real|int'')
              , ''5%'' , percentile_disc(0.05) within group (order by '||rec.fid||')
                          filter (where f.data_type ~* ''real|int'')
              , ''25%'' , percentile_disc(0.25) within group (order by '||rec.fid||')
                          filter (where f.data_type ~* ''real|int'')
              , ''50%'' , percentile_disc(0.5) within group (order by '||rec.fid||')
                          filter (where f.data_type ~* ''real|int'')
              , ''75%'' , percentile_disc(0.75) within group (order by '||rec.fid||')
                          filter (where f.data_type ~* ''real|int'')
              , ''95%'' , percentile_disc(0.95) within group (order by '||rec.fid||')
                          filter (where f.data_type ~* ''real|int'')
              ), ''{}''::jsonb)';
    elsif rec.data_type ~* 'bool' then
      q = q || '
      coalesce(jsonb_build_object(
                ''cnt_true''  , sum('||rec.fid||'::int) filter (where f.data_type ~* ''bool''),
                ''cnt_false'' , sum((not '||rec.fid||')::int) filter (where f.data_type ~* ''bool'')
              ), ''{}''::jsonb)';
    else
      q = q || '''{}''::jsonb';
    end if;
    q = q || ' stats from '|| _table ||' s
    inner join cdm_feature f
    on s.dataset_id = f.dataset_id
    where s.dataset_id = '|| _dataset_id ||' and f.fid = '''||rec.fid||'''
      and s.tsp between '''|| start_tsp ||'''::timestamptz
      and '''||end_tsp||'''::timestamptz
    ) M';
    if use_hist then
      q = q || ', histogram_agg ht';
    end if;
    q = q || ' on conflict(dataset_id, id, id_type, cdm_table) do update set stats = excluded.stats';
    queries = array_append(queries, q);
  end loop;
  perform distribute(server, queries, nprocs);
else
  T = '';
  if _table in ('cdm_t', 'criteria_meas') then
    T = ' and t.tsp between '''|| start_tsp ||'''::timestamptz
      and '''||end_tsp||'''::timestamptz';
  end if;
  q = '
  with v as (
    select t.fid,
    (case when f.data_type ~* ''real|int'' then t.value
    else t.value::json->>''dose'' end) as value
    from '||_table||' t inner join cdm_feature f
    on f.dataset_id = t.dataset_id
    and t.fid = f.fid
    where t.dataset_id = '||_dataset_id||' and
    (
      (f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
      or (f.data_type = ''JSON'' and f.fid ~* ''_dose'')
    ) '|| T ||'
  ),
  s as (
    select fid, min(value::numeric) as min, max(value::numeric) as max
    from v
    group by v.fid
  ),
  histogram as (
    select s.fid,
      width_bucket(value::numeric, min, max, 99) as bucket,
      numrange(min(value::numeric)::numeric, max(value::numeric)::numeric, ''[]'') as range,
      count(*) as freq
    from s inner join v on s.fid = v.fid
    where min <> max
    group by s.fid, bucket
    order by s.fid, bucket
  ),
  histogram_json as(
  select fid,
  jsonb_build_object(''bucket'', bucket, ''range'', range, ''freq'', freq) v
  from histogram
  ),
  histogram_agg as(
    select fid, coalesce(jsonb_agg(v), ''{}''::jsonb) hist from histogram_json group by fid order by fid
  )
  insert into cdm_stats
  select M.dataset_id dataset_id,
  M.fid id,
  ''f'' id_type,
  ' || quote_literal(_table) || ' cdm_table,
  cnt || stats || jsonb_build_object(
    ''is_measured'', is_measured,
    ''data_type'', data_type,
    ''histogram'', hist
  ) stats from
  (select
    t.dataset_id
    , t.fid
    , jsonb_build_object(''cnt'', count(*)) cnt
    , last(f.is_measured) is_measured
    , last(f.data_type) data_type
    , (
        case
        when last(f.data_type) ~* ''real|int'' then
          coalesce(jsonb_build_object(
              ''min'' , min(value::numeric) filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''max'' , max(value::numeric) filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''mean'', avg(value::numeric) filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''5%'' , percentile_disc(0.05) within group (order by value::numeric)
                        filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''25%'' , percentile_disc(0.25) within group (order by value::numeric)
                        filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''50%'' , percentile_disc(0.5) within group (order by value::numeric)
                        filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''75%'' , percentile_disc(0.75) within group (order by value::numeric)
                        filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            , ''95%'' , percentile_disc(0.95) within group (order by value::numeric)
                        filter (where f.data_type ~* ''real|int'' and value <> ''nan'' and value <> ''None'')
            ), ''{}''::jsonb)
        when last(f.data_type) ~* ''json'' and last(f.fid) ~* ''_dose'' then
          coalesce(jsonb_build_object(
              ''min'' , min((value::json->>''dose'')::numeric) filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''max'' , max((value::json->>''dose'')::numeric) filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''mean'', avg((value::json->>''dose'')::numeric) filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''5%'' , percentile_disc(0.05) within group (order by (value::json->>''dose'')::numeric)
                        filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''25%'' , percentile_disc(0.25) within group (order by (value::json->>''dose'')::numeric)
                        filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''50%'' , percentile_disc(0.5) within group (order by (value::json->>''dose'')::numeric)
                        filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''75%'' , percentile_disc(0.75) within group (order by (value::json->>''dose'')::numeric)
                        filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            , ''95%'' , percentile_disc(0.95) within group (order by (value::json->>''dose'')::numeric)
                        filter (where f.data_type ~* ''json'' and f.fid ~* ''_dose'' and value <> ''nan'' and value <> ''None'')
            ), ''{}''::jsonb)
        when last(f.data_type) ~* ''bool'' then
          coalesce(jsonb_build_object(
              ''cnt_true''  , count(*) filter (where f.data_type ~* ''bool'' and value::boolean and value <> ''nan'' and value <> ''None''),
              ''cnt_false'' , count(*) filter (where f.data_type ~* ''bool'' and not value::boolean and value <> ''nan'' and value <> ''None'')
            ), ''{}''::jsonb)
        when last(f.data_type) ~* ''String'' and t.fid ~* ''_time'' then
          coalesce(jsonb_build_object(
              ''min'' , min(value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None''),
              ''max'' , max(value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None''),
              ''mean'', avg(value::timestamptz - ''2010-01-01''::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None'') + ''2010-01-01''::timestamptz,
              ''5%'' , percentile_disc(0.05) within group (order by value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None''),
              ''25%'' , percentile_disc(0.25) within group (order by value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None''),
              ''50%'' , percentile_disc(0.5) within group (order by value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None''),
              ''75%'' , percentile_disc(0.75) within group (order by value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None''),
              ''95%'' , percentile_disc(0.95) within group (order by value::timestamptz)
                        filter (where f.data_type ~* ''String'' and t.fid ~* ''_time'' and value <> ''nan'' and value <> ''None'')
            ), ''{}''::jsonb)
        else
          ''{}''::jsonb
        end
      ) stats
  from '|| _table ||' t
  inner join cdm_feature f
  on t.dataset_id = f.dataset_id and t.fid = f.fid
  where t.dataset_id = '|| _dataset_id || T ||'
  group by t.dataset_id, t.fid
  order by t.fid) M left join histogram_agg ht on ht.fid = M.fid
  order by M.fid
  on conflict(dataset_id, id, id_type, cdm_table) do update set stats = excluded.stats';
end if;
execute q;
end;
$func$ LANGUAGE plpgsql;


CREATE or replace FUNCTION rowcount_all(schema_name text default 'public')
  RETURNS table(table_name text, cnt bigint) as
$$
declare
 table_name text;
begin
  for table_name in SELECT c.relname FROM pg_class c
    JOIN pg_namespace s ON (c.relnamespace=s.oid)
    WHERE c.relkind = 'r' AND s.nspname=schema_name
    order by c.relname
  LOOP
    RETURN QUERY EXECUTE format('select cast(%L as text),count(*) from %I.%I',
       table_name, schema_name, table_name);
  END LOOP;
end
$$ language plpgsql;

create or replace function cdm_feature_present(_dataset_id int)
    returns table(id text, cdm_table text, count int) as
$$
begin
return query
select f.fid::text, s.cdm_table, coalesce((s.stats->>'cnt')::int, 0)
from cdm_feature f left join cdm_stats s
    on f.fid = s.id and f.dataset_id = s.dataset_id
     and
(
    (s.cdm_table = 'cdm_twf' and f.category = 'TWF')
  or (s.cdm_table = 'cdm_t' and f.category = 'T')
  or (s.cdm_table = 'cdm_s' and f.category = 'S')
  or (s.cdm_table = 'criteria_meas')
)
where f.dataset_id = _dataset_id and f.category in ('T', 'S', 'TWF')
order by coalesce((s.stats->>'cnt')::int, 0);
end
$$ language plpgsql;

create or replace function cdm_feature_diff(dataset_id_left int, dataset_id_right int)
    returns table(id text, cdm_table text, diff jsonb, left_stats jsonb, right_stats jsonb) as
$$
begin
return query
with L as
(
    select f.fid::text, s.cdm_table, s.stats from cdm_feature f inner join cdm_stats s
        on f.fid = s.id and f.dataset_id = s.dataset_id
    where f.dataset_id = dataset_id_left
),
R as
(
    select f.fid::text, s.cdm_table, s.stats from cdm_feature f inner join cdm_stats s
        on f.fid = s.id and f.dataset_id = s.dataset_id
    where f.dataset_id = dataset_id_right
)
select L.fid, L.cdm_table,
    (
        case when L.stats->>'data_type' = 'Boolean' then
            jsonb_build_object(
              'true_diff_ratio',
              round(abs((L.stats->>'cnt_true')::numeric / (L.stats->>'cnt')::numeric)
            - ((R.stats->>'cnt_true')::numeric / (R.stats->>'cnt')::numeric), 3)
            )
        when L.stats->>'data_type' ~* 'real|int' then
            jsonb_build_object(
              'mean_diff_ratio',
              round(abs(((L.stats->>'mean')::numeric - (R.stats->>'mean')::numeric) / (R.stats->>'mean')::numeric), 3),
              'min_diff_ratio',
              round(abs(((L.stats->>'min')::numeric - (R.stats->>'min')::numeric) / (R.stats->>'mean')::numeric), 3),
              'max_diff_ratio',
              round(abs(((L.stats->>'max')::numeric - (R.stats->>'max')::numeric) / (R.stats->>'mean')::numeric), 3),
              '5%_diff_ratio',
              round(abs(((L.stats->>'5%')::numeric - (R.stats->>'5%')::numeric) / (R.stats->>'mean')::numeric), 3),
              '25%_diff_ratio',
              round(abs(((L.stats->>'25%')::numeric - (R.stats->>'25%')::numeric) / (R.stats->>'mean')::numeric), 3),
              '50%_diff_ratio',
              round(abs(((L.stats->>'50%')::numeric - (R.stats->>'50%')::numeric) / (R.stats->>'mean')::numeric), 3),
              '75%_diff_ratio',
              round(abs(((L.stats->>'75%')::numeric - (R.stats->>'75%')::numeric) / (R.stats->>'mean')::numeric), 3),
              '95%_diff_ratio',
              round(abs(((L.stats->>'95%')::numeric - (R.stats->>'95%')::numeric) / (R.stats->>'mean')::numeric), 3)
            )
        else
        '{}'::jsonb
        end
    ) diff,
    L.stats, R.stats from L inner join R on L.fid = R.fid and L.cdm_table = R.cdm_table
;
end
$$ language plpgsql;


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

--#############################
--## clarity stats functions ##
--#############################

create or replace function run_clarity_stats(_clarity_workspace text, server text default 'dev_dw', nprocs int default 2)
returns void as
$$
declare queries text[];
begin
  -- adt_feed
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ADT_Feed', 'EventType', 'DEPARTMENT_NAME'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ADT_Feed', 'DEPARTMENT_NAME', 'EventType'));
  -- diagnoses
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Diagnoses', 'DX_ID', 'Code'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Diagnoses', 'Code', 'CSN_ID'));
  -- flowsheet
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue', 'FLO_MEAS_NAME', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue', 'FLO_MEAS_NAME', 'UNITS'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue', 'FLO_MEAS_ID', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue', 'FLO_MEAS_ID', 'UNITS'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue', 'DISP_NAME', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue', 'DISP_NAME', 'UNITS'));
  -- flowsheet 643
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue_643', 'FLO_MEAS_NAME', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue_643', 'FLO_MEAS_NAME', 'UNITS'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue_643', 'FLO_MEAS_ID', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue_643', 'FLO_MEAS_ID', 'UNITS'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue_643', 'DISP_NAME', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue_643', 'DISP_NAME', 'UNITS'));
  perform distribute(server, queries, nprocs);
  -- flowsheet lda
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue-LDA', 'FLO_MEAS_NAME', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue-LDA', 'FLO_MEAS_NAME', 'UNITS'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue-LDA', 'FLO_MEAS_ID', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue-LDA', 'FLO_MEAS_ID', 'UNITS'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue-LDA', 'DISP_NAME', 'Value'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'FlowsheetValue-LDA', 'DISP_NAME', 'UNITS'));
  -- labs
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'COMPONENT_ID', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'COMPONENT_ID', 'REFERENCE_UNIT'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'BASE_NAME', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'BASE_NAME', 'REFERENCE_UNIT'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'NAME', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'NAME', 'REFERENCE_UNIT'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'EXTERNAL_NAME', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs', 'EXTERNAL_NAME', 'REFERENCE_UNIT'));
  -- labs_643
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'COMPONENT_ID', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'COMPONENT_ID', 'REFERENCE_UNIT'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'BASE_NAME', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'BASE_NAME', 'REFERENCE_UNIT'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'NAME', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'NAME', 'REFERENCE_UNIT'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'EXTERNAL_NAME', 'ResultValue'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Labs_643', 'EXTERNAL_NAME', 'REFERENCE_UNIT'));
  -- ldas
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'LDAs', 'FLO_MEAS_NAME', 'PAT_ID'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'LDAs', 'DISP_NAME', 'PAT_ID'));
  -- medicalhistory
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicalHistory', 'diagName', 'CSN_ID'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicalHistory', 'Code', 'CSN_ID'));
  -- medication administration
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicationAdministration', 'display_name', 'ActionTaken'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicationAdministration', 'display_name', 'Dose'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicationAdministration', 'display_name', 'MedUnit'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicationAdministration', 'MEDICATION_ID', 'ActionTaken'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicationAdministration', 'MEDICATION_ID', 'Dose'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'MedicationAdministration', 'MEDICATION_ID', 'MedUnit'));
  -- notes
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Notes', 'AuthorType', 'NOTE_ID'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'Notes', 'NoteType', 'NOTE_ID'));
  -- order med
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderMed', 'display_name', 'CSN_ID'));
  -- order med home
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderMedHome', 'display_name', 'CSN_ID'));
  -- order procs
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcs', 'display_name', 'OrderStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcs', 'proc_name', 'OrderStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcs', 'proc_cat_name', 'proc_name'));
  -- order procs 643
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcs_643', 'display_name', 'OrderStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcs_643', 'proc_name', 'OrderStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcs_643', 'proc_cat_name', 'proc_name'));
  -- order procs image
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcsImage', 'display_name', 'OrderStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcsImage', 'proc_name', 'OrderStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcsImage', 'proc_cat_name', 'proc_name'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcsImage', 'display_name', 'LabStatus'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'OrderProcsImage', 'proc_name', 'LabStatus'));
  -- problem list
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ProblemList', 'departmentid', 'diagname'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ProblemList', 'diagname', 'departmentid'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ProblemList', 'departmentid', 'code'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ProblemList', 'code', 'departmentid'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ProblemList', 'departmentid', 'codecategory'));
  queries = array_append(queries, format('select * from run_clarity_stats(%L, %L, %L, %L)',
    _clarity_workspace, 'ProblemList', 'codecategory', 'departmentid'));
  perform distribute(server, queries, nprocs);
end
$$ language plpgsql;

create or replace function run_clarity_stats(_clarity_workspace text, _clarity_staging_table text, key text, value text)
returns void as
$$
begin
execute format(
'with kv_cnt as(
  select %I k, %I v, count(*) cnt from %I.%I
  where not isnumeric(%I) and %I is not null
  group by %I, %I
),
kv_cnt_top100 as (
  select k, v, cnt from (
    select *, ROW_NUMBER() OVER (PARTITION BY k order by cnt desc) as row_id
    from kv_cnt
  ) as A
  where row_id <= 100 order by k
),
kv_cnt_jsonb as (
  select k, jsonb_object_agg(v, cnt) str_cnt
  from kv_cnt_top100 group by k
)
insert into clarity_stats
  select M.id, %L || '' <-> '' || %L id_type, M.clarity_workspace, M.clarity_staging_table,
    M.stats || coalesce(jsonb_build_object(''distinct_str_cnt'', kv.str_cnt), ''{}''::jsonb) from
  (select %I id,''%I''::text clarity_workspace,
    ''%I''::text clarity_staging_table,
    jsonb_build_object(
      ''cnt'', count(*),
      ''cnt_numeric'', count(*) filter (where isnumeric(%I)),
      ''cnt_str'', count(*) filter (where not isnumeric(%I)),
      ''cnt_distinct_str'', count(distinct %I) filter (where not isnumeric(%I)),
      ''min'', min(%I::numeric) filter (where isnumeric(%I)),
      ''max'', max(%I::numeric) filter (where isnumeric(%I)),
      ''mean'', avg(%I::numeric) filter (where isnumeric(%I)),
      ''5%%'' , percentile_disc(0.05) within group (order by %I::numeric)
                        filter (where isnumeric(%I)),
      ''25%%'' , percentile_disc(0.25) within group (order by %I::numeric)
                        filter (where isnumeric(%I)),
      ''50%%'' , percentile_disc(0.50) within group (order by %I::numeric)
                        filter (where isnumeric(%I)),
      ''75%%'' , percentile_disc(0.75) within group (order by %I::numeric)
                        filter (where isnumeric(%I)),
      ''95%%'' , percentile_disc(0.95) within group (order by %I::numeric)
                        filter (where isnumeric(%I))
    ) stats
  from %I.%I where %I is not null group by %I) M left join kv_cnt_jsonb kv on M.id = kv.k
on conflict(id, id_type, clarity_workspace, clarity_staging_table) do update
set stats = excluded.stats
', key, value, _clarity_workspace, _clarity_staging_table, value, key, key, value,
   key, value, key, _clarity_workspace, _clarity_staging_table, value, value, value, value, value, value, value, value, value,
   value, value, value, value, value, value, value, value, value, value,
   value, _clarity_workspace, _clarity_staging_table, key, key);
end
$$ language plpgsql;