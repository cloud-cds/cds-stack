-----------------------------------------
-- Notes processing on Clarity tables.
------------------------------------------

-----------------------------------------
-- Candidate search via positive version

drop function if exists match_clarity_infection_positives(integer, integer);

create or replace function match_clarity_infection_positives(num_matches integer, match_offset integer)
 RETURNS table(csn_id text, start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;
  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;
  match_query :=
     'select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" at time zone ''UTC'' as start_ts,'
  || '       regexp_replace(array_to_string(array_agg("NOTE_TEXT"), ''\n''), E''' || grouped_positive || ''', E''##**\\1**##'', ''g'') as note'
  || ' from "Notes"'
  || ' where "NOTE_TEXT" ~ E''' || positive || ''''
  || ' and "AuthorType" <> ''Pharmacist'''
  || ' group by "CSN_ID", "CREATE_INSTANT_DTTM"'
  || ' order by "CSN_ID", "CREATE_INSTANT_DTTM" limit ' || num_matches::text || ' offset ' || match_offset::text;
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


----------------------------------------
-- Infection matching

--- TODO: Add suffixes
--- TODO: Handle groups of items
---       e.g., negated lists e.g., "no UTI or pneumonia" currently matches pneumonia
---       e.g., PAST MEDICAL HISTORY followed by list of items
--- Does attribution matter? I.e., should something mentioned in nurse's note trigger critieria
---  or restrict this to physician notes?

drop function if exists match_clarity_infections(text,integer,integer);

create or replace function match_clarity_infections(this_csn_id text, rows_before integer, rows_after integer)
 RETURNS table(csn_id text, start_ts timestamptz, ngram text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;
  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;
  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     ' select csn_id, start_ts at time zone ''UTC'', array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.csn_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select csn_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''##**\\1**##'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" as start_ts,'
  || '              regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from "Notes"'
  || '       where "CSN_ID" = coalesce(E''' || this_csn_id || ''', "CSN_ID")'
  || '       and "AuthorType" <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%##**%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%##**%'' ) > 0 )'
  || ' order by csn_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------
-- Negation removal

drop function if exists match_clarity_infection_negatives(text);

create or replace function match_clarity_infection_negatives(this_csn_id text)
 RETURNS table(csn_id text, start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
    'select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" at time zone ''UTC'' as start_ts,'
  || '       regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || ' from "Notes"'
  || ' where "CSN_ID" = coalesce(E''' || this_csn_id || ''', "CSN_ID")'
  || ' and "AuthorType" <> ''Pharmacist'''
  || ' order by "CREATE_INSTANT_DTTM"';
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------------------
-- Batch version

drop function if exists match_clarity_infections_multi(text[],integer,integer);

create or replace function match_clarity_infections_multi(csn_ids text[], rows_before integer, rows_after integer)
 RETURNS table(csn_id integer, start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN

  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;

  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;

  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;

  create temporary table match_csns as
    select * from unnest(csn_ids) E(match_csn_id);

  match_query :=
     ' select csn_id, start_ts at time zone ''UTC'', array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.csn_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select csn_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''##**\\1**##'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" as start_ts,'
  || '              regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'') as body'
  || '       from "Notes"'
  || '       where "CSN_ID" in (select * from match_csns)'
  || '       and "AuthorType" <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '     and "AuthorType" <> ''Pharmacist'''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%##**%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%##**%'' ) > 0 )'
  || ' order by csn_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
  drop table if exists match_csns;
  return;
END; $function$;



--------------------------------------------------------
-- Notes processing on cdm tables.
--------------------------------------------------------

--
-- Helper functions to abstract over Clarity and Mulesoft data representations.

create or replace function note_date(dates json) returns timestamptz
  language plpgsql
as $func$ begin
  return
    case json_typeof(dates)
      when 'array' then (select min(dt->>'Date') from json_array_elements(dates) D(dt) where dt->>'DateType' = 'NoteDate')::timestamp at time zone 'UTC'
      when 'object' then (dates->>'create_instant_dttm')::timestamp at time zone 'UTC'
      else null
    end;
end; $func$;

create or replace function note_provider_type(providers json) returns text
  language plpgsql
as $func$ begin
  return
    case json_typeof(providers)
      when 'array' then providers->0->>'ProviderType'
      when 'object' then providers->>'AuthorType'
      else null
    end;
end; $func$;

-----------------------------------------
-- Candidate search via positive version

drop function if exists match_cdm_infection_positives(integer, integer, integer);

create or replace function match_cdm_infection_positives(this_dataset_id integer, num_matches integer, match_offset integer)
 RETURNS table(pat_id varchar(50), start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;
  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;
  match_query :=
     'select pat_id, note_date(dates) as start_ts,'
  || '       regexp_replace(array_to_string(array_agg(note_body), E''\n''), E''' || grouped_positive || ''', E''##**\\1**##'', ''g'') as note'
  || ' from cdm_notes'
  || ' where dataset_id = coalesce(' || this_dataset_id::text || '::integer, dataset_id)'
  || ' and note_body ~ E''' || positive || ''''
  || ' and note_provider_type(providers) <> ''Pharmacist'''
  || ' group by pat_id, note_date(dates)'
  || ' order by pat_id, note_date(dates) limit ' || num_matches::text || ' offset ' || match_offset::text;
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


----------------------------------------
-- Infection matching

--- TODO: Add suffixes
--- TODO: Handle groups of items
---       e.g., negated lists e.g., "no UTI or pneumonia" currently matches pneumonia
---       e.g., PAST MEDICAL HISTORY followed by list of items
--- Does attribution matter? I.e., should something mentioned in nurse's note trigger critieria
---  or restrict this to physician notes?

drop function if exists match_cdm_infections(text,integer,integer,integer);

create or replace function match_cdm_infections(this_pat_id text, this_dataset_id integer, rows_before integer, rows_after integer)
 RETURNS table(pat_id varchar(50), start_ts timestamptz, ngram text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;
  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;
  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     ' select pat_id, start_ts, array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.pat_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select pat_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''##**\\1**##'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select pat_id, note_date(dates) as start_ts,'
  || '              regexp_replace(note_body, E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from cdm_notes'
  || '       where pat_id = coalesce(' || coalesce('E''' || this_pat_id || '''', 'null::text') || ', pat_id)'
  || '       and dataset_id = coalesce(' || this_dataset_id::text || '::integer, dataset_id)'
  || '       and note_provider_type(providers) <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%##**%'') '
  || ' or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text
  ||       ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%##**%'' ) > 0 )'
  || ' order by pat_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------
-- Negation removal

drop function if exists match_cdm_infection_negatives(text, integer);

create or replace function match_cdm_infection_negatives(this_pat_id text, this_dataset_id integer)
 RETURNS table(pat_id varchar(50), start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     'select pat_id, note_date(dates) as start_ts,'
  || '       regexp_replace(note_body, E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || ' from cdm_notes'
  || ' where pat_id = coalesce(' || coalesce('E''' || this_pat_id || '''', 'null::text') || ', pat_id)'
  || ' and dataset_id = coalesce(' || this_dataset_id::text || '::integer, dataset_id)'
  || ' and note_provider_type(providers) <> ''Pharmacist'''
  || ' order by note_date(dates)';
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------------------
-- Batch version

drop function if exists match_cdm_infections_multi(text[],integer,integer,integer);

create or replace function match_cdm_infections_multi(pat_ids text[], this_dataset_id integer, rows_before integer, rows_after integer)
 RETURNS table(pat_id varchar(50), start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN

  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;

  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;

  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;

  create temporary table match_pats as
    select * from unnest(pats_ids) E(match_pat_id);

  match_query :=
     ' select pat_id, start_ts, array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.pat_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select pat_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''##**\\1**##'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select pat_id, note_date(dates) as start_ts,'
  || '              regexp_replace(note_body, E''' || negative || ''', ''NEGATED_PHRASE'') as body'
  || '       from cdm_notes'
  || '       where pat_id in (select * from match_pats)'
  || '       and dataset_id = coalesce(' || this_dataset_id::text || '::integer, dataset_id)'
  || '       and note_provider_type(providers) <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%##**%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%##**%'' ) > 0 )'
  || ' order by pat_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
  drop table if exists match_csns;
  return;
END; $function$;


-----------------------------------------
-- Windowed matching.

drop function if exists match_cdm_infections_from_candidates(text,integer,text,integer,integer);

create or replace function match_cdm_infections_from_candidates(
                              this_pat_id     text,
                              this_dataset_id integer,
                              candidate_table text,
                              rows_before     integer,
                              rows_after      integer
                            )
  RETURNS table(
    dataset_id      integer,
    pat_id          varchar(50),
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    start_ts        timestamptz,
    ngram           text
  )
  LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  positive text := '';
  grouped_positive text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(keyword), '|') into positive
  from infection_keywords;
  select '(' || array_to_string(array_agg(keyword), '|') || ')' into grouped_positive
  from infection_keywords;
  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     ' select $2 as dataset_id, NGRAMS.pat_id, NGRAMS.note_id, NGRAMS.note_type, NGRAMS.note_status, NGRAMS.start_ts, array_to_string(NGRAMS.ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.pat_id, DOCS.note_id, DOCS.note_type, DOCS.note_status, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select NEG.pat_id, NEG.note_id, NEG.note_type, NEG.note_status, NEG.start_ts, '
  || '           regexp_split_to_array(regexp_replace(NEG.body, ''' || grouped_positive || ''', E''##**\\1**##'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select N.pat_id, N.note_id, N.note_type, N.note_status, note_date(N.dates) as start_ts,'
  || '              regexp_replace(N.note_body, E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from cdm_notes N'
  || '       inner join ' || candidate_table || ' NC on N.note_id = NC.note_id'
  || '       where N.pat_id = coalesce($1, N.pat_id)'
  || '       and N.dataset_id = coalesce($2, N.dataset_id)'
  || '       and note_provider_type(N.providers) <> ''Pharmacist'''
  || '     ) NEG'
  || '     where NEG.body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%##**%'') '
  || ' or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text
  ||       ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%##**%'' ) > 0 )'
  || ' order by pat_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query using this_pat_id, this_dataset_id;
END; $function$;
