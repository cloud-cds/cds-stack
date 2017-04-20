----------------------------------------
-- Clarity Notes Processing
----------------------------------------

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
  select array_to_string(array_agg(N.keyword || E'\\\\s+' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     ' select csn_id, start_ts at time zone ''EST'', array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.csn_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select csn_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" as start_ts,'
  || '              regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from "Notes"'
  || '       where "CSN_ID" = coalesce(E''' || this_csn_id || ''', "CSN_ID")'
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )';
  raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------
-- Negative version
-------------------------

drop function if exists match_clarity_infection_negatives(text);

create or replace function match_clarity_infection_negatives(this_csn_id text)
 RETURNS table(csn_id text, start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(N.keyword || E'\\\\s+' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     'select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" at time zone ''EST'' as start_ts,'
  || '       regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || ' from "Notes"'
  || '  where "CSN_ID" = coalesce(E''' || this_csn_id || ''', "CSN_ID") order by "CREATE_INSTANT_DTTM"';
  raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;

-------------------------------------
-- Batch version
-------------------------------------
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

  select array_to_string(array_agg(N.keyword || E'\\\\s+' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;

  create temporary table match_csns as
    select * from unnest(csn_ids) E(match_csn_id);

  match_query :=
     ' select csn_id, start_ts at time zone ''EST'', array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.csn_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select csn_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" as start_ts,'
  || '              regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'') as body'
  || '       from "Notes"'
  || '       where "CSN_ID" in (select * from match_csns)'
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )';
  raise notice 'query %', match_query;
  return query execute match_query;
  drop table if exists match_csns;
  return;
END; $function$;


----------------------------------------
-- CDM Notes Processing
----------------------------------------

drop function if exists match_infections(character varying(50),integer,integer);

create or replace function match_infections(this_enc_id character varying(50), rows_before integer, rows_after integer)
 RETURNS table(enc_id character varying(50), t timestamptz, note text)
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
  select array_to_string(array_agg(N.keyword || E'\\\\s+' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     ' select enc_id, spec_note_time, array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.enc_id, DOCS.spec_note_time, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select enc_id, spec_note_time, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select enc_id, spec_note_time::text::timestamptz,'
  || '              regexp_replace(note_text, E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from ('
  || '          select pat_id as enc_id, json_array_elements(dates)::json->''Date'' as spec_note_time, json_array_elements(dates)::json->''DateType'' as date_type, '
  || '          note_body as note_text from cdm_notes ) notes '
  || '       where enc_id = '''|| this_enc_id || ''' and date_type::text~''NoteDate'' '
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )';
  raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;
