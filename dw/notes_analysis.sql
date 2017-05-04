drop table if exists infection_keywords;
create table infection_keywords(keyword text);
insert into infection_keywords (keyword) values
('(VRE |vre )*([^a-zA-Z]UTI[^a-zA-Z]|[^a-zA-Z]uti[^a-zA-Z])'),
('[uU]rinary [tT]ract [iI]nfection'),
(' CAUTI '),
('[uU]rosepsis'),
('[pP]yelo(nephritis)*'),
('[bB]iliary [sS]epsis'),
('[cC]holecystitis'),
('[cC]ystitis'),
('[aA]ppendicitis'),
('[cC]holangitis'),
('[cC]holelithiasis'),
('[hH]epatic [aA]bscess'),
('[iI]ntra [aA]bdominal [sS]epsis'),
('[pP]neumoperitoneum'),
('[pP]neumotosis'),
('[pP]eritonitis'),
('[sS]pontaneous [bB]acerial [pP]eritonitis'),
('(MRSA )*[cC](lostridium)*[. ]*[dD]if[f]*(icile)*( colitis)*'),
('[cC]olitis'),
('([vV]ent associated|[hH]ospital[- ]*[aA]cquired |([rR]ight|[lL]eft) [lL]ower [lL]obe|RLL|MRSA |[aA]spiration |[pP]ost[- ]obst(ructive)* )*[pP]neumonia'),
('[^a-zA-Z]PNA[^a-zA-Z]'),
(' HCAP '),
(' CAP '),
('[eE]mpyema'),
('[lL]ine [sS]epsis'),
('[eE]ndocarditis'),
('[bB]lood [sS]tream [iI]nfection'),
('[rRlL]*(ight)*(eft)*[ ]*[lL]*(ower)*[ ]*[eE]*(xtremity)*[ ]*[cC]ellulitis'),
('[mM]ediastinitis'),
('[cC]entral [lL]ine [iI]nfection'),
('CLABSI'),
('[mM]yocarditis'),
('[oO]steomyelitis'),
('[nN]ecrotizing [fF]asciitis'),
('[pP]seudomonas'),
('[iI]nfluenza'),
('([sS]ubcutaneous )*([iI]ntraperitoneal )*[aA]bscess')
;

drop table if exists negation_keywords;
create table negation_keywords(keyword text);
insert into negation_keywords (keyword) values
('[hH](istory)*(x)* of( recurrent|recent)*'),
('[hH]/[oO]'),
('[Nn]o( sign)*( evidence)*( diagnosis)*( of)*( acute)*'),
('[nN]ot'),
('[Nn]eg(ative)*'),
('[rR]ecently had'),
('[dD]enies'),
('[dD]enies any'),
('[rR]ecently hospitalized for'),
('[rR]ecurrent'),
('[aA]bsence of'),
('[hH]ave had'),
('•[ ]*')
;


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
  || '       regexp_replace(array_to_string(array_agg("NOTE_TEXT"), ''\n''), E''' || grouped_positive || ''', E''__MATCH__\\1'', ''g'') as note'
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
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select "CSN_ID" as csn_id, "CREATE_INSTANT_DTTM" as start_ts,'
  || '              regexp_replace("NOTE_TEXT", E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from "Notes"'
  || '       where "CSN_ID" = coalesce(E''' || this_csn_id || ''', "CSN_ID")'
  || '       and "AuthorType" <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '     and "AuthorType" <> ''Pharmacist'''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )'
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
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
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
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )'
  || ' order by csn_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
  drop table if exists match_csns;
  return;
END; $function$;



--------------------------------------------------------
-- Notes processing on cdm tables, for Clarity datasets.
--------------------------------------------------------

-----------------------------------------
-- Candidate search via positive version

drop function if exists match_cdm_infection_positives(integer, integer);

create or replace function match_cdm_infection_positives(num_matches integer, match_offset integer)
 RETURNS table(pat_id text, start_ts timestamptz, note text)
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
     'select pat_id, (dates->>''create_instant_dttm'')::timestamp at time zone ''UTC'' as start_ts,'
  || '       regexp_replace(array_to_string(array_agg(note_body), E''\n''), E''' || grouped_positive || ''', E''__MATCH__\\1'', ''g'') as note'
  || ' from cdm_notes'
  || ' where note_body ~ E''' || positive || ''''
  || ' and providers->>''AuthorType'' <> ''Pharmacist'''
  || ' group by pat_id, dates->>''create_instant_dttm'''
  || ' order by pat_id, dates->>''create_instant_dttm'' limit ' || num_matches::text || ' offset ' || match_offset::text;
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

drop function if exists match_cdm_infections(text,integer,integer);

create or replace function match_cdm_infections(this_pat_id text, rows_before integer, rows_after integer)
 RETURNS table(pat_id text, start_ts timestamptz, ngram text)
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
     ' select pat_id, start_ts at time zone ''UTC'', array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.pat_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select pat_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select pat_id, dates->>''create_instant_dttm'' as start_ts,'
  || '              regexp_replace(note_body, E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || '       from cdm_notes'
  || '       where pat_id = coalesce(E''' || this_pat_id || ''', pat_id)'
  || '       and providers->>''AuthorType'' <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )'
  || ' order by pat_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------
-- Negation removal

drop function if exists match_cdm_infection_negatives(text);

create or replace function match_cdm_infection_negatives(this_pat_id text)
 RETURNS table(pat_id text, start_ts timestamptz, note text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  negative text := '';
  match_query text := '';
BEGIN
  select array_to_string(array_agg(N.keyword || E'\\\\s*' || I.keyword), '|') into negative
  from infection_keywords I, negation_keywords N;
  match_query :=
     'select pat_id, (dates->>''create_instant_dttm'')::timestamp at time zone ''UTC'' as start_ts,'
  || '       regexp_replace(note_body, E''' || negative || ''', ''NEGATED_PHRASE'', ''g'') as body'
  || ' from cdm_notes'
  || ' where pat_id = coalesce(E''' || this_pat_id || ''', pat_id)'
  || ' and providers->>''AuthorType'' <> ''Pharmacist'''
  || ' order by dates->>''create_instant_dttm''';
  --raise notice 'query %', match_query;
  return query execute match_query;
END; $function$;


-------------------------------------
-- Batch version

drop function if exists match_cdm_infections_multi(text[],integer,integer);

create or replace function match_cdm_infections_multi(pat_ids text[], rows_before integer, rows_after integer)
 RETURNS table(pat_id integer, start_ts timestamptz, note text)
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
     ' select pat_id, start_ts at time zone ''UTC'', array_to_string(ngram_arr, '' '') as ngram  '
  || ' from ('
  || '   select DOCS.pat_id, DOCS.start_ts, '
  || '          array_agg(W.word) over ( ROWS BETWEEN ' || rows_before::text || ' PRECEDING AND ' || rows_after::text || ' FOLLOWING ) as ngram_arr'
  || '   from ('
  || '     select pat_id, start_ts, '
  || '           regexp_split_to_array(regexp_replace(body, ''' || grouped_positive || ''', E''__MATCH__\\1'', ''g''), E''\\s+'') as words'
  || '     from ('
  || '       select pat_id, dates->>''create_instant_dttm'' as start_ts,'
  || '              regexp_replace(note_body, E''' || negative || ''', ''NEGATED_PHRASE'') as body'
  || '       from cdm_notes'
  || '       where pat_id in (select * from match_pats)'
  || '       and providers->>''AuthorType'' <> ''Pharmacist'''
  || '     ) NEG'
  || '     where body ~ ''' || positive || ''''
  || '   ) DOCS, lateral unnest(words) W(word)'
  || ' ) NGRAMS'
  || ' where ( ngram_arr[4] like ''%__MATCH__%'') or ( array_length(ngram_arr, 1) < ' || (rows_before+rows_after+1)::text || ' and (select count(*) from unnest(ngram_arr) W(word) where word like ''%__MATCH__%'' ) > 0 )'
  || ' order by pat_id, start_ts';
  --raise notice 'query %', match_query;
  return query execute match_query;
  drop table if exists match_csns;
  return;
END; $function$;
