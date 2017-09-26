-----------------------------------------
-- Export helpers for the RDS DW.
--

-- Max note size found in 1yr datasets: ~140K
create or replace function chunk_cdm_notes(_dataset_id integer, notes_workspace text)
returns table(dataset_id        integer,
              pat_id            varchar(50),
              note_id           text,
              author_type       text,
              note_type         text,
              contact_date_real numeric,
              note_status       text,
              dates             json,
              note_body1        text,
              note_body2        text,
              note_body3        text)
as $func$
begin
  return query execute format(
    'select dataset_id,
           pat_id,
           note_id,
           author_type,
           note_type,
           contact_date_real,
           note_status,
           dates,
           convert_from(substring(note_body for 65532), ''UTF-8'') as note_body1,
           convert_from(case when octet_length(note_body) > 65532 then substring(note_body from 65532+1 for 65532) else convert_to('''', ''UTF-8'') end, ''UTF-8'') as note_body2,
           convert_from(case when octet_length(note_body) > 2*65532 then substring(note_body from 2*65532+1 for 65532) else convert_to('''', ''UTF-8'') end, ''UTF-8'') as note_body3
    from (
      select
          PE.dataset_id                                                         as dataset_id,
          PE.pat_id                                                             as pat_id,
          "NOTE_ID"                                                             as note_id,
          "AuthorType"                                                          as author_type,
          "NoteType"                                                            as note_type,
          "CONTACT_DATE_REAL"::numeric                                          as contact_date_real,
          coalesce("NoteStatus", ''unknown'')                                   as note_status,
          convert_to(string_agg("NOTE_TEXT", E''\n'' order by line), ''UTF-8'') as note_body,

          json_build_object(
            ''create_instant_dttm'', "CREATE_INSTANT_DTTM"::timestamptz(0),
            ''spec_note_time_dttm'', "SPEC_NOTE_TIME_DTTM"::timestamptz(0),
            ''entry_instant_dttm'', "ENTRY_ISTANT_DTTM"::timestamptz(0)
          ) as dates

      from %s."Notes" N
      inner join pat_enc PE
      on N."CSN_ID" = PE.visit_id and PE.dataset_id = coalesce(%s, PE.dataset_id)
      group by
        PE.dataset_id, PE.pat_id,
        "NOTE_ID", "AuthorType", "NoteType", "NoteStatus",
        "CREATE_INSTANT_DTTM", "CONTACT_DATE_REAL"::numeric, "SPEC_NOTE_TIME_DTTM", "ENTRY_ISTANT_DTTM"
    ) full_notes;'
    , notes_workspace, _dataset_id
  );
end $func$ LANGUAGE plpgsql;

