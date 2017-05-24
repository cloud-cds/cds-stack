------------------------------------------
-- Specific note types to join against

drop table if exists narrative_note_types;
create table narrative_note_types(note_type varchar(50));
insert into narrative_note_types (note_type) values
('H&P'),
('CDI/CDE Progress Note'),
('Critical Care Progress Note'),
('ED Progress Note'),
('Observation Progress Note'),
('Progress Notes'),
('ED Provider Note'),
('Discharge Summaries')--,
--('Discharge as Deceased Summary'),
--('Discharge Instr - Meds'),
--('Discharge Instr - Lab'),
--('Discharge Instr - Appointments')
;

------------------------------------------
-- Get the text portion of the clinical
--  narrative, restricted by note type.

drop function if exists get_restricted_narrative(text, text, integer);

create or replace function get_restricted_narrative(this_pat_id text, this_note_status text, this_dataset_id integer)
  RETURNS table(
    pat_id      varchar(50),
    note_id     varchar(50),
    note_type   varchar(50),
    note_status varchar(50),
    note_body   text,
    create_time timestamp with time zone,
    providers   json)
  LANGUAGE plpgsql
AS $function$
BEGIN
  return query
  select notes.pat_id, notes.note_id, notes.note_type, notes.note_status, notes.note_body, to_timestamp(notes.dates ->> 'create_instant_dttm', 'YYYY-MM-DD HH24:MI:SS') as create_time, notes.providers
    from (cdm_notes inner join narrative_note_types using (note_type)) as notes
  where notes.pat_id = coalesce(this_pat_id, notes.pat_id)
    and notes.note_status = coalesce(this_note_status, notes.note_status)
    and notes.dataset_id = coalesce(this_dataset_id::integer, notes.dataset_id);
END; $function$
