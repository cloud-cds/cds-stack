drop table if exists flowsheet_dict;
create table flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text
 );
\copy flowsheet_dict from '~/clarity-dw/flowsheet_dict.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists lab_dict;
create table lab_dict
(
 component_id text,
 name text,
 external_name text
 );
\copy lab_dict from '~/clarity-dw/lab_dict.rpt' with csv header delimiter as E'\t' NULL 'NULL';

drop table if exists lab_proc_dict;
create table lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy lab_proc_dict from '~/clarity-dw/lab_proc.rpt' with csv header delimiter as E'\t' NULL 'NULL';