-- create clarity2dw dataset
-- raise notice '';

INSERT INTO dw_version (dataset_id, created, description)
VALUES (15,
        now(),
        'Andong''s test')
on conflict do NOTHING ;

-- ======================================
-- Upsert CDM_function
-- ======================================
DROP TABLE IF EXISTS cdm_function_temp;
CREATE TABLE cdm_function_temp(
    func_id         varchar(50) NOT NULL,
    func_type       varchar(20) NOT NULL,
    description     text        NOT NULL,
    CHECK (func_type SIMILAR TO 'transform|fillin|derive')
);
\COPY cdm_function_temp FROM 'CDM_Function.csv' WITH csv header DELIMITER AS ',';

insert into cdm_function (dataset_id, func_id, func_type, description)
select 15, func_id,func_type, description from cdm_function_temp
on conflict (dataset_id, func_id)
do update set func_type = EXCLUDED.func_type, description = EXCLUDED.description;
DROP TABLE IF EXISTS cdm_function_temp;

-- ======================================
-- Upsert CDM feature
-- ======================================
DROP TABLE IF EXISTS cdm_feature_temp;
CREATE TABLE cdm_feature_temp (
    fid                     varchar(50) NOT NULL,
    category                varchar(50) NOT NULL,
    data_type               varchar(20) NOT NULL,
    is_measured             boolean NOT NULL,
    is_deprecated           boolean NOT NULL,
    fillin_func_id          varchar(50),
    window_size_in_hours    varchar(100),
    derive_func_id          varchar(50),
    derive_func_input       text,
    description             text,
    version                 varchar(50),
    unit                    varchar(50)
);

\COPY cdm_feature_temp FROM 'CDM_Feature.csv' WITH csv header DELIMITER AS ',';

insert into cdm_feature (dataset_id, fid, category, data_type, is_measured, is_deprecated,
                         fillin_func_id, window_size_in_hours, derive_func_id,
                         derive_func_input, description, version, unit)

select 15, fid, category, data_type, is_measured, is_deprecated,
                         fillin_func_id, window_size_in_hours, derive_func_id,
                         derive_func_input, description, version, unit
from cdm_feature_temp
on conflict (dataset_id, fid)
do update set category = EXCLUDED.category, data_type = EXCLUDED.data_type, is_measured=EXCLUDED.is_measured,
  is_deprecated=EXCLUDED.is_deprecated,
  fillin_func_id=EXCLUDED.fillin_func_id, window_size_in_hours=EXCLUDED.window_size_in_hours, derive_func_id=EXCLUDED.derive_func_id,
  derive_func_input=EXCLUDED.derive_func_input, description=EXCLUDED.description, version=EXCLUDED.version, unit=EXCLUDED.unit;
DROP TABLE IF EXISTS cdm_feature_temp;



-- ======================================
-- Upsert parameters
-- ======================================
DELETE
FROM parameters
WHERE dataset_id = 15;

CREATE TEMP TABLE parameters_temp (
  name text,
  value text
);
 \COPY parameters_temp FROM 'parameters.csv' WITH csv header DELIMITER AS ',';

insert into parameters (dataset_id, name, value)
  select 15, name, value from parameters_temp
  on conflict (dataset_id, name)
  do update set value = excluded.value;

CREATE TEMP TABLE criteria_default_temp (
  name  varchar(50),
  fid   varchar(50),
  override_value json,
  category  varchar(50)
);

-- ======================================
-- Upsert criteria_default
-- ======================================
DELETE
FROM criteria_default
WHERE dataset_id = 15;

 \COPY criteria_default_temp FROM 'criteria_default.csv' WITH csv header DELIMITER AS ',';

insert into criteria_default (dataset_id, name, fid, override_value, category)
  select 15, name, fid, override_value, category from criteria_default_temp
  on conflict (dataset_id, name, fid, category)
  do update set override_value = excluded.override_value;
-- ======================================
-- Upsert CDM_g
-- ======================================
DELETE
FROM cdm_g
WHERE dataset_id = 15;

CREATE TEMP TABLE cdm_g_temp (
  fid  varchar(50),
  value   text,
  confidence int
);

 \COPY cdm_g_temp FROM 'CDM_G.csv' WITH csv header DELIMITER AS ',';

insert into cdm_g (dataset_id, fid, value, confidence)
  select 15, fid, value, confidence from cdm_g_temp
  on conflict(dataset_id, fid)
  do update set value = excluded.value, confidence = excluded.confidence;

-- drop table if exists flowsheet_dict;
-- create table flowsheet_dict
-- (
--  FLO_MEAS_ID text,
--  FLO_MEAS_NAME text
--  );
-- \copy flowsheet_dict from '/home/ubuntu/clarity-dw/flowsheet_dict.rpt' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

-- drop table if exists lab_dict;
-- create table lab_dict
-- (
--  component_id text,
--  name text,
--  base_name text,
--  external_name text
--  );
-- \copy lab_dict from '/home/ubuntu/clarity-dw/lab_dict.rpt' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

-- drop table if exists lab_proc_dict;
-- create table lab_proc_dict
-- (
--  proc_id text,
--  proc_name text,
--  proc_code text
--  );
-- \copy lab_proc_dict from '/home/ubuntu/clarity-dw/lab_proc.rpt' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';
