-- create clarity2dw dataset
-- raise notice '';

INSERT INTO dw_version (dataset_id, created, description)
VALUES (13,
        now(),
        'BMC one year')
on conflict do NOTHING ;

-- ======================================
-- Upsert CDM_function
-- ======================================
DROP TABLE IF EXISTS cdm_function_temp;
CREATE TABLE cdm_function_temp(
    func_id         varchar(50) NOT NULL,
    func_type       varchar(20) NOT NULL,
    description     text        NOT NULL,
    PRIMARY KEY     (func_id),
    CHECK (func_type SIMILAR TO 'transform|fillin|derive')
);
\COPY cdm_function_temp FROM '../../CDM_Function.csv' WITH csv header DELIMITER AS ',';

insert into cdm_function (dataset_id, func_id, func_type, description)
select 13, func_id,func_type, description from cdm_function_temp
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
    unit                    varchar(50),
    PRIMARY KEY             (fid)
);

\COPY cdm_feature_temp FROM '../../CDM_Feature.csv' WITH csv header DELIMITER AS ',';

insert into cdm_feature (dataset_id, fid, category, data_type, is_measured, is_deprecated,
                         fillin_func_id, window_size_in_hours, derive_func_id,
                         derive_func_input, description, version, unit)

select 13, fid, category, data_type, is_measured, is_deprecated,
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
WHERE dataset_id = 13;

DROP TABLE IF EXISTS parameters_temp;
CREATE TABLE parameters_temp
(
    name        text,
    value       text not null
);
 \COPY parameters_temp FROM '../../parameters.csv' WITH csv header DELIMITER AS ',';
insert into parameters
  select 13, name, value
  from parameters_temp
on conflict(dataset_id, name) do update set
  name = excluded.name, value = excluded.value;
DROP TABLE IF EXISTS parameters_temp;
-- ======================================
-- Upsert CDM_g
-- ======================================
DELETE
FROM cdm_g
WHERE dataset_id = 13;



 \COPY cdm_g FROM 'CDM_G.csv' WITH csv header DELIMITER AS ',';

-- ======================================
-- Upsert criteria_default
-- ======================================
DELETE
FROM criteria_default
WHERE dataset_id = 13;


DROP TABLE IF EXISTS criteria_default_temp;
CREATE TABLE criteria_default_temp
(
    name                varchar(50),
    fid                 varchar(50),
    override_value      json,
    category            varchar(50)
);

 \COPY criteria_default_temp FROM '../../criteria_default.csv' WITH csv header DELIMITER AS ',';

delete from criteria_default where dataset_id = 13;
insert into criteria_default
  select 13, name, fid, override_value, category
  from criteria_default_temp
on conflict(dataset_id, name, fid, category) do nothing;
DROP TABLE IF EXISTS criteria_default_temp;