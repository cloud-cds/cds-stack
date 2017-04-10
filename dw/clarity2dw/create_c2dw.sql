-- create clarity2dw dataset
-- raise notice '';

INSERT INTO dw_version (dataset_id, created, description)
VALUES (1,
        now(),
        'clarity2dw')
on conflict do NOTHING ;

INSERT INTO model_version (model_id, created, description)
VALUES (1,
        now(),
        'clarity2dw default model')
on conflict do NOTHING;

-- ======================================
-- Upsert CDM_function
-- ======================================
DROP TABLE IF EXISTS cdm_function_temp;
CREATE TABLE cdm_function_temp(
    dataset_id      integer REFERENCES dw_version(dataset_id),
    func_id         varchar(50) NOT NULL,
    func_type       varchar(20) NOT NULL,
    description     text        NOT NULL,
    PRIMARY KEY     (dataset_id, func_id),
    CHECK (func_type SIMILAR TO 'transform|fillin|derive')
);
\COPY cdm_function_temp FROM 'CDM_Function.csv' WITH csv header DELIMITER AS ',';

insert into cdm_function (dataset_id, func_id, func_type, description)
select dataset_id, func_id,func_type, description from cdm_function_temp
on conflict (dataset_id, func_id)
do update set func_type = EXCLUDED.func_type, description = EXCLUDED.description;
DROP TABLE IF EXISTS cdm_function_temp;

-- ======================================
-- Upsert CDM feature
-- ======================================
DROP TABLE IF EXISTS cdm_feature_temp;
CREATE TABLE cdm_feature_temp (
    dataset_id              integer,
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
    PRIMARY KEY             (dataset_id, fid),
    FOREIGN KEY             (dataset_id, fillin_func_id) REFERENCES cdm_function(dataset_id, func_id),
    FOREIGN KEY             (dataset_id, derive_func_id) REFERENCES cdm_function(dataset_id, func_id),
    CHECK (category SIMILAR TO 'S|M|T|TWF|G')
);

\COPY cdm_feature_temp FROM 'CDM_Feature.csv' WITH csv header DELIMITER AS ',';

insert into cdm_feature (dataset_id, fid, category, data_type, is_measured, is_deprecated,
                         fillin_func_id, window_size_in_hours, derive_func_id,
                         derive_func_input, description, version, unit)

select dataset_id, fid, category, data_type, is_measured, is_deprecated,
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
-- Upsert CDM_g
-- ======================================
DELETE
FROM cdm_g
WHERE dataset_id = 1 and model_id = 1;



 \COPY cdm_g FROM 'CDM_G.csv' WITH csv header DELIMITER AS ',';

drop table if exists flowsheet_dict;
create table flowsheet_dict
(
 FLO_MEAS_ID text,
 FLO_MEAS_NAME text
 );
\copy flowsheet_dict from '/home/ubuntu/clarity-dw/flowsheet_dict.rpt' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists lab_dict;
create table lab_dict
(
 component_id text,
 name text,
 base_name text,
 external_name text
 );
\copy lab_dict from '/home/ubuntu/clarity-dw/lab_dict.rpt' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';

drop table if exists lab_proc_dict;
create table lab_proc_dict
(
 proc_id text,
 proc_name text,
 proc_code text
 );
\copy lab_proc_dict from '/home/ubuntu/clarity-dw/lab_proc.rpt' with csv header delimiter as E'\t' NULL 'NULL' QUOTE E'\b';