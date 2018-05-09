/*
 * create_dbschema.sql
 * create relation database for the dashan instance
 * NOTE: cdm_twf is not created here
 */

-------------------------------------------------
-- DW Version Information
-- dataset_id: this captures the schema version and etl (transform/fillin/derive)
-- version used to populate the DW.
-- Versions allow us to maintain multiple datasets for comparison purposes.
------------------------------------------------
DROP TABLE IF EXISTS dw_version CASCADE;
CREATE TABLE dw_version (
    dataset_id      serial primary key,
    created         timestamptz,
    updated         timestamptz,
    description     text
);

DROP TABLE IF EXISTS model_version CASCADE;
CREATE TABLE model_version (
    model_id        serial PRIMARY KEY,
    created         timestamptz,
    description     text
);

--------------------------------------------
-- ETL/Feature Extraction Metadata Tables
--------------------------------------------

DROP TABLE IF EXISTS datalink CASCADE;
CREATE TABLE datalink (
    dataset_id                  integer REFERENCES dw_version(dataset_id),
    datalink_id                 varchar(50),
    datalink_type               varchar(20) NOT NULL,
    schedule                    text,
    data_load_type              varchar(20) NOT NULL,
    connection_type             varchar(20) NOT NULL,
    connection_setting_json     json NOT NULL,
    import_patients_sql         text NOT NULL,
    PRIMARY KEY                 (dataset_id, datalink_id),
    CHECK (datalink_type SIMILAR TO 'DBLink|WSLink'),
    CHECK (data_load_type SIMILAR TO 'incremental|full')
);

DROP TABLE IF EXISTS cdm_function CASCADE;
CREATE TABLE cdm_function (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    func_id         varchar(50) NOT NULL,
    func_type       varchar(20) NOT NULL,
    description     text        NOT NULL,
    PRIMARY KEY     (dataset_id, func_id),
    CHECK (func_type SIMILAR TO 'transform|fillin|derive')
);

DROP TABLE IF EXISTS cdm_feature CASCADE;
CREATE TABLE cdm_feature (
    dataset_id              integer REFERENCES dw_version(dataset_id),
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
    CHECK (category SIMILAR TO 'S|M|T|TWF|G|N')
);

DROP TABLE IF EXISTS datalink_feature_mapping;
CREATE TABLE datalink_feature_mapping (
    dataset_id          integer,
    fid                 varchar(50),
    is_no_add           boolean,
    is_med_action       boolean,
    datalink_id         varchar(20),
    dbtable             text,
    select_cols         text,
    where_conditions    text,
    transform_func_id   varchar(50),
    api                 varchar(50),
    api_method          varchar(50),
    api_method_args     varchar(200),
    FOREIGN KEY         (dataset_id, datalink_id)       REFERENCES datalink(dataset_id, datalink_id),
    FOREIGN KEY         (dataset_id, fid)               REFERENCES cdm_feature(dataset_id, fid),
    FOREIGN KEY         (dataset_id, transform_func_id) REFERENCES cdm_function(dataset_id, func_id)
);


-------------------------------------------------
-- CDM Data Tables
------------------------------------------------

DROP TABLE IF EXISTS pat_enc CASCADE;
CREATE TABLE pat_enc (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          serial,
    visit_id        varchar(50) NOT NULL,
    pat_id          varchar(50) NOT NULL,
    dept_id         varchar(50),
    meta_data       json,
    PRIMARY KEY     (dataset_id, enc_id),
    UNIQUE          (dataset_id, visit_id, pat_id),
    UNIQUE          (dataset_id, enc_id, visit_id, pat_id)
);

DROP TABLE IF EXISTS cdm_g;
CREATE TABLE cdm_g (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    fid             varchar(50), -- REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY     (dataset_id, fid)
);

DROP TABLE IF EXISTS cdm_s;
CREATE TABLE cdm_s (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          integer,
    fid             varchar(50),
    value           text,
    confidence      integer,
    PRIMARY KEY     (dataset_id, enc_id, fid),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id),
    FOREIGN KEY     (dataset_id, fid)    REFERENCES cdm_feature(dataset_id, fid)
);

DROP TABLE IF EXISTS cdm_m;
CREATE TABLE cdm_m (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          integer,
    fid             varchar(50),
    line            smallint,
    value           text,
    confidence      integer,
    PRIMARY KEY     (dataset_id, enc_id, fid, line),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id),
    FOREIGN KEY     (dataset_id, fid)    REFERENCES cdm_feature(dataset_id, fid)
);

DROP TABLE IF EXISTS cdm_t;
CREATE TABLE cdm_t (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          integer,
    tsp             timestamptz,
    fid             varchar(50),
    value           text,
    confidence      integer,
    PRIMARY KEY     (dataset_id, enc_id, tsp, fid),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id),
    FOREIGN KEY     (dataset_id, fid)    REFERENCES cdm_feature(dataset_id, fid)
);

DROP TABLE IF EXISTS cdm_notes;
CREATE TABLE cdm_notes (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          int,
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           text,
    providers       text,
    PRIMARY KEY (dataset_id, enc_id, note_id, note_type, note_status)
);

drop table if exists cdm_reports;
create table cdm_reports (
    dataset_id                         integer,
    label_id                           integer,
    w_max_state                        integer,
    w_start                            timestamptz,
    w_end                              timestamptz,
    enc_id                             int,
    name                               varchar(50),
    measurement_time                   timestamptz,
    value                              text,
    override_time                      timestamptz,
    override_user                      text,
    override_value                     json,
    is_met                             boolean,
    update_date                        timestamptz,
    severe_sepsis_onset                timestamptz,
    severe_sepsis_wo_infection_onset   timestamptz,
    septic_shock_onset                 timestamptz,
    w_severe_sepsis_onset              timestamptz,
    w_severe_sepsis_wo_infection_onset timestamptz,
    w_septic_shock_onset               timestamptz,
    primary key (dataset_id, label_id, w_max_state, w_start, w_end, enc_id, name)
);


DROP TABLE IF EXISTS trews;


DROP TABLE IF EXISTS criteria_meas;
CREATE TABLE criteria_meas
(
    dataset_id      integer REFERENCES dw_version(dataset_id),
    pat_id          varchar(50),
    tsp             timestamptz,
    fid             varchar(50),
    value           text,
    update_date     timestamptz,
    primary key     (dataset_id, pat_id, tsp, fid)
);

DO $$
BEGIN

IF to_regclass('criteria_meas_idx') IS NULL THEN
    CREATE INDEX criteria_meas_idx ON criteria_meas (pat_id, tsp, fid);
END IF;

END$$;

DROP TABLE IF EXISTS criteria;
CREATE TABLE criteria
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              varchar(50),
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date         timestamptz,
    primary key         (dataset_id, pat_id, name)
);

DROP TABLE IF EXISTS suspicion_of_infection_hist;
CREATE TABLE suspicion_of_infection_hist
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              varchar(50),
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date         timestamptz,
    primary key         (dataset_id, pat_id, name, override_time)
);

DROP TABLE IF EXISTS suspicion_of_infection_buff;
CREATE TABLE suspicion_of_infection_buff
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              varchar(50),
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date         timestamptz,
    primary key         (dataset_id, pat_id, name)
);


DROP TABLE IF EXISTS criteria_events;
CREATE SEQUENCE IF NOT EXISTS criteria_event_ids;
CREATE TABLE criteria_events
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    event_id            int,
    pat_id              varchar(50),
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date         timestamptz,
    flag                int,
    primary key (dataset_id, event_id, pat_id, name)
);

DO $$
BEGIN

IF to_regclass('criteria_idx') IS NULL THEN
    CREATE INDEX criteria_idx ON criteria (pat_id, name);
END IF;

END$$;


DROP TABLE IF EXISTS criteria_log;
CREATE TABLE criteria_log
(
    dataset_id      integer REFERENCES dw_version(dataset_id),
    log_id          serial,
    pat_id          varchar(50),
    tsp             timestamptz,
    event           json,
    update_date     timestamptz,
    PRIMARY KEY     (dataset_id, log_id)
);

DO $$
BEGIN
IF to_regclass('criteria_log_idx') IS NULL THEN
    CREATE INDEX criteria_log_idx ON criteria_log (pat_id, tsp);
END IF;
END$$;


DROP TABLE IF EXISTS criteria_meas_archive;
CREATE TABLE criteria_meas_archive
(
    dataset_id      integer REFERENCES dw_version(dataset_id),
    pat_id          varchar(50)     not null,
    tsp             timestamptz     not null,
    fid             varchar(50)     not null,
    value           text,
    update_date     timestamptz
);

DROP TABLE IF EXISTS criteria_archive;
CREATE TABLE criteria_archive
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              varchar(50)     not null,
    name                varchar(50)     not null,
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    orveride_user       text,
    value               text,
    update_date         timestamptz
);

DROP TABLE IF EXISTS criteria_default;
CREATE TABLE criteria_default
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    name                varchar(50),
    fid                 varchar(50),
    override_value      json,
    category            varchar(50),
    primary key         (dataset_id, name, fid, category)
);

\copy criteria_default from 'criteria_default.csv' with csv header delimiter as ',';


DROP TABLE IF EXISTS notifications;
CREATE  TABLE notifications
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    model_id            integer REFERENCES model_version(model_id),
    notification_id     serial,
    pat_id              varchar(50) not null,
    message             json,
    primary key         (dataset_id, model_id, notification_id)
);

DROP TABLE IF EXISTS parameters;
CREATE  TABLE parameters
(
    dataset_id  integer REFERENCES dw_version(dataset_id),
    name        text,
    value       text not null,
    primary key (dataset_id, name)
);

-----------------------------------
-- tables for trews model
-----------------------------------

DROP TABLE IF EXISTS trews_scaler;
CREATE TABLE trews_scaler
(
    model_id    integer REFERENCES model_version(model_id),
    fid         text,
    mean        real,
    var         real,
    scale       real,
    PRIMARY KEY (model_id, fid)
);

DROP TABLE IF EXISTS trews_feature_weights;
CREATE TABLE trews_feature_weights
(
    model_id    integer REFERENCES model_version(model_id),
    fid         text,
    weight      real,
    PRIMARY KEY (model_id, fid)
);

DROP TABLE IF EXISTS trews_parameters;
CREATE TABLE trews_parameters
(
    model_id    integer REFERENCES model_version(model_id),
    name        text,
    value       real,
    PRIMARY KEY (model_id, name)
);

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf (
    dataset_id                             integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    pao2                                   real,
    pao2_c                                 integer,
    meta_data                              json,
    PRIMARY KEY     (dataset_id, enc_id, tsp),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id)
);

DROP TABLE IF EXISTS trews;
CREATE TABLE trews (
    dataset_id                             integer,
    model_id                               integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    trewscore                              numeric,
    age                                    double precision,
    gender                                 double precision,

    PRIMARY KEY     (dataset_id, model_id, enc_id, tsp),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id)
);

CREATE SCHEMA IF NOT EXISTS workspace;

DROP TABLE IF EXISTS pat_status;
CREATE TABLE pat_status (
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              varchar(50),
    deactivated         boolean,
    deactivated_tsp     timestamptz,
    primary key         (dataset_id, pat_id)
);

DROP TABLE IF EXISTS deterioration_feedback;
CREATE TABLE deterioration_feedback (
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              varchar(50),
    tsp                 timestamptz,
    deterioration       json,
    uid                 varchar(50),
    primary key         (dataset_id, pat_id)
);

DROP TABLE IF EXISTS feedback_log;
CREATE TABLE feedback_log (
    dataset_id          integer REFERENCES dw_version(dataset_id),
    doc_id              varchar(50),
    tsp                 timestamptz,
    pat_id              varchar(50),
    dep_id              varchar(50),
    feedback            text
);

DROP TABLE IF EXISTS pat_group;
CREATE TABLE pat_group (
    pat_id              varchar(50),
    model_id            varchar(50),
    group_id            integer,
    update_tsp          timestamptz,
    primary key (pat_id, model_id, group_id)
);

DROP TABLE IF EXISTS group_table;
CREATE TABLE group_table (
    group_id            integer,
    group_description   text,
    update_tsp          timestamptz,
    primary key         (group_id)
);

DROP TABLE IF EXISTS historical_criteria;
CREATE TABLE historical_criteria (
    pat_id              text,
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_state           integer,
    window_ts           timestamptz,
    primary key         (pat_id,dataset_id,window_ts)
);

DROP TABLE IF EXISTS historical_notifications;
CREATE TABLE historical_notifications (
    dataset_id          integer REFERENCES dw_version(dataset_id),
    pat_id              text,
    message             json
);

DROP TABLE IF EXISTS usr_web_log;
CREATE TABLE usr_web_log (
    dataset_id          integer REFERENCES dw_version(dataset_id),
    model_id            integer REFERENCES model_version(model_id),
    doc_id      varchar(50),
    tsp         timestamptz,
    pat_id      varchar(50),
    visit_id    varchar(50),
    loc         varchar(50),
    dep         varchar(50),
    raw_url     text,
    PRIMARY KEY (dataset_id, model_id, doc_id, tsp, pat_id)
);

DROP TABLE IF EXISTS model_training_report;
CREATE TABLE model_training_report (
    report_id       serial PRIMARY KEY,
    report          json,
    create_at       timestamptz
);
-- event_time (dataset_id, enc_id, tsp, event)

DROP TABLE IF EXISTS event_time;
create table event_time(
    dataset_id integer REFERENCES dw_version(dataset_id),
    enc_id integer,
    tsp timestamptz,
    event text
);

DROP TABLE IF EXISTS sub_populations;
create table sub_populations(
    dataset_id integer REFERENCES dw_version(dataset_id),
    enc_id integer,
    population_name text
);

DROP TABLE IF EXISTS care_unit;
create table care_unit(
    dataset_id integer REFERENCES dw_version(dataset_id),
    enc_id integer,
    enter_time timestamptz,
    leave_time timestamptz,
    care_unit text
);

----------------------
-- cdm label tables --
----------------------
DROP TABLE IF EXISTS label_version CASCADE;
CREATE TABLE label_version (
    label_id        serial primary key,
    created         timestamptz,
    description     text
);

DROP TABLE IF EXISTS cdm_labels;
CREATE TABLE cdm_labels (
    dataset_id          integer references dw_version(dataset_id),
    label_id            integer references label_version(label_id),
    enc_id              int,
    tsp                 timestamptz,
    label_type          text,
    label               integer,
    primary key         (dataset_id, label_id, enc_id, tsp)
);


DROP TABLE IF EXISTS cdm_processed_notes;
CREATE TABLE cdm_processed_notes (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          int,
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    tsps            timestamptz[],
    ngrams          text[],
    PRIMARY KEY (dataset_id, enc_id, note_id, note_type, note_status)
);


----------------------
-- cdm stats tables --
----------------------
DROP TABLE IF EXISTS cdm_stats cascade;
CREATE TABLE cdm_stats(
    dataset_id integer REFERENCES dw_version(dataset_id),
    id text,
    id_type text,
    cdm_table text,
    stats jsonb,
    PRIMARY KEY (dataset_id, id, id_type, cdm_table)
);

DROP VIEW IF EXISTS cdm_stats_view;
CREATE VIEW cdm_stats_view AS
    select dataset_id, id, id_type, cdm_table, jsonb_pretty(stats) from cdm_stats;

--------------------------
-- clarity stats tables --
--------------------------
DROP TABLE IF EXISTS clarity_stats cascade;
CREATE TABLE clarity_stats(
    id text,
    id_type text,
    clarity_workspace text,
    clarity_staging_table text,
    stats jsonb,
    PRIMARY KEY (id, id_type, clarity_workspace, clarity_staging_table)
);

DROP VIEW IF EXISTS clarity_stats_view;
CREATE VIEW clarity_stats_view AS
    select id, id_type, clarity_workspace, clarity_staging_table, jsonb_pretty(stats) from clarity_stats;