/*
 * create_dbschema.sql
 * create relation database for the dashan instance
 * NOTE: cdm_twf is not created here
 */
CREATE SCHEMA IF NOT EXISTS event_workspace;
CREATE SCHEMA IF NOT EXISTS workspace;


DROP TABLE IF EXISTS pat_enc CASCADE;
CREATE TABLE pat_enc (
    enc_id          serial PRIMARY KEY,
    visit_id        varchar(50) NOT NULL,
    pat_id          varchar(50) NOT NULL,
    dept_id         varchar(50),
    zid             varchar(50),
    UNIQUE          (visit_id, pat_id),
    UNIQUE          (enc_id, visit_id, pat_id)
);

DROP TABLE IF EXISTS cdm_function CASCADE;
CREATE TABLE cdm_function (
    func_id         varchar(50) PRIMARY KEY,
    func_type       varchar(20) NOT NULL,
    description     text        NOT NULL,
    CHECK (func_type SIMILAR TO 'transform|fillin|derive')
);

DROP TABLE IF EXISTS cdm_feature CASCADE;
CREATE TABLE cdm_feature (
    fid                     varchar(50) PRIMARY KEY,
    category                varchar(50) NOT NULL,
    data_type               varchar(20) NOT NULL,
    is_measured             boolean NOT NULL,
    is_deprecated           boolean NOT NULL,
    fillin_func_id          varchar(50) REFERENCES cdm_function(func_id),
    window_size_in_hours    varchar(100),
    derive_func_id          varchar(50) REFERENCES cdm_function(func_id),
    derive_func_input       text,
    description             text,
    version                 varchar(50),
    unit                    varchar(50),
    CHECK (category SIMILAR TO 'S|M|T|TWF|G|N')
);

DROP TABLE IF EXISTS cdm_g;
CREATE TABLE cdm_g (
    fid             varchar(50), -- REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer
);

CREATE UNIQUE INDEX cdm_g_idx ON cdm_g (fid);

DROP TABLE IF EXISTS cdm_s;
CREATE TABLE cdm_s (
    enc_id          integer REFERENCES pat_enc(enc_id),
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (enc_id, fid)
);

DROP TABLE IF EXISTS event_workspace.cdm_s;
CREATE TABLE event_workspace.cdm_s (
    job_id          text,
    enc_id          integer REFERENCES pat_enc(enc_id),
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (job_id, enc_id, fid)
);

DROP TABLE IF EXISTS workspace.cdm_s;
CREATE TABLE workspace.cdm_s (
    job_id          text,
    enc_id          integer REFERENCES pat_enc(enc_id),
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (job_id, enc_id, fid)
);

DROP TABLE IF EXISTS cdm_t;
CREATE TABLE cdm_t (
    enc_id          integer REFERENCES pat_enc(enc_id),
    tsp             timestamptz,
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (enc_id, tsp, fid)
);

DROP TABLE IF EXISTS event_workspace.cdm_t;
CREATE TABLE event_workspace.cdm_t (
    job_id          text,
    enc_id          integer REFERENCES pat_enc(enc_id),
    tsp             timestamptz,
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (job_id, enc_id, tsp, fid)
);

DROP TABLE IF EXISTS workspace.cdm_t;
CREATE TABLE workspace.cdm_t (
    job_id          text,
    enc_id          integer REFERENCES pat_enc(enc_id),
    tsp             timestamptz,
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (job_id, enc_id, tsp, fid)
);

DROP TABLE IF EXISTS cdm_notes;
CREATE TABLE cdm_notes (
    enc_id          int,
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           json,
    providers       json,
    PRIMARY KEY (enc_id, note_id, note_type, note_status)
);

DROP TABLE IF EXISTS workspace.cdm_notes;
CREATE TABLE workspace.cdm_notes (
    job_id          text,
    enc_id          int,
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           json,
    providers       json,
    PRIMARY KEY (job_id, enc_id, note_id, note_type, note_status)
);

DROP TABLE IF EXISTS event_workspace.cdm_notes;
CREATE TABLE event_workspace.cdm_notes (
    job_id          text,
    enc_id          int,
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           json,
    providers       json,
    PRIMARY KEY (job_id, enc_id, note_id, note_type, note_status)
);

DROP TABLE IF EXISTS workspace.cdm_notes;
CREATE TABLE workspace.cdm_notes (
    job_id          text,
    enc_id          int,
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           json,
    providers       json,
    PRIMARY KEY (job_id, enc_id, note_id, note_type, note_status)
);

DROP TABLE IF EXISTS metrics_events;
CREATE TABLE metrics_events
(
    eid                     text,
    tsp                     timestamptz,
    event_type        text,
    event                 json
);

DO $$
BEGIN
IF to_regclass('metrics_events_idx') IS NULL THEN
    CREATE INDEX metrics_events_idx ON metrics_events (eid, tsp);
END IF;
END$$;

DROP TABLE IF EXISTS criteria;
CREATE TABLE criteria
(
    enc_id              int,
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date         timestamptz,
    is_acute            boolean,
    primary key (enc_id, name)
);

DROP TABLE IF EXISTS criteria_events;
CREATE SEQUENCE IF NOT EXISTS criteria_event_ids;
CREATE TABLE criteria_events
(
    event_id            int,
    enc_id              int,
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date         timestamptz,
    is_acute            boolean,
    flag                int,
    primary key (event_id, enc_id, name)
);
DO $$
BEGIN
IF to_regclass('criteria_events_idx') IS NULL THEN
    CREATE UNIQUE INDEX criteria_events_idx ON criteria_events (enc_id, event_id, name, flag);
END IF;
END$$;


DROP TABLE IF EXISTS criteria_log;
CREATE TABLE criteria_log
(
    log_id          serial primary key,
    enc_id          int,
    tsp             timestamptz,
    event         json,
    update_date     timestamptz
);

DO $$
BEGIN
IF to_regclass('criteria_log_idx') IS NULL THEN
    CREATE INDEX criteria_log_idx ON criteria_log (enc_id, tsp);
END IF;
END$$;

DROP TABLE IF EXISTS criteria_default;
CREATE TABLE criteria_default
(
    name    varchar(50) ,
    fid       varchar(50) ,
    override_value  json,
    category varchar(50)
);

DROP TABLE IF EXISTS notifications;
CREATE  TABLE notifications
(
    notification_id     serial PRIMARY KEY,
    enc_id      int     not null,
    message     json
);

DROP TABLE IF EXISTS epic_notifications_history;
CREATE  TABLE epic_notifications_history
(
    tsp         timestamptz     not null,
    enc_id      int             not null,
    count       int             not null,
    trewscore   text,
    threshold   text,
    flag        int,
    PRIMARY KEY(tsp, enc_id)
);

DROP TABLE IF EXISTS epic_trewscores_history;
CREATE  TABLE epic_trewscores_history
(
    tsp         timestamptz         not null,
    enc_id      int                 not null,
    trewscore   double precision    not null,
    PRIMARY KEY(tsp, enc_id)
);



DROP TABLE IF EXISTS refreshed_pats;
CREATE TABLE refreshed_pats
(
    id serial PRIMARY KEY,
    refreshed_tsp timestamptz,
    pats jsonb
);

DROP TABLE IF EXISTS parameters;
CREATE  TABLE parameters
(
    name     text PRIMARY KEY,
    value      text not null
);

-----------------------------------
-- tables for trews model
-----------------------------------

DROP TABLE IF EXISTS trews_scaler;
CREATE  TABLE trews_scaler
(
    fid     text PRIMARY KEY,
    mean      real,
    var real,
    scale real
);

DROP TABLE IF EXISTS trews_feature_weights;
CREATE  TABLE trews_feature_weights
(
    fid     text PRIMARY KEY,
    weight      real
);

DROP TABLE IF EXISTS trews_parameters;
CREATE  TABLE trews_parameters
(
    name     text PRIMARY KEY,
    value      real
);

\copy cdm_function     from 'CDM_Function.csv' with csv header delimiter as ',';
\copy cdm_feature      from 'CDM_Feature.csv' with csv header delimiter as ',';
\copy parameters       from 'ops/parameters.csv' with csv header delimiter as ',';
\copy trews_parameters       from 'ops/trews-model/trews_parameters.csv' with csv header delimiter as ',';
\copy trews_scaler      from 'ops/trews-model/lactateConstrStdScale.csv' with csv header delimiter as ',';
\copy trews_feature_weights     from 'ops/trews-model/lactateConstrFeatureWeights.csv' with csv header delimiter as ',';
\copy criteria_default from 'criteria_default.csv' with csv header delimiter as ',';
\copy cdm_g from 'ops/CDM_G.csv' with csv header delimiter as ',';

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf (
    enc_id  integer REFERENCES pat_enc(enc_id),
    tsp     timestamptz,
    meta_data json,
    PRIMARY KEY (enc_id, tsp)
);

DROP TABLE IF EXISTS trews;
CREATE TABLE trews (
    enc_id  integer REFERENCES pat_enc(enc_id),
    tsp                                    timestamptz
    PRIMARY KEY     (enc_id, tsp)
);



DROP TABLE IF EXISTS pat_status;
CREATE TABLE pat_status (
    enc_id                    int primary key,
    deactivated               boolean,
    deactivated_tsp          timestamptz
);

DROP TABLE IF EXISTS deterioration_feedback;
CREATE TABLE deterioration_feedback (
    enc_id                      int primary key,
    tsp                                 timestamptz,
    deterioration               json,
    uid                             varchar(50)
);

DROP TABLE IF EXISTS feedback_log;
CREATE TABLE feedback_log (
    doc_id      varchar(50),
    tsp         timestamptz,
    enc_id      int,
    dep_id      varchar(50),
    feedback    text
);

DROP TABLE IF EXISTS usr_web_log;
CREATE TABLE usr_web_log (
    doc_id      varchar(50),
    tsp         timestamptz,
    pat_id      varchar(50),
    visit_id    varchar(50),
    loc         varchar(50),
    dep         varchar(50),
    raw_url     text,
    PRIMARY KEY (doc_id, tsp, pat_id)
);

DROP TABLE IF EXISTS user_interactions;
CREATE TABLE user_interactions (
    tsp             timestamptz,
    addr            cidr,
    host_session    char(40),
    user_session    char(48),
    uid             varchar(16),
    action          text,
    enc_id          int,
    csn             varchar(50),
    loc             varchar(16),
    dep             varchar(16),
    action_data     jsonb,
    render_data     jsonb,
    log_entry       text
);


DROP TABLE IF EXISTS lmcscore;
CREATE TABLE lmcscore (
    enc_id integer NOT NULL
);

DROP TABLE IF EXISTS orgdf_baselines;
CREATE TABLE orgdf_baselines (
    pat_id          varchar(50) primary key,
);

DROP TABLE IF EXISTS trews_jit_score;
CREATE TABLE trews_jit_score(
model_id           integer                 ,
enc_id             integer                 ,
tsp                timestamp with time zone,
score              double precision
PRIMARY KEY (model_id, enc_id, tsp, alert_state, orgdf_state)
);

DROP TABLE IF EXISTS predictor_times;
CREATE TABLE predictor_times (
    name        varchar(40) primary key,
    tsp         timestamptz
);


----------------------------------
-- Load testing
--
create unlogged table locust_stats (
  job_id        int,
  t_start       timestamptz,
  t_end         timestamptz,
  latencies     jsonb,
  load          jsonb,
  locust_stats  jsonb
);


----------------------------------
-- Labeling
--
DROP TABLE IF EXISTS label_version CASCADE;
CREATE TABLE label_version (
    label_id        serial primary key,
    created         timestamptz,
    description     text
);

DROP TABLE IF EXISTS cdm_labels;
CREATE TABLE cdm_labels (
    dataset_id integer,
    label_id   integer,
    enc_id     integer,
    tsp        timestamp with time zone,
    label_type text,
    label      integer,
    CONSTRAINT cdm_labels_uq UNIQUE (dataset_id, label_type, label, enc_id, tsp)
);


DROP TABLE IF EXISTS sep2_label_details;
CREATE TABLE sep2_label_details(
    label_id            integer,
    dataset_id          integer,
    enc_id              integer,
    CONSTRAINT sep2_label_details_uq UNIQUE (dataset_id, enc_id, sepsis_onset, infection1_tsp, infection1_name, infection2_tsp, infection2_name, orgdf_tsp, vent_orgdf, creatinine_orgdf, bilirubin_orgdf, platelets_orgdf, gcs_orgdf, inr_orgdf, hypotension_orgdf, vasopressors_orgdf, lactate_orgdf, sbpm_hypotension, delta_hypotension, map_hypotension, baseline_inr, baseline_creatinine, baseline_platelets, baseline_bilirubin)
);

DROP TABLE IF EXISTS sep2_sirs;
CREATE TABLE sep2_sirs(
    label_id         integer,
    dataset_id       integer,
    enc_id           integer,
    tsp              timestamp with time zone,
    CONSTRAINT sep2_sirs_uq UNIQUE (dataset_id, enc_id, tsp, heart_rate_sirs, resp_rate_sirs, wbc_sirs, temperature_sirs)
);

DROP TABLE IF EXISTS sep2_suspicion_of_infection;
CREATE TABLE sep2_suspicion_of_infection(
    label_id         integer,
    dataset_id       integer,
    enc_id           integer,
    CONSTRAINT sep2_suspicion_of_infection_uq  UNIQUE (dataset_id, enc_id, infection1_tsp, infection1_name, infection2_tsp, infection2_name, heart_rate_sirs, resp_rate_sirs, wbc_sirs, temperature_sirs)
);

DROP TABLE IF EXISTS etl_job;
CREATE TABLE etl_job(
    serial_id       serial primary key,
    job_id          text,
    tsp             timestamptz,
    hospital        text,
    workspace       text,
    unique          (job_id)
);

DROP TABLE IF EXISTS nurse_eval;
CREATE TABLE nurse_eval(
    enc_id           int,
    tsp              timestamptz,
    uid              str,
    eval             json
);
