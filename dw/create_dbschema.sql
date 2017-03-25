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
CREATE TABLE dw_version (
    dataset_id      serial primary key,
    created         timestamptz,
    description     text
);

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
    PRIMARY KEY     (dataset_id, enc_id)
);

DROP TABLE IF EXISTS cdm_g;
CREATE TABLE cdm_g (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    model_id        smallint REFERENCES model_version(model_id),
    fid             varchar(50), -- REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer
);

CREATE UNIQUE INDEX cdm_g_idx ON cdm_g (fid);

DROP TABLE IF EXISTS cdm_s;
CREATE TABLE cdm_s (
    dataset_id      integer,
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
    dataset_id      integer,
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
    dataset_id      integer,
    enc_id          integer,
    tsp             timestamptz,
    fid             varchar(50),
    value           text,
    confidence      integer,
    PRIMARY KEY     (dataset_id, enc_id, tsp, fid),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id),
    FOREIGN KEY     (dataset_id, fid)    REFERENCES cdm_feature(dataset_id, fid)
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

DROP TABLE IF EXISTS notifications;
CREATE  TABLE notifications
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    notification_id     serial,
    pat_id              varchar(50) not null,
    message             json,
    primary key         (dataset_id, notification_id)
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

-- TODO: load these constants/parameters via prod2dw ETL.
--\copy cdm_function     from '/home/ubuntu/dashan_core/opsdx/deploy/CDM_Function.csv' with csv header delimiter as ',';
--\copy cdm_feature      from '/home/ubuntu/dashan_core/opsdx/deploy/CDM_Feature.csv' with csv header delimiter as ',';
--\copy parameters       from '/home/ubuntu/dashan_core/opsdx/deploy/parameters.csv' with csv header delimiter as ',';
--\copy criteria_default from '/home/ubuntu/dashan_core/opsdx/deploy/criteria_default.csv' with csv header delimiter as ',';
--\copy cdm_g from '/home/ubuntu/dashan_core/opsdx/deploy/CDM_G.csv' with csv header delimiter as ',';

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf (
    dataset_id                             integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    pao2                                   real,
    hepatic_sofa                           integer,
    paco2                                  real,
    abp_mean                               real,
    sodium                                 real,
    obstructive_pe_shock                   integer,
    metabolic_acidosis                     int,
    troponin                               real,
    rass                                   real,
    sirs_raw                               boolean,
    pao2_to_fio2                           real,
    qsofa                                  integer,
    fio2                                   real,
    neurologic_sofa                        integer,
    hematologic_sofa                       integer,
    renal_sofa                             integer,
    nbp_sys                                real,
    sirs_hr_oor                            boolean,
    resp_sofa                              integer,
    bun_to_cr                              real,
    cmi                                    boolean,
    cardio_sofa                            integer,
    acute_pancreatitis                     integer,
    wbc                                    real,
    shock_idx                              real,
    weight                                 real,
    platelets                              real,
    arterial_ph                            real,
    nbp_dias                               real,
    fluids_intake_1hr                      real,
    co2                                    real,
    dbpm                                   real,
    ddimer                                 real,
    ast_liver_enzymes                      real,
    fluids_intake_24hr                     real,
    ptt                                    real,
    abp_sys                                real,
    magnesium                              real,
    severe_sepsis                          boolean,
    bicarbonate                            real,
    lipase                                 real,
    hypotension_raw                        boolean,
    sbpm                                   real,
    heart_rate                             real,
    nbp_mean                               real,
    anion_gap                              real,
    vasopressor_resuscitation              boolean,
    urine_output_24hr                      real,
    amylase                                real,
    septic_shock_iii                       integer,
    hematocrit                             real,
    temperature                            real,
    sirs_wbc_oor                           boolean,
    hemoglobin_minutes_since_measurement   real,
    urine_output_6hr                       real,
    chloride                               real,
    spo2                                   real,
    resp_rate                              real,
    hemorrhagic_shock                      integer,
    potassium                              real,
    acute_liver_failure                    integer,
    bun                                    real,
    hemoglobin_change                      real,
    mi                                     int,
    hypotension_intp                       boolean,
    calcium                                real,
    abp_dias                               real,
    acute_organ_failure                    boolean,
    worst_sofa                             integer,
    hemoglobin                             real,
    any_organ_failure                      boolean,
    inr                                    real,
    creatinine                             real,
    fluid_resuscitation                    boolean,
    bilirubin                              real,
    alt_liver_enzymes                      real,
    mapm                                   real,
    gcs                                    real,
    sirs_intp                              boolean,
    minutes_since_any_antibiotics          integer,
    fluids_intake_3hr                      real,
    sirs_temperature_oor                   boolean,
    sirs_resp_oor                          boolean,
    septic_shock                           integer,
    lactate                                real,
    minutes_since_any_organ_fail           integer,
    PRIMARY KEY     (dataset_id, enc_id, tsp),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id)
);

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf_c (
    dataset_id                             integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    pao2_c                                 integer,
    hepatic_sofa_c                         integer,
    paco2_c                                integer,
    abp_mean_c                             integer,
    sodium_c                               integer,
    obstructive_pe_shock_c                 integer,
    metabolic_acidosis_c                   integer,
    troponin_c                             integer,
    rass_c                                 integer,
    sirs_raw_c                             integer,
    pao2_to_fio2_c                         integer,
    qsofa_c                                integer,
    fio2_c                                 integer,
    neurologic_sofa_c                      integer,
    hematologic_sofa_c                     integer,
    renal_sofa_c                           integer,
    nbp_sys_c                              integer,
    sirs_hr_oor_c                          integer,
    resp_sofa_c                            integer,
    bun_to_cr_c                            integer,
    cmi_c                                  integer,
    cardio_sofa_c                          integer,
    acute_pancreatitis_c                   integer,
    wbc_c                                  integer,
    shock_idx_c                            integer,
    weight_c                               integer,
    platelets_c                            integer,
    arterial_ph_c                          integer,
    nbp_dias_c                             integer,
    fluids_intake_1hr_c                    integer,
    co2_c                                  integer,
    dbpm_c                                 integer,
    ddimer_c                               integer,
    ast_liver_enzymes_c                    integer,
    fluids_intake_24hr_c                   integer,
    ptt_c                                  integer,
    abp_sys_c                              integer,
    magnesium_c                            integer,
    severe_sepsis_c                        integer,
    bicarbonate_c                          integer,
    lipase_c                               integer,
    hypotension_raw_c                      integer,
    sbpm_c                                 integer,
    heart_rate_c                           integer,
    nbp_mean_c                             integer,
    anion_gap_c                            integer,
    vasopressor_resuscitation_c            integer,
    urine_output_24hr_c                    integer,
    amylase_c                              integer,
    septic_shock_iii_c                     integer,
    hematocrit_c                           integer,
    temperature_c                          integer,
    sirs_wbc_oor_c                         integer,
    hemoglobin_minutes_since_measurement_c integer,
    urine_output_6hr_c                     integer,
    chloride_c                             integer,
    spo2_c                                 integer,
    resp_rate_c                            integer,
    hemorrhagic_shock_c                    integer,
    potassium_c                            integer,
    acute_liver_failure_c                  integer,
    bun_c                                  integer,
    hemoglobin_change_c                    integer,
    mi_c                                   integer,
    hypotension_intp_c                     integer,
    calcium_c                              integer,
    abp_dias_c                             integer,
    acute_organ_failure_c                  integer,
    worst_sofa_c                           integer,
    hemoglobin_c                           integer,
    any_organ_failure_c                    integer,
    inr_c                                  integer,
    creatinine_c                           integer,
    fluid_resuscitation_c                  integer,
    bilirubin_c                            integer,
    alt_liver_enzymes_c                    integer,
    mapm_c                                 integer,
    gcs_c                                  integer,
    sirs_intp_c                            integer,
    minutes_since_any_antibiotics_c        integer,
    fluids_intake_3hr_c                    integer,
    sirs_temperature_oor_c                 integer,
    sirs_resp_oor_c                        integer,
    septic_shock_c                         integer,
    lactate_c                              integer,
    minutes_since_any_organ_fail_c         integer,
    meta_data                              json,
    PRIMARY KEY     (dataset_id, enc_id, tsp),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id)
);

DROP TABLE IF EXISTS trews;
CREATE TABLE trews (
    dataset_id                             integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    trewscore                              real,
    pao2                                   real,
    hepatic_sofa                           integer,
    paco2                                  real,
    abp_mean                               real,
    sodium                                 real,
    obstructive_pe_shock                   integer,
    metabolic_acidosis                     int,
    troponin                               real,
    rass                                   real,
    sirs_raw                               boolean,
    pao2_to_fio2                           real,
    qsofa                                  integer,
    fio2                                   real,
    neurologic_sofa                        integer,
    hematologic_sofa                       integer,
    renal_sofa                             integer,
    nbp_sys                                real,
    sirs_hr_oor                            boolean,
    resp_sofa                              integer,
    bun_to_cr                              real,
    cmi                                    boolean,
    cardio_sofa                            integer,
    acute_pancreatitis                     integer,
    wbc                                    real,
    shock_idx                              real,
    weight                                 real,
    platelets                              real,
    arterial_ph                            real,
    nbp_dias                               real,
    fluids_intake_1hr                      real,
    co2                                    real,
    dbpm                                   real,
    ddimer                                 real,
    ast_liver_enzymes                      real,
    fluids_intake_24hr                     real,
    ptt                                    real,
    abp_sys                                real,
    magnesium                              real,
    severe_sepsis                          boolean,
    bicarbonate                            real,
    lipase                                 real,
    hypotension_raw                        boolean,
    sbpm                                   real,
    heart_rate                             real,
    nbp_mean                               real,
    anion_gap                              real,
    vasopressor_resuscitation              boolean,
    urine_output_24hr                      real,
    amylase                                real,
    septic_shock_iii                       integer,
    hematocrit                             real,
    temperature                            real,
    sirs_wbc_oor                           boolean,
    hemoglobin_minutes_since_measurement   real,
    urine_output_6hr                       real,
    chloride                               real,
    spo2                                   real,
    resp_rate                              real,
    hemorrhagic_shock                      integer,
    potassium                              real,
    acute_liver_failure                    integer,
    bun                                    real,
    hemoglobin_change                      real,
    mi                                     int,
    hypotension_intp                       boolean,
    calcium                                real,
    abp_dias                               real,
    acute_organ_failure                    boolean,
    worst_sofa                             integer,
    hemoglobin                             real,
    any_organ_failure                      boolean,
    inr                                    real,
    creatinine                             real,
    fluid_resuscitation                    boolean,
    bilirubin                              real,
    alt_liver_enzymes                      real,
    mapm                                   real,
    gcs                                    real,
    sirs_intp                              boolean,
    minutes_since_any_antibiotics          integer,
    fluids_intake_3hr                      real,
    sirs_temperature_oor                   boolean,
    sirs_resp_oor                          boolean,
    septic_shock                           integer,
    lactate                                real,
    minutes_since_any_organ_fail           integer,
    PRIMARY KEY     (dataset_id, enc_id, tsp),
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
