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
    fid             varchar(50), -- REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY     (dataset_id, fid)
);

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

DROP TABLE IF EXISTS cdm_notes;
CREATE TABLE cdm_notes (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    pat_id          varchar(50),
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           json,
    providers       json,
    PRIMARY KEY (dataset_id, pat_id, note_id, note_type, note_status)
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
    dataset_id          integer,
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

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf (
    dataset_id                             integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    pao2                                   real,
    pao2_c                                 integer,
    hepatic_sofa                           integer,
    hepatic_sofa_c                         integer,
    paco2                                  real,
    paco2_c                                integer,
    abp_mean                               real,
    abp_mean_c                             integer,
    sodium                                 real,
    sodium_c                               integer,
    obstructive_pe_shock                   integer,
    obstructive_pe_shock_c                 integer,
    metabolic_acidosis                     int,
    metabolic_acidosis_c                   integer,
    troponin                               real,
    troponin_c                             integer,
    rass                                   real,
    rass_c                                 integer,
    sirs_raw                               boolean,
    sirs_raw_c                             integer,
    pao2_to_fio2                           real,
    pao2_to_fio2_c                         integer,
    qsofa                                  integer,
    qsofa_c                                integer,
    fio2                                   real,
    fio2_c                                 integer,
    neurologic_sofa                        integer,
    neurologic_sofa_c                      integer,
    hematologic_sofa                       integer,
    hematologic_sofa_c                     integer,
    renal_sofa                             integer,
    renal_sofa_c                           integer,
    nbp_sys                                real,
    nbp_sys_c                              integer,
    sirs_hr_oor                            boolean,
    sirs_hr_oor_c                          integer,
    resp_sofa                              integer,
    resp_sofa_c                            integer,
    bun_to_cr                              real,
    bun_to_cr_c                            integer,
    cmi                                    boolean,
    cmi_c                                  integer,
    cardio_sofa                            integer,
    cardio_sofa_c                          integer,
    acute_pancreatitis                     integer,
    acute_pancreatitis_c                   integer,
    wbc                                    real,
    wbc_c                                  integer,
    shock_idx                              real,
    shock_idx_c                            integer,
    weight                                 real,
    weight_c                               integer,
    platelets                              real,
    platelets_c                            integer,
    arterial_ph                            real,
    arterial_ph_c                          integer,
    nbp_dias                               real,
    nbp_dias_c                             integer,
    fluids_intake_1hr                      real,
    fluids_intake_1hr_c                    integer,
    co2                                    real,
    co2_c                                  integer,
    dbpm                                   real,
    dbpm_c                                 integer,
    ddimer                                 real,
    ddimer_c                               integer,
    ast_liver_enzymes                      real,
    ast_liver_enzymes_c                    integer,
    fluids_intake_24hr                     real,
    fluids_intake_24hr_c                   integer,
    ptt                                    real,
    ptt_c                                  integer,
    abp_sys                                real,
    abp_sys_c                              integer,
    magnesium                              real,
    magnesium_c                            integer,
    severe_sepsis                          boolean,
    severe_sepsis_c                        integer,
    bicarbonate                            real,
    bicarbonate_c                          integer,
    lipase                                 real,
    lipase_c                               integer,
    hypotension_raw                        boolean,
    hypotension_raw_c                      integer,
    sbpm                                   real,
    sbpm_c                                 integer,
    heart_rate                             real,
    heart_rate_c                           integer,
    nbp_mean                               real,
    nbp_mean_c                             integer,
    anion_gap                              real,
    anion_gap_c                            integer,
    vasopressor_resuscitation              boolean,
    vasopressor_resuscitation_c            integer,
    urine_output_24hr                      real,
    urine_output_24hr_c                    integer,
    amylase                                real,
    amylase_c                              integer,
    septic_shock_iii                       integer,
    septic_shock_iii_c                     integer,
    hematocrit                             real,
    hematocrit_c                           integer,
    temperature                            real,
    temperature_c                          integer,
    sirs_wbc_oor                           boolean,
    sirs_wbc_oor_c                         integer,
    hemoglobin_minutes_since_measurement   real,
    hemoglobin_minutes_since_measurement_c integer,
    urine_output_6hr                       real,
    urine_output_6hr_c                     integer,
    chloride                               real,
    chloride_c                             integer,
    spo2                                   real,
    spo2_c                                 integer,
    resp_rate                              real,
    resp_rate_c                            integer,
    hemorrhagic_shock                      integer,
    hemorrhagic_shock_c                    integer,
    potassium                              real,
    potassium_c                            integer,
    acute_liver_failure                    integer,
    acute_liver_failure_c                  integer,
    bun                                    real,
    bun_c                                  integer,
    hemoglobin_change                      real,
    hemoglobin_change_c                    integer,
    mi                                     int,
    mi_c                                   integer,
    hypotension_intp                       boolean,
    hypotension_intp_c                     integer,
    calcium                                real,
    calcium_c                              integer,
    abp_dias                               real,
    abp_dias_c                             integer,
    acute_organ_failure                    boolean,
    acute_organ_failure_c                  integer,
    worst_sofa                             integer,
    worst_sofa_c                           integer,
    hemoglobin                             real,
    hemoglobin_c                           integer,
    any_organ_failure                      boolean,
    any_organ_failure_c                    integer,
    inr                                    real,
    inr_c                                  integer,
    creatinine                             real,
    creatinine_c                           integer,
    fluid_resuscitation                    boolean,
    fluid_resuscitation_c                  integer,
    bilirubin                              real,
    bilirubin_c                            integer,
    alt_liver_enzymes                      real,
    alt_liver_enzymes_c                    integer,
    mapm                                   real,
    mapm_c                                 integer,
    map                                    real,
    map_c                                  integer,
    gcs                                    real,
    gcs_c                                  integer,
    sirs_intp                              boolean,
    sirs_intp_c                            integer,
    minutes_since_any_antibiotics          integer,
    minutes_since_any_antibiotics_c        integer,
    fluids_intake_3hr                      real,
    fluids_intake_3hr_c                    integer,
    sirs_temperature_oor                   boolean,
    sirs_temperature_oor_c                 integer,
    sirs_resp_oor                          boolean,
    sirs_resp_oor_c                        integer,
    septic_shock                           integer,
    septic_shock_c                         integer,
    lactate                                real,
    lactate_c                              integer,
    minutes_since_any_organ_fail           integer,
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
    trewscore                              numeric,
    age                                    double precision,
    gender                                 double precision,
    chronic_pulmonary_hist                 double precision,
    chronic_bronchitis_diag                double precision,
    esrd_prob                              double precision,
    esrd_diag                              double precision,
    emphysema_hist                         double precision,
    heart_arrhythmias_prob                 double precision,
    heart_arrhythmias_diag                 double precision,
    heart_failure_hist                     double precision,
    heart_failure_diag                     double precision,
    pao2                                   double precision,
    hepatic_sofa                           double precision,
    paco2                                  double precision,
    abp_mean                               double precision,
    sodium                                 double precision,
    obstructive_pe_shock                   double precision,
    metabolic_acidosis                     double precision,
    troponin                               double precision,
    rass                                   double precision,
    sirs_raw                               double precision,
    pao2_to_fio2                           double precision,
    qsofa                                  double precision,
    fio2                                   double precision,
    neurologic_sofa                        double precision,
    hematologic_sofa                       double precision,
    renal_sofa                             double precision,
    nbp_sys                                double precision,
    sirs_hr_oor                            double precision,
    resp_sofa                              double precision,
    bun_to_cr                              double precision,
    cmi                                    double precision,
    cardio_sofa                            double precision,
    acute_pancreatitis                     double precision,
    wbc                                    double precision,
    shock_idx                              double precision,
    weight                                 double precision,
    platelets                              double precision,
    arterial_ph                            double precision,
    nbp_dias                               double precision,
    fluids_intake_1hr                      double precision,
    co2                                    double precision,
    dbpm                                   double precision,
    ddimer                                 double precision,
    ast_liver_enzymes                      double precision,
    fluids_intake_24hr                     double precision,
    ptt                                    double precision,
    abp_sys                                double precision,
    magnesium                              double precision,
    severe_sepsis                          double precision,
    bicarbonate                            double precision,
    lipase                                 double precision,
    hypotension_raw                        double precision,
    sbpm                                   double precision,
    heart_rate                             double precision,
    nbp_mean                               double precision,
    anion_gap                              double precision,
    vasopressor_resuscitation              double precision,
    urine_output_24hr                      double precision,
    amylase                                double precision,
    septic_shock_iii                       double precision,
    hematocrit                             double precision,
    temperature                            double precision,
    sirs_wbc_oor                           double precision,
    hemoglobin_minutes_since_measurement   double precision,
    urine_output_6hr                       double precision,
    chloride                               double precision,
    spo2                                   double precision,
    resp_rate                              double precision,
    hemorrhagic_shock                      double precision,
    potassium                              double precision,
    acute_liver_failure                    double precision,
    bun                                    double precision,
    hemoglobin_change                      double precision,
    mi                                     double precision,
    hypotension_intp                       double precision,
    calcium                                double precision,
    abp_dias                               double precision,
    acute_organ_failure                    double precision,
    worst_sofa                             double precision,
    hemoglobin                             double precision,
    any_organ_failure                      double precision,
    inr                                    double precision,
    creatinine                             double precision,
    fluid_resuscitation                    double precision,
    bilirubin                              double precision,
    alt_liver_enzymes                      double precision,
    mapm                                   double precision,
    map                                   double precision,
    gcs                                    double precision,
    sirs_intp                              double precision,
    minutes_since_any_antibiotics          double precision,
    fluids_intake_3hr                      double precision,
    sirs_temperature_oor                   double precision,
    sirs_resp_oor                          double precision,
    septic_shock                           double precision,
    lactate                                double precision,
    minutes_since_any_organ_fail           double precision,
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
