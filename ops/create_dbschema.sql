/*
 * create_dbschema.sql
 * create relation database for the dashan instance
 * NOTE: cdm_twf is not created here
 */
DROP TABLE IF EXISTS datalink CASCADE;
CREATE TABLE datalink (
    datalink_id                   varchar(50) PRIMARY KEY,
    datalink_type                 varchar(20) NOT NULL,
    schedule                    text,
    data_load_type              varchar(20) NOT NULL,
    connection_type             varchar(20) NOT NULL,
    connection_setting_json     json NOT NULL,
    import_patients_sql         text NOT NULL,
    CHECK (datalink_type SIMILAR TO 'DBLink|WSLink'),
    CHECK (data_load_type SIMILAR TO 'incremental|full')
);


DROP TABLE IF EXISTS pat_enc CASCADE;
CREATE TABLE pat_enc (
    enc_id          serial PRIMARY KEY,
    visit_id        varchar(50) NOT NULL,
    pat_id          varchar(50) NOT NULL,
    dept_id         varchar(50),
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

DROP TABLE IF EXISTS datalink_feature_mapping;
CREATE TABLE datalink_feature_mapping (
    fid                 varchar(50) REFERENCES cdm_feature(fid),
    is_no_add           boolean,
    is_med_action       boolean,
    datalink_id         varchar(20) REFERENCES datalink(datalink_id) NOT NULL,
    dbtable             text,
    select_cols         text,
    where_conditions    text,
    transform_func_id   varchar(50) REFERENCES cdm_function(func_id),
    api                 varchar(50),
    api_method          varchar(50),
    api_method_args     varchar(200)
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

DROP TABLE IF EXISTS cdm_m;
CREATE TABLE cdm_m (
    enc_id          integer REFERENCES pat_enc(enc_id),
    fid             varchar(50) REFERENCES cdm_feature(fid),
    line            smallint,
    value           text,
    confidence      integer,
    PRIMARY KEY (enc_id, fid, line)
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

DROP TABLE IF EXISTS cdm_notes;
CREATE TABLE cdm_notes (
    pat_id          varchar(50),
    note_id         varchar(50),
    note_type       varchar(50),
    note_status     varchar(50),
    note_body       text,
    dates           json,
    providers       json,
    PRIMARY KEY (pat_id, note_id, note_type, note_status)
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


--\timing
--SELECT Fillin('creatinine',1);
--SELECT Fillin_1('abp_dias',1);
DROP TABLE IF EXISTS criteria_meas;
CREATE TABLE criteria_meas
(
    pat_id          varchar(50),
    tsp             timestamptz,
    fid             varchar(50),
    value           text,
    update_date     timestamptz,
    primary key (pat_id, tsp, fid)
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
    pat_id              varchar(50),
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date     timestamptz,
    primary key (pat_id, name)
);

DROP TABLE IF EXISTS criteria_events;
CREATE SEQUENCE IF NOT EXISTS criteria_event_ids;
CREATE TABLE criteria_events
(
    event_id            int,
    pat_id              varchar(50),
    name                varchar(50),
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    override_user       text,
    override_value      json,
    value               text,
    update_date     timestamptz,
    flag           int,
    primary key (event_id, pat_id, name)
);
CREATE UNIQUE INDEX criteria_events_idx ON criteria_events (pat_id, event_id, name, flag);

DO $$
BEGIN

IF to_regclass('criteria_idx') IS NULL THEN
    CREATE INDEX criteria_idx ON criteria (pat_id, name);
END IF;

END$$;


DROP TABLE IF EXISTS criteria_log;
CREATE TABLE criteria_log
(
    log_id          serial primary key,
    pat_id          varchar(50),
    tsp             timestamptz,
    event         json,
    update_date     timestamptz
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
    pat_id          varchar(50)     not null,
    tsp             timestamptz       not null,
    fid             varchar(50)     not null,
    value           text,
    update_date     timestamptz
);

DROP TABLE IF EXISTS criteria_archive;
CREATE TABLE criteria_archive
(
    pat_id          varchar(50)     not null,
    name            varchar(50)     not null,
    is_met              boolean,
    measurement_time    timestamptz,
    override_time       timestamptz,
    orveride_user       text,
    value           text,
    update_date     timestamptz
);

DROP TABLE IF EXISTS criteria_default;
CREATE TABLE criteria_default
(
    name    varchar(50) ,
    fid       varchar(50) ,
    override_value  json,
    category varchar(50),
    primary key(name, fid, category)
);

DROP TABLE IF EXISTS notifications;
CREATE  TABLE notifications
(
    notification_id     serial PRIMARY KEY,
    pat_id      varchar(50)     not null,
    message     json
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
\copy parameters       from 'parameters.csv' with csv header delimiter as ',';
\copy criteria_default from 'criteria_default.csv' with csv header delimiter as ',';
\copy cdm_g from 'CDM_G.csv' with csv header delimiter as ',';

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf (
    enc_id  integer REFERENCES pat_enc(enc_id),
    tsp     timestamptz,
    pao2 Real,pao2_c integer,hepatic_sofa Integer,hepatic_sofa_c integer,paco2 Real,paco2_c integer,abp_mean Real,abp_mean_c integer,sodium Real,sodium_c integer,obstructive_pe_shock Integer,obstructive_pe_shock_c integer,metabolic_acidosis int,metabolic_acidosis_c integer,troponin Real,troponin_c integer,rass Real,rass_c integer,sirs_raw Boolean,sirs_raw_c integer,pao2_to_fio2 Real,pao2_to_fio2_c integer,qsofa Integer,qsofa_c integer,fio2 Real,fio2_c integer,neurologic_sofa Integer,neurologic_sofa_c integer,hematologic_sofa Integer,hematologic_sofa_c integer,renal_sofa Integer,renal_sofa_c integer,nbp_sys Real,nbp_sys_c integer,sirs_hr_oor Boolean,sirs_hr_oor_c integer,resp_sofa Integer,resp_sofa_c integer,bun_to_cr Real,bun_to_cr_c integer,cmi Boolean,cmi_c integer,cardio_sofa Integer,cardio_sofa_c integer,acute_pancreatitis integer,acute_pancreatitis_c integer,wbc Real,wbc_c integer,shock_idx Real,shock_idx_c integer,weight Real,weight_c integer,platelets Real,platelets_c integer,arterial_ph Real,arterial_ph_c integer,nbp_dias Real,nbp_dias_c integer,fluids_intake_1hr Real,fluids_intake_1hr_c integer,co2 Real,co2_c integer,dbpm Real,dbpm_c integer,ddimer Real,ddimer_c integer,ast_liver_enzymes Real,ast_liver_enzymes_c integer,fluids_intake_24hr Real,fluids_intake_24hr_c integer,ptt Real,ptt_c integer,abp_sys Real,abp_sys_c integer,magnesium Real,magnesium_c integer,severe_sepsis Boolean,severe_sepsis_c integer,bicarbonate Real,bicarbonate_c integer,lipase Real,lipase_c integer,hypotension_raw Boolean,hypotension_raw_c integer,sbpm Real,sbpm_c integer,heart_rate Real,heart_rate_c integer,nbp_mean Real,nbp_mean_c integer,anion_gap Real,anion_gap_c integer,vasopressor_resuscitation Boolean,vasopressor_resuscitation_c integer,urine_output_24hr Real,urine_output_24hr_c integer,amylase Real,amylase_c integer,septic_shock_iii Integer,septic_shock_iii_c integer,hematocrit Real,hematocrit_c integer,temperature Real,temperature_c integer,sirs_wbc_oor Boolean,sirs_wbc_oor_c integer,hemoglobin_minutes_since_measurement Real,hemoglobin_minutes_since_measurement_c integer,urine_output_6hr Real,urine_output_6hr_c integer,chloride Real,chloride_c integer,spo2 Real,spo2_c integer,resp_rate Real,resp_rate_c integer,hemorrhagic_shock Integer,hemorrhagic_shock_c integer,potassium Real,potassium_c integer,acute_liver_failure Integer,acute_liver_failure_c integer,bun Real,bun_c integer,hemoglobin_change Real,hemoglobin_change_c integer,mi int,mi_c integer,hypotension_intp Boolean,hypotension_intp_c integer,calcium Real,calcium_c integer,abp_dias Real,abp_dias_c integer,acute_organ_failure Boolean,acute_organ_failure_c integer,worst_sofa Integer,worst_sofa_c integer,hemoglobin Real,hemoglobin_c integer,any_organ_failure Boolean,any_organ_failure_c integer,inr Real,inr_c integer,creatinine Real,creatinine_c integer,fluid_resuscitation Boolean,fluid_resuscitation_c integer,bilirubin Real,bilirubin_c integer,alt_liver_enzymes Real,alt_liver_enzymes_c integer,mapm Real,mapm_c integer,map Real,map_c integer,gcs Real,gcs_c integer,sirs_intp Boolean,sirs_intp_c integer,minutes_since_any_antibiotics Integer,minutes_since_any_antibiotics_c integer,fluids_intake_3hr Real,fluids_intake_3hr_c integer,sirs_temperature_oor Boolean,sirs_temperature_oor_c integer,sirs_resp_oor Boolean,sirs_resp_oor_c integer,septic_shock Integer,septic_shock_c integer,lactate Real,lactate_c integer,minutes_since_any_organ_fail Integer,minutes_since_any_organ_fail_c integer,
    meta_data json,
    PRIMARY KEY (enc_id, tsp)
);

DROP TABLE IF EXISTS trews;
CREATE TABLE trews (
    enc_id  integer REFERENCES pat_enc(enc_id),
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
    PRIMARY KEY     (enc_id, tsp)
);


CREATE SCHEMA IF NOT EXISTS workspace;

DROP TABLE IF EXISTS pat_status;
CREATE TABLE pat_status (
    pat_id                        varchar(50) primary key,
    deactivated               boolean,
    deactivated_tsp          timestamptz
);

DROP TABLE IF EXISTS deterioration_feedback;
CREATE TABLE deterioration_feedback (
    pat_id                        varchar(50) primary key,
    tsp                                 timestamptz,
    deterioration               json,
    uid                             varchar(50)
);

DROP TABLE IF EXISTS feedback_log;
CREATE TABLE feedback_log (
    doc_id      varchar(50),
    tsp         timestamptz,
    pat_id      varchar(50),
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


DROP TABLE IF EXISTS lmcscore;
CREATE TABLE lmcscore (
    enc_id integer NOT NULL,
    tsp timestamp without time zone NOT NULL,
    mean double precision,
    var double precision,
    score double precision,
    lower_score double precision,
    upper_score double precision,
    temperature double precision,
    resp_rate double precision,
    shock_idx double precision,
    mapm double precision,
    nbp_dias double precision,
    spo2 double precision,
    heart_rate double precision,
    urine_output_24hr double precision,
    urine_output_6hr double precision,
    fluids_intake_1hr double precision,
    fluids_intake_24hr double precision,
    rass double precision,
    hematologic_sofa double precision,
    neurologic_sofa double precision,
    renal_sofa double precision,
    resp_sofa double precision,
    worst_sofa double precision,
    cardio_sofa double precision,
    bun double precision,
    creatinine double precision,
    gcs double precision,
    hemoglobin double precision,
    paco2 double precision,
    pao2 double precision,
    sodium double precision,
    wbc double precision,
    amylase double precision,
    lipase double precision,
    arterial_ph double precision,
    bun_to_cr double precision,
    hypotension_raw double precision,
    qsofa double precision,
    organ_insufficiency_hist double precision,
    chronic_pulmonary_hist double precision,
    organ_insufficiency_diag double precision,
    chronic_kidney_hist double precision,
    gender double precision,
    heart_arrhythmias_prob double precision,
    heart_failure_hist double precision,
    esrd_diag double precision,
    liver_disease_hist double precision,
    diabetes_hist double precision,
    heart_failure_diag double precision,
    renal_insufficiency_diag double precision,
    emphysema_hist double precision,
    diabetes_diag double precision,
    liver_disease_diag double precision,
    esrd_prob double precision,
    chronic_bronchitis_diag double precision,
    heart_arrhythmias_diag double precision,
    renal_insufficiency_hist double precision,
    age double precision,
    sirs_raw double precision,
    sirs_hr_oor double precision,
    sirs_wbc_oor double precision,
    sirs_temperature_oor double precision,
    sirs_resp_oor double precision,
    minutes_since_any_organ_fail double precision,
    minutes_since_any_antibiotics double precision,
    acute_liver_failure double precision,
    acute_organ_failure double precision,
    any_organ_failure double precision,
    weight double precision
);