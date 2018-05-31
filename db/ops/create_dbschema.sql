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


\copy cdm_function     from 'CDM_Function.csv' with csv header delimiter as ',';
\copy cdm_feature      from 'CDM_Feature.csv' with csv header delimiter as ',';
\copy parameters       from 'ops/parameters.csv' with csv header delimiter as ',';
\copy criteria_default from 'criteria_default.csv' with csv header delimiter as ',';
\copy cdm_g from 'ops/CDM_G.csv' with csv header delimiter as ',';

DROP TABLE IF EXISTS cdm_twf;
CREATE TABLE cdm_twf (
    enc_id  integer REFERENCES pat_enc(enc_id),
    tsp     timestamptz,
    pao2 Real,pao2_c integer,hepatic_sofa Integer,hepatic_sofa_c integer,paco2 Real,paco2_c integer,abp_mean Real,abp_mean_c integer,sodium Real,sodium_c integer,obstructive_pe_shock Integer,obstructive_pe_shock_c integer,metabolic_acidosis int,metabolic_acidosis_c integer,troponin Real,troponin_c integer,rass Real,rass_c integer,sirs_raw Boolean,sirs_raw_c integer,pao2_to_fio2 Real,pao2_to_fio2_c integer,qsofa Integer,qsofa_c integer,fio2 Real,fio2_c integer,neurologic_sofa Integer,neurologic_sofa_c integer,hematologic_sofa Integer,hematologic_sofa_c integer,renal_sofa Integer,renal_sofa_c integer,nbp_sys Real,nbp_sys_c integer,sirs_hr_oor Boolean,sirs_hr_oor_c integer,resp_sofa Integer,resp_sofa_c integer,bun_to_cr Real,bun_to_cr_c integer,cmi Boolean,cmi_c integer,cardio_sofa Integer,cardio_sofa_c integer,acute_pancreatitis integer,acute_pancreatitis_c integer,wbc Real,wbc_c integer,shock_idx Real,shock_idx_c integer,weight Real,weight_c integer,platelets Real,platelets_c integer,arterial_ph Real,arterial_ph_c integer,nbp_dias Real,nbp_dias_c integer,fluids_intake_1hr Real,fluids_intake_1hr_c integer,co2 Real,co2_c integer,dbpm Real,dbpm_c integer,ddimer Real,ddimer_c integer,ast_liver_enzymes Real,ast_liver_enzymes_c integer,fluids_intake_24hr Real,fluids_intake_24hr_c integer,ptt Real,ptt_c integer,abp_sys Real,abp_sys_c integer,magnesium Real,magnesium_c integer,severe_sepsis Boolean,severe_sepsis_c integer,bicarbonate Real,bicarbonate_c integer,lipase Real,lipase_c integer,hypotension_raw Boolean,hypotension_raw_c integer,sbpm Real,sbpm_c integer,heart_rate Real,heart_rate_c integer,nbp_mean Real,nbp_mean_c integer,anion_gap Real,anion_gap_c integer,vasopressor_resuscitation Boolean,vasopressor_resuscitation_c integer,urine_output_24hr Real,urine_output_24hr_c integer,amylase Real,amylase_c integer,septic_shock_iii Integer,septic_shock_iii_c integer,hematocrit Real,hematocrit_c integer,temperature Real,temperature_c integer,sirs_wbc_oor Boolean,sirs_wbc_oor_c integer,hemoglobin_minutes_since_measurement Real,hemoglobin_minutes_since_measurement_c integer,urine_output_6hr Real,urine_output_6hr_c integer,chloride Real,chloride_c integer,spo2 Real,spo2_c integer,resp_rate Real,resp_rate_c integer,hemorrhagic_shock Integer,hemorrhagic_shock_c integer,potassium Real,potassium_c integer,acute_liver_failure Integer,acute_liver_failure_c integer,bun Real,bun_c integer,hemoglobin_change Real,hemoglobin_change_c integer,mi int,mi_c integer,hypotension_intp Boolean,hypotension_intp_c integer,calcium Real,calcium_c integer,abp_dias Real,abp_dias_c integer,acute_organ_failure Boolean,acute_organ_failure_c integer,worst_sofa Integer,worst_sofa_c integer,hemoglobin Real,hemoglobin_c integer,any_organ_failure Boolean,any_organ_failure_c integer,inr Real,inr_c integer,creatinine Real,creatinine_c integer,fluid_resuscitation Boolean,fluid_resuscitation_c integer,bilirubin Real,bilirubin_c integer,alt_liver_enzymes Real,alt_liver_enzymes_c integer,mapm Real,mapm_c integer,map Real,map_c integer,gcs Real,gcs_c integer,sirs_intp Boolean,sirs_intp_c integer,minutes_since_any_antibiotics Integer,minutes_since_any_antibiotics_c integer,fluids_intake_3hr Real,fluids_intake_3hr_c integer,sirs_temperature_oor Boolean,sirs_temperature_oor_c integer,sirs_resp_oor Boolean,sirs_resp_oor_c integer,septic_shock Integer,septic_shock_c integer,lactate Real,lactate_c integer,minutes_since_any_organ_fail Integer,minutes_since_any_organ_fail_c integer,
    meta_data json,
    PRIMARY KEY (enc_id, tsp)
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
    sepsis_onset        timestamp with time zone,
    infection1_tsp      timestamp with time zone,
    infection1_name     text,
    infection2_tsp      timestamp with time zone,
    infection2_name     text,
    orgdf_tsp           timestamp with time zone,
    vent_orgdf          real,
    creatinine_orgdf    real,
    bilirubin_orgdf     real,
    platelets_orgdf     real,
    gcs_orgdf           real,
    inr_orgdf           real,
    hypotension_orgdf   real,
    vasopressors_orgdf  real,
    lactate_orgdf       real,
    sbpm_hypotension    real,
    delta_hypotension   real,
    map_hypotension     real,
    baseline_inr        real,
    baseline_creatinine real,
    baseline_platelets  real,
    baseline_bilirubin  real,
    CONSTRAINT sep2_label_details_uq UNIQUE (dataset_id, enc_id, sepsis_onset, infection1_tsp, infection1_name, infection2_tsp, infection2_name, orgdf_tsp, vent_orgdf, creatinine_orgdf, bilirubin_orgdf, platelets_orgdf, gcs_orgdf, inr_orgdf, hypotension_orgdf, vasopressors_orgdf, lactate_orgdf, sbpm_hypotension, delta_hypotension, map_hypotension, baseline_inr, baseline_creatinine, baseline_platelets, baseline_bilirubin)
);

DROP TABLE IF EXISTS sep2_sirs;
CREATE TABLE sep2_sirs(
    label_id         integer,
    dataset_id       integer,
    enc_id           integer,
    tsp              timestamp with time zone,
    heart_rate_sirs  integer,
    resp_rate_sirs   integer,
    wbc_sirs         integer,
    temperature_sirs integer,
    CONSTRAINT sep2_sirs_uq UNIQUE (dataset_id, enc_id, tsp, heart_rate_sirs, resp_rate_sirs, wbc_sirs, temperature_sirs)
);

DROP TABLE IF EXISTS sep2_suspicion_of_infection;
CREATE TABLE sep2_suspicion_of_infection(
    label_id         integer,
    dataset_id       integer,
    enc_id           integer,
    infection1_tsp   timestamp with time zone,
    infection1_name  text,
    infection2_tsp   timestamp with time zone,
    infection2_name  text,
    created          timestamp with time zone,
    heart_rate_sirs  integer,
    resp_rate_sirs   integer,
    wbc_sirs         integer,
    temperature_sirs integer,
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
    uid              text,
    eval             json
);
