DROP TABLE IF EXISTS dw_version CASCADE;
CREATE TABLE dw_version (
    dataset_id      integer primary key,
    created         timestamptz,
    description     text,
    updated         timestamptz
)
diststyle all;

DROP TABLE IF EXISTS pat_enc CASCADE;
CREATE TABLE pat_enc (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          integer,
    visit_id        varchar(50) NOT NULL,
    pat_id          varchar(50) NOT NULL,
    dept_id         varchar(50),
    meta_data       varchar(max), -- json
    PRIMARY KEY     (dataset_id, enc_id),
    UNIQUE          (dataset_id, visit_id, pat_id),
    UNIQUE          (dataset_id, enc_id, visit_id, pat_id)
)
diststyle all;

DROP TABLE IF EXISTS cdm_g;
CREATE TABLE cdm_g (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    fid             varchar(50), -- REFERENCES cdm_feature(fid),
    value           varchar,
    confidence      integer,
    PRIMARY KEY     (dataset_id, fid)
)
diststyle all;

DROP TABLE IF EXISTS cdm_t;
CREATE TABLE cdm_t (
    dataset_id      integer REFERENCES dw_version(dataset_id),
    enc_id          integer,
    tsp             timestamptz,
    fid             varchar(50),
    value           varchar(8192),
    confidence      integer,
    PRIMARY KEY     (dataset_id, enc_id, tsp, fid),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id) --,
    --FOREIGN KEY     (dataset_id, fid)    REFERENCES cdm_feature(dataset_id, fid)
)
diststyle key
distkey (enc_id)
sortkey (dataset_id, enc_id, tsp);

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
    PRIMARY KEY     (dataset_id, enc_id, tsp),
    FOREIGN KEY     (dataset_id, enc_id) REFERENCES pat_enc(dataset_id, enc_id)
)
diststyle key
distkey (enc_id)
sortkey (dataset_id, enc_id, tsp);
;

DROP TABLE IF EXISTS criteria_default;
CREATE TABLE criteria_default
(
    dataset_id          integer REFERENCES dw_version(dataset_id),
    name                varchar(50),
    fid                 varchar(50),
    override_value      varchar(max), -- json
    category            varchar(50),
    primary key         (dataset_id, name, fid, category)
)
diststyle all;

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
)
diststyle all
sortkey (dataset_id, pat_id, tsp);

DROP TABLE IF EXISTS cdm_notes;
CREATE TABLE cdm_notes (
    dataset_id        integer REFERENCES dw_version(dataset_id),
    enc_id            int,
    note_id           varchar(50),
    author_type       varchar(50),
    note_type         varchar(50),
    contact_date_real numeric,
    note_status       varchar(50),
    dates             varchar(max), -- json
    note_body1        varchar(max),
    note_body2        varchar(max),
    note_body3        varchar(max),
    PRIMARY KEY (dataset_id, enc_id, note_id, author_type, note_type, contact_date_real, note_status)
)
diststyle key
distkey (enc_id);

DROP TABLE IF EXISTS cdm_window_offsets_15mins;
CREATE TABLE cdm_window_offsets_15mins
(
    window_offset   integer
)
diststyle all;


DROP TABLE IF EXISTS cdm_window_offsets_3hr;
CREATE TABLE cdm_window_offsets_3hr
(
    window_offset   integer
)
diststyle all;
