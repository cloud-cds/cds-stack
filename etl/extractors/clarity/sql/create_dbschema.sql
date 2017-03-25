/*
 * create_dbschema.sql 
 * create relation database for the dashan instance
 * NOTE: cdm_twf is not created here
 */
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


CREATE TABLE pat_enc (
    enc_id          serial PRIMARY KEY,
    visit_id        varchar(50) NOT NULL,
    pat_id          varchar(50) NOT NULL,
    dept_id         varchar(50)
);

CREATE TABLE cdm_function (
    func_id         varchar(50) PRIMARY KEY,
    func_type       varchar(20) NOT NULL,
    description     text        NOT NULL,
    CHECK (func_type SIMILAR TO 'transform|fillin|derive')  
);

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
    CHECK (category SIMILAR TO 'S|M|T|TWF|G')
);

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

CREATE TABLE cdm_g (
    fid             varchar(50), -- REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer
);

CREATE UNIQUE INDEX cdm_g_idx ON cdm_g (fid);

CREATE TABLE cdm_s (
    enc_id          integer REFERENCES pat_enc(enc_id),
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (enc_id, fid)
);

CREATE TABLE cdm_m (
    enc_id          integer REFERENCES pat_enc(enc_id),
    fid             varchar(50) REFERENCES cdm_feature(fid),
    line            smallint,
    value           text,
    confidence      integer,
    PRIMARY KEY (enc_id, fid, line)
);

CREATE TABLE cdm_t (
    enc_id          integer REFERENCES pat_enc(enc_id),
    tsp             timestamp,
    fid             varchar(50) REFERENCES cdm_feature(fid),
    value           text,
    confidence      integer,
    PRIMARY KEY (enc_id, tsp, fid)
);



