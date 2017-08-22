DROP TABLE IF EXISTS test_bedded_patients;
DROP TABLE IF EXISTS test_flowsheet;
DROP TABLE IF EXISTS test_lab_orders;
DROP TABLE IF EXISTS test_lab_procedures;
DROP TABLE IF EXISTS test_lab_results;
DROP TABLE IF EXISTS test_med_admin;
DROP TABLE IF EXISTS test_med_orders;
DROP TABLE IF EXISTS test_location_history;
DROP TABLE IF EXISTS test_active_procedures;

CREATE TABLE test_bedded_patients (
    index bigint,
    pat_id text,
    admittime text,
    visit_id text,
    age bigint,
    gender text,
    diagnosis text,
    history text,
    problem text,
    hospital text
);

CREATE TABLE test_flowsheet (
    index text,
    unit text,
    tsp text,
    value double precision,
    pat_id text,
    visit_id text,
    fid text
);

CREATE TABLE test_lab_orders (
    index bigint,
    visit_id text,
    pat_id text,
    order_id text,
    status text,
    tsp text,
    fid text
);

CREATE TABLE test_lab_procedures (
    index bigint,
    collection_time text,
    visit_id text,
    collection_date text,
    pat_id text,
    result_time text,
    notes text,
    result_date text,
    order_id text,
    component_id text,
    status text,
    fid text
);

CREATE TABLE test_lab_results (
    index text,
    value text,
    unit text,
    pat_id text,
    visit_id text,
    tsp text,
    fid text
);

CREATE TABLE test_med_admin (
    index bigint,
    tsp text,
    fid text,
    full_name text,
    action text,
    dose_value text,
    dose_unit text,
    rate_value text,
    rate_unit text,
    pat_id text,
    visit_id text
);

CREATE TABLE test_med_orders (
    index bigint,
    tsp text,
    fid text,
    full_name text,
    friendly_name text,
    dose double precision,
    dose_unit text,
    frequency text,
    ids text,
    pat_id text,
    visit_id text
);

CREATE TABLE test_location_history (
    index bigint,
    pat_id   text,
    tsp      text,
    visit_id text,
    value    text,
    fid      text
);

CREATE TABLE test_active_procedures (
 INDEX bigint,
 pat_id   text,
 visit_id text,
 tsp      text,
 order_id text,
 fid      text,
 status   text
);

CREATE INDEX ix_test_bedded_patients_index ON test_bedded_patients USING btree (index);

CREATE INDEX ix_test_flowsheet_index ON test_flowsheet USING btree (index);

CREATE INDEX ix_test_lab_orders_index ON test_lab_orders USING btree (index);

CREATE INDEX ix_test_lab_procedures_index ON test_lab_procedures USING btree (index);

CREATE INDEX ix_test_lab_results_index ON test_lab_results USING btree (index);

CREATE INDEX ix_test_med_admin_index ON test_med_admin USING btree (index);

CREATE INDEX ix_test_med_orders_index ON test_med_orders USING btree (index);

CREATE INDEX ix_test_location_history_index ON test_location_history USING btree (index);

CREATE INDEX ix_test_active_procedures_index ON test_active_procedures USING btree (index);

