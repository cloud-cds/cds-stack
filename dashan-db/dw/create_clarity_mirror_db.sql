drop table if exists clarity_flowsheet;
create table clarity_flowsheet (
  pat_id                    text,
  PAT_ENC_CSN_ID            text,
  FLO_MEAS_NAME             text,
  DISP_NAME                 text,
  FLO_MEAS_ID               text,
  ROW_TYPE                  text,
  INTAKE_TYPE               text,
  OUTPUT_TYPE               text,
  TimeTaken                 text,
  ConvertedWeightValue      text,
  Value                     text,
  UNITS                     text,
  TEMPLATE_ID               text,
  TEMPLATE_NAME             text,
  TEMPLATE_DISP_NAME        text
);

drop table if exists clarity_lab_procedures;
create table clarity_lab_procedures (
  pat_id                    text,
  PAT_ENC_CSN_ID            text,
  proc_id                   text,
  proc_name                 text,
  display_name              text,
  proc_cat_id               text,
  proc_cat_name             text,
  FrequencyOfOrder          text,
  ORDER_TIME                text,
  RESULT_TIME               text,
  ParentOrderTime           text,
  PROC_START_TIME           text,
  PROC_ENDING_TIME          text,
  ParentStarttime           text,
  ParentEndingTime          text,
  OrderStatus               text
);

drop table if exists clarity_lab_results;
create table clarity_lab_results (
  pat_id                    text,
  PAT_ENC_CSN_ID            text,
  COMPONENT_ID              text,
  BASE_NAME                 text,
  NAME                      text,
  EXTERNAL_NAME             text,
  RESULT_TIME               text,
  REFERENCE_UNIT            text,
  ResultValue               text,
  COMPONENT_COMMENT         text
);

drop table if exists clarity_mar;
create table clarity_mar (
  pat_id              text,
  PAT_ENC_CSN_ID      text,
  display_name        text,
  MEDICATION_ID       text,
  Thera_Class_c       text,
  pharm_class_c       text,
  ORDER_INST          text,
  TimeActionTaken     text,
  ActionTaken         text,
  MAR_ORIG_DUE_TM     text,
  SCHEDULED_TIME      text,
  MedRoute            text,
  Dose                text,
  MedUnit             text,
  AdminSite           text,
  INFUSION_RATE       text,
  MAR_INF_RATE_UNIT   text,
  mar_duration        text,
  MAR_DURATION_UNIT   text,
  MIN_DISCRETE_DOSE   text,
  MAX_DISCRETE_DOSE   text
);

drop table if exists clarity_med_order;
create table clarity_med_order (
  pat_id              text,
  PAT_ENC_CSN_ID      text,
  display_name        text,
  ORDER_INST          text,
  MedRoute            text,
  Dose                text,
  MedUnit             text,
  MIN_DISCRETE_DOSE   text,
  MAX_DISCRETE_DOSE   text
);

\copy clarity_flowsheet      from '/home/ubuntu/clarity-db/flowsheet.csv'     with csv header delimiter as ',';
\copy clarity_lab_procedures from '/home/ubuntu/clarity-db/lab_procedure.csv' with csv header delimiter as ',';
\copy clarity_lab_results    from '/home/ubuntu/clarity-db/lab_results.csv'   with csv header delimiter as ',';
\copy clarity_mar            from '/home/ubuntu/clarity-db/mar.csv'           with csv header delimiter as ',';
\copy clarity_med_order      from '/home/ubuntu/clarity-db/med_orders.csv'    with csv header delimiter as ',';
