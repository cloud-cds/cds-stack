Data Warehouse ETL Checklist V0.2
=============
Extract clarity staging tables
---------------------------
- [ ] Run extraction queries on SQL Sever 2012
  - [ ] Check the queries to make sure the `departmentid` and `date range` are correct
  - [ ] Also check other constraints, e.g., `age`
  - [ ] Run it in SQLCMD mode
  - [ ] Make sure headers are not included (if they are included, please turn it off and restart SQL Server)
  - [ ] Files larger than 4gb have to be broken up and extracted in chunks
- [ ] Remove the last lines for each files
  ```bash
  sh rm_last_lines.sh $src_folder $dst_folder
  ```
- [ ] Upload to S3

Load clarity staging tables
---------------------------
- [ ] Mount S3 through goofys
- [ ] Generate SQL query for loading staging
  ```bash
  cd $dashan-db/dw/test
  ./mk_load_clarity_sql.sh $mounted_s3_folder $clarity_workspace $ext
  # the sql file will be named load_clarity.$clarity_workspace.sql
  ```
- [ ] Load files to database
  ```bash
  psql -h $dw_host -U opsdx_root -d $dw_name -p 5432 -f load_clarity.$clarity_workspace.sql
  ```
  NOTE: make sure no `ERROR` in the output while loading

Before ETL
----------
- [ ] Check clarity staging tables
  - [ ] Check counts
    ```
    select * from rowcount_all('schema_name');
    ```
    Below are reference counts of HCGH-1m, HCGH-1y, and HCGH-3y

    ```
    JHH-10d: 2017-06-16
            table_name        |  cnt
    --------------------------+--------
     ADT_Feed                 |  30575
     Demographics             |  14417
     Diagnoses                |  14546
     FlowsheetValue           | 552850
     FlowsheetValue-LDA       |     18
     FlowsheetValue_643       | 131375
     LDAs                     |  70340
     Labs                     | 438899
     Labs_643                 |   3647
     MedicalHistory           | 866701
     MedicationAdministration | 180605
     Notes                    | 182850
     OrderMed                 |  79419
     OrderMedHome             |   6404
     OrderProcs               | 230743
     OrderProcsImage          |   8195
     OrderProcs_643           |    167
     ProblemList              |  66925
     cdm_twf_temp_0           | 123538
     cdm_twf_temp_1           | 123538
     cdm_twf_temp_2           | 123538
     cdm_twf_temp_3           | 123538
     cdm_twf_temp_4           | 123538
     cdm_twf_temp_5           | 123538
     cdm_twf_temp_6           | 123538
     cdm_twf_temp_7           | 123538
     flowsheet_dict           |  66208
     lab_dict                 |  45007
     lab_proc_dict            |  80379
     med_dict                 | 140653
    (30 rows)

    BMC-10d: 2017-06-16
            table_name        |  cnt
    --------------------------+--------
     ADT_Feed                 |   8641
     Demographics             |   3986
     Diagnoses                |   3540
     FlowsheetValue           | 197613
     FlowsheetValue-LDA       |      0
     FlowsheetValue_643       |  36967
     LDAs                     |  20532
     Labs                     | 152028
     Labs_643                 |   1200
     MedicalHistory           | 404722
     MedicationAdministration |  73387
     Notes                    |  94444
     OrderMed                 |  29269
     OrderMedHome             |   2590
     OrderProcs               |  79967
     OrderProcsImage          |   2530
     OrderProcs_643           |      0
     ProblemList              |  28417
     cdm_twf_temp_0           |  52398
     cdm_twf_temp_1           |  52398
     cdm_twf_temp_2           |  52398
     cdm_twf_temp_3           |  52398
     cdm_twf_temp_4           |  52398
     cdm_twf_temp_5           |  52398
     cdm_twf_temp_6           |  52398
     cdm_twf_temp_7           |  52398
     flowsheet_dict           |  66283
     lab_dict                 |  45007
     lab_proc_dict            |  80382
     med_dict                 | 140662
    (30 rows)

    JHH-1m: 2017-06-16
            table_name        |   cnt
    --------------------------+---------
     ADT_Feed                 |  100961
     Demographics             |   45960
     Diagnoses                |   46195
     FlowsheetValue           | 1636762
     FlowsheetValue-LDA       |      61
     FlowsheetValue_643       |  380562
     LDAs                     |  152960
     Labs                     | 1299961
     Labs_643                 |   10232
     MedicalHistory           | 2001458
     MedicationAdministration |  527770
     Notes                    |  540585
     OrderMed                 |  249386
     OrderMedHome             |   20395
     OrderProcs               |  698583
     OrderProcsImage          |   25477
     OrderProcs_643           |     318
     ProblemList              |  159316
     cdm_twf_temp_0           |  371711
     cdm_twf_temp_1           |  371711
     cdm_twf_temp_2           |  371711
     cdm_twf_temp_3           |  371711
     cdm_twf_temp_4           |  371711
     cdm_twf_temp_5           |  371711
     cdm_twf_temp_6           |  371711
     cdm_twf_temp_7           |  371711
     flowsheet_dict           |   66283
     lab_dict                 |   45018
     lab_proc_dict            |   80383
     med_dict                 |  140666
    (30 rows)

    JHH-1y: 2017-07-09
            table_name        |   cnt
    --------------------------+----------
     ADT_Feed                 |  1674664
     Demographics             |   574518
     Diagnoses                |   526971
     FlowsheetValue           | 27687483
     FlowsheetValue-LDA       |      402
     FlowsheetValue_643       |  6392482
     LDAs                     |  1525245
     Labs                     | 25820838
     Labs_643                 |   263876
     MedicalHistory           | 21697119
     MedicationAdministration |  8873934
     Notes                    |  4981415
     OrderMed                 |  3416801
     OrderMedHome             |   279985
     OrderProcs               | 11911209
     OrderProcsImage          |   376554
     OrderProcs_643           |    29033
     ProblemList              |  1782261
     cdm_twf_temp_0           |  3718865
     cdm_twf_temp_1           |  3718865
     cdm_twf_temp_2           |  3718865
     cdm_twf_temp_3           |  3718865
     cdm_twf_temp_4           |  3718865
     cdm_twf_temp_5           |  3718865
     cdm_twf_temp_6           |  3718865
     cdm_twf_temp_7           |  3718865
     flowsheet_dict           |    66282
     lab_dict                 |    45022
     lab_proc_dict            |    80382
     med_dict                 |        0
    (30 rows)


    BMC-1m: 2017-06-16
            table_name        |  cnt
    --------------------------+--------
     ADT_Feed                 |  27823
     Demographics             |  12838
     Diagnoses                |  11254
     FlowsheetValue           | 611207
     FlowsheetValue-LDA       |      1
     FlowsheetValue_643       | 110673
     LDAs                     |  49208
     Labs                     | 481892
     Labs_643                 |   3665
     MedicalHistory           | 965019
     MedicationAdministration | 215281
     Notes                    | 294651
     OrderMed                 |  92556
     OrderMedHome             |   8052
     OrderProcs               | 249476
     OrderProcsImage          |   8589
     OrderProcs_643           |      0
     ProblemList              |  69582
     cdm_twf_temp_0           | 163961
     cdm_twf_temp_1           | 163961
     cdm_twf_temp_2           | 163961
     cdm_twf_temp_3           | 163961
     cdm_twf_temp_4           | 163961
     cdm_twf_temp_5           | 163961
     cdm_twf_temp_6           | 163961
     cdm_twf_temp_7           | 163961
     flowsheet_dict           |  66283
     lab_dict                 |  45018
     lab_proc_dict            |  80383
     med_dict                 | 140666
    (30 rows)

    BMC-1y
    2017-06-20
            table_name        |   cnt
    --------------------------+---------
     ADT_Feed                 |  320768
     Demographics             |  141884
     Diagnoses                |  124993
     FlowsheetValue           | 8389511
     FlowsheetValue-LDA       |      53
     FlowsheetValue_643       | 1415430
     LDAs                     |  434332
     Labs                     | 6005458
     Labs_643                 |   54737
     MedicalHistory           | 8407928
     MedicationAdministration | 3115906
     Notes                    | 3562020
     OrderMed                 | 1122119
     OrderMedHome             |   99027
     OrderProcs               | 3022851
     OrderProcsImage          |   87905
     OrderProcs_643           |    2358
     ProblemList              |  623329
     flowsheet_dict           |   66815
     lab_dict                 |   45043
     lab_proc_dict            |   80408
     med_dict                 |       0
    (22 rows)


    HCGH-1m: 2017-06-16
            table_name        |  cnt
    --------------------------+--------
     ADT_Feed                 |  21970
     Demographics             |   6154
     Diagnoses                |   6234
     FlowsheetValue           | 456231
     FlowsheetValue-LDA       |      0
     FlowsheetValue_643       |  80509
     LDAs                     |  29951
     Labs                     | 335376
     Labs_643                 |   3031
     MedicalHistory           | 174289
     MedicationAdministration | 136766
     Notes                    | 182113
     OrderMed                 |  69315
     OrderMedHome             |   6929
     OrderProcs               | 162261
     OrderProcsImage          |   3319
     OrderProcs_643           |   7837
     ProblemList              |  20334
     cdm_twf_temp_0           | 119241
     cdm_twf_temp_1           | 119241
     cdm_twf_temp_2           | 119241
     cdm_twf_temp_3           | 119241
     cdm_twf_temp_4           | 119241
     cdm_twf_temp_5           | 119241
     cdm_twf_temp_6           | 119241
     cdm_twf_temp_7           | 119241
     flowsheet_dict           |  66184
     lab_dict                 |  44976
     lab_proc_dict            |  80359
     med_dict                 |      0


    HCGH-1y(3y): 2017-06-16
            table_name        |   cnt
    --------------------------+----------
     ADT_Feed                 |   830497
     Demographics             |   223945
     Diagnoses                |   235862
     FlowsheetValue           | 13377076
     FlowsheetValue-LDA       |       86
     FlowsheetValue_643       |  2417836
     LDAs                     |   415052
     Labs                     | 12462606
     Labs_643                 |   268240
     MedicalHistory           |  2369927
     MedicationAdministration |  5483771
     Notes                    |  9484922
     OrderMed                 |  2322884
     OrderMedHome             |   253926
     OrderProcs               |  6159402
     OrderProcsImage          |   116102
     OrderProcs_643           |   360545
     ProblemList              |   327727
     cdm_twf_temp_0           |  4344791
     cdm_twf_temp_1           |  4344791
     cdm_twf_temp_2           |  4344791
     cdm_twf_temp_3           |  4344791
     cdm_twf_temp_4           |  4344791
     cdm_twf_temp_5           |  4344791
     cdm_twf_temp_6           |  4344791
     cdm_twf_temp_7           |  4344791
     flowsheet_dict           |    66110
     lab_dict                 |    44797
     lab_proc_dict            |    80163
     med_dict                 |   140014
    (30 rows)
    ```
    Note: `HCGH-1y` and `HCGH-3y` has the same clarity staging tables except `FlowsheetValue_643`.
  - [ ] Check statistical summary (optional)
    ```
    select * from run_clarity_stats('schema_name');
    select * from clarity_stats_view;
    ```

- [ ] Create dataset
  - [ ] Create dataset folder under `$dashan-db/dw/clarity2dw/`, e.g., `dataset99`.
  - [ ] Load the dataset configuration to the data warehouse.
    ```bash
    cd $dashan-db/dw/clarity2dw/dataset99
    psql -h $dw_host -U opsdx_root -d $dw_name -p 5432 -f create_c2dw.sql
    ```
- [ ] Check CDM config tables
    Compare the pending dataset with a stable dataset, e.g.,
    ```sql
    select * from dataset_config_compare(dataset_id_pending, dataset_id_stable)
    -- covered tables: cdm_feature, cdm_function, parameters, criteria_default
    ```
    It should return 0 rows
- [ ] Clean all data in the dataset before ETL
    ```sql
    select * from delete_dataset(_dataset_id);
    -- it runs following queries:
    -- delete from cdm_s where dataset_id = _dataset_id;
    -- delete from cdm_t where dataset_id = _dataset_id;
    -- delete from cdm_twf where dataset_id = _dataset_id;
    -- delete from cdm_notes where dataset_id = _dataset_id;
    -- delete from criteria_meas where dataset_id = _dataset_id;
    -- delete from trews where dataset_id = _dataset_id;
    -- delete from pat_enc where dataset_id = _dataset_id;
    ```
- [ ] Vacuum all tables used in the dataset
    ```sql
    vacuum full analyze cdm_s;
    vacuum full analyze cdm_t;
    vacuum full analyze cdm_twf;
    vacuum full analyze cdm_notes;
    vacuum full analyze criteria_meas;
    vacuum full analyze trews;
    vacuum full analyze pat_enc;
    ```
If above all passed, then start ETL

During ETL
----------
- [ ] Periodically check is the ETL still progressing
  - Check ETL engine status (e.g. `2017-06-03 03:56:00,274|engine-c2dw|32585-140671924950848|INFO|Engine progress (iter 3): 4 / 76 tasks completed` means 4 tasks completed and 72 tasks remain)
  - Transform tasks:
    - Number of rows extracted: e.g. `2017-06-03 03:56:00,370|transform_task_1|32593-140671924950848|INFO|extracted 6000 rows for fid wbc`
    - Number of rows loaded: e.g., `2017-06-03 03:56:01,015|transform_task_0|32596-140671924950848|INFO|loading chuck 0` (2000 rows per chuck by default)
  - Derive tasks:
    - Check CPU usage: at lease one CPU should be fully used during deriving process
    - Check `pg_stat_activity`
      ```sql
      select * from pg_stat_activity where state = 'active';
      -- the derive query should be an active process in the result
      ```
      NOTE: Kill the ETL if it was over one hour. Currently, all our derive functions should not execute longer than one hour. If it is, there should be something wrong. We can kill the ETL by `Ctrl-C` and terminate the backend process in the database `select * from pg_terminate_backend('pid');`
    - Retries: Derive task will keep retry if any error happens. If it is deadlock error, e.g. `2017-05-31 19:52:46,400|derive_pao2_to_fio2|3818-139668207580992|ERROR|PSQL Error derive: pao2_to_fio2 deadlock detected`, then it is okay to wait until it finishes; otherwise, any other error starting with 'PSQL Error derive' is a bug and need to be fixed and restart ETL.

After ETL
---------
- [ ] Did the ETL complete successfully?
    - [ ] Check the ETL log
        - [ ] ETL engine should complete with message `2017-06-01 01:50:42,211|planner-c2dw|3809-139668207580992|INFO|job completed`
        - [ ] Check if any derive feature failed to run: search `dismatch`, e.g., `2017-05-31 19:59:55,243|derive_any_beta_blocker|3817-139668207580992|ERROR|fid_input dismatch`
    - [ ] Check the ETL graph: all tasks should be green (by default the graph file is under the local directory called `etl_graph.pdf`; we can specify the name of the file by using environment varialbe `etl_graph`)
      + Green tasks means succeed
      + Yellow tasks means failed
      + Blank tasks means they did not run
- [ ] Run CDM statistic function
    ```sql
    delete from cdm_stats where dataset_id = _dataset_id
    select * from run_cdm_stats(_dataset_id, server, nprocs);
    -- server = 'dev_dw' in opsdx_dev_dw; sever = 'prod_dw' in opsdx_prod_dw
    -- tables includes: pat_enc, cdm_s, cdm_t, cdm_twf, and criteria_meas
    ```
- [ ] Vacuum all tables used in the dataset (optional)
    ```sql
    vacuum full analyze cdm_s;
    vacuum full analyze cdm_t;
    vacuum full analyze cdm_twf;
    vacuum full analyze cdm_notes;
    vacuum full analyze criteria_meas;
    vacuum full analyze trews;
    vacuum full analyze pat_enc;
    ```
- [ ] View the statistics
    ```sql
    select * from cdm_stats_view where dataset_id = this_dataset_id
    ```
- [ ] Check is any value is null from `cdm_twf` table
    ```sql
    select * from cdm_stats where dataset_id = this_dataset_id and cdm_table = 'cdm_twf' and (stats->>'cnt_null')::int <> 0;
    ```
    Note that it should only return one row for `hemoglobin_minutes_since_measurement`
- [ ] Check if all the features are present? list the non-present features
  ```sql
  select * from cdm_feature_present(_dataset_id) order by count,id;
  ```
- [ ] Compare the distribution for the same feature between two different datasets
    - [ ] Simple version: compare the summary of the feature distribution
      ```sql
      -- list all stats difference
      select id, cdm_table, jsonb_pretty(diff) diff
      from cdm_feature_diff(dataset_id_left, dataset_id_right)
      where (diff->>'mean_diff_ratio')::numeric > 0.01 order by diff->>'mean_diff_ratio' desc;
      -- show top mean_diff_ratio features
      select id, cdm_table,
             jsonb_pretty(diff) diff,
             jsonb_pretty(left_stats) left_stats,
             jsonb_pretty(right_stats) right_stats
      from cdm_feature_diff(dataset_id_left, dataset_id_right)
      where (diff->>'mean_diff_ratio')::numeric > 0.01 order by diff->>'mean_diff_ratio' desc;
      ```