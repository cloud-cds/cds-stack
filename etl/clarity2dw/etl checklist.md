Data Warehouse ETL Checklist
TODO: turn them into checkbox list
=============
Before ETL
----------
- [ ] Check clarity staging tables
    ```
    select * from rowcount_all('schema_name');
    ```
    Below are reference counts of HCGH-1m, HCGH-1y, and HCGH-3y

    ```
            HCGH-1m           |  cnt
    --------------------------+--------
     Demographics             |   6154
     ADT_Feed                 |  21970
     Diagnoses                |   6234
     FlowsheetValue-LDA       |      0
     FlowsheetValue           | 450520
     Labs                     | 335376
     Labs_643                 |   3031
     LDAs                     |  29951
     MedicationAdministration | 136766
     MedicalHistory           | 174289
     Notes                    | 182113
     OrderMed                 |  69315
     ProblemList              |  20334
     OrderMedHome             |   6929
     OrderProcs               | 162261
     OrderProcsImage          |   3319
     OrderProcs_643           |   7837
     flowsheet_dict           |  66184
     lab_dict                 |  44976
     lab_proc_dict            |  80359
     med_dict                 |      0
     FlowsheetValue_643       |  80509


             HCGH-1y          |   cnt
    --------------------------+----------
     Demographics             |   223945
     ADT_Feed                 |   830497
     Diagnoses                |   235862
     FlowsheetValue-LDA       |       86
     FlowsheetValue           | 13377076
     Labs                     | 12462606
     Labs_643                 |   268240
     LDAs                     |   415052
     MedicationAdministration |  5483771
     MedicalHistory           |  2369927
     OrderMed                 |  2322884
     OrderMedHome             |   253926
     OrderProcs               |  6159402
     OrderProcsImage          |   116102
     OrderProcs_643           |   360545
     ProblemList              |   327727
     flowsheet_dict           |    66110
     lab_dict                 |    44797
     lab_proc_dict            |    80163
     med_dict                 |   140014
     FlowsheetValue_643       |  1008662
     Notes                    |  9484922

        HCGH-3y               |   cnt
    --------------------------+----------
     Demographics             |   223945
     ADT_Feed                 |   830497
     Diagnoses                |   235862
     FlowsheetValue-LDA       |       86
     FlowsheetValue           | 13377076
     Labs                     | 12462606
     Labs_643                 |   268240
     LDAs                     |   415052
     MedicationAdministration |  5483771
     MedicalHistory           |  2369927
     Notes                    |  9484922
     OrderMed                 |  2322884
     OrderMedHome             |   253926
     OrderProcs               |  6159402
     OrderProcsImage          |   116102
     OrderProcs_643           |   360545
     ProblemList              |   327727
     flowsheet_dict           |    66110
     lab_dict                 |    44797
     lab_proc_dict            |    80163
     med_dict                 |   140014
     FlowsheetValue_643       |  2417836
    ```
    Note: `HCGH-1y` and `HCGH-3y` has the same clarity staging tables except `FlowsheetValue_643`.
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
- [ ] vacuum all tables used in the dataset
    ```sql
    select * from vacuum_full_analyze_cdm();
    -- it vacuums following tables:
    -- vacuum full analyze cdm_s;
    -- vacuum full analyze cdm_t;
    -- vacuum full analyze cdm_twf;
    -- vacuum full analyze cdm_notes;
    -- vacuum full analyze criteria_meas;
    -- vacuum full analyze trews;
    -- vacuum full analyze pat_enc;
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
    select * from run_cdm_stats(dataset_id, datalink_id, nprocs);
    -- tables includes: pat_enc, cdm_s, cdm_t, cdm_twf, and criteria_meas
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
  select * from cdm_feature_present(_dataset_id) order by count;
  -- there is no fid with zero count so far in HCGH datasets
  ```
- [ ] Compare the distribution for the same feature between two different datasets
    - [ ] Simple version: compare the summary of the feature distribution
      ```sql
      -- list all stats difference
      select id, cdm_table, jsonb_pretty(diff) diff, jsonb_pretty(left_stats) left, jsonb_pretty(right_stats) right from cdm_feature_diff(dataset_id_left, dataset_id_right);

      -- show top mean_diff_ratio features
      select id, cdm_table, jsonb_pretty(diff) diff, jsonb_pretty(left_stats) left, jsonb_pretty(right_stats) right from cdm_feature_diff(dataset_id_left, dataset_id_right) where diff->>'mean_diff_ratio' is not null order by diff->>'mean_diff_ratio' desc;
      ```
    - TODO advanced: do some statistical test
    - TODO plus: month level distribution comparison, e.g. # in each month, # in Jan.
    - NOTE: cover numeric features, TODO: string features
completed all steps above, the labelling can start.