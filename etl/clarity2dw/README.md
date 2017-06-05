Data Warehouse ETL Checklist
=============
Before ETL
----------
1. check clarity staging tables
    ```
    select * from rowcount_all(schema_name);
    ```
2. check CDM config tables
    - compare the pending dataset with a stable dataset, e.g.,
    ```sql
    select * from dataset_config_compare(dataset_id_pending, dataset_id_stable)
    ```
    It should return 0 rows
3. clean all data in the dataset before ETL
    ```sql
    select * from delete_dataset(dataset_id);
    ```
4. vacuum all tables used in the dataset
    ```sql
    select * from vacuum_full_cdm();
    ```
5. If above all passed, then start ETL

After ETL
---------
1. Did the ETL complete successfully?
    - check the ETL log
    - view the ETL graph (all tasks should be green)
2. run CDM statistic function
    ```sql
    select * from run_cdm_stats(dataset_id, datalink_id, nprocs);
    ```
3. check is any value is null from `cdm_twf` table
    ```sql
    select * from cdm_stats where dataset_id = this_dataset_id and cdm_table = 'cdm_twf' and (stats->>'cnt_null')::int <> 0;
    ```
    Note that it should only return one row for `hemoglobin_minutes_since_measurement`