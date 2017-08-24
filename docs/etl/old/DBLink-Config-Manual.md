# DBLink Configuration Manual

This document describes how to configure DBLink by taking the HC_EPIC database at rambo as an example.

## Step 0. Create the DBLink folder
We define a `DBLink folder` is the place to save all configuration files for a DBLink used in the system. All DBLink folders are under [/conf](../tree/master/conf). The name of the DBLink folder should be the DBLink ID. For example, we have MIMIC and EPIC DBLink folders currently.

## Step 1. Edit dblink.conf
The dblink.conf is the first file to edit within the DBLink folder (e.g., [/conf/EPIC/dblink.conf](../tree/master/conf/EPIC/dblink.conf). You need to configure

* DBLink basic settings: e.g., DBLink_ID, DBLink_Type (scheduled or streaming), etc.
* Connection settings: e.g., host, port, user, password, etc.
* Feature extraction settings: e.g., feature mapping file, sql files, etc.

## Step 2. Edit import_patients.sql
You need to provide a SQL script to import patients. (e.g., [import_patients.sql](../tree/master/conf/EPIC/import_patients.sql)). Note that you must specify which columns are `VISIT_ID` and `PAT_ID`, e.g., 

```sql
SELECT CSN_ID visit_id, pat_id
FROM Demographics
```

## Step 3. Edit feature_mapping.csv
Each row in the feature_mapping.csv defines how to extract and transform feature values from the source database into CDM. (e.g., feature examples in [feature_mapping.csv](../tree/master/conf/EPIC/feature_mapping.csv)
Notes:
* SELECT_COLS: 
  + Single feature category: must have one column name (or multiple column names delimited be comman for json data type)
  + Multiple feature category: must have one column name to show where the value locates; meanwhile, can explicitly `line` column, otherwise, system will generate `line` value for you
  + Timestamped and TWF feature category: must explicitly specify one `value` column and one `tsp` column. (Except for json data type, all column names instead of `tsp` will be saved in json format)
  

