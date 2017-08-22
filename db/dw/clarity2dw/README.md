### Steps to create clarity2dw dataset in prod DW

#### Settings
 - dataset_id: 1
 - model_id: 1

#### Create database schema (only if the DW database schema does not exist)
 - run `psql -h $dw_host -U opsdx_root -d $dw_name -p 5432 -f create_dbschema.sql`
 - run `psql -h $dw_host -U opsdx_root -d $dw_name -p 5432 -f create_udf.sql`

#### create dataset record and load default parameters
 - define the dataset_id and model_id in create_c2dw.sql and all CDM csv files. make sure the paths are as you intend
 - run `psql -h $dw_host -U opsdx_root -d $dw_name -p 5432 -f create_clarity2dw.sql` (make sure your copy paths are correct in the file)