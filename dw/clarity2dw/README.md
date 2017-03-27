### Steps to create clarity2dw dataset in DW

#### Create database schema (only if the DW database schema does not exist)
 - run `create_dbschema.sql`
 - run `create_udf.sql`

#### create dataset record and load default parameters
 - run `create_clarity2dw.sql` (make sure your copy paths are correct in the file)