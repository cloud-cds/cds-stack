# Best Practice
Deploy or update the database schema and functions on a controller instance on AWS

## Setup (or clean the database)
```{bash}
psql --host=xxx.opsdx.io --port=5432 --username=opsdx_root --dbname=opsdx_xxx -f create_dbschema.sql

psql --host=xxx.opsdx.io --port=5432 --username=opsdx_root --dbname=opsdx_xxx -f create_udf.sql
```
## Deploy new version with lose data
```{bash}
psql --host=xxx.opsdx.io --port=5432 --username=opsdx_root --dbname=opsdx_xxx -f create_udf.sql
```