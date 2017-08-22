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

## Setup parallel dblink
```{sql}
CREATE EXTENSION IF NOT EXISTS dblink_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;
DROP SERVER IF EXISTS dblink_dist cascade;
CREATE SERVER dblink_dist
    FOREIGN DATA WRAPPER dblink_fdw OPTIONS (host 'dw.dev.opsdx.io', dbname 'opsdx_dev_dw');
DROP USER MAPPING IF EXISTS FOR opsdx_root SERVER dblink_dist;
CREATE USER MAPPING FOR opsdx_root SERVER dblink_dist OPTIONS (user 'opsdx_root', password 'XXXXX');
```