--
-- FDW setup for data warehouse Postgres instance.
-- This will pull tables from the operational DB into the DW.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;

DROP SERVER IF EXISTS opsdx_dev_dw_srv;
CREATE SERVER opsdx_dev_dw_srv
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '@@RDBHOST@@', port '5432', dbname 'opsdx_dw', sslmode 'disable');

DROP USER MAPPING IF EXISTS FOR @@LDBUSER@@ SERVER opsdx_dev_dw_srv;
CREATE USER MAPPING FOR @@LDBUSER@@
  SERVER opsdx_dev_dw_srv
  OPTIONS (user '@@RDBUSER@@', password '@@RDBPW@@');
