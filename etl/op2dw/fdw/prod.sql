--
-- FDW setup for data warehouse Postgres instance.
-- This will pull tables from the operational DB into the DW.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;

DROP SERVER IF EXISTS opsdx_prod_srv;
CREATE SERVER opsdx_prod_srv
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '@@RDBHOST@@', port '5432', dbname 'opsdx_prod', sslmode 'require');

DROP USER MAPPING IF EXISTS FOR opsdx_prod_srv_user SERVER opsdx_prod_srv;
CREATE USER MAPPING FOR opsdx_prod_srv_user
  SERVER opsdx_prod_srv
  OPTIONS (user '@@RDBUSER@@', password '@@RDBPW@@');