--
-- FDW setup for data warehouse Postgres instance.
-- This will pull tables from the operational DB into the DW.
CREATE EXTENSION postgres_fdw;
CREATE EXTENSION dblink;

CREATE SERVER opsdx_opdb
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '@@RDBHOST@@', port '5432', dbname '@@RDBNAME@@', sslmode 'require');

CREATE USER MAPPING FOR @@LDBUSER@@
  SERVER foreign_server
  OPTIONS (user '@@RDBUSER@@', password '@@RDBPW@@');