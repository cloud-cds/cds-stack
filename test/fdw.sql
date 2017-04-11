--
-- FDW setup for data warehouse Postgres instance.
-- This will pull tables from the operational DB into the DW.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;

DROP SERVER IF EXISTS @@db_to_test@@ cascade;
CREATE SERVER @@db_to_test@@
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '@@RDBHOST@@', port '@@RDBPORT@@', dbname '@@RDBNAME@@', sslmode 'require');

DROP USER MAPPING IF EXISTS FOR @@LDBUSER@@ SERVER @@db_to_test@@;
CREATE USER MAPPING FOR @@LDBUSER@@
  SERVER @@db_to_test@@
  OPTIONS (user '@@RDBUSER@@', password '@@RDBPW@@');