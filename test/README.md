Steps to compare dashan datasets
===========================
Database Setup
----------------------
#### Create database in opsdx
Run commands in psql:
```sql
create database test_epic2op;
create database test_c2dw;
create database test_c2dw_a;
```

#### Initialize database schema and configuration
Run commands under `dashan-db/ops`
```bash
PGPASSWORD=$db_password
psql -h db.dev.opsdx.io -U opsdx_root -d test_epic2op -p 5432 -f create_dbschema.sql
psql -h db.dev.opsdx.io -U opsdx_root -d test_epic2op -p 5432 -f create_udf.sql
```

Run commands under `dashan-db/ops/trews-model`
```bash
python deploy_model.py test_c2dw
python deploy_model.py test_c2dw_a
```

Run commands under `dashan-db/dw`
```bash
PGPASSWORD=$db_password
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw -p 5432 -f create_dbschema.sql
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw -p 5432 -f create_udf.sql
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw_a -p 5432 -f create_dbschema.sql
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw_a -p 5432 -f create_udf.sql
```

Run commands under `dashan-db/dw/clarity2dw`
```bash
PGPASSWORD=$db_password
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw -p 5432 -f create_clarity2dw.sql
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw_a -p 5432 -f create_clarity2dw.sql
```

Run commands under `dashan-db/dw/trews-model`
```bash
python deploy_model.py test_c2dw 1
python deploy_model.py test_c2dw_a 1
```

Run commands in `test_c2dw` to connect other two databases using `dblink` (PLEASE replace @@RDBPW@@ with real password)
```sql
-- This will pull tables from the operational DB into the DW.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;

DROP SERVER IF EXISTS test_c2dw_a;
CREATE SERVER test_c2dw_a
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'db.dev.opsdx.io', port '5432', dbname 'test_c2dw_a', sslmode 'require');

DROP USER MAPPING IF EXISTS FOR opsdx_root SERVER test_c2dw_a;
CREATE USER MAPPING FOR opsdx_root
  SERVER test_c2dw_a
  OPTIONS (user 'opsdx_root', password '@@RDBPW@@');

DROP SERVER IF EXISTS test_epic2op;
CREATE SERVER test_epic2op
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'db.dev.opsdx.io', port '5432', dbname 'test_epic2op', sslmode 'require');

DROP USER MAPPING IF EXISTS FOR opsdx_root SERVER test_epic2op;
CREATE USER MAPPING FOR opsdx_root
  SERVER test_epic2op
  OPTIONS (user 'opsdx_root', password '@@RDBPW@@');
```

Also, run commands in `test_c2dw_a` to connect with `test_epic2op`
```sql
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;

DROP SERVER IF EXISTS test_epic2op;
CREATE SERVER test_epic2op
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'db.dev.opsdx.io', port '5432', dbname 'test_epic2op', sslmode 'require');

DROP USER MAPPING IF EXISTS FOR opsdx_root SERVER test_epic2op;
CREATE USER MAPPING FOR opsdx_root
  SERVER test_epic2op
  OPTIONS (user 'opsdx_root', password '@@RDBPW@@');
```

Populate Data to Databases
--------------------------------------
#### Populate test_epic2op
Run command under `dashan-etl/etl/epic2op` (PLEASE double check the `db_name` is `test_epic2op` in `engine.py`)
```bash
python engine.py
```
#### Copy `pat_enc` from `test_epic2op` to both `test_c2dw` and `test_c2dw_a`
From both `test_c2dw` and `test_c2dw_a`, run followling commands in SQL:
```sql
insert into pat_enc (dataset_id, enc_id, pat_id, visit_id)
(
  select * from dblink('test_epic2op', $OPDB$
        select 1, enc_id, pat_id, visit_id from pat_enc
      $OPDB$) as pe (dataset_id int, enc_id int, pat_id text, visit_id text)
);
```
#### Populate both `test_c2dw` and `test_c2dw_a` databases
Download rpts files from Clarity using `epic-clarity-sql-queries/ccda643/ccda_all_queries_phi.sql` to a local folder and then upload it to `opsdx-dev/clarity-db-staging/<date>` (PLEASE change set the window, e.g., one week, in the queries)

Load rpt files into databases on `opsdx-dev` (PLEASE change the file path in `load_clarity.sql`)
```bash
cd /home/ubuntu/clarity-db-staging
bash rm_last_line.sh <the rpt folder>
cd /home/ubuntu/epic-clarity-sql-queries/ccda643
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw -p 5432 -f load_clarity.sql
psql -h db.dev.opsdx.io -U opsdx_root -d test_c2dw_a -p 5432 -f load_clarity.sql
```

#### Populate `test_c2dw`
config:
```python
job = {
  'reset_dataset': {
    # 'remove_pat_enc': True,
    'remove_data': True,
    'start_enc_id': '(select max(enc_id) from pat_enc)'
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      # 'plan': False,
      # 'fid': 'propofol_dose',
    },
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive': {
    # 'fid': 'cardio_sofa'
  },
}

config_args = {
  'dataset_id': 1,
  'debug': True,
  'db_name': 'test_c2dw',
  # 'db_host': 'dev.opsdx.io',
  'conf': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf'),
}
```
run `python engine.py 2>&1 | tee engine.log` under `\etl\clarity2dw`

#### Populate `test_c2dw_a` TODO
run scripts under `/zad/dashan_core/scripts/share`

### Compare `test_epic2op` with `test_c2dw`
Setup environment variables:
```bash
export db_name=test_c2dw
export cmp_remote_server=test_epic2op
```
Compare database:
```bash
python compare_cdm.py --dstdid 1 --dstmid 1
python compare_cdm.py --dstdid 1 --dstmid 1 --counts
```

Backup and restore database
---------------------------------------
#### Setup
We need to use `pg_dump` and `pg_restore` to backup and restore database. First of all, we need to install `postgresql-9.5` on our dev controller. [link](https://www.tqhosting.com/kb/617/How-to-install-PostgreSQL-95-on-Ubuntu-1404-LTS-Trusty-Tahr.html)

#### Backup
```bash
pg_dump -h db.dev.opsdx.io -U opsdx_root -d test_c2dw -p 5432 -F c -b -v -f ~/clarity-db-staging/c2dw_a/2017-04-05.sql
```

#### Restore
```bash
pg_restore --clean -h db.dev.opsdx.io -U opsdx_root -d test_c2dw_a -p 5432 -v  ~/clarity-db-staging/c2dw_a/2017-04-05.sql
```
