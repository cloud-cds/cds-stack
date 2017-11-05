ssh -i ~/keys/tf-opsdx -N -p 22 ubuntu@controller.jh.opsdx.io -L localhost:5432:dw.jh.opsdx.io:5432
os.environ['PGPASSWORD'] = opsdx_password
dump = "pg_dump -h {h} -U opsdx_root --schema={n} -d opsdx_dev_dw > {d}{n}.dump"
os.system(dump.format_map({'h': host, 'n': namespace, 'd': datadir}))

os.environ['PGPASSWORD'] = cardiac_password
restore = "pg_restore -h {h} -U opsdx_root -d cardiac_db_small {d}{n}.dump"
os.system(restore.format_map({'h': host, 'n': namespace, 'd': datadir}))
	
os.environ['db_password'] = cardiac_password
os.environ['clarity_workspace'] = namespace
os.environ['dataset_is'] = str(dataset_id)
os.environ['nprocs'] = nprocs
os.environ['num_derive_groups'] = "8"

os.environ['min_tsp'] = '1990-01-01'
os.environ['vacuum_temp_table'] = 'True'
os.environ['db_host'] = 'dw.jh.opsdx.io'
os.environ['db_port'] = '5432'
os.environ['db_name'] = 'cardiac_db_small'

os.environ['feature_mapping'] = "feature_mapping.csv,feature_mapping_cardiac.csv"

os.environ['offline_criteria_processing'] = 'False'
os.environ['extract_init'] = 'True'
os.environ['populate_patients'] = 'True'
os.environ['fillin'] = 'True'
os.environ['derive'] = 'True'

python ../etl/clarity2dw/planner.py
