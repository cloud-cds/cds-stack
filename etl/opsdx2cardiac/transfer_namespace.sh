# open ssh tunnel through opsdx dev controller
ssh -i ~/keys/tf-opsdx -N -p 22 ubuntu@controller.jh.opsdx.io -L localhost:5432:dw.jh.opsdx.io:5432

$schema=clarity_1m
$dataset_id=2

# dump relevant schema from opsdx_dev_dw
pg_dump -h dw.jh.opsdx.io -U opsdx_root --schema=schema -d opsdx_dev_dw > `$schema`.dump

# restore schema to cardiac_db_small
pg_restore -h {h} -U opsdx_root -d cardiac_db_small {d}{n}.dump
	
export db_password=cardiac_password
export clarity_workspace=namespace
export dataset_is=str(dataset_id)
export nprocs=nprocs
export num_derive_groups="8"

export min_tsp='1990-01-01'
export vacuum_temp_table='True'
export db_host='dw.jh.opsdx.io'
export db_port='5432'
export db_name='cardiac_db_small'

export feature_mapping="feature_mapping.csv,feature_mapping_cardiac.csv"

export offline_criteria_processing='False'
export extract_init='True'
export populate_patients='True'
export fillin='True'
export derive='True'

python ../etl/clarity2dw/planner.py
