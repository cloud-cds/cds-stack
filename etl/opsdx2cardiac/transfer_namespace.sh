# open ssh tunnel through opsdx dev controller
ssh -i ~/keys/tf-opsdx -N -p 22 ubuntu@controller.jh.opsdx.io -L localhost:5432:dw.jh.opsdx.io:5432 &
TUNNEL_PID=$!

# mount rambo for space on disk
sshfs -o IdentityFile=~/keys/noam_rsa noam@rambo.isi.jhu.edu:/udata/noam ~/mnt/rambo/ &
MOUNT_ID=$!

# define dump file path
DUMP_PATH="~/mnt/rambo/$1.dump"

# dump relevant schema from opsdx_dev_dw
pg_dump -h 127.0.0.1 -U opsdx_root --schema=schema -d opsdx_dev_dw > $(echo $DUMP_PATH)

# restore schema to cardiac_db_small
pg_restore -h 127.0.0.1 -U opsdx_root -d cardiac_db_small $(echo $DUMP_PATH)

export clarity_workspace=$1
export dataset_is=$2
export nprocs="8"
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

# close tunnel
kill $TUNNEL_PID

# unmount rambo
kill $MOUNT_ID
