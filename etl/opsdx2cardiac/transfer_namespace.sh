# mount rambo for space on disk
sshfs -o IdentityFile=/home/nfinkel1/keys/noam_rsa noam@rambo.isi.jhu.edu:/udata/noam /home/nfinkel1/mnt/rambo/

# open ssh tunnel through opsdx dev controller
ssh -i ~/keys/tf-opsdx -N -p 22 ubuntu@controller.jh.opsdx.io -L localhost:5432:dw.jh.opsdx.io:5432 &
TUNNEL_PID=$!

# wait for tunnel
sleep 2

# define dump file path
DUMP_PATH="/home/nfinkel1/mnt/rambo/$1.dump"

# dump relevant schema from opsdx_dev_dw
echo "Enter database password"
pg_dump -h 127.0.0.1 -U opsdx_root --schema=$1 -d opsdx_dev_dw > $(echo $DUMP_PATH)

# restore schema to cardiac_db_small
echo "Enter database password"
psql -h 127.0.0.1 -U opsdx_root -d cardiac_db_small < $(echo $DUMP_PATH)

export clarity_workspace="$1"
export dataset_id="$2"
export db_password="$3"

export nprocs='8'
export num_derive_groups='8'
export min_tsp='1990-01-01'
export vacuum_temp_table='True'
export db_host='127.0.0.1'
export db_user='opsdx_root'
export db_port='5432'
export db_name='cardiac_db_small'

export feature_mapping="feature_mapping.csv,feature_mapping_cardiac.csv"
export offline_criteria_processing='False'

python $(dirname $0)/../clarity2dw/planner.py

# close tunnel
kill $TUNNEL_PID
sudo umount /home/nfinkel1/mnt/rambo
