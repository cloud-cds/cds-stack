db_host=$1
db_name=$2
dataset_id=$3
dashan_dw_clarity2sw_path=`pwd`
PGPASSWORD=$db_password

echo "load default dataset parameters"
cd ${dashan_dw_clarity2sw_path}/dataset${dataset_id}
psql -h $db_host -U $db_user -d $db_name -p $db_port -f create_c2dw.sql

cd ..