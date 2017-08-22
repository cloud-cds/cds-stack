db_host=$1
db_name=$2
dataset_id=$3
model_id=$4
dashan_db_dw_path=`pwd`
PGPASSWORD=$db_password

echo "create database" $db_host $db_name
dropdb -h $db_host -U $db_user $db_name -p $db_port
createdb -h $db_host -U $db_user $db_name -p $db_port

echo "create database schema"
cd ${dashan_db_dw_path}
psql -h $db_host -U $db_user -d $db_name -p $db_port -f create_dbschema.sql
psql -h $db_host -U $db_user -d $db_name -p $db_port -f create_udf.sql

echo "load default dataset parameters"
cd ${dashan_db_dw_path}/clarity2dw/dataset${dataset_id}
psql -h $db_host -U $db_user -d $db_name -p $db_port -f create_c2dw.sql

cd ${dashan_db_dw_path}/op2dw
psql -h $db_host -U $db_user -d $db_name -p $db_port -f create_op2dw.sql


echo "load model parameters"
cd ${dashan_db_dw_path}/trews-model
python deploy_model.py $db_name $db_host $model_id

cd ..