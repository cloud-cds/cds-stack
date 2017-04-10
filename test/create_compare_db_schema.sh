export db1=daily_test_epic2op
export db2=daily_test_c2dw
export dashan_db_path=$(dirname `pwd`)
PGPASSWORD=$db_password

echo "create two database"
dropdb -h $db_host -U $db_user $db1 -p $db_port
createdb -h $db_host -U $db_user $db1 -p $db_port

dropdb -h $db_host -U $db_user $db2 -p $db_port
createdb -h $db_host -U $db_user $db2 -p $db_port

echo "create database schema"
cd ${dashan_db_path}/ops
psql -h $db_host -U $db_user -d $db1 -p $db_port -f create_dbschema.sql
psql -h $db_host -U $db_user -d $db1 -p $db_port -f create_udf.sql

echo "load model parameters"
cd ${dashan_db_path}/ops/trews-model
python deploy_model.py $db1


echo "create database schema"
cd ${dashan_db_path}/dw
psql -h $db_host -U $db_user -d $db2 -p $db_port -f create_dbschema.sql
psql -h $db_host -U $db_user -d $db2 -p $db_port -f create_udf.sql
cd ${dashan_db_path}/dw/clarity2dw
psql -h $db_host -U $db_user -d $db2 -p $db_port -f create_c2dw.sql

echo "load model parameters"
cd ${dashan_db_path}/dw/trews-model
python deploy_model.py $db2 $db_host 1

echo "create dblink"
cd ${dashan_db_path}/test
sed -e "s/@@RDBHOST@@/$db_host/" -e "s/@@RDBPORT@@/$db_port/" -e "s/@@RDBNAME@@/$db1/" -e "s/@@RDBUSER@@/$db_user/" -e "s/@@RDBPW@@/$db_password/" -e "s/@@LDBUSER@@/$db_user/" -e "s/@@db_to_test@@/$db1/" fdw.sql > fdw.$db2.sql
psql -h $db_host -U $db_user -d $db2 -p $db_port -f fdw.$db2.sql
rm fdw.$db2.sql