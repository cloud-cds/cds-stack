db_host=$1
db1=$2
db2=$3
model_id=$4

dashan_db_path=$(dirname `pwd`)
PGPASSWORD=$db_password

echo "create database" $db1
cd $dashan_db_path/ops
sh create_db.sh $db_host $db1

echo "create database" $db2
cd $dashan_db_path/dw
sh create_db.sh $db_host $db2 $model_id

echo "create dblink" $db2
cd ${dashan_db_path}/test
sed -e "s/@@RDBHOST@@/$db_host/" -e "s/@@RDBPORT@@/$db_port/" -e "s/@@RDBNAME@@/$db1/" -e "s/@@RDBUSER@@/$db_user/" -e "s/@@RDBPW@@/$db_password/" -e "s/@@LDBUSER@@/$db_user/" -e "s/@@db_to_test@@/$db1/" fdw.sql > fdw.$db2.sql
psql -h $db_host -U $db_user -d $db2 -p $db_port -f fdw.$db2.sql
rm fdw.$db2.sql