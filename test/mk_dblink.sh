db_host_1=$1
db1=$2
db_host_2=$3
db2=$4
model_id=$5

export PGPASSWORD=$db_password


echo "create dblink from " $db2 " to " $db1
cd ${dashan_db_path}/test
sed -e "s/@@RDBHOST@@/$db_host_1/" -e "s/@@RDBPORT@@/$db_port/" -e "s/@@RDBNAME@@/$db1/" -e "s/@@RDBUSER@@/$db_user/" -e "s/@@RDBPW@@/$db_password/" -e "s/@@LDBUSER@@/$db_user/" -e "s/@@db_to_test@@/$db1/" fdw.sql > fdw.$db2.sql
psql -h $db_host_2 -U $db_user -d $db2 -p $db_port -f fdw.$db2.sql
rm fdw.$db2.sql
