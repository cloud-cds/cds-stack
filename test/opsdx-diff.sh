host1=$1
db1=$2

host2=$3
db2=$4

pg_dump -h $host1 -U $db_user -d $db1 -p $db_port --schema-only > $db1.dump
pg_dump -h $host2 -U $db_user -d $db2 -p $db_port --schema-only > $db2.dump

createdb -h $host1 -U $db_user -p $db_port -T template0 schema_$db1
createdb -h $host2 -U $db_user -p $db_port -T template0 schema_$db1

pg_restore -h $host1 -U $db_user -d schema_$db1 -p $db_port $db1.dump
pg_restore -h $host2 -U $db_user -d schema_$db2 -p $db_port $db1.dump

