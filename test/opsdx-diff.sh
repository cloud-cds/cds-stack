#!/bin/bash
PGPASSWORD=$db_password
host1=$1
db1=$2

host2=$3
db2=$4

function setup() {
  local db_host=$1
  local db_name=$2
  echo "Setup database $db_host $db_name"
  pg_dump -h $db_host -U $db_user -d $db_name -p $db_port --schema='public' --schema-only > $db_name.sql
  dropdb -h $db_host -U $db_user -p $db_port schema_$db_name
  createdb -h $db_host -U $db_user -p $db_port -T template0 schema_$db_name
  psql -h $db_host -U $db_user -d schema_$db_name -p $db_port -f $db_name.sql
}

setup $host1 $db1
setup $host2 $db2


./pgdiff.sh $host1 schema_$db1 $host2 schema_$db2