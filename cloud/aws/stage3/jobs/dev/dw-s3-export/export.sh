#!/bin/bash

aws s3 cp s3://opsdx-deployment/goofys-0.0.17 /tmp/goofys
chmod 755 /tmp/goofys
service rsyslog start
/tmp/goofys opsdx-clarity-etl-stage /mnt/

# Wait for mount.
sleep 5

if [ -d /mnt/$output_dir ]; then
  export PGPASSWORD=$db_password

  # Export query
  if [ -n "$export_tables" ]; then
    # Export tables
    for i in $export_tables; do
      echo -n "Exporting $i ... "
      time (psql -h $db_host -U $db_user -d $db_name -p $db_port -c "\copy $i to '/mnt/$output_dir/$i.csv' csv delimiter E'\t' quote E'\b'" && echo "[OK]" || echo "[Failed]")
    done
  elif [ -n "$export_query" ]; then
    if [ -n "$query_id" ]; then
      # Export query
      echo -n "Exporting query ... "
      time (psql -h $db_host -U $db_user -d $db_name -p $db_port -c "\copy ( $export_query ) to '/mnt/$output_dir/$query_id.csv' csv delimiter E'\t' quote E'\b'" && echo "[OK]" || echo "[Failed]")
    else
      echo "A 'query_id' parameter must be given when exporting with a query."
      exit 1
    fi
  else
    echo "No query or tables specified for exporting."
    exit 1
  fi

else
  echo "Failed to find output directory in /mnt/$output_dir"
  exit 1
fi
