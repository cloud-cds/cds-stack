#!/bin/bash

aws s3 cp s3://opsdx-deployment/goofys-0.0.17 /tmp/goofys
chmod 755 /tmp/goofys
service rsyslog start
/tmp/goofys opsdx-clarity-etl-stage /mnt/

# Wait for mount.
sleep 5

if [ -d /mnt/$output_dir ]; then
  export PGPASSWORD=$db_password

  # Export tables
  for i in $export_tables; do
    echo -n "Exporting $i ... "
    time (psql -h dw.jh.opsdx.io -U opsdx_root -d opsdx_dev_dw -p 5432 -c "\copy $i to '/mnt/$output_dir/$i.csv' csv delimiter E'\t' quote E'\b'" && echo "[OK]" || echo "[Failed]")
  done
else
  echo "Failed to find output directory in /mnt/$output_dir"
  exit 1
fi
