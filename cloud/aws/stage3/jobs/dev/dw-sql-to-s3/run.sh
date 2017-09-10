#!/bin/bash

# Environment variables:
# - db_host
# - db_name
# - db_port
# - db_user
# - db_password
# - output_bucket
# - output_dir
# - sql_files
# - result_tables
# - cleanup_file

aws s3 cp s3://opsdx-deployment/goofys-0.0.17 /tmp/goofys
chmod 755 /tmp/goofys
service rsyslog start
/tmp/goofys $output_bucket /mnt/

# Wait for mount.
sleep 5

if [ -d /mnt/$output_dir ]; then
  export PGPASSWORD=$db_password
  script_status=0
  save_status=0

  # Export tables
  for sql_file in $sql_files; do
    echo -n "Running SQL script $sql_file ... "
    time psql -h $db_host -U $db_user -d $db_name -p $db_port -v "ON_ERROR_STOP=1" -f $sql_file
    script_status=$?
    if [ $script_status -eq 0 ]; then
      echo "[OK]"
    else
      break
      echo "[Failed]"
    fi
  done

  if [ $script_status -eq 0 ]; then
    for result_table in $result_tables; do
      echo -n "Saving results $result_table ... "
      time psql -h $db_host -U $db_user -d $db_name -p $db_port -c "\copy $result_table to '/mnt/$output_dir/$result_table.csv' csv delimiter E'\t'"
      save_status=$?
      if [ $save_status -eq 0 ]; then
        echo "[OK]"
      else
        break
        echo "[Failed]"
      fi
    done

    if [ -n "$cleanup_file" ]; then
      if [ $save_status -eq 0 ]; then
        echo -n "Running cleanup script $cleanup_file ... "
        time (psql -h $db_host -U $db_user -d $db_name -p $db_port -v "ON_ERROR_STOP=1"  -f $cleanup_file && echo "[OK]" || echo "[Failed]")
      fi
    else
      echo "Skipping cleanup, no cleanup script specifiec."
    fi
  fi

else
  echo "Failed to find output directory in /mnt/$output_dir"
  exit 1
fi
