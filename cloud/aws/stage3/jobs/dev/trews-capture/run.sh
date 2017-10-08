#!/bin/bash

# Environment variables:
# - db_host
# - db_name
# - db_port
# - db_user
# - db_password
# - output_bucket
# - output_dir
# - capture_query
# - trews_url

aws s3 cp s3://opsdx-deployment/goofys-0.0.17 /tmp/goofys
chmod 755 /tmp/goofys
service rsyslog start
/tmp/goofys $output_bucket /mnt/

# Wait for mount.
sleep 5

if [ -d /mnt/$output_dir ]; then
  export PGPASSWORD=$db_password
  save_dir=$output_dir/`date +"%m-%d-%Y-%H-%M"`

  if [ -n "$capture_query" ]; then
    if [ -n "$query_id" ]; then
      echo -n "Running capture query ... "
      time (psql -h $db_host -U $db_user -d $db_name -p $db_port -c "\copy ( $capture_query ) to '/mnt/$output_dir/$query_id.csv' csv delimiter E'\t' quote E'\b'" && echo "[OK]" || echo "[Failed]")
      test -d /mnt/$save_dir || mkdir -p /mnt/$save_dir
      for patid in `cat /mnt/$output_dir/$query_id.csv`; do
        fid=`echo $patid | sed 's/E\([0-9]\{4\}\)\([0-9]*\)/T\2\1/'`
        python /bin/chrome_screenshot.py $trews_url\&PATID=$patid /mnt/$save_dir/screen-$fid.png
      done
    else
      echo "A 'query_id' parameter must be given when capturing TREWS with a query."
      exit 1
    fi
  fi

else
  echo "Failed to find output directory in /mnt/$output_dir"
  exit 1
fi
