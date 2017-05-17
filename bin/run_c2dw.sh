#!/bin/bash

DATE=`date +%Y-%m-%d`

echo -n "Mounting Clarity ETL S3 bucket... "
cd bin
./goofys opsdx-clarity-etl-stage $clarity_stage_mnt

if [ ! -d "$clarity_stage_mnt/ssis/" ]; then
  echo "[FAILED]"
  exit 1
else
  echo "[OK]"
fi

# Check for an existing status file or stale files.
echo -n "C2DW ETL checking if etl job has already run today... "
if [ -f "$clarity_stage_mnt/ssis/status/$DATE" ]; then
  echo "[FOUND]"
  exit 0
else
  echo "[OK]"
fi

file_epoch=`find ${clarity_stage_mnt}/ssis/ -maxdepth 1 -type f | sort | head -n 1 | xargs date +%s -r`
today_epoch=`date -d "today 0" +%s`
echo -n "C2DW ETL checking file dates... "
if [ $file_epoch -le $today_epoch ]; then
  echo "[OLD]"
  exit 0
else
  echo "[NEW]"
fi

# loading clarity stage file to database
echo -n "Loading C2DW staging tables in the data warehouse... "
./mk_load_clarity_sql.sh ${clarity_stage_mnt}/ssis/ clarity_daily csv
export PGPASSWORD=$db_password
psql -h $db_host -U $db_user -d $db_name -p $db_port -f load_clarity.clarity_daily.sql
status=$?

if [ $status -ne 0 ]; then
  echo "[FAILED]"
  exit 1
else
  echo "[OK]"
fi

echo -n "Archiving extracted C2DW files on S3... "
mkdir ${clarity_stage_mnt}/ssis/backup/$DATE && \
  cp ${clarity_stage_mnt}/ssis/*.csv ${clarity_stage_mnt}/ssis/backup/$DATE/ && \
  rm -f ${clarity_stage_mnt}/ssis/*.csv
status=$?

if [ $status -ne 0 ]; then
  echo "[FAILED]"
  exit 1
else
  echo "[OK]"
fi

echo "Starting C2DW ETL into the data warehouse"
cd ../../dashan-etl
nice -20 python ./etl/clarity2dw/planner.py
status=$?

# Save status file.
echo "$status" > ${clarity_stage_mnt}/ssis/status/$DATE
echo "C2DW ETL finished, status: $?"