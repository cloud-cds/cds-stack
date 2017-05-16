#!/bin/bash


echo -n "Mounting Clarity ETL S3 bucket... "
service rsyslog start
./bin/goofys opsdx-clarity-etl-stage $clarity_stage_mnt

if [ ! -d "$clarity_stage_mnt/ssis/" ]; then
  echo "[FAILED]"
  exit 1
else
  echo "[OK]"
fi

# loading clarity stage file to database
echo -n "Loading C2DW staging tables in the data warehouse... "
cd bin
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
DATE=`date +%Y-%m-%d`
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

echo "C2DW ETL finished, status: $?"