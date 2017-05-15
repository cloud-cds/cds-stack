cd bin
./goofys opsdx-clarity-etl-stage $clarity_stage_mnt
# loading clarity stage file to database
./mk_load_clarity_sql.sh ${clarity_stage_mnt}/ssis/ clarity_daily csv
export PGPASSWORD=$db_password
psql -h $db_host -U $db_user -d $db_name -p $db_port -f load_clarity.clarity_daily.sql
DATE=`date +%Y-%m-%d`
mkdir ${clarity_stage_mnt}/ssis/backup/$DATE
cp ${clarity_stage_mnt}/ssis/*.csv ${clarity_stage_mnt}/ssis/backup/$DATE/
rm -f ${clarity_stage_mnt}/ssis/*.csv
cd ../../dashan-etl
nice -20 python ./etl/clarity2dw/planner.py