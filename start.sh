gunicorn -b 0.0.0.0:$PORT trews:app &
GUNICORN_PID=$!
CUR_DIR=$(pwd)
echo $GUNICORN_PID
while true
do
  CHILD_PID=$(pgrep -P $GUNICORN_PID)
  if kill -0 $CHILD_PID > /dev/null 2>&1
  then
    DATE=$(date -u +%Y_%m_%d)
    FULL_DATE=$(date -u +%Y_%m_%d_%H_%M_%S)
    OUTFILE=$CUR_DIR/$FULL_DATE.svg
    sudo pyflame -s 30 -x $CHILD_PID | $CUR_DIR/utils/flamegraph.pl > $OUTFILE
    aws s3 cp $OUTFILE s3://opsdx-flamegraph-dev/$DATE/$FULL_DATE.svg
    rm $OUTFILE
  else
    echo "No gunicorn workers"
    sleep 10
  fi
done
