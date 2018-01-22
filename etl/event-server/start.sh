gunicorn -b 0.0.0.0:8000 event-server:app --worker-class aiohttp.GunicornUVLoopWebWorker -c gunicorn_conf.py &
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
    pyflame -s 60 -x $CHILD_PID | $CUR_DIR/utils/flamegraph.pl > $OUTFILE
    aws s3 cp $OUTFILE s3://opsdx-webservice-flamegraphs/flamegraphs-dev/event_rest_dev.$FULL_DATE.svg
    rm *.svg
  else
    echo "No gunicorn workers"
  fi
  sleep 10
done
