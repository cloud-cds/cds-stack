gunicorn -b localhost:8001 trews:app &
GUNICORN_PID=$!
echo $GUNICORN_PID
while true
do
  CHILD_PID=$(pgrep -P $GUNICORN_PID)
  if kill -0 $CHILD_PID > /dev/null 2>&1
  then
    OUTFILE=$(date -u +pyflame_out/%Y_%m_%H_%M_%S.out)
    sudo pyflame -s 10 -o $OUTFILE -x $CHILD_PID
  else
    echo "No gunicorn workers"
    sleep 10
  fi
done
