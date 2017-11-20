gunicorn -b 0.0.0.0:8000 server:app --worker-class aiohttp.GunicornUVLoopWebWorker -c gunicorn_conf.py &
GUNICORN_PID=$!
CUR_DIR=$(pwd)
echo $GUNICORN_PID