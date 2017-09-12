echo "start trews-ml"

echo -n "Mounting Clarity ETL S3 bucket... "
service rsyslog start
mkdir mnt
/bin/goofys opsdx-ml-model mnt
sleep 3

echo "run training script"
python main.py