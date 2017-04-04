rm final.zip
aws s3 rm  s3://opsdx-lambda-repo/opsdx-behavior-monitor
scp dev:/home/ubuntu/peterm/cloud/global/lambda/behavior-monitor/dist/* .
mv 2017* final.zip
chmod 755 final.zip
aws s3 cp  final.zip s3://opsdx-lambda-repo/opsdx-behavior-monitor
