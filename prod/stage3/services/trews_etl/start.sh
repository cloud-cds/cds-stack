#!/bin/bash

cd services/trews_etl/aws_lambda/
VIRTUALENVDIR=$1
LOCAL_SUM=($(md5sum service.py))
GIT_SUM=($(cat hash.txt))

if [ "$LOCAL_SUM" == "$GIT_SUM" ]
then
  echo "Lambda file up-to-date. No rebuild necessary"
else
  echo "Rebuilding lambda file with venv $VIRTUALENVDIR"
  source $VIRTUALENVDIR/bin/activate
  lambda build
  build_file=$(ls -1t dist/ | head -1)
  cp dist/$build_file dist/lambda.zip
  deactivate
  echo $(md5sum service.py)>hash.txt
fi
cd ../../../