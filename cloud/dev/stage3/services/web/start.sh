#!/bin/bash

kubectl create -f services/web/nginx-deployment.yml,services/web/nginx-svc.yml >> services/web/log

selector=$1
external_ip=""
while [ -z $external_ip ]; do
   echo "Waiting for service hostname..."
   sleep 10
   external_ip=$(kubectl get svc -l "$selector" -o jsonpath='{..hostname}')
done
echo "$external_ip" > services/web/hostname
