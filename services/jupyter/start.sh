#!/bin/bash

kubectl create -f web/nginx-deployment.yml,web/nginx-svc.yml >> web/log

selector=$1
external_ip=""
while [ -z $external_ip ]; do
   echo "Waiting for service hostname..."
   sleep 10
   external_ip=$(kubectl get svc -l "$selector" -o jsonpath='{..hostname}')
done
echo "$external_ip" > web/hostname
