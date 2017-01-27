#!/bin/bash

kubectl delete -f web/nginx-deployment.yml,web/nginx-svc.yml >> web/log
