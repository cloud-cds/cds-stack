#!/bin/bash

kubectl delete -f services/web/nginx-deployment.yml,services/web/nginx-svc.yml >> services/web/log
