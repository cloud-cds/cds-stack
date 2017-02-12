#!/bin/bash

kubectl delete -f services/confluent/confluent.yaml >> services/confluent/log
kubectl delete pvc -l app=confluent >> services/confluent/log