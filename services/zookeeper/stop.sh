#!/bin/bash

kubectl delete -f services/zookeeper/zookeeper.yaml >> services/zookeeper/log
kubectl delete pvc -l app=zk >> services/zookeeper/log