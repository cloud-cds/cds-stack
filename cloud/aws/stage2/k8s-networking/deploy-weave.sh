#!/bin/bash

for i in `kubectl get pods -n kube-system -l name=weave-net | grep weave-net | sed 's/\ .*//'`; do
  kubectl delete pods/$i -n kube-system
  sleep 60
done
