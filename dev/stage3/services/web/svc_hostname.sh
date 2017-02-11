#!/bin/bash

kubectl get svc/trews-rest-svc -o json | jq '.status.loadBalancer.ingress[0].hostname' | sed 's/\"//g' > services/web/hostname
