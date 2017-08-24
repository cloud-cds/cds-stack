#!/bin/bash

if [ ! -f k8s/kubernetes_key_name ]; then
  jq '.modules[].resources|to_entries|.[]|select(.key|startswith("aws_key_pair"))|.value.primary.id|select(startswith("kub"))' .terraform/terraform.tfstate | sed 's/\"//g' > k8s/kubernetes_key_name
else
  echo "Found k8s key... skipping extraction"
fi
