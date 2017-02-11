#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <tfstate subdir, i.e., dev|prod>"
  exit 1
fi

DIR=$1
jq '.modules[].resources|to_entries|.[]|select(.key|startswith("aws_key_pair"))|.value.primary.id|select(startswith("kub"))' tfstate/$DIR/terraform.tfstate | sed 's/\"//g' > k8s/kubernetes_key_name

