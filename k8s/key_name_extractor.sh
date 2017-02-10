#!/bin/bash

jq '.modules[].resources|to_entries|.[]|select(.key|startswith("aws_key_pair"))|.value.primary.id|select(startswith("kub"))' tfstate/dev/terraform.tfstate | sed 's/\"//g' > k8s/kubernetes_key_name

