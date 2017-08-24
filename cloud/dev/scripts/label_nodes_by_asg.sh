#!/bin/bash

# Labeling
if [ $# -lt 1 ]; then
  echo "Usage: $0 <on|off> [tag]"
  exit 1
fi

TAG=${2:-opsdx/role}

case "$1" in
  on)
     aws ec2 describe-instances --filter Name=instance-state-name,Values=running | jq --arg ltag "$TAG" '[.Reservations|.[]|.Instances[0]|{key: {instance: .InstanceId, dns: .PrivateDnsName}, value: .Tags|.[]|select(.Key == "aws:autoscaling:groupName" and (.Value|startswith("master")|not)).Value|gsub(".cluster.opsdx.daiware.io"; "")}]|group_by(.value)|map(. as $group | map("kubectl label nodes " + .key.dns + " " + $ltag + "=" + $group[0].value))|flatten|.[]' | sed 's/\"//g' > foo
     ./foo
     rm foo
     ;;
  off)
     aws ec2 describe-instances --filter Name=instance-state-name,Values=running | jq --arg ltag "$TAG" '[.Reservations|.[]|.Instances[0]|{key: {instance: .InstanceId, dns: .PrivateDnsName}, value: .Tags|.[]|select(.Key == "aws:autoscaling:groupName" and (.Value|startswith("master")|not)).Value|gsub(".cluster.opsdx.daiware.io"; "")}]|group_by(.value)|map(map("kubectl label nodes " + .key.dns + " " + $ltag + "-"))|flatten|.[]' | sed 's/\"//g' > foo
     ./foo
     rm foo
     ;; 
  *)
     echo "Invalid action: $1"
     echo "Usage: $0 <on|off>"
     exit 1
     ;;
esac
