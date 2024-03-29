export NAME=cluster.prod.opsdx.io
export KOPS_STATE_STORE=s3://opsdx-kops-prod
export VPC_ID=$(terraform output -state=prod/stage1/.terraform/terraform.tfstate -json | jq '.vpc_id.value' | sed 's/\"//g')
export NETWORK_CIDR=10.0.0.0/16
export ECRURL=$(terraform output -state=prod/stage1/.terraform/terraform.tfstate -json | jq '.registry_url.value' | sed 's/\"//g; s/\(.*\)\/.*/\1/')
