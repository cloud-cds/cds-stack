export NAME=cluster.dev.opsdx.io
export KOPS_STATE_STORE=s3://opsdx-kops-dev
export VPC_ID=$(terraform output -state=dev/stage1/.terraform/terraform.tfstate -json | jq '.vpc_id.value' | sed 's/\"//g')
export NETWORK_CIDR=10.0.0.0/16
export ECRURL=$(terraform output -state=dev/stage1/.terraform/terraform.tfstate -json | jq '.registry_url.value' | sed 's/\"//g; s/\(.*\)\/.*/\1/')
