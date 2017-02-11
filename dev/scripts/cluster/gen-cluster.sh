export NAME=cluster.dev.opsdx.io
export KOPS_STATE_STORE=s3://opsdx-kops-dev
export VPC_ID=$(terraform output -state=stage1/.terraform/terraform.tfstate -json | jq '.vpc_id.value' | sed 's/\"//g')
export NETWORK_CIDR=10.0.0.0/16

kops create cluster \
    --node-count 3 \
    --zones us-east-1d \
    --master-zones us-east-1d \
    --dns-zone dev.opsdx.io \
    --node-size t2.large \
    --master-size t2.medium \
    --ssh-public-key keys/tf-opsdx-dev.pub \
    --topology=private \
    --networking=weave \
    --network-cidr="10.0.0.0/16" \
    --vpc=${VPC_ID} \
    ${NAME}

