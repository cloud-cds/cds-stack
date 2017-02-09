export NAME=cluster.opsdx.daiware.io
export KOPS_STATE_STORE=s3://opsdx-kops-state-store
export VPC_ID=$(terraform output -json | jq '.vpc_id.value' | sed 's/\"//g')
export NETWORK_CIDR=10.0.0.0/16

kops create cluster \
    --node-count 3 \
    --zones us-east-1b,us-east-1c,us-east-1d \
    --master-zones us-east-1b,us-east-1c,us-east-1d \
    --dns-zone opsdx.daiware.io \
    --node-size t2.medium \
    --master-size t2.medium \
    --ssh-public-key keys/tf-opsdx.pub \
    --topology=private \
    --networking=weave \
    --network-cidr="10.0.0.0/16" \
    --vpc=${VPC_ID} \
    ${NAME}

