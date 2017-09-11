module "k8s_dev" {
  source = "./k8s-dev"
  node_sg_id = "sg-68a88618"
  controller_sg_id = "sg-6592cb15"
}

module "k8s_prod" {
  source = "./k8s-prod"
  node_sg_id = "sg-f7bb9587"
  controller_sg_id = "sg-6592cb15"
}

module "k8s_dev_ml" {
  source = "./k8s-dev-ml"
  node_sg_id = "sg-55e50426"
  controller_sg_id = "sg-6592cb15"
}
