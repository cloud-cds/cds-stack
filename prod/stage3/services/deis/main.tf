variable "deploy_prefix" {}
variable "local_shell" {}

resource "aws_s3_bucket" "deis_builder" {
    bucket = "${var.deploy_prefix}-deis-builder"
    acl = "private"
}

resource "aws_s3_bucket" "deis_registry" {
    bucket = "${var.deploy_prefix}-deis-registry"
    acl = "private"
}

resource "aws_s3_bucket" "deis_database" {
    bucket = "${var.deploy_prefix}-deis-database"
    acl = "private"
}

# Deis container registry.
resource "aws_ecr_repository" "deis" {
  name = "${var.deploy_prefix}-deis"
}

output "deis_registry_url" {
  value = "${aws_ecr_repository.deis.repository_url}"
}