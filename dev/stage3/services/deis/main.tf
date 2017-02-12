variable "local_shell" {}

resource "aws_s3_bucket" "deis_builder" {
    bucket = "opsdx-deis-builder"
    acl = "private"
}

resource "aws_s3_bucket" "deis_registry" {
    bucket = "opsdx-deis-registry"
    acl = "private"
}

resource "aws_s3_bucket" "deis_database" {
    bucket = "opsdx-deis-database"
    acl = "private"
}

# Deis container registry.
resource "aws_ecr_repository" "deis" {
  name = "deis"
}

output "deis_registry_url" {
  value = "${aws_ecr_repository.deis.repository_url}"
}