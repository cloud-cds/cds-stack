resource "aws_s3_bucket" "kops-state-store" {
    bucket = "opsdx-kops-state-store"
    acl = "private"

    versioning {
        enabled = true
    }
}

resource "aws_s3_bucket" "kops-prod-state-store" {
    bucket = "opsdx-kops-prod-state-store"
    acl = "private"

    versioning {
        enabled = true
    }
}

# Tensorlow container registry.
resource "aws_ecr_repository" "tensorflow" {
  name = "tensorflow"
}

output "tensorflow_registry_url" {
  value = "${aws_ecr_repository.tensorflow.repository_url}"
}

# Trews-ETL container registry.
resource "aws_ecr_repository" "trews_etl" {
  name = "trews-etl"
}

# Trews-rest API container registry.
resource "aws_ecr_repository" "trews_rest_api" {
  name = "trews-rest-api"
}

output "trews_etl_registry_url" {
  value = "${aws_ecr_repository.trews_etl.repository_url}"
}

output "trews_rest_api_registry_url" {
  value = "${aws_ecr_repository.trews_rest_api.repository_url}"
}
