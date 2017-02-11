resource "aws_s3_bucket" "kops-dev" {
    bucket = "opsdx-kops-dev"
    acl = "private"

    versioning {
        enabled = true
    }
}

resource "aws_s3_bucket" "kops-prod" {
    bucket = "opsdx-kops-prod"
    acl = "private"

    versioning {
        enabled = true
    }
}

# Tensorlow container registry.
resource "aws_ecr_repository" "tensorflow" {
  name = "tensorflow"
}

# Trews-ETL container registry.
resource "aws_ecr_repository" "trews_etl" {
  name = "trews-etl"
}

# Trews-rest API container registry.
resource "aws_ecr_repository" "trews_rest_api" {
  name = "trews-rest-api"
}

# Trews-nginx API container registry.
resource "aws_ecr_repository" "trews_nginx" {
  name = "trews-nginx"
}

#############################
# Outputs

output "tensorflow_registry_url" {
  value = "${aws_ecr_repository.tensorflow.repository_url}"
}

output "trews_etl_registry_url" {
  value = "${aws_ecr_repository.trews_etl.repository_url}"
}

output "trews_rest_api_registry_url" {
  value = "${aws_ecr_repository.trews_rest_api.repository_url}"
}

output "trews_nginx_registry_url" {
  value = "${aws_ecr_repository.trews_nginx.repository_url}"
}
