#####################################
# S3 buckets

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

resource "aws_s3_bucket" "kops-ml" {
    bucket = "opsdx-kops-ml"
    acl = "private"

    versioning {
        enabled = true
    }
}

resource "aws_s3_bucket" "lambda-repo" {
    bucket = "opsdx-lambda-repo"
    acl = "private"

    versioning {
        enabled = true
    }
}

resource "aws_s3_bucket" "webservice-flamegraphs" {
    bucket = "opsdx-webservice-flamegraphs"
    acl = "public-read"

    lifecycle_rule {
      prefix  = "flamegraphs-dev/"
      enabled = true

      expiration {
        days = 2
      }
    }

    lifecycle_rule {
      prefix  = "flamegraphs-prod/"
      enabled = true

      expiration {
        days = 2
      }
    }

    versioning {
        enabled = true
    }
}


############################################
# ECR repositories

# Fluentd cluster-wide logging container registry.
resource "aws_ecr_repository" "opsdx_cluster_logging" {
  name = "opsdx-cluster-logging"
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

# Redash container registry.
resource "aws_ecr_repository" "redash" {
  name = "redash"
}

#############################
# Outputs

output "logging_registry_url" {
  value = "${aws_ecr_repository.opsdx_cluster_logging.repository_url}"
}

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

output "redash_registry_url" {
  value = "${aws_ecr_repository.redash.repository_url}"
}
