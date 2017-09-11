variable "aws_id" {}

#####################################
# S3 buckets

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

###########################################
# Clarity2DW extract staging

resource "aws_iam_user" "clarity_etl_stage_user" {
  name = "opsdx-clarity-etl-stage"
}

resource "aws_iam_access_key" "clarity_etl_stage_user" {
  user = "${aws_iam_user.clarity_etl_stage_user.name}"
}

resource "aws_iam_user_policy" "clarity_etl_s3_all" {
  name = "opsdx-clarity-etl-stage-policy"
  user = "${aws_iam_user.clarity_etl_stage_user.name}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::opsdx-clarity-etl-stage",
        "arn:aws:s3:::opsdx-clarity-etl-stage/*"
      ]
    }
  ]
}
EOF
}

resource "aws_s3_bucket" "clarity_etl_stage" {
    bucket = "opsdx-clarity-etl-stage"
    acl = "private"

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowClarityETLUpload",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${var.aws_id}:user/${aws_iam_user.clarity_etl_stage_user.name}"
      },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::opsdx-clarity-etl-stage",
        "arn:aws:s3:::opsdx-clarity-etl-stage/*"
      ]
    }
  ]
}
POLICY
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
