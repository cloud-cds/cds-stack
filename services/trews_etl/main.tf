############################
# ETL via AWS Lambda

variable "aws_trews_etl_package" {}
variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}
variable "jhapi_client_id" {}
variable "jhapi_client_secret" {}

resource "aws_iam_role" "prod_etl_lambda_role" {
    name = "opsdx-prod-role-etl-lambda"
    assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "prod_etl_lambda_policy" {
  name = "opsdx-prod-policy-etl-lambda"
  role = "${aws_iam_role.prod_etl_lambda_role.id}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [ "lambda:InvokeFunction",
                  "logs:CreateLogGroup",
                  "logs:CreateLogStream",
                  "logs:PutLogEvents",
                  "ec2:CreateNetworkInterface",
                  "ec2:DescribeNetworkInterfaces",
                  "ec2:DeleteNetworkInterface",
                  "kms:Decrypt",
                  "kms:DescribeKey",
                  "kms:GetKeyPolicy"
                  ],
      "Resource": [
        "*"
      ]
    }
  ]
}
POLICY
}


# ETL Lambda functions for production and development databases.

resource "aws_lambda_function" "prod_etl_lambda" {
    function_name    = "opsdx-prod-etl-lambda"
    handler          = "service.handler"
    filename         = "${var.aws_trews_etl_package}"
    role             = "${aws_iam_role.prod_etl_lambda_role.arn}"
    runtime          = "python2.7"
    source_code_hash = "${base64sha256(file("${var.aws_trews_etl_package}"))}"
    timeout          = 300
    environment {
      variables {
        db_host  = "${var.db_host}"
        db_port  = "${var.db_port}"
        db_name  = "${var.db_name}"
        db_user  = "${var.db_username}"
        db_password = "${var.db_password}"
	jhapi_client_id = "${var.jhapi_client_id}"
        jhapi_client_secret = "${var.jhapi_client_secret}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "prod_etl_schedule_rule" {
    name = "opsdx-prod-etl-schedule-rule"
    description = "Fires every 15 minutes"
    schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "prod_etl_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.prod_etl_schedule_rule.name}"
    target_id = "etl_lambda"
    arn       = "${aws_lambda_function.prod_etl_lambda.arn}"
}

resource "aws_lambda_permission" "prod_etl_cloudwatch_permissions" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.prod_etl_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.prod_etl_schedule_rule.arn}"
}
