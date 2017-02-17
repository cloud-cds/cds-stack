############################
# ETL via AWS Lambda

variable "deploy_prefix" {}

variable "aws_trews_etl_package" {}

variable "k8s_server_host" {}
variable "k8s_server_port" {}

variable "k8s_name" {}
variable "k8s_server" {}
variable "k8s_user" {}
variable "k8s_pass" {}
variable "k8s_cert_auth" {}
variable "k8s_cert" {}
variable "k8s_key" {}
variable "k8s_token" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}
variable "jhapi_client_id" {}
variable "jhapi_client_secret" {}

variable "TREWS_ETL_SERVER" {}
variable "TREWS_ETL_HOSPITAL" {}
variable "TREWS_ETL_HOURS" {}

resource "aws_iam_role" "etl_lambda_role" {
    name = "${var.deploy_prefix}-role-etl-lambda"
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

resource "aws_iam_role_policy" "etl_lambda_policy" {
  name = "${var.deploy_prefix}-policy-etl-lambda"
  role = "${aws_iam_role.etl_lambda_role.id}"
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

resource "aws_lambda_function" "etl_lambda" {
    function_name    = "${var.deploy_prefix}-etl-lambda"
    handler          = "service.handler"
    filename         = "${var.aws_trews_etl_package}"
    role             = "${aws_iam_role.etl_lambda_role.arn}"
    runtime          = "python2.7"
    source_code_hash = "${base64sha256(file("${var.aws_trews_etl_package}"))}"
    timeout          = 300
    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_server_port}"

        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        #kube_cert      = "${var.k8s_cert}"
        #kube_key       = "${var.k8s_key}"
        #kube_token     = "${var.k8s_token}"

        db_host  = "${var.db_host}"
        db_port  = "${var.db_port}"
        db_name  = "${var.db_name}"
        db_user  = "${var.db_username}"
        db_password = "${var.db_password}"
	      jhapi_client_id = "${var.jhapi_client_id}"
        jhapi_client_secret = "${var.jhapi_client_secret}"
        TREWS_ETL_SERVER = "${var.TREWS_ETL_SERVER}"
        TREWS_ETL_HOSPITAL = "${var.TREWS_ETL_HOSPITAL}"
        TREWS_ETL_HOURS = "${var.TREWS_ETL_HOURS}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "etl_schedule_rule" {
    name = "${var.deploy_prefix}-etl-schedule-rule"
    description = "Fires every 15 minutes"
    schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda"
    arn       = "${aws_lambda_function.etl_lambda.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.etl_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}
