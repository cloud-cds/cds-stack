variable "aws_region" {}

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}

variable "behamon_log_group_name" {}
variable "behamon_log_group_arn" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}

variable "db_subnet1_id" {}
variable "db_subnet2_id" {}
variable "db_sg_id" {}

data "aws_subnet" "db_subnet1" {
  id = "${var.db_subnet1_id}"
}

data "aws_subnet" "db_subnet2" {
  id = "${var.db_subnet2_id}"
}

data "aws_security_group" "db_sg" {
  id = "${var.db_sg_id}"
}

resource "aws_lambda_function" "behamon_lambda" {

    function_name    = "${var.deploy_prefix}-behamon-lambda"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    vpc_config {
      subnet_ids         = ["${data.aws_subnet.db_subnet1.id}", "${data.aws_subnet.db_subnet2.id}"]
      security_group_ids = ["${data.aws_security_group.db_sg.id}"]
    }

    environment {
      variables {
        db_host     = "${var.db_host}"
        db_port     = "${var.db_port}"
        db_name     = "${var.db_name}"
        db_user     = "${var.db_username}"
        db_password = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_log_subscription_filter" "behamon_lambda_logfilter" {
  name            = "${var.deploy_prefix}-behamon-lambda-logfilter"
  log_group_name  = "${var.behamon_log_group_name}"
  filter_pattern  = "{ $.req.url = \"*USERID*\" }"
  destination_arn = "${aws_lambda_function.behamon_lambda.arn}"
}

resource "aws_lambda_permission" "behamon_cloudwatch_permissions" {
    statement_id  = "LogBasedExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.behamon_lambda.function_name}"
    principal     = "logs.${var.aws_region}.amazonaws.com"
    source_arn    = "${var.behamon_log_group_arn}"
}
