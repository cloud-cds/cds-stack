variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}

resource "aws_lambda_function" "behamon_lambda" {

    function_name    = "${var.deploy_prefix}-behamon-lambda"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

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