

##
# Behavior Monitor Log Extractor
#

resource "aws_lambda_function" "test_behamon_log_extractor" {

    function_name    = "${var.deploy_prefix}_test_behamon_log_extractor"
    handler          = "log_extractor.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python3.6"
    timeout          = 300

    vpc_config {
      subnet_ids         = ["${data.aws_subnet.lambda_subnet1.id}", "${data.aws_subnet.lambda_subnet2.id}"]
      security_group_ids = ["${data.aws_security_group.lambda_sg.id}"]
    }

    environment {
      variables {
        db_host     = "${var.db_host}"
        db_port     = "${var.db_port}"
        db_name     = "opsdx_test"
        db_user     = "${var.db_username}"
        db_password = "${var.db_password}"
      }
    }
}

resource "aws_lambda_permission" "test_behamon_log_extractor_permissions" {
    statement_id  = "LogBasedExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.test_behamon_log_extractor.function_name}"
    principal     = "logs.${var.aws_region}.amazonaws.com"
    source_arn    = "arn:aws:logs:us-east-1:359300513585:log-group:opsdx-web-logs-test:*"
}

resource "aws_cloudwatch_log_subscription_filter" "test_behamon_log_extractor_filter" {
  depends_on      = ["aws_lambda_permission.behamon_log_extractor_permissions"]
  name            = "${var.deploy_prefix}_test_behamon_log_extractor_filter"
  log_group_name  = "opsdx-web-logs-test"
  filter_pattern  = "{ ( ($.resp.body.q != \"null\" && $.resp.body.s != \"null\" && $.resp.body.u != \"PINGUSER\" && $.resp.body.u != \"LOADTESTUSER\") || $.resp.body.session-close != \"null\") || ($.resp.url = \"*PATID*\" && $.resp.url = \"*TSESSID*\" && $.resp.url != \"*PINGUSER*\" && $.resp.url != \"*LOADTESTUSER*\") }"
  destination_arn = "${aws_lambda_function.test_behamon_log_extractor.arn}"
}

