############################
# ETL via AWS Lambda

variable "etl_zip" {}

resource "aws_iam_role" "etl_lambda_role" {
    name = "opsdx-role-etl-lambda"
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
  name = "opsdx-policy-etl-lambda"
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

# TODO: runtime = "nodejs4.3"
resource "aws_lambda_function" "etl_lambda" {
    function_name    = "opsdx-etl-lambda"
    handler          = "service.handler"
    filename         = "${var.etl_zip}"
    role             = "${aws_iam_role.etl_lambda_role.arn}"
    runtime          = "python2.7"
    source_code_hash = "${base64sha256(file("${var.etl_zip}"))}"
    timeout          = 300
}

resource "aws_cloudwatch_event_rule" "etl_schedule_rule" {
    name = "opsdx-etl-schedule-rule"
    description = "Fires every 15 minutes"
    schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "etl_lambda"
    arn       = "${aws_lambda_function.etl_lambda.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.etl_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}
