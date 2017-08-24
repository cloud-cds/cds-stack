############################
# k8s job launcher via AWS Lambda

resource "aws_iam_role" "behamon_lambda_role" {
    name = "behamon-lambda-role"
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

resource "aws_iam_role_policy" "behamon_lambda_policy" {
  name = "behamon-lambda-policy"
  role = "${aws_iam_role.behamon_lambda_role.id}"
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
                  "kms:GetKeyPolicy",
                  "cloudwatch:PutMetricData",
                  "ses:SendEmail",
                  "ses:SendRawEmail"
                  ],
      "Resource": [
        "*"
      ]
    }
  ]
}
POLICY
}

output "behamon_lambda_role_arn" {
  value = "${aws_iam_role.behamon_lambda_role.arn}"
}
