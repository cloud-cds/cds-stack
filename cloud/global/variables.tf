##################################
# AWS account

variable "aws_id" {}
variable "access_key" {}
variable "secret_key" {}
variable "aws_region" { default = "us-east-1" }

# DNS

variable "spf" {}
variable "dkim" {}
variable "dkim_selector" {}
