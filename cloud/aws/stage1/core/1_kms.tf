############################
# AWS Key Pair

resource "aws_key_pair" "auth" {
  key_name   = "tf-${var.deploy_prefix}-key"
  public_key = "${file(var.public_key_path)}"
}

#############################################################
# KMS key for encrypting EC2 instance operating systems.

resource "aws_kms_key" "ami_key" {
    description = "${var.deploy_stack} KMS key for AMI encryption"
    enable_key_rotation = true
}

resource "aws_kms_alias" "ami_key" {
    name = "alias/${var.deploy_prefix}-ami-encrypt"
    target_key_id = "${aws_kms_key.ami_key.key_id}"
}

################################
## Outputs

output "auth_key" {
  value = "${aws_key_pair.auth.id}"
}

output "ami_key" {
  value = "${aws_kms_key.ami_key.key_id}"
}

