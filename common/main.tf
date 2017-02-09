# AWS Key Pair
resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}"
  public_key = "${file(var.public_key_path)}"
}

##############################
# Controller instance

# AMI key construction in KMS

resource "aws_kms_key" "ami_key" {
    description = "KMS key for AMI encryption"
    enable_key_rotation = true
}

resource "aws_kms_alias" "ami_key" {
    name = "alias/opsdx-ami-encrypt"
    target_key_id = "${aws_kms_key.ami_key.key_id}"
}

# AMI copying and encryption

resource "aws_ami_copy" "controller_ami" {
    name              = "opsdx_controller_ami"
    description       = "An encrypted AMI for the OpsDX controller"
    source_ami_id     = "${lookup(var.aws_base_ami, var.aws_region)}"
    source_ami_region = "${var.aws_region}"
    encrypted         = true
    kms_key_id        = "${aws_kms_key.ami_key.arn}"
    tags {
        Name = "${var.deployment_tag} Controller AMI"
    }
}

################################
## Outputs

output "auth_key" {
  value = "${aws_key_pair.auth.id}"
}

output "controller_ami" {
  value = "${aws_ami_copy.controller_ami.id}"
}

