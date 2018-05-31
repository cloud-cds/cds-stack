#######################################
# Encrypted AMI construction.

resource "aws_ami_copy" "controller_ami" {
    name              = "${var.deploy_prefix}-controller-ami"
    description       = "An encrypted AMI for the MC controller"
    source_ami_id     = "${lookup(var.aws_base_ami, var.aws_region)}"
    source_ami_region = "${var.aws_region}"
    encrypted         = true
    kms_key_id        = "${aws_kms_key.ami_key.arn}"
    tags {
        Name = "${var.deploy_name}"
        Stack = "${var.deploy_stack}"
        Component = "Controller AMI"
    }
}

##############################
# Controller instance

# Our controller security group, to access the instance over SSH.
resource "aws_security_group" "controller_sg" {
  name        = "${var.deploy_prefix}-controller-sg"
  description = "MC Controller SG"
  vpc_id      = "${aws_vpc.main.id}"

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Unrestricted outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Controller Security Group"
  }
}


resource "aws_instance" "cluster_controller" {
  connection {
    type        = "ssh"
    user        = "ubuntu"
    agent       = false
    private_key = "${file(var.private_key_path)}"
  }

  instance_type = "t2.medium"
  count         = "1"

  # Lookup the correct AMI based on the region we specified
  ami = "${aws_ami_copy.controller_ami.id}"

  # The name of our SSH keypair we created above.
  key_name = "${aws_key_pair.auth.id}"

  # Block device specifications
  root_block_device {
    volume_size = 100
  }

  # Our Security group to allow HTTP and SSH access
  vpc_security_group_ids = ["${aws_security_group.controller_sg.id}"]

  subnet_id = "${aws_subnet.public_utility.id}"

  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Controller"
  }
}

resource "aws_route53_record" "controller" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.controller_dns_name}"
   type    = "CNAME"
   ttl     = "300"
   records = ["${aws_instance.cluster_controller.public_dns}"]
}


##################################
# Outputs

output "controller_ami" {
  value = "${aws_ami_copy.controller_ami.id}"
}

output "controller_security_group_id" {
  value = "${aws_security_group.controller_sg.id}"
}

