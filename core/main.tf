# AWS Key Pair
resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}"
  public_key = "${file(var.public_key_path)}"
}

# Create a VPC for our cloud components.
resource "aws_vpc" "default" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "default" {
  vpc_id = "${aws_vpc.default.id}"
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.default.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.default.id}"
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "utility" {
  vpc_id = "${aws_vpc.default.id}"
  cidr_block = "10.0.200.0/24"
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "public_utility" {
  vpc_id = "${aws_vpc.default.id}"
  cidr_block = "10.0.201.0/24"
  map_public_ip_on_launch = true
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
        Name = "Controller-AMI"
    }
}

# Our controller security group, to access the instance over SSH.
resource "aws_security_group" "opsdx_controller_sg" {
  name        = "opsdx-controller-sg"
  description = "OpsDX Controller SG"
  vpc_id      = "${aws_vpc.default.id}"

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
  #ami = "${lookup(var.aws_amis, var.aws_region)}"
  ami = "${aws_ami_copy.controller_ami.id}"

  # The name of our SSH keypair we created above.
  key_name = "${aws_key_pair.auth.id}"

  # Block device specifications
  root_block_device {
    volume_size = 20
  }

  # Our Security group to allow HTTP and SSH access
  vpc_security_group_ids = ["${aws_security_group.opsdx_controller_sg.id}"]

  subnet_id = "${aws_subnet.public_utility.id}"

  tags {
    Name = "opsdx-controller"
  }
}

resource "aws_route53_record" "controller" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.controller_dns_name}"
   type    = "A"
   ttl     = "300"
   records = ["${aws_instance.cluster_controller.public_ip}"]
}

#################
# redash.io instance

# Our redash.io security group, to access the instance over SSH.
resource "aws_security_group" "opsdx_redash_sg" {
  name        = "opsdx-redash-sg"
  description = "OpsDX Redash SG"
  vpc_id      = "${aws_vpc.default.id}"

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow incoming HTTP connections
  ingress {
    from_port = 80
    to_port = 80
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow incoming HTTPs connections
  ingress {
    from_port = 443
    to_port = 443
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Unrestricted outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "cluster_redash" {
  connection {
    type        = "ssh"
    user        = "ubuntu"
    agent       = false
    private_key = "${file(var.private_key_path)}"
  }

  instance_type = "t2.medium"
  count         = "1"

  # Lookup the correct AMI based on the region we specified
  ami = "ami-3ff16228"

  # The name of our SSH keypair we created above.
  key_name = "${aws_key_pair.auth.id}"

  # Block device specifications
  root_block_device {
    volume_size = 20
  }

  # Our Security group to allow HTTP and SSH access
  vpc_security_group_ids = ["${aws_security_group.opsdx_redash_sg.id}"]

  subnet_id = "${aws_subnet.public_utility.id}"

  tags {
    Name = "opsdx-redash"
  }
}

resource "aws_route53_record" "redash" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.redash_dns_name}"
   type    = "A"
   ttl     = "300"
   records = ["${aws_instance.cluster_redash.public_ip}"]
}

################
# TODO TREWS Rest API

#resource "aws_route53_record" "trews" {
#  zone_id = "${var.domain_zone_id}"
#  name = "${var.trews_dns_name}"
#  type = "A"
#
#  alias {
#    name = "${trews.main.dns_name}"
#    zone_id = "${aws_elb.main.zone_id}"
#    evaluate_target_health = true
#  }
#}


###########
# Outputs

output "vpc_id" {
  value = "${aws_vpc.default.id}"
}

output "utility_subnet_id" {
  value = "${aws_subnet.utility.id}"
}
