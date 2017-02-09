# Create a VPC for our cloud components.
resource "aws_vpc" "prod" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags {
    Name = "${var.deployment_tag} VPC"
  }
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "prod" {
  vpc_id = "${aws_vpc.prod.id}"
  tags {
    Name = "${var.deployment_tag} GW"
  }
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.prod.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.prod.id}"
}

# Create an elastic IP for the NAT gateway.
resource "aws_eip" "prod-natgw" {
  vpc = true
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "prod_utility" {
  vpc_id = "${aws_vpc.prod.id}"
  cidr_block = "10.0.200.0/24"
  tags {
    Name = "${var.deployment_tag} Utility"
  }
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "prod_public_utility" {
  vpc_id = "${aws_vpc.prod.id}"
  cidr_block = "10.0.201.0/24"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deployment_tag} Public Utility"
  }
}


##############################
# Controller instance

# Our controller security group, to access the instance over SSH.
resource "aws_security_group" "prod_controller_sg" {
  name        = "prod-controller-sg"
  description = "OpsDX Controller Prod SG"
  vpc_id      = "${aws_vpc.prod.id}"

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
    Name = "${var.deployment_tag} Controller SG"
  }
}


resource "aws_instance" "prod_cluster_controller" {
  connection {
    type        = "ssh"
    user        = "ubuntu"
    agent       = false
    private_key = "${file(var.private_key_path)}"
  }

  instance_type = "t2.medium"
  count         = "1"

  # Lookup the correct AMI based on the region we specified
  ami = "${var.controller_ami}"

  # The name of our SSH keypair we created above.
  key_name = "${var.auth_key}"

  # Block device specifications
  root_block_device {
    volume_size = 20
  }

  # Our Security group to allow HTTP and SSH access
  vpc_security_group_ids = ["${aws_security_group.prod_controller_sg.id}"]

  subnet_id = "${aws_subnet.prod_public_utility.id}"

  tags {
    Name = "${var.deployment_tag} Controller"
  }
}

resource "aws_route53_record" "prod_controller" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.controller_dns_name}"
   type    = "A"
   ttl     = "300"
   records = ["${aws_instance.prod_cluster_controller.public_ip}"]
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
  value = "${aws_vpc.prod.id}"
}

output "utility_subnet_id" {
  value = "${aws_subnet.prod_utility.id}"
}
