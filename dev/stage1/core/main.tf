# Create a VPC for our cloud components.
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "VPC"
  }
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "main" {
  vpc_id = "${aws_vpc.main.id}"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "GW"
  }
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.main.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.main.id}"
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "utility" {
  vpc_id = "${aws_vpc.main.id}"
  cidr_block = "10.0.200.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Private Utility Subnet"
  }
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "public_utility" {
  vpc_id = "${aws_vpc.main.id}"
  cidr_block = "10.0.201.0/24"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Public Utility Subnet"
  }
}

# Create an elastic IP for the NAT gateway.
resource "aws_eip" "natgw" {
  vpc = true
}

# Create a NAT gateway for AWS Lambda functions in the private VPC
resource "aws_nat_gateway" "lambda" {
  allocation_id = "${aws_eip.natgw.id}"
  subnet_id     = "${aws_subnet.utility.id}"

  depends_on = ["aws_internet_gateway.main"]
}

# Create a route to the NAT gateway in the lambda subnet
resource "aws_route_table" "lambda" {
  vpc_id = "${aws_vpc.main.id}"
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = "${aws_nat_gateway.lambda.id}"
  }
}

resource "aws_subnet" "lambda_subnet1" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "10.0.202.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Lambda Subnet 1"
  }
}

resource "aws_subnet" "lambda_subnet2" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "10.0.203.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Lambda Subnet 2"
  }
}

resource "aws_route_table_association" "lambda_subnet1" {
  subnet_id      = "${aws_subnet.lambda_subnet1.id}"
  route_table_id = "${aws_route_table.lambda.id}"
}

resource "aws_route_table_association" "lambda_subnet2" {
  subnet_id      = "${aws_subnet.lambda_subnet2.id}"
  route_table_id = "${aws_route_table.lambda.id}"
}


##############################
# Controller instance

# Our controller security group, to access the instance over SSH.
resource "aws_security_group" "controller_sg" {
  name        = "${var.deploy_prefix}-controller-sg"
  description = "OpsDX Controller SG"
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
  ami = "${var.controller_ami}"

  # The name of our SSH keypair we created above.
  key_name = "${var.auth_key}"

  # Block device specifications
  root_block_device {
    volume_size = 20
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
  value = "${aws_vpc.main.id}"
}

output "vpc_cidr" {
  value = "${aws_vpc.main.cidr}"
}

output "utility_subnet_id" {
  value = "${aws_subnet.utility.id}"
}

output "public_utility_subnet_id" {
  value = "${aws_subnet.public_utility.id}"
}
