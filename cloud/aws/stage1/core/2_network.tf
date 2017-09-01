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
  cidr_block = "10.0.253.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Private Utility Subnet"
  }
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "public_utility" {
  vpc_id = "${aws_vpc.main.id}"
  cidr_block = "10.0.254.0/24"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Public Utility Subnet"
  }
}


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
