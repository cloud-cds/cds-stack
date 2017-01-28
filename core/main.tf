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

# Create an elastic IP for the NAT gateway.
resource "aws_eip" "natgw" {
  vpc = true
}

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "utility" {
  vpc_id = "${aws_vpc.default.id}"
  cidr_block = "10.0.200.0/24"
}

###########
# Outputs

output "vpc_id" {
  value = "${aws_vpc.default.id}"
}

output "utility_subnet_id" {
  value = "${aws_subnet.utility.id}"
}