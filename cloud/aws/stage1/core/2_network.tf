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


##################################################
# Public utility subnet (e.g., for the controller)
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


####################################################
# Shared subnets by AZ.
# Note these are also used for k8s utility subnets,
# as well as for AWS NAT gateways.

# Generic utility subnet (e.g., for extra ALBs)
resource "aws_subnet" "utility1" {
  availability_zone = "${var.az1}"
  vpc_id = "${aws_vpc.main.id}"
  cidr_block = "10.0.251.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Private Utility Subnet1"
    "kubernetes.io/cluster/cluster-dev.jh.opsdx.io" = "shared"
    "kubernetes.io/cluster/cluster-prod.jh.opsdx.io" = "shared"
  }
}

resource "aws_subnet" "utility2" {
  availability_zone = "${var.az2}"
  vpc_id = "${aws_vpc.main.id}"
  cidr_block = "10.0.252.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Private Utility Subnet2"
    "kubernetes.io/cluster/cluster-dev.jh.opsdx.io" = "shared"
    "kubernetes.io/cluster/cluster-prod.jh.opsdx.io" = "shared"
  }
}

resource "aws_subnet" "utility3" {
  availability_zone = "${var.az3}"
  vpc_id = "${aws_vpc.main.id}"
  cidr_block = "10.0.253.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Private Utility Subnet3"
    "kubernetes.io/cluster/cluster-dev.jh.opsdx.io" = "shared"
    "kubernetes.io/cluster/cluster-prod.jh.opsdx.io" = "shared"
  }
}

####################################################################
# NAT gateways per AZ, placed in the corresponding utility subnet.

# NAT GW1
resource "aws_eip" "natgw1" {
  vpc = true
}

# Create a NAT gateway in the private utility subnet in the VPC.
resource "aws_nat_gateway" "natgw1" {
  allocation_id = "${aws_eip.natgw1.id}"
  subnet_id     = "${aws_subnet.utility1.id}"

  depends_on = ["aws_internet_gateway.main"]
}

# Create a route table that defaults to the NAT gateway
resource "aws_route_table" "natgw1" {
  vpc_id = "${aws_vpc.main.id}"
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = "${aws_nat_gateway.natgw1.id}"
  }
}


# NAT GW2
resource "aws_eip" "natgw2" {
  vpc = true
}

# Create a NAT gateway in the private utility subnet in the VPC.
resource "aws_nat_gateway" "natgw2" {
  allocation_id = "${aws_eip.natgw2.id}"
  subnet_id     = "${aws_subnet.utility2.id}"

  depends_on = ["aws_internet_gateway.main"]
}

# Create a route table that defaults to the NAT gateway
resource "aws_route_table" "natgw2" {
  vpc_id = "${aws_vpc.main.id}"
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = "${aws_nat_gateway.natgw2.id}"
  }
}


# NAT GW3
resource "aws_eip" "natgw3" {
  vpc = true
}

# Create a NAT gateway in the private utility subnet in the VPC.
resource "aws_nat_gateway" "natgw3" {
  allocation_id = "${aws_eip.natgw3.id}"
  subnet_id     = "${aws_subnet.utility3.id}"

  depends_on = ["aws_internet_gateway.main"]
}

# Create a route table that defaults to the NAT gateway
resource "aws_route_table" "natgw3" {
  vpc_id = "${aws_vpc.main.id}"
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = "${aws_nat_gateway.natgw3.id}"
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

output "public_utility_subnet_id" {
  value = "${aws_subnet.public_utility.id}"
}

output "utility1_subnet_id" {
  value = "${aws_subnet.utility1.id}"
}

output "utility2_subnet_id" {
  value = "${aws_subnet.utility2.id}"
}

output "utility3_subnet_id" {
  value = "${aws_subnet.utility3.id}"
}

output "natgw1_id" {
  value = "${aws_nat_gateway.natgw1.id}"
}

output "natgw2_id" {
  value = "${aws_nat_gateway.natgw2.id}"
}

output "natgw3_id" {
  value = "${aws_nat_gateway.natgw3.id}"
}

