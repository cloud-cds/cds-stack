####################################################
# Shared subnets and NAT gateways for k8s clusters.
#

resource "aws_subnet" "k8s1" {
  availability_zone = "${var.az1}"
  vpc_id            = "${aws_vpc.main.id}"
  cidr_block        = "10.0.16.0/20"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Kubernetes Subnet 1"
  }
}

resource "aws_subnet" "k8s2" {
  availability_zone = "${var.az2}"
  vpc_id            = "${aws_vpc.main.id}"
  cidr_block        = "10.0.32.0/20"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Kubernetes Subnet 2"
  }
}

resource "aws_subnet" "k8s3" {
  availability_zone = "${var.az3}"
  vpc_id            = "${aws_vpc.main.id}"
  cidr_block        = "10.0.48.0/20"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Kubernetes Subnet 3"
  }
}

# Per-zone NAT routing in each subnet
resource "aws_route_table_association" "k8s1" {
  subnet_id      = "${aws_subnet.k8s1.id}"
  route_table_id = "${aws_route_table.natgw1.id}"
}

resource "aws_route_table_association" "k8s2" {
  subnet_id      = "${aws_subnet.k8s2.id}"
  route_table_id = "${aws_route_table.natgw2.id}"
}

resource "aws_route_table_association" "k8s3" {
  subnet_id      = "${aws_subnet.k8s3.id}"
  route_table_id = "${aws_route_table.natgw3.id}"
}



output "k8s1_subnet_id" {
  value = "${aws_subnet.k8s1.id}"
}

output "k8s2_subnet_id" {
  value = "${aws_subnet.k8s2.id}"
}

output "k8s3_subnet_id" {
  value = "${aws_subnet.k8s3.id}"
}
