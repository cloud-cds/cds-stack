##################################################
# AWS Lambda support inside a VPC.
#

resource "aws_subnet" "lambda_subnet1" {
  availability_zone = "${var.az1}"
  vpc_id            = "${aws_vpc.main.id}"
  cidr_block        = "10.0.246.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Lambda Subnet 1"
  }
}

resource "aws_subnet" "lambda_subnet2" {
  availability_zone = "${var.az2}"
  vpc_id            = "${aws_vpc.main.id}"
  cidr_block        = "10.0.247.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Lambda Subnet 2"
  }
}

resource "aws_subnet" "lambda_subnet3" {
  availability_zone = "${var.az3}"
  vpc_id            = "${aws_vpc.main.id}"
  cidr_block        = "10.0.248.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Lambda Subnet 3"
  }
}

resource "aws_route_table_association" "lambda_subnet1" {
  subnet_id      = "${aws_subnet.lambda_subnet1.id}"
  route_table_id = "${aws_route_table.natgw1.id}"
}

resource "aws_route_table_association" "lambda_subnet2" {
  subnet_id      = "${aws_subnet.lambda_subnet2.id}"
  route_table_id = "${aws_route_table.natgw2.id}"
}

resource "aws_route_table_association" "lambda_subnet3" {
  subnet_id      = "${aws_subnet.lambda_subnet3.id}"
  route_table_id = "${aws_route_table.natgw3.id}"
}

# A lambda security group with only (unrestricted) outbound access.
resource "aws_security_group" "lambda_sg" {
  name        = "${var.deploy_prefix}-lambda-sg"
  description = "OpsDX Lambda SG"
  vpc_id      = "${aws_vpc.main.id}"

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
    Component = "Lambda Security Group"
  }
}


###########
# Outputs

output "lambda_subnet1_id" {
  value = "${aws_subnet.lambda_subnet1.id}"
}

output "lambda_subnet2_id" {
  value = "${aws_subnet.lambda_subnet2.id}"
}

output "lambda_subnet3_id" {
  value = "${aws_subnet.lambda_subnet3.id}"
}

output "lambda_sg_id" {
  value = "${aws_security_group.lambda_sg.id}"
}

