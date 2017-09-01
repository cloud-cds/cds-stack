##################################################
# AWS Lambda support inside a VPC.
#
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
  cidr_block = "10.0.251.0/24"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Lambda Subnet 1"
  }
}

resource "aws_subnet" "lambda_subnet2" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "10.0.252.0/24"
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

output "lambda_sg_id" {
  value = "${aws_security_group.lambda_sg.id}"
}

