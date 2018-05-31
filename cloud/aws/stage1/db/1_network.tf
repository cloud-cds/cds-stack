#############################################
# DB subnets and security groups.

resource "aws_subnet" "db_subnet1" {
  vpc_id            = "${data.aws_vpc.main.id}"
  cidr_block        = "${var.db_subnet1_cidr}"
  availability_zone = "${var.db_availability_zone1}"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DB SN1"
  }
}

resource "aws_subnet" "db_subnet2" {
  vpc_id            = "${data.aws_vpc.main.id}"
  cidr_block        = "${var.db_subnet2_cidr}"
  availability_zone = "${var.db_availability_zone2}"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DB SN2"
  }
}

resource "aws_db_subnet_group" "db_subnet_group" {
  name        = "${var.deploy_prefix}-db-subnet-group"
  description = "${var.deploy_name} ${var.deploy_stack} DB SNG"
  subnet_ids  = ["${aws_subnet.db_subnet1.id}", "${aws_subnet.db_subnet2.id}"]
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DB SNG"
  }
}

resource "aws_security_group" "db_sg" {
  name        = "${var.deploy_prefix}-db-sg"
  description = "${var.deploy_name} ${var.deploy_stack} DB SG"
  vpc_id      =  "vpc-1eaaf17a" #"${data.aws_vpc.main.id}"

  # Postgres access from within the VPC.
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # Redshift access from within the VPC.
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
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
    Component = "DB SG"
  }
}

resource "aws_redshift_subnet_group" "dw_subnet_group" {
  name       = "${var.deploy_prefix}-dw-subnet-group"
  description = "${var.deploy_name} ${var.deploy_stack} DW SNG"
  subnet_ids  = ["${aws_subnet.db_subnet1.id}", "${aws_subnet.db_subnet2.id}"]
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DW SNG"
  }
}


###########
# Outputs

output "db_subnet1_id" {
  value = "${aws_subnet.db_subnet1.id}"
}

output "db_subnet2_id" {
  value = "${aws_subnet.db_subnet2.id}"
}

output "db_subnet_group_id" {
  value = "${aws_db_subnet_group.db_subnet_group.id}"
}

output "db_sg_id" {
  value = "${aws_security_group.db_sg.id}"
}

