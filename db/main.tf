resource "aws_subnet" "prod_db_subnet1" {
  vpc_id            = "${data.aws_vpc.prod.id}"
  cidr_block        = "${var.db_subnet1_cidr}"
  availability_zone = "${var.db_availability_zone1}"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deployment_tag} DB SN1"
  }
}

resource "aws_subnet" "prod_db_subnet2" {
  vpc_id            = "${data.aws_vpc.prod.id}"
  cidr_block        = "${var.db_subnet2_cidr}"
  availability_zone = "${var.db_availability_zone2}"
  map_public_ip_on_launch = true
  tags {
    Name = "${var.deployment_tag} DB SN2"
  }
}

resource "aws_db_subnet_group" "prod_db_subnet_group" {
  name        = "prod-db-subnet-group"
  description = "OpsDX Production DB SNG"
  subnet_ids  = ["${aws_subnet.prod_db_subnet1.id}", "${aws_subnet.prod_db_subnet2.id}"]
  tags {
     Name = "${var.deployment_tag} DB SNG"
  }
}

resource "aws_security_group" "prod_db_sg" {
  name        = "prod-db-sg"
  description = "OpsDX DB SG"
  vpc_id      = "${data.aws_vpc.prod.id}"

  # Postgres access from anywhere
  ingress {
    from_port   = 5432
    to_port     = 5432
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
    Name = "${var.deployment_tag} DB SG"
  }
}

###########################################
# RDS database

# API DB
resource "aws_db_instance" "db_prod" {
  depends_on              = ["aws_security_group.prod_db_sg"]
  identifier              = "${var.db_identifier}"
  allocated_storage       = "${var.db_storage}"
  engine                  = "${var.db_engine}"
  engine_version          = "${lookup(var.db_engine_version, var.db_engine)}"
  instance_class          = "${var.db_instance_class}"
  name                    = "${var.db_name_prod}"
  username                = "${var.db_username}"
  password                = "${var.db_password}"
  vpc_security_group_ids  = ["${aws_security_group.prod_db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.prod_db_subnet_group.id}"
  backup_retention_period = 2
  multi_az                = true
  publicly_accessible     = true
  storage_encrypted       = true
  tags {
    Name = "${var.deployment_tag} DB"
  }
}

###########
# Outputs

output "db_subnet_group_id" {
  value = "${aws_db_subnet_group.prod_db_subnet_group.id}"
}

output "db_ip" {
  value = "${aws_db_instance.db_prod.address}"
}

