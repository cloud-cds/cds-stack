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
  vpc_id      = "${data.aws_vpc.main.id}"

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
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DB SG"
  }
}

###########################################
# RDS database

# API DB
resource "aws_db_instance" "db" {
  depends_on              = ["aws_security_group.db_sg"]
  identifier              = "${var.db_identifier}"
  allocated_storage       = "${var.db_storage}"
  engine                  = "${var.db_engine}"
  engine_version          = "${lookup(var.db_engine_version, var.db_engine)}"
  instance_class          = "${var.db_instance_class}"
  name                    = "${var.db_name}"
  username                = "${var.db_username}"
  password                = "${var.db_password}"
  vpc_security_group_ids  = ["${aws_security_group.db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.db_subnet_group.id}"
  backup_retention_period = 2
  multi_az                = true
  publicly_accessible     = false
  storage_encrypted       = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DB RDS"
  }
}

resource "aws_route53_record" "db" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.db_dns_name}"
   type    = "CNAME"
   ttl     = "300"
   records = ["${aws_db_instance.db.address}"]
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

output "db_ip" {
  value = "${aws_db_instance.db.address}"
}

output "db_name" {
  value = "${var.db_name}"
}

