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

###################
# Data Warehouse
# For now, this uses the same engine type as the Op DB
resource "aws_db_instance" "dw" {
  depends_on              = ["aws_security_group.db_sg"]
  identifier              = "${var.dw_identifier}"
  allocated_storage       = "${var.db_storage}"
  engine                  = "${var.db_engine}"
  engine_version          = "${lookup(var.db_engine_version, var.db_engine)}"
  instance_class          = "${var.db_instance_class}"
  name                    = "${var.dw_name}"
  username                = "${var.dw_username}"
  password                = "${var.dw_password}"
  vpc_security_group_ids  = ["${aws_security_group.db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.db_subnet_group.id}"
  backup_retention_period = 2
  multi_az                = true
  publicly_accessible     = false
  storage_encrypted       = true
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DW RDS"
  }
}

resource "aws_route53_record" "dw" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.dw_dns_name}"
   type    = "CNAME"
   ttl     = "300"
   records = ["${aws_db_instance.dw.address}"]
}

resource "aws_db_parameter_group" "pgstats" {
  name   = "${var.deploy_prefix}-pgstats-pg"
  family = "postgres9.5"

  parameter {
    name = "shared_preload_libraries"
    value = "pg_stat_statements"
    apply_method = "pending-reboot"
  }

  parameter {
    name = "track_activity_query_size"
    value = "2048"
    apply_method = "pending-reboot"
  }

  parameter {
    name = "pg_stat_statements.track"
    value = "ALL"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_parameter_group" "pgbadger" {
  name   = "${var.deploy_prefix}-pgbadger-pg"
  family = "postgres9.5"

  parameter {
    name  = "lc_messages"
    value = "C"
  }

  parameter {
    name  = "log_autovacuum_min_duration"
    value = "0"
  }

  parameter {
    name  = "log_connections"
    value = "1"
  }

  parameter {
    name  = "log_disconnections"
    value = "1"
  }

  parameter {
    name  = "log_duration"
    value = "0"
  }

  parameter {
    name  = "log_error_verbosity"
    value = "default"
  }

  parameter {
    name  = "log_filename"
    value = "postgresql.log.%Y-%m-%d"
  }

  parameter {
    name  = "log_lock_waits"
    value = "1"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "5"
  }

  parameter {
    name  = "log_rotation_age"
    value = "1440"
  }

  parameter {
    name  = "log_rotation_size"
    value = "2097151"
  }

  parameter {
    name  = "log_statement"
    value = "none"
  }

  parameter {
    name  = "log_temp_files"
    value = "0"
  }

  parameter {
    name  = "rds.log_retention_period"
    value = "10080"
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

output "db_ip" {
  value = "${aws_db_instance.db.address}"
}

output "db_name" {
  value = "${var.db_name}"
}

