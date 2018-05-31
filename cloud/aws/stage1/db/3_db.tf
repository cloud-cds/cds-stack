###########################################
# Operational databases

# Dev DB
resource "aws_db_instance" "dev_db" {
  depends_on              = ["aws_security_group.db_sg"]
  identifier              = "${var.dev_db_identifier}"
  name                    = "${var.dev_db_name}"
  username                = "${var.dev_db_username}"
  password                = "${var.dev_db_password}"
  allocated_storage       = "${var.db_storage}"
  storage_type            = "${var.db_storage_type}"
  engine                  = "${var.db_engine}"
  engine_version          = "${lookup(var.db_engine_version, var.db_engine)}"
  instance_class          = "${var.db_instance_class}"
  vpc_security_group_ids  = ["${aws_security_group.db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.db_subnet_group.id}"
  backup_retention_period = 2
  multi_az                = false
  publicly_accessible     = true
  storage_encrypted       = true
  skip_final_snapshot     = true
  parameter_group_name    = "${var.db_parameter_group}"
  snapshot_identifier     = "${var.dev_db_snapshot_id}"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Dev DB RDS"
  }
}

resource "aws_route53_record" "dev_db" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.dev_db_dns_name}"
   type    = "CNAME"
   ttl     = "300"
   records = ["${aws_db_instance.dev_db.address}"]
}

# Prod DB
resource "aws_db_instance" "prod_db" {
  depends_on              = ["aws_security_group.db_sg"]
  identifier              = "${var.prod_db_identifier}"
  name                    = "${var.prod_db_name}"
  username                = "${var.prod_db_username}"
  password                = "${var.prod_db_password}"
  allocated_storage       = "${var.db_storage}"
  storage_type            = "${var.db_storage_type}"
  engine                  = "${var.db_engine}"
  engine_version          = "${lookup(var.db_engine_version, var.db_engine)}"
  instance_class          = "${var.db_instance_class}"
  vpc_security_group_ids  = ["${aws_security_group.db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.db_subnet_group.id}"
  backup_retention_period = 2
  multi_az                = true
  publicly_accessible     = false
  storage_encrypted       = true
  skip_final_snapshot     = true
  parameter_group_name    = "${var.db_parameter_group}"
  snapshot_identifier     = "${var.prod_db_snapshot_id}"

  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Prod DB RDS"
  }
}

resource "aws_route53_record" "prod_db" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.prod_db_dns_name}"
   type    = "CNAME"
   ttl     = "300"
   records = ["${aws_db_instance.prod_db.address}"]
}


###########
# Outputs

output "dev_db_ip" {
  value = "${aws_db_instance.dev_db.address}"
}

output "dev_db_name" {
  value = "${var.dev_db_name}"
}

output "prod_db_ip" {
  value = "${aws_db_instance.prod_db.address}"
}

output "prod_db_name" {
  value = "${var.prod_db_name}"
}

