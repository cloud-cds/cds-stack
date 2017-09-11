###################
# Data Warehouse

resource "aws_db_instance" "dw2" {
  depends_on              = ["aws_security_group.db_sg"]
  identifier              = "${var.dw2_identifier}"
  name                    = "${var.dw2_name}"
  username                = "${var.dw2_username}"
  password                = "${var.dw2_password}"
  allocated_storage       = "${var.dw2_storage}"
  storage_type            = "${var.dw2_storage_type}"
  engine                  = "${var.dw2_engine}"
  engine_version          = "${lookup(var.dw2_engine_version, var.dw2_engine)}"
  instance_class          = "${var.dw2_instance_class}"
  vpc_security_group_ids  = ["${aws_security_group.db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.db_subnet_group.id}"
  backup_retention_period = 0
  multi_az                = false
  publicly_accessible     = false
  storage_encrypted       = true
  skip_final_snapshot     = true
  parameter_group_name    = "${var.dw2_parameter_group}"
  snapshot_identifier     = "${var.dw2_snapshot_id}"

  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DW2 RDS"
  }
}

resource "aws_route53_record" "dw2" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.dw2_dns_name}"
   type    = "CNAME"
   ttl     = "300"
   records = ["${aws_db_instance.dw2.address}"]
}

###########
# Outputs

output "dw2_ip" {
  value = "${aws_db_instance.dw2.address}"
}

output "dw2_name" {
  value = "${var.dw2_name}"
}
