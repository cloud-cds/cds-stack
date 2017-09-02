###################
# Data Warehouse

resource "aws_db_instance" "dw" {
  depends_on              = ["aws_security_group.db_sg"]
  identifier              = "${var.dw_identifier}"
  name                    = "${var.dw_name}"
  username                = "${var.dw_username}"
  password                = "${var.dw_password}"
  allocated_storage       = "${var.dw_storage}"
  storage_type            = "${var.dw_storage_type}"
  engine                  = "${var.dw_engine}"
  engine_version          = "${lookup(var.dw_engine_version, var.dw_engine)}"
  instance_class          = "${var.dw_instance_class}"
  vpc_security_group_ids  = ["${aws_security_group.db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.db_subnet_group.id}"
  backup_retention_period = 0
  multi_az                = false
  publicly_accessible     = false
  storage_encrypted       = true
  skip_final_snapshot     = true
  #snapshot_identifier     = "opsdx-dev-backup"

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

###########
# Outputs

output "dw_ip" {
  value = "${aws_db_instance.dw.address}"
}

output "dw_name" {
  value = "${var.dw_name}"
}
