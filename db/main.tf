###########################################
# RDS database

# API DB
resource "aws_db_instance" "db_prod" {
  depends_on              = ["aws_security_group.opsdx_db_sg"]
  identifier              = "${var.db_identifier}"
  allocated_storage       = "${var.db_storage}"
  engine                  = "${var.db_engine}"
  engine_version          = "${lookup(var.db_engine_version, var.db_engine)}"
  instance_class          = "${var.db_instance_class}"
  name                    = "${var.db_name_prod}"
  username                = "${var.db_username}"
  password                = "${var.db_password}"
  vpc_security_group_ids  = ["${aws_security_group.opsdx_db_sg.id}"]
  db_subnet_group_name    = "${aws_db_subnet_group.default.id}"
  backup_retention_period = 2
  multi_az                = true
  publicly_accessible     = true
  storage_encrypted       = true
}

###########
# Outputs

output "db_subnet_group_id" {
  value = "${aws_db_subnet_group.default.id}"
}

output "db_ip" {
  value = "${aws_db_instance.db_prod.address}"
}

