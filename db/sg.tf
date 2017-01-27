resource "aws_security_group" "opsdx_db_sg" {
  name        = "opsdx-rds-sg"
  description = "OpsDX DB SG"
  vpc_id      = "${data.aws_vpc.default.id}"

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
}
