resource "aws_subnet" "db_subnet1" {
  vpc_id            = "${data.aws_vpc.default.id}"
  cidr_block        = "${var.db_subnet1_cidr}"
  availability_zone = "${var.db_availability_zone1}"
  map_public_ip_on_launch = true
  tags {
    Name = "db_subnet1"
  }
}

resource "aws_subnet" "db_subnet2" {
  vpc_id            = "${data.aws_vpc.default.id}"
  cidr_block        = "${var.db_subnet2_cidr}"
  availability_zone = "${var.db_availability_zone2}"
  map_public_ip_on_launch = true
  tags {
    Name = "db_subnet2"
  }
}

resource "aws_db_subnet_group" "default" {
  name        = "opsdx_db_subnet_group"
  description = "OpsDX DB subnet group"
  subnet_ids  = ["${aws_subnet.db_subnet1.id}", "${aws_subnet.db_subnet2.id}"]
}