##########################################
# Misc VPC components.

# Create a VPC endpoint for S3.
resource "aws_vpc_endpoint" "private-s3" {
  vpc_id       = "${aws_vpc.main.id}"
  service_name = "com.amazonaws.us-east-1.s3"
}

# Per-zone S3 VPC endpoint routing in each subnet
resource "aws_vpc_endpoint_route_table_association" "s3_1" {
  vpc_endpoint_id = "${aws_vpc_endpoint.private-s3.id}"
  route_table_id  = "${aws_route_table.natgw1.id}"
}

resource "aws_vpc_endpoint_route_table_association" "s3_2" {
  vpc_endpoint_id = "${aws_vpc_endpoint.private-s3.id}"
  route_table_id  = "${aws_route_table.natgw2.id}"
}

resource "aws_vpc_endpoint_route_table_association" "s3_3" {
  vpc_endpoint_id = "${aws_vpc_endpoint.private-s3.id}"
  route_table_id  = "${aws_route_table.natgw3.id}"
}
