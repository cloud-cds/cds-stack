###########################################
# Analytics engine: Redshift cluster

resource "aws_iam_role" "rs_s3_role" {
  name = "redshift_role"
  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      }
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "rs_s3_role_policy" {
  name  = "rs_s3_role_policy"
  role = "${aws_iam_role.rs_s3_role.id}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AWSRedshiftS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:Put*",
        "s3:Get*",
        "s3:List*",
        "s3:Delete*"
      ],
      "Resource": [
        "*"
      ]
    }
  ]
}
POLICY
}


resource "aws_redshift_cluster" "default" {
  cluster_identifier      = "${var.dw_identifier}"
  database_name           = "${var.dw_name}"
  master_username         = "${var.dw_username}"
  master_password         = "${var.dw_password}"
  node_type               = "${var.dwa_node_type}"

  cluster_type            = "multi-node"
  #cluster_version         = "${var.dwa_cluster_version}"
  number_of_nodes         = "4"
  encrypted               = true
  enhanced_vpc_routing    = true
  skip_final_snapshot     = true

  # Restore from backup
  #snapshot_identifier         = ""
  #snapshot_cluster_identifier = ""

  availability_zone         = "${aws_db_instance.dw.availability_zone}"
  vpc_security_group_ids    = ["${aws_security_group.db_sg.id}"]
  cluster_subnet_group_name = "${aws_redshift_subnet_group.dw_subnet_group.id}"

  iam_roles = [ "${aws_iam_role.rs_s3_role.arn}" ]

  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "DW Redshift"
  }
}

resource "aws_route53_record" "dwa" {
   zone_id = "${var.domain_zone_id}"
   name    = "${var.dwa_dns_name}"
   type    = "CNAME"
   ttl     = "60"
   records = ["${aws_redshift_cluster.default.endpoint}"]
}

###########
# Outputs

output "dwa_ip" {
  value = "${aws_redshift_cluster.default.endpoint}"
}
