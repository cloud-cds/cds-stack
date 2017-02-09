provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1b-masters-cluster-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1b-masters-cluster-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1c-masters-cluster-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1c-masters-cluster-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-opsdx-io.id}"
}

resource "aws_autoscaling_group" "master-us-east-1b-masters-cluster-opsdx-io" {
  name                 = "master-us-east-1b.masters.cluster.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1b-masters-cluster-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1b-cluster-opsdx-io.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1b.masters.cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1c-masters-cluster-opsdx-io" {
  name                 = "master-us-east-1c.masters.cluster.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1c-masters-cluster-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1c-cluster-opsdx-io.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1c.masters.cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-opsdx-io" {
  name                 = "master-us-east-1d.masters.cluster.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-cluster-opsdx-io.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "nodes-cluster-opsdx-io" {
  name                 = "nodes.cluster.opsdx.io"
  launch_configuration = "${aws_launch_configuration.nodes-cluster-opsdx-io.id}"
  max_size             = 3
  min_size             = 3
  vpc_zone_identifier  = ["${aws_subnet.us-east-1b-cluster-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-opsdx-io.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "nodes.cluster.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "b-etcd-events-cluster-opsdx-io" {
  availability_zone = "us-east-1b"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.opsdx.io"
    Name                 = "b.etcd-events.cluster.opsdx.io"
    "k8s.io/etcd/events" = "b/b,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "b-etcd-main-cluster-opsdx-io" {
  availability_zone = "us-east-1b"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.opsdx.io"
    Name                 = "b.etcd-main.cluster.opsdx.io"
    "k8s.io/etcd/main"   = "b/b,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-events-cluster-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.opsdx.io"
    Name                 = "c.etcd-events.cluster.opsdx.io"
    "k8s.io/etcd/events" = "c/b,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-main-cluster-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.opsdx.io"
    Name                 = "c.etcd-main.cluster.opsdx.io"
    "k8s.io/etcd/main"   = "c/b,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-events-cluster-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.opsdx.io"
    Name                 = "d.etcd-events.cluster.opsdx.io"
    "k8s.io/etcd/events" = "d/b,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-cluster-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.opsdx.io"
    Name                 = "d.etcd-main.cluster.opsdx.io"
    "k8s.io/etcd/main"   = "d/b,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_eip" "us-east-1b-cluster-opsdx-io" {
  vpc = true
}

resource "aws_eip" "us-east-1c-cluster-opsdx-io" {
  vpc = true
}

resource "aws_eip" "us-east-1d-cluster-opsdx-io" {
  vpc = true
}

resource "aws_elb" "api-cluster-opsdx-io" {
  name = "api-cluster"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-cluster-opsdx-io.id}"]
  subnets         = ["${aws_subnet.utility-us-east-1b-cluster-opsdx-io.id}", "${aws_subnet.utility-us-east-1c-cluster-opsdx-io.id}", "${aws_subnet.utility-us-east-1d-cluster-opsdx-io.id}"]

  health_check = {
    target              = "TCP:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "api.cluster.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-opsdx-io" {
  name  = "masters.cluster.opsdx.io"
  roles = ["${aws_iam_role.masters-cluster-opsdx-io.name}"]
}

resource "aws_iam_instance_profile" "nodes-cluster-opsdx-io" {
  name  = "nodes.cluster.opsdx.io"
  roles = ["${aws_iam_role.nodes-cluster-opsdx-io.name}"]
}

resource "aws_iam_role" "masters-cluster-opsdx-io" {
  name               = "masters.cluster.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-cluster-opsdx-io" {
  name               = "nodes.cluster.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-opsdx-io" {
  name   = "masters.cluster.opsdx.io"
  role   = "${aws_iam_role.masters-cluster-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-opsdx-io" {
  name   = "nodes.cluster.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster.opsdx.io_policy")}"
}

resource "aws_internet_gateway" "cluster-opsdx-io" {
  vpc_id = "${aws_vpc.cluster-opsdx-io.id}"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "cluster.opsdx.io"
  }
}

resource "aws_key_pair" "kubernetes-cluster-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23" {
  key_name   = "kubernetes.cluster.opsdx.io-94:a2:2f:95:b3:cc:fd:3f:4d:a6:d2:15:22:59:2b:23"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster.opsdx.io-94a22f95b3ccfd3f4da6d21522592b23_public_key")}"
}

resource "aws_launch_configuration" "master-us-east-1b-masters-cluster-opsdx-io" {
  name_prefix                 = "master-us-east-1b.masters.cluster.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1b.masters.cluster.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1c-masters-cluster-opsdx-io" {
  name_prefix                 = "master-us-east-1c.masters.cluster.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1c.masters.cluster.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.cluster.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "nodes-cluster-opsdx-io" {
  name_prefix                 = "nodes.cluster.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_nat_gateway" "us-east-1b-cluster-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1b-cluster-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1b-cluster-opsdx-io.id}"
}

resource "aws_nat_gateway" "us-east-1c-cluster-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1c-cluster-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1c-cluster-opsdx-io.id}"
}

resource "aws_nat_gateway" "us-east-1d-cluster-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1d-cluster-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1d-cluster-opsdx-io.id}"
}

resource "aws_route" "0-0-0-0--0" {
  route_table_id         = "${aws_route_table.cluster-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.cluster-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1b-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1b-cluster-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1b-cluster-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1c-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1c-cluster-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1c-cluster-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1d-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1d-cluster-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1d-cluster-opsdx-io.id}"
}

resource "aws_route53_record" "api-cluster-opsdx-io" {
  name = "api.cluster.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-cluster-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-cluster-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z1JW3JKAMTAMS1"
}

resource "aws_route_table" "cluster-opsdx-io" {
  vpc_id = "${aws_vpc.cluster-opsdx-io.id}"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "cluster.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1b-cluster-opsdx-io" {
  vpc_id = "${aws_vpc.cluster-opsdx-io.id}"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "private-us-east-1b.cluster.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1c-cluster-opsdx-io" {
  vpc_id = "${aws_vpc.cluster-opsdx-io.id}"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "private-us-east-1c.cluster.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1d-cluster-opsdx-io" {
  vpc_id = "${aws_vpc.cluster-opsdx-io.id}"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "private-us-east-1d.cluster.opsdx.io"
  }
}

resource "aws_route_table_association" "private-us-east-1b-cluster-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1b-cluster-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1b-cluster-opsdx-io.id}"
}

resource "aws_route_table_association" "private-us-east-1c-cluster-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1c-cluster-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1c-cluster-opsdx-io.id}"
}

resource "aws_route_table_association" "private-us-east-1d-cluster-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1d-cluster-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1d-cluster-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1b-cluster-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1b-cluster-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1c-cluster-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1c-cluster-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1d-cluster-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1d-cluster-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-opsdx-io.id}"
}

resource "aws_security_group" "api-elb-cluster-opsdx-io" {
  name        = "api-elb.cluster.opsdx.io"
  vpc_id      = "${aws_vpc.cluster-opsdx-io.id}"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "api-elb.cluster.opsdx.io"
  }
}

resource "aws_security_group" "masters-cluster-opsdx-io" {
  name        = "masters.cluster.opsdx.io"
  vpc_id      = "${aws_vpc.cluster-opsdx-io.id}"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "masters.cluster.opsdx.io"
  }
}

resource "aws_security_group" "nodes-cluster-opsdx-io" {
  name        = "nodes.cluster.opsdx.io"
  vpc_id      = "${aws_vpc.cluster-opsdx-io.id}"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "nodes.cluster.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-cluster-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-4194" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port                = 4194
  to_port                  = 4194
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-443" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-6783" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port                = 6783
  to_port                  = 6783
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-6783" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port                = 6783
  to_port                  = 6783
  protocol                 = "udp"
}

resource "aws_security_group_rule" "node-to-master-udp-6784" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port                = 6784
  to_port                  = 6784
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "ssh-external-to-node-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_subnet" "us-east-1b-cluster-opsdx-io" {
  vpc_id            = "${aws_vpc.cluster-opsdx-io.id}"
  cidr_block        = "10.0.32.0/19"
  availability_zone = "us-east-1b"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "us-east-1b.cluster.opsdx.io"
  }
}

resource "aws_subnet" "us-east-1c-cluster-opsdx-io" {
  vpc_id            = "${aws_vpc.cluster-opsdx-io.id}"
  cidr_block        = "10.0.64.0/19"
  availability_zone = "us-east-1c"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "us-east-1c.cluster.opsdx.io"
  }
}

resource "aws_subnet" "us-east-1d-cluster-opsdx-io" {
  vpc_id            = "${aws_vpc.cluster-opsdx-io.id}"
  cidr_block        = "10.0.96.0/19"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "us-east-1d.cluster.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1b-cluster-opsdx-io" {
  vpc_id            = "${aws_vpc.cluster-opsdx-io.id}"
  cidr_block        = "10.0.0.0/22"
  availability_zone = "us-east-1b"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "utility-us-east-1b.cluster.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1c-cluster-opsdx-io" {
  vpc_id            = "${aws_vpc.cluster-opsdx-io.id}"
  cidr_block        = "10.0.4.0/22"
  availability_zone = "us-east-1c"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "utility-us-east-1c.cluster.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1d-cluster-opsdx-io" {
  vpc_id            = "${aws_vpc.cluster-opsdx-io.id}"
  cidr_block        = "10.0.8.0/22"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "utility-us-east-1d.cluster.opsdx.io"
  }
}

resource "aws_vpc" "cluster-opsdx-io" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "cluster.opsdx.io"
  }
}

resource "aws_vpc_dhcp_options" "cluster-opsdx-io" {
  domain_name         = "ec2.internal"
  domain_name_servers = ["AmazonProvidedDNS"]

  tags = {
    KubernetesCluster = "cluster.opsdx.io"
    Name              = "cluster.opsdx.io"
  }
}

resource "aws_vpc_dhcp_options_association" "cluster-opsdx-io" {
  vpc_id          = "${aws_vpc.cluster-opsdx-io.id}"
  dhcp_options_id = "${aws_vpc_dhcp_options.cluster-opsdx-io.id}"
}
