output "cluster_name" {
  value = "ml-cluster.dev.opsdx.io"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"]
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
}

output "node_subnet_ids" {
  value = ["subnet-7fb46137"]
}

output "region" {
  value = "us-east-1"
}

output "subnet_ids" {
  value = ["subnet-20b56068", "subnet-7fb46137"]
}

output "vpc_id" {
  value = "vpc-6fd4b409"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-ml-cluster-dev-opsdx-io" {
  elb                    = "${aws_elb.api-ml-cluster-dev-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-ml-cluster-dev-opsdx-io.id}"
}

resource "aws_autoscaling_group" "c2dw-etl-ml-cluster-dev-opsdx-io" {
  name                 = "c2dw-etl.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.c2dw-etl-ml-cluster-dev-opsdx-io.id}"
  max_size             = 5
  min_size             = 0
  vpc_zone_identifier  = ["subnet-7fb46137"]

  tag = {
    key                 = "Component"
    value               = "C2DW ETL"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "c2dw-etl.ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-ml-cluster-dev-opsdx-io" {
  name                 = "master-us-east-1d.masters.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-ml-cluster-dev-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-7fb46137"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "train-ml-cluster-dev-opsdx-io" {
  name                 = "train.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.train-ml-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-7fb46137"]

  tag = {
    key                 = "Component"
    value               = "Training  Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "train.ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "utility-ml-cluster-dev-opsdx-io" {
  name                 = "utility.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.utility-ml-cluster-dev-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-7fb46137"]

  tag = {
    key                 = "Component"
    value               = "Cluster Utility Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "utility.ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "d-etcd-events-ml-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "ml-cluster.dev.opsdx.io"
    Name                 = "d.etcd-events.ml-cluster.dev.opsdx.io"
    "k8s.io/etcd/events" = "d/d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-ml-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "ml-cluster.dev.opsdx.io"
    Name                 = "d.etcd-main.ml-cluster.dev.opsdx.io"
    "k8s.io/etcd/main"   = "d/d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_elb" "api-ml-cluster-dev-opsdx-io" {
  name = "api-ml-cluster-dev-opsdx--a2ljfg"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-ml-cluster-dev-opsdx-io.id}"]
  subnets         = ["subnet-20b56068"]

  health_check = {
    target              = "TCP:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  idle_timeout = 300

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "api.ml-cluster.dev.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-ml-cluster-dev-opsdx-io" {
  name  = "masters.ml-cluster.dev.opsdx.io"
  roles = ["${aws_iam_role.masters-ml-cluster-dev-opsdx-io.name}"]
}

resource "aws_iam_instance_profile" "nodes-ml-cluster-dev-opsdx-io" {
  name  = "nodes.ml-cluster.dev.opsdx.io"
  roles = ["${aws_iam_role.nodes-ml-cluster-dev-opsdx-io.name}"]
}

resource "aws_iam_role" "masters-ml-cluster-dev-opsdx-io" {
  name               = "masters.ml-cluster.dev.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.ml-cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-ml-cluster-dev-opsdx-io" {
  name               = "nodes.ml-cluster.dev.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.ml-cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-ml-cluster-dev-opsdx-io" {
  name   = "masters.ml-cluster.dev.opsdx.io"
  role   = "${aws_iam_role.masters-ml-cluster-dev-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.ml-cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-ml-cluster-dev-opsdx-io" {
  name   = "nodes.ml-cluster.dev.opsdx.io"
  role   = "${aws_iam_role.nodes-ml-cluster-dev-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.ml-cluster.dev.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23" {
  key_name   = "kubernetes.ml-cluster.dev.opsdx.io-94:a2:2f:95:b3:cc:fd:3f:4d:a6:d2:15:22:59:2b:23"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.ml-cluster.dev.opsdx.io-94a22f95b3ccfd3f4da6d21522592b23_public_key")}"
}

resource "aws_launch_configuration" "c2dw-etl-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "c2dw-etl.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "m4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_c2dw-etl.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1d-masters-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "train-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "train.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "m4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_train.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "utility-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "utility.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.micro"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_utility.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "api-ml-cluster-dev-opsdx-io" {
  name = "api.ml-cluster.dev.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-ml-cluster-dev-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-ml-cluster-dev-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z2GCTM75P01WXX"
}

resource "aws_security_group" "api-elb-ml-cluster-dev-opsdx-io" {
  name        = "api-elb.ml-cluster.dev.opsdx.io"
  vpc_id      = "vpc-6fd4b409"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "api-elb.ml-cluster.dev.opsdx.io"
  }
}

resource "aws_security_group" "masters-ml-cluster-dev-opsdx-io" {
  name        = "masters.ml-cluster.dev.opsdx.io"
  vpc_id      = "vpc-6fd4b409"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "masters.ml-cluster.dev.opsdx.io"
  }
}

resource "aws_security_group" "nodes-ml-cluster-dev-opsdx-io" {
  name        = "nodes.ml-cluster.dev.opsdx.io"
  vpc_id      = "vpc-6fd4b409"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "nodes.ml-cluster.dev.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-ml-cluster-dev-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-ml-cluster-dev-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-ml-cluster-dev-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  from_port                = 1
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-ml-cluster-dev-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "ssh-external-to-node-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}