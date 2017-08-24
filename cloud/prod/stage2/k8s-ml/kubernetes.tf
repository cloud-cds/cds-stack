output "cluster_name" {
  value = "ml-cluster.prod.opsdx.io"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"]
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"]
}

output "node_subnet_ids" {
  value = ["subnet-50db4918"]
}

output "region" {
  value = "us-east-1"
}

output "subnet_ids" {
  value = ["subnet-39de4c71", "subnet-50db4918"]
}

output "vpc_id" {
  value = "vpc-36c6a650"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-ml-cluster-prod-opsdx-io" {
  elb                    = "${aws_elb.api-ml-cluster-prod-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-ml-cluster-prod-opsdx-io.id}"
}

resource "aws_autoscaling_group" "c2dw-etl-ml-cluster-prod-opsdx-io" {
  name                 = "c2dw-etl.ml-cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.c2dw-etl-ml-cluster-prod-opsdx-io.id}"
  max_size             = 5
  min_size             = 0
  vpc_zone_identifier  = ["subnet-50db4918"]

  tag = {
    key                 = "Component"
    value               = "C2DW ETL"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "c2dw-etl.ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Prod-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-ml-cluster-prod-opsdx-io" {
  name                 = "master-us-east-1d.masters.ml-cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-ml-cluster-prod-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-50db4918"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "train-ml-cluster-prod-opsdx-io" {
  name                 = "train.ml-cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.train-ml-cluster-prod-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-50db4918"]

  tag = {
    key                 = "Component"
    value               = "Training Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "train.ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Prod-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "utility-ml-cluster-prod-opsdx-io" {
  name                 = "utility.ml-cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.utility-ml-cluster-prod-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-50db4918"]

  tag = {
    key                 = "Component"
    value               = "Cluster Utility Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "utility.ml-cluster.prod.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Prod-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "d-etcd-events-ml-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "ml-cluster.prod.opsdx.io"
    Name                 = "d.etcd-events.ml-cluster.prod.opsdx.io"
    "k8s.io/etcd/events" = "d/d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-ml-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "ml-cluster.prod.opsdx.io"
    Name                 = "d.etcd-main.ml-cluster.prod.opsdx.io"
    "k8s.io/etcd/main"   = "d/d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_elb" "api-ml-cluster-prod-opsdx-io" {
  name = "api-ml-cluster-prod-opsdx-gfb626"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-ml-cluster-prod-opsdx-io.id}"]
  subnets         = ["subnet-39de4c71"]

  health_check = {
    target              = "TCP:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  idle_timeout = 300

  tags = {
    KubernetesCluster = "ml-cluster.prod.opsdx.io"
    Name              = "api.ml-cluster.prod.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-ml-cluster-prod-opsdx-io" {
  name  = "masters.ml-cluster.prod.opsdx.io"
  roles = ["${aws_iam_role.masters-ml-cluster-prod-opsdx-io.name}"]
}

resource "aws_iam_instance_profile" "nodes-ml-cluster-prod-opsdx-io" {
  name  = "nodes.ml-cluster.prod.opsdx.io"
  roles = ["${aws_iam_role.nodes-ml-cluster-prod-opsdx-io.name}"]
}

resource "aws_iam_role" "masters-ml-cluster-prod-opsdx-io" {
  name               = "masters.ml-cluster.prod.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.ml-cluster.prod.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-ml-cluster-prod-opsdx-io" {
  name               = "nodes.ml-cluster.prod.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.ml-cluster.prod.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-ml-cluster-prod-opsdx-io" {
  name   = "masters.ml-cluster.prod.opsdx.io"
  role   = "${aws_iam_role.masters-ml-cluster-prod-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.ml-cluster.prod.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-ml-cluster-prod-opsdx-io" {
  name   = "nodes.ml-cluster.prod.opsdx.io"
  role   = "${aws_iam_role.nodes-ml-cluster-prod-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.ml-cluster.prod.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-ml-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824" {
  key_name   = "kubernetes.ml-cluster.prod.opsdx.io-55:1d:14:81:20:28:ed:6f:ac:47:5b:92:d6:89:38:24"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.ml-cluster.prod.opsdx.io-551d14812028ed6fac475b92d6893824_public_key")}"
}

resource "aws_launch_configuration" "c2dw-etl-ml-cluster-prod-opsdx-io" {
  name_prefix                 = "c2dw-etl.ml-cluster.prod.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-prod-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_c2dw-etl.ml-cluster.prod.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1d-masters-ml-cluster-prod-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.ml-cluster.prod.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-ml-cluster-prod-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.ml-cluster.prod.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "train-ml-cluster-prod-opsdx-io" {
  name_prefix                 = "train.ml-cluster.prod.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-prod-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_train.ml-cluster.prod.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "utility-ml-cluster-prod-opsdx-io" {
  name_prefix                 = "utility.ml-cluster.prod.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "t2.micro"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-prod-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_utility.ml-cluster.prod.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "api-ml-cluster-prod-opsdx-io" {
  name = "api.ml-cluster.prod.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-ml-cluster-prod-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-ml-cluster-prod-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z2SG1R6D1TVBL3"
}

resource "aws_security_group" "api-elb-ml-cluster-prod-opsdx-io" {
  name        = "api-elb.ml-cluster.prod.opsdx.io"
  vpc_id      = "vpc-36c6a650"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "ml-cluster.prod.opsdx.io"
    Name              = "api-elb.ml-cluster.prod.opsdx.io"
  }
}

resource "aws_security_group" "masters-ml-cluster-prod-opsdx-io" {
  name        = "masters.ml-cluster.prod.opsdx.io"
  vpc_id      = "vpc-36c6a650"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "ml-cluster.prod.opsdx.io"
    Name              = "masters.ml-cluster.prod.opsdx.io"
  }
}

resource "aws_security_group" "nodes-ml-cluster-prod-opsdx-io" {
  name        = "nodes.ml-cluster.prod.opsdx.io"
  vpc_id      = "vpc-36c6a650"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "ml-cluster.prod.opsdx.io"
    Name              = "nodes.ml-cluster.prod.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-ml-cluster-prod-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-ml-cluster-prod-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-ml-cluster-prod-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  from_port                = 1
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-ml-cluster-prod-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "ssh-external-to-node-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-ml-cluster-prod-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}
