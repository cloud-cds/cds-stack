output "cluster_name" {
  value = "cluster.dev.metaboliccompass.com"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"]
}

output "masters_role_arn" {
  value = "${aws_iam_role.masters-cluster-dev-metaboliccompass-com.arn}"
}

output "masters_role_name" {
  value = "${aws_iam_role.masters-cluster-dev-metaboliccompass-com.name}"
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"]
}

output "node_subnet_ids" {
  value = ["${aws_subnet.us-east-1a-cluster-dev-metaboliccompass-com.id}", "${aws_subnet.us-east-1c-cluster-dev-metaboliccompass-com.id}", "${aws_subnet.us-east-1d-cluster-dev-metaboliccompass-com.id}"]
}

output "nodes_role_arn" {
  value = "${aws_iam_role.nodes-cluster-dev-metaboliccompass-com.arn}"
}

output "nodes_role_name" {
  value = "${aws_iam_role.nodes-cluster-dev-metaboliccompass-com.name}"
}

output "region" {
  value = "us-east-1"
}

output "vpc_id" {
  value = "vpc-3aeaeb41"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1a-masters-cluster-dev-metaboliccompass-com" {
  elb                    = "${aws_elb.api-cluster-dev-metaboliccompass-com.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1a-masters-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1c-masters-cluster-dev-metaboliccompass-com" {
  elb                    = "${aws_elb.api-cluster-dev-metaboliccompass-com.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1c-masters-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-dev-metaboliccompass-com" {
  elb                    = "${aws_elb.api-cluster-dev-metaboliccompass-com.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_autoscaling_group" "master-us-east-1a-masters-cluster-dev-metaboliccompass-com" {
  name                 = "master-us-east-1a.masters.cluster.dev.metaboliccompass.com"
  launch_configuration = "${aws_launch_configuration.master-us-east-1a-masters-cluster-dev-metaboliccompass-com.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-metaboliccompass-com.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1a.masters.cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/kops.k8s.io/instancegroup"
    value               = "master-us-east-1a"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }

  metrics_granularity = "1Minute"
  enabled_metrics     = ["GroupDesiredCapacity", "GroupInServiceInstances", "GroupMaxSize", "GroupMinSize", "GroupPendingInstances", "GroupStandbyInstances", "GroupTerminatingInstances", "GroupTotalInstances"]
}

resource "aws_autoscaling_group" "master-us-east-1c-masters-cluster-dev-metaboliccompass-com" {
  name                 = "master-us-east-1c.masters.cluster.dev.metaboliccompass.com"
  launch_configuration = "${aws_launch_configuration.master-us-east-1c-masters-cluster-dev-metaboliccompass-com.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1c-cluster-dev-metaboliccompass-com.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1c.masters.cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/kops.k8s.io/instancegroup"
    value               = "master-us-east-1c"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }

  metrics_granularity = "1Minute"
  enabled_metrics     = ["GroupDesiredCapacity", "GroupInServiceInstances", "GroupMaxSize", "GroupMinSize", "GroupPendingInstances", "GroupStandbyInstances", "GroupTerminatingInstances", "GroupTotalInstances"]
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-dev-metaboliccompass-com" {
  name                 = "master-us-east-1d.masters.cluster.dev.metaboliccompass.com"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-dev-metaboliccompass-com.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-cluster-dev-metaboliccompass-com.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/kops.k8s.io/instancegroup"
    value               = "master-us-east-1d"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }

  metrics_granularity = "1Minute"
  enabled_metrics     = ["GroupDesiredCapacity", "GroupInServiceInstances", "GroupMaxSize", "GroupMinSize", "GroupPendingInstances", "GroupStandbyInstances", "GroupTerminatingInstances", "GroupTotalInstances"]
}

resource "aws_autoscaling_group" "nodes-cluster-dev-metaboliccompass-com" {
  name                 = "nodes.cluster.dev.metaboliccompass.com"
  launch_configuration = "${aws_launch_configuration.nodes-cluster-dev-metaboliccompass-com.id}"
  max_size             = 3
  min_size             = 3
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-metaboliccompass-com.id}", "${aws_subnet.us-east-1c-cluster-dev-metaboliccompass-com.id}", "${aws_subnet.us-east-1d-cluster-dev-metaboliccompass-com.id}"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "nodes.cluster.dev.metaboliccompass.com"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/kops.k8s.io/instancegroup"
    value               = "nodes"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }

  metrics_granularity = "1Minute"
  enabled_metrics     = ["GroupDesiredCapacity", "GroupInServiceInstances", "GroupMaxSize", "GroupMinSize", "GroupPendingInstances", "GroupStandbyInstances", "GroupTerminatingInstances", "GroupTotalInstances"]
}

resource "aws_ebs_volume" "a-etcd-events-cluster-dev-metaboliccompass-com" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "a.etcd-events.cluster.dev.metaboliccompass.com"
    "k8s.io/etcd/events"                                     = "a/a,c,d"
    "k8s.io/role/master"                                     = "1"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_ebs_volume" "a-etcd-main-cluster-dev-metaboliccompass-com" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "a.etcd-main.cluster.dev.metaboliccompass.com"
    "k8s.io/etcd/main"                                       = "a/a,c,d"
    "k8s.io/role/master"                                     = "1"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_ebs_volume" "c-etcd-events-cluster-dev-metaboliccompass-com" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "c.etcd-events.cluster.dev.metaboliccompass.com"
    "k8s.io/etcd/events"                                     = "c/a,c,d"
    "k8s.io/role/master"                                     = "1"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_ebs_volume" "c-etcd-main-cluster-dev-metaboliccompass-com" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "c.etcd-main.cluster.dev.metaboliccompass.com"
    "k8s.io/etcd/main"                                       = "c/a,c,d"
    "k8s.io/role/master"                                     = "1"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_ebs_volume" "d-etcd-events-cluster-dev-metaboliccompass-com" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "d.etcd-events.cluster.dev.metaboliccompass.com"
    "k8s.io/etcd/events"                                     = "d/a,c,d"
    "k8s.io/role/master"                                     = "1"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_ebs_volume" "d-etcd-main-cluster-dev-metaboliccompass-com" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "d.etcd-main.cluster.dev.metaboliccompass.com"
    "k8s.io/etcd/main"                                       = "d/a,c,d"
    "k8s.io/role/master"                                     = "1"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_eip" "us-east-1a-cluster-dev-metaboliccompass-com" {
  vpc = true

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1a.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_eip" "us-east-1c-cluster-dev-metaboliccompass-com" {
  vpc = true

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1c.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_eip" "us-east-1d-cluster-dev-metaboliccompass-com" {
  vpc = true

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1d.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_elb" "api-cluster-dev-metaboliccompass-com" {
  name = "api-cluster-dev-metabolic-2c870g"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-cluster-dev-metaboliccompass-com.id}"]
  subnets         = ["${aws_subnet.utility-us-east-1a-cluster-dev-metaboliccompass-com.id}", "${aws_subnet.utility-us-east-1c-cluster-dev-metaboliccompass-com.id}", "${aws_subnet.utility-us-east-1d-cluster-dev-metaboliccompass-com.id}"]

  health_check = {
    target              = "SSL:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  idle_timeout = 300

  tags = {
    KubernetesCluster = "cluster.dev.metaboliccompass.com"
    Name              = "api.cluster.dev.metaboliccompass.com"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-dev-metaboliccompass-com" {
  name = "masters.cluster.dev.metaboliccompass.com"
  role = "${aws_iam_role.masters-cluster-dev-metaboliccompass-com.name}"
}

resource "aws_iam_instance_profile" "nodes-cluster-dev-metaboliccompass-com" {
  name = "nodes.cluster.dev.metaboliccompass.com"
  role = "${aws_iam_role.nodes-cluster-dev-metaboliccompass-com.name}"
}

resource "aws_iam_role" "masters-cluster-dev-metaboliccompass-com" {
  name               = "masters.cluster.dev.metaboliccompass.com"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster.dev.metaboliccompass.com_policy")}"
}

resource "aws_iam_role" "nodes-cluster-dev-metaboliccompass-com" {
  name               = "nodes.cluster.dev.metaboliccompass.com"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster.dev.metaboliccompass.com_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-dev-metaboliccompass-com" {
  name   = "masters.cluster.dev.metaboliccompass.com"
  role   = "${aws_iam_role.masters-cluster-dev-metaboliccompass-com.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster.dev.metaboliccompass.com_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-dev-metaboliccompass-com" {
  name   = "nodes.cluster.dev.metaboliccompass.com"
  role   = "${aws_iam_role.nodes-cluster-dev-metaboliccompass-com.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster.dev.metaboliccompass.com_policy")}"
}

resource "aws_key_pair" "kubernetes-cluster-dev-metaboliccompass-com-0b64dcc9ed6b85395fea97c10c2d19b2" {
  key_name   = "kubernetes.cluster.dev.metaboliccompass.com-0b:64:dc:c9:ed:6b:85:39:5f:ea:97:c1:0c:2d:19:b2"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster.dev.metaboliccompass.com-0b64dcc9ed6b85395fea97c10c2d19b2_public_key")}"
}

resource "aws_launch_configuration" "master-us-east-1a-masters-cluster-dev-metaboliccompass-com" {
  name_prefix                 = "master-us-east-1a.masters.cluster.dev.metaboliccompass.com-"
  image_id                    = "ami-dbd611a6"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-metaboliccompass-com-0b64dcc9ed6b85395fea97c10c2d19b2.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-metaboliccompass-com.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1a.masters.cluster.dev.metaboliccompass.com_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  enable_monitoring = false
}

resource "aws_launch_configuration" "master-us-east-1c-masters-cluster-dev-metaboliccompass-com" {
  name_prefix                 = "master-us-east-1c.masters.cluster.dev.metaboliccompass.com-"
  image_id                    = "ami-dbd611a6"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-metaboliccompass-com-0b64dcc9ed6b85395fea97c10c2d19b2.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-metaboliccompass-com.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1c.masters.cluster.dev.metaboliccompass.com_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  enable_monitoring = false
}

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-dev-metaboliccompass-com" {
  name_prefix                 = "master-us-east-1d.masters.cluster.dev.metaboliccompass.com-"
  image_id                    = "ami-dbd611a6"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-metaboliccompass-com-0b64dcc9ed6b85395fea97c10c2d19b2.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-metaboliccompass-com.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster.dev.metaboliccompass.com_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  enable_monitoring = false
}

resource "aws_launch_configuration" "nodes-cluster-dev-metaboliccompass-com" {
  name_prefix                 = "nodes.cluster.dev.metaboliccompass.com-"
  image_id                    = "ami-dbd611a6"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-metaboliccompass-com-0b64dcc9ed6b85395fea97c10c2d19b2.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-metaboliccompass-com.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.dev.metaboliccompass.com_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  enable_monitoring = false
}

resource "aws_nat_gateway" "us-east-1a-cluster-dev-metaboliccompass-com" {
  allocation_id = "${aws_eip.us-east-1a-cluster-dev-metaboliccompass-com.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1a-cluster-dev-metaboliccompass-com.id}"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1a.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_nat_gateway" "us-east-1c-cluster-dev-metaboliccompass-com" {
  allocation_id = "${aws_eip.us-east-1c-cluster-dev-metaboliccompass-com.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1c-cluster-dev-metaboliccompass-com.id}"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1c.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_nat_gateway" "us-east-1d-cluster-dev-metaboliccompass-com" {
  allocation_id = "${aws_eip.us-east-1d-cluster-dev-metaboliccompass-com.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1d-cluster-dev-metaboliccompass-com.id}"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1d.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_route" "0-0-0-0--0" {
  route_table_id         = "${aws_route_table.cluster-dev-metaboliccompass-com.id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "igw-40256a38"
}

resource "aws_route" "private-us-east-1a-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1a-cluster-dev-metaboliccompass-com.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1a-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route" "private-us-east-1c-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1c-cluster-dev-metaboliccompass-com.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1c-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route" "private-us-east-1d-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1d-cluster-dev-metaboliccompass-com.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1d-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route53_record" "api-cluster-dev-metaboliccompass-com" {
  name = "api.cluster.dev.metaboliccompass.com"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-cluster-dev-metaboliccompass-com.dns_name}"
    zone_id                = "${aws_elb.api-cluster-dev-metaboliccompass-com.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/ZW5ZBVCD3D0MB"
}

resource "aws_route_table" "cluster-dev-metaboliccompass-com" {
  vpc_id = "vpc-3aeaeb41"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/kops/role"                                = "public"
  }
}

resource "aws_route_table" "private-us-east-1a-cluster-dev-metaboliccompass-com" {
  vpc_id = "vpc-3aeaeb41"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "private-us-east-1a.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/kops/role"                                = "private-us-east-1a"
  }
}

resource "aws_route_table" "private-us-east-1c-cluster-dev-metaboliccompass-com" {
  vpc_id = "vpc-3aeaeb41"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "private-us-east-1c.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/kops/role"                                = "private-us-east-1c"
  }
}

resource "aws_route_table" "private-us-east-1d-cluster-dev-metaboliccompass-com" {
  vpc_id = "vpc-3aeaeb41"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "private-us-east-1d.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/kops/role"                                = "private-us-east-1d"
  }
}

resource "aws_route_table_association" "private-us-east-1a-cluster-dev-metaboliccompass-com" {
  subnet_id      = "${aws_subnet.us-east-1a-cluster-dev-metaboliccompass-com.id}"
  route_table_id = "${aws_route_table.private-us-east-1a-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route_table_association" "private-us-east-1c-cluster-dev-metaboliccompass-com" {
  subnet_id      = "${aws_subnet.us-east-1c-cluster-dev-metaboliccompass-com.id}"
  route_table_id = "${aws_route_table.private-us-east-1c-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route_table_association" "private-us-east-1d-cluster-dev-metaboliccompass-com" {
  subnet_id      = "${aws_subnet.us-east-1d-cluster-dev-metaboliccompass-com.id}"
  route_table_id = "${aws_route_table.private-us-east-1d-cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route_table_association" "utility-us-east-1a-cluster-dev-metaboliccompass-com" {
  subnet_id      = "${aws_subnet.utility-us-east-1a-cluster-dev-metaboliccompass-com.id}"
  route_table_id = "${aws_route_table.cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route_table_association" "utility-us-east-1c-cluster-dev-metaboliccompass-com" {
  subnet_id      = "${aws_subnet.utility-us-east-1c-cluster-dev-metaboliccompass-com.id}"
  route_table_id = "${aws_route_table.cluster-dev-metaboliccompass-com.id}"
}

resource "aws_route_table_association" "utility-us-east-1d-cluster-dev-metaboliccompass-com" {
  subnet_id      = "${aws_subnet.utility-us-east-1d-cluster-dev-metaboliccompass-com.id}"
  route_table_id = "${aws_route_table.cluster-dev-metaboliccompass-com.id}"
}

resource "aws_security_group" "api-elb-cluster-dev-metaboliccompass-com" {
  name        = "api-elb.cluster.dev.metaboliccompass.com"
  vpc_id      = "vpc-3aeaeb41"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "api-elb.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_security_group" "masters-cluster-dev-metaboliccompass-com" {
  name        = "masters.cluster.dev.metaboliccompass.com"
  vpc_id      = "vpc-3aeaeb41"
  description = "Security group for masters"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "masters.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_security_group" "nodes-cluster-dev-metaboliccompass-com" {
  name        = "nodes.cluster.dev.metaboliccompass.com"
  vpc_id      = "vpc-3aeaeb41"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "nodes.cluster.dev.metaboliccompass.com"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-metaboliccompass-com.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-metaboliccompass-com.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-dev-metaboliccompass-com.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-2379" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port                = 1
  to_port                  = 2379
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-2382-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port                = 2382
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-dev-metaboliccompass-com.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "ssh-external-to-node-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-metaboliccompass-com.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_subnet" "us-east-1a-cluster-dev-metaboliccompass-com" {
  vpc_id            = "vpc-3aeaeb41"
  cidr_block        = "10.0.32.0/19"
  availability_zone = "us-east-1a"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1a.cluster.dev.metaboliccompass.com"
    SubnetType                                               = "Private"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/role/internal-elb"                        = "1"
  }
}

resource "aws_subnet" "us-east-1c-cluster-dev-metaboliccompass-com" {
  vpc_id            = "vpc-3aeaeb41"
  cidr_block        = "10.0.64.0/19"
  availability_zone = "us-east-1c"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1c.cluster.dev.metaboliccompass.com"
    SubnetType                                               = "Private"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/role/internal-elb"                        = "1"
  }
}

resource "aws_subnet" "us-east-1d-cluster-dev-metaboliccompass-com" {
  vpc_id            = "vpc-3aeaeb41"
  cidr_block        = "10.0.96.0/19"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "us-east-1d.cluster.dev.metaboliccompass.com"
    SubnetType                                               = "Private"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/role/internal-elb"                        = "1"
  }
}

resource "aws_subnet" "utility-us-east-1a-cluster-dev-metaboliccompass-com" {
  vpc_id            = "vpc-3aeaeb41"
  cidr_block        = "10.0.0.0/22"
  availability_zone = "us-east-1a"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "utility-us-east-1a.cluster.dev.metaboliccompass.com"
    SubnetType                                               = "Utility"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/role/elb"                                 = "1"
  }
}

resource "aws_subnet" "utility-us-east-1c-cluster-dev-metaboliccompass-com" {
  vpc_id            = "vpc-3aeaeb41"
  cidr_block        = "10.0.4.0/22"
  availability_zone = "us-east-1c"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "utility-us-east-1c.cluster.dev.metaboliccompass.com"
    SubnetType                                               = "Utility"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/role/elb"                                 = "1"
  }
}

resource "aws_subnet" "utility-us-east-1d-cluster-dev-metaboliccompass-com" {
  vpc_id            = "vpc-3aeaeb41"
  cidr_block        = "10.0.8.0/22"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster                                        = "cluster.dev.metaboliccompass.com"
    Name                                                     = "utility-us-east-1d.cluster.dev.metaboliccompass.com"
    SubnetType                                               = "Utility"
    "kubernetes.io/cluster/cluster.dev.metaboliccompass.com" = "owned"
    "kubernetes.io/role/elb"                                 = "1"
  }
}

terraform = {
  required_version = ">= 0.9.3"
}
