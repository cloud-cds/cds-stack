provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1a-masters-cluster-prod-opsdx-io" {
  elb = "${aws_elb.api-cluster-prod-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1a-masters-cluster-prod-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1c-masters-cluster-prod-opsdx-io" {
  elb = "${aws_elb.api-cluster-prod-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1c-masters-cluster-prod-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-prod-opsdx-io" {
  elb = "${aws_elb.api-cluster-prod-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-prod-opsdx-io.id}"
}

resource "aws_autoscaling_group" "master-us-east-1a-masters-cluster-prod-opsdx-io" {
  name = "master-us-east-1a.masters.cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1a-masters-cluster-prod-opsdx-io.id}"
  max_size = 1
  min_size = 1
  vpc_zone_identifier = ["${aws_subnet.us-east-1a-cluster-prod-opsdx-io.id}"]
  tag = {
    key = "Component"
    value = "Master-1a"
    propagate_at_launch = true
  }
  tag = {
    key = "KubernetesCluster"
    value = "cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "master-us-east-1a.masters.cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Stack"
    value = "Production"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/master"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1c-masters-cluster-prod-opsdx-io" {
  name = "master-us-east-1c.masters.cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1c-masters-cluster-prod-opsdx-io.id}"
  max_size = 1
  min_size = 1
  vpc_zone_identifier = ["${aws_subnet.us-east-1c-cluster-prod-opsdx-io.id}"]
  tag = {
    key = "Component"
    value = "Master-1c"
    propagate_at_launch = true
  }
  tag = {
    key = "KubernetesCluster"
    value = "cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "master-us-east-1c.masters.cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Stack"
    value = "Production"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/master"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-prod-opsdx-io" {
  name = "master-us-east-1d.masters.cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-prod-opsdx-io.id}"
  max_size = 1
  min_size = 1
  vpc_zone_identifier = ["${aws_subnet.us-east-1d-cluster-prod-opsdx-io.id}"]
  tag = {
    key = "Component"
    value = "Master-1d"
    propagate_at_launch = true
  }
  tag = {
    key = "KubernetesCluster"
    value = "cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "master-us-east-1d.masters.cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Stack"
    value = "Production"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/master"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "nodes-cluster-prod-opsdx-io" {
  name = "nodes.cluster.prod.opsdx.io"
  launch_configuration = "${aws_launch_configuration.nodes-cluster-prod-opsdx-io.id}"
  max_size = 10
  min_size = 3
  vpc_zone_identifier = ["${aws_subnet.us-east-1a-cluster-prod-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-prod-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-prod-opsdx-io.id}"]
  tag = {
    key = "Component"
    value = "Node"
    propagate_at_launch = true
  }
  tag = {
    key = "KubernetesCluster"
    value = "cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "nodes.cluster.prod.opsdx.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Stack"
    value = "Production"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/node"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "us-east-1a-etcd-events-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1a"
  size = 20
  type = "gp2"
  encrypted = false
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1a.etcd-events.cluster.prod.opsdx.io"
    "k8s.io/etcd/events" = "us-east-1a/us-east-1a,us-east-1c,us-east-1d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "us-east-1a-etcd-main-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1a"
  size = 20
  type = "gp2"
  encrypted = false
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1a.etcd-main.cluster.prod.opsdx.io"
    "k8s.io/etcd/main" = "us-east-1a/us-east-1a,us-east-1c,us-east-1d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "us-east-1c-etcd-events-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1c"
  size = 20
  type = "gp2"
  encrypted = false
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1c.etcd-events.cluster.prod.opsdx.io"
    "k8s.io/etcd/events" = "us-east-1c/us-east-1a,us-east-1c,us-east-1d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "us-east-1c-etcd-main-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1c"
  size = 20
  type = "gp2"
  encrypted = false
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1c.etcd-main.cluster.prod.opsdx.io"
    "k8s.io/etcd/main" = "us-east-1c/us-east-1a,us-east-1c,us-east-1d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "us-east-1d-etcd-events-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1d"
  size = 20
  type = "gp2"
  encrypted = false
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1d.etcd-events.cluster.prod.opsdx.io"
    "k8s.io/etcd/events" = "us-east-1d/us-east-1a,us-east-1c,us-east-1d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "us-east-1d-etcd-main-cluster-prod-opsdx-io" {
  availability_zone = "us-east-1d"
  size = 20
  type = "gp2"
  encrypted = false
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1d.etcd-main.cluster.prod.opsdx.io"
    "k8s.io/etcd/main" = "us-east-1d/us-east-1a,us-east-1c,us-east-1d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_eip" "us-east-1a-cluster-prod-opsdx-io" {
  vpc = true
}

resource "aws_eip" "us-east-1c-cluster-prod-opsdx-io" {
  vpc = true
}

resource "aws_eip" "us-east-1d-cluster-prod-opsdx-io" {
  vpc = true
}

resource "aws_elb" "api-cluster-prod-opsdx-io" {
  name = "api-prod-cluster"
  listener = {
    instance_port = 443
    instance_protocol = "TCP"
    lb_port = 443
    lb_protocol = "TCP"
  }
  security_groups = ["${aws_security_group.api-elb-cluster-prod-opsdx-io.id}"]
  subnets = ["${aws_subnet.utility-us-east-1a-cluster-prod-opsdx-io.id}", "${aws_subnet.utility-us-east-1c-cluster-prod-opsdx-io.id}", "${aws_subnet.utility-us-east-1d-cluster-prod-opsdx-io.id}"]
  health_check = {
    target = "TCP:443"
    healthy_threshold = 2
    unhealthy_threshold = 2
    interval = 10
    timeout = 5
  }
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "api.cluster.prod.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-prod-opsdx-io" {
  name = "masters.cluster.prod.opsdx.io"
  roles = ["${aws_iam_role.masters-cluster-prod-opsdx-io.name}"]
}

resource "aws_iam_instance_profile" "nodes-cluster-prod-opsdx-io" {
  name = "nodes.cluster.prod.opsdx.io"
  roles = ["${aws_iam_role.nodes-cluster-prod-opsdx-io.name}"]
}

resource "aws_iam_role" "masters-cluster-prod-opsdx-io" {
  name = "masters.cluster.prod.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster.prod.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-cluster-prod-opsdx-io" {
  name = "nodes.cluster.prod.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster.prod.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-prod-opsdx-io" {
  name = "masters.cluster.prod.opsdx.io"
  role = "${aws_iam_role.masters-cluster-prod-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster.prod.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-prod-opsdx-io" {
  name = "nodes.cluster.prod.opsdx.io"
  role = "${aws_iam_role.nodes-cluster-prod-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster.prod.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824" {
  key_name = "kubernetes.cluster.prod.opsdx.io-55:1d:14:81:20:28:ed:6f:ac:47:5b:92:d6:89:38:24"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster.prod.opsdx.io-551d14812028ed6fac475b92d6893824_public_key")}"
}

resource "aws_launch_configuration" "master-us-east-1a-masters-cluster-prod-opsdx-io" {
  name_prefix = "master-us-east-1a.masters.cluster.prod.opsdx.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "t2.medium"
  key_name = "${aws_key_pair.kubernetes-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile = "${aws_iam_instance_profile.masters-cluster-prod-opsdx-io.id}"
  security_groups = ["${aws_security_group.masters-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1a.masters.cluster.prod.opsdx.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1c-masters-cluster-prod-opsdx-io" {
  name_prefix = "master-us-east-1c.masters.cluster.prod.opsdx.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "t2.medium"
  key_name = "${aws_key_pair.kubernetes-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile = "${aws_iam_instance_profile.masters-cluster-prod-opsdx-io.id}"
  security_groups = ["${aws_security_group.masters-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1c.masters.cluster.prod.opsdx.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-prod-opsdx-io" {
  name_prefix = "master-us-east-1d.masters.cluster.prod.opsdx.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "t2.medium"
  key_name = "${aws_key_pair.kubernetes-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile = "${aws_iam_instance_profile.masters-cluster-prod-opsdx-io.id}"
  security_groups = ["${aws_security_group.masters-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster.prod.opsdx.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "nodes-cluster-prod-opsdx-io" {
  name_prefix = "nodes.cluster.prod.opsdx.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "t2.large"
  key_name = "${aws_key_pair.kubernetes-cluster-prod-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile = "${aws_iam_instance_profile.nodes-cluster-prod-opsdx-io.id}"
  security_groups = ["${aws_security_group.nodes-cluster-prod-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.prod.opsdx.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_nat_gateway" "us-east-1a-cluster-prod-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1a-cluster-prod-opsdx-io.id}"
  subnet_id = "${aws_subnet.utility-us-east-1a-cluster-prod-opsdx-io.id}"
}

resource "aws_nat_gateway" "us-east-1c-cluster-prod-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1c-cluster-prod-opsdx-io.id}"
  subnet_id = "${aws_subnet.utility-us-east-1c-cluster-prod-opsdx-io.id}"
}

resource "aws_nat_gateway" "us-east-1d-cluster-prod-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1d-cluster-prod-opsdx-io.id}"
  subnet_id = "${aws_subnet.utility-us-east-1d-cluster-prod-opsdx-io.id}"
}

resource "aws_route" "0-0-0-0--0" {
  route_table_id = "${aws_route_table.cluster-prod-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id = "igw-820026e5"
}

resource "aws_route" "private-us-east-1a-0-0-0-0--0" {
  route_table_id = "${aws_route_table.private-us-east-1a-cluster-prod-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id = "${aws_nat_gateway.us-east-1a-cluster-prod-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1c-0-0-0-0--0" {
  route_table_id = "${aws_route_table.private-us-east-1c-cluster-prod-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id = "${aws_nat_gateway.us-east-1c-cluster-prod-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1d-0-0-0-0--0" {
  route_table_id = "${aws_route_table.private-us-east-1d-cluster-prod-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id = "${aws_nat_gateway.us-east-1d-cluster-prod-opsdx-io.id}"
}

resource "aws_route53_record" "api-cluster-prod-opsdx-io" {
  name = "api.cluster.prod.opsdx.io"
  type = "A"
  alias = {
    name = "${aws_elb.api-cluster-prod-opsdx-io.dns_name}"
    zone_id = "${aws_elb.api-cluster-prod-opsdx-io.zone_id}"
    evaluate_target_health = false
  }
  zone_id = "/hostedzone/Z2SG1R6D1TVBL3"
}

resource "aws_route_table" "cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "cluster.prod.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1a-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "private-us-east-1a.cluster.prod.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1c-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "private-us-east-1c.cluster.prod.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1d-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "private-us-east-1d.cluster.prod.opsdx.io"
  }
}

resource "aws_route_table_association" "private-us-east-1a-cluster-prod-opsdx-io" {
  subnet_id = "${aws_subnet.us-east-1a-cluster-prod-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1a-cluster-prod-opsdx-io.id}"
}

resource "aws_route_table_association" "private-us-east-1c-cluster-prod-opsdx-io" {
  subnet_id = "${aws_subnet.us-east-1c-cluster-prod-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1c-cluster-prod-opsdx-io.id}"
}

resource "aws_route_table_association" "private-us-east-1d-cluster-prod-opsdx-io" {
  subnet_id = "${aws_subnet.us-east-1d-cluster-prod-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1d-cluster-prod-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1a-cluster-prod-opsdx-io" {
  subnet_id = "${aws_subnet.utility-us-east-1a-cluster-prod-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-prod-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1c-cluster-prod-opsdx-io" {
  subnet_id = "${aws_subnet.utility-us-east-1c-cluster-prod-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-prod-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1d-cluster-prod-opsdx-io" {
  subnet_id = "${aws_subnet.utility-us-east-1d-cluster-prod-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-prod-opsdx-io.id}"
}

resource "aws_security_group" "api-elb-cluster-prod-opsdx-io" {
  name = "api-elb.cluster.prod.opsdx.io"
  vpc_id = "vpc-36c6a650"
  description = "Security group for api ELB"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "api-elb.cluster.prod.opsdx.io"
  }
}

resource "aws_security_group" "masters-cluster-prod-opsdx-io" {
  name = "masters.cluster.prod.opsdx.io"
  vpc_id = "vpc-36c6a650"
  description = "Security group for masters"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "masters.cluster.prod.opsdx.io"
  }
}

resource "aws_security_group" "nodes-cluster-prod-opsdx-io" {
  name = "nodes.cluster.prod.opsdx.io"
  vpc_id = "vpc-36c6a650"
  description = "Security group for nodes"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "nodes.cluster.prod.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  from_port = 0
  to_port = 0
  protocol = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  from_port = 0
  to_port = 0
  protocol = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 0
  to_port = 0
  protocol = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-prod-opsdx-io.id}"
  from_port = 0
  to_port = 0
  protocol = "-1"
  cidr_blocks = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-prod-opsdx-io.id}"
  from_port = 443
  to_port = 443
  protocol = "tcp"
  cidr_blocks = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-prod-opsdx-io.id}"
  from_port = 443
  to_port = 443
  protocol = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type = "egress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  from_port = 0
  to_port = 0
  protocol = "-1"
  cidr_blocks = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 0
  to_port = 0
  protocol = "-1"
  cidr_blocks = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-4194" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 4194
  to_port = 4194
  protocol = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-443" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 443
  to_port = 443
  protocol = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-6783" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 6783
  to_port = 6783
  protocol = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-6783" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 6783
  to_port = 6783
  protocol = "udp"
}

resource "aws_security_group_rule" "node-to-master-udp-6784" {
  type = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-opsdx-io.id}"
  from_port = 6784
  to_port = 6784
  protocol = "udp"
}

resource "aws_subnet" "us-east-1a-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  cidr_block = "10.0.32.0/19"
  availability_zone = "us-east-1a"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1a.cluster.prod.opsdx.io"
  }
}

resource "aws_subnet" "us-east-1c-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  cidr_block = "10.0.64.0/19"
  availability_zone = "us-east-1c"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1c.cluster.prod.opsdx.io"
  }
}

resource "aws_subnet" "us-east-1d-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  cidr_block = "10.0.96.0/19"
  availability_zone = "us-east-1d"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "us-east-1d.cluster.prod.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1a-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  cidr_block = "10.0.0.0/22"
  availability_zone = "us-east-1a"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "utility-us-east-1a.cluster.prod.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1c-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  cidr_block = "10.0.4.0/22"
  availability_zone = "us-east-1c"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "utility-us-east-1c.cluster.prod.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1d-cluster-prod-opsdx-io" {
  vpc_id = "vpc-36c6a650"
  cidr_block = "10.0.8.0/22"
  availability_zone = "us-east-1d"
  tags = {
    KubernetesCluster = "cluster.prod.opsdx.io"
    Name = "utility-us-east-1d.cluster.prod.opsdx.io"
  }
}
