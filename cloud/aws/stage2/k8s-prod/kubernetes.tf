output "cluster_name" {
  value = "cluster-prod.jh.opsdx.io"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"]
}

output "masters_role_arn" {
  value = "${aws_iam_role.masters-cluster-prod-jh-opsdx-io.arn}"
}

output "masters_role_name" {
  value = "${aws_iam_role.masters-cluster-prod-jh-opsdx-io.name}"
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
}

output "node_subnet_ids" {
  value = ["subnet-52acb31a", "subnet-89c49da5", "subnet-b8b3e1e2"]
}

output "nodes_role_arn" {
  value = "${aws_iam_role.nodes-cluster-prod-jh-opsdx-io.arn}"
}

output "nodes_role_name" {
  value = "${aws_iam_role.nodes-cluster-prod-jh-opsdx-io.name}"
}

output "region" {
  value = "us-east-1"
}

output "subnet_ids" {
  value = ["subnet-19c39a35", "subnet-52acb31a", "subnet-89c49da5", "subnet-b8b3e1e2", "subnet-e5beecbf", "subnet-e9a9b6a1"]
}

output "vpc_id" {
  value = "vpc-0234067b"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1a-masters-cluster-prod-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-prod-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1a-masters-cluster-prod-jh-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1c-masters-cluster-prod-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-prod-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1c-masters-cluster-prod-jh-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-prod-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-prod-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-prod-jh-opsdx-io.id}"
}

resource "aws_autoscaling_group" "etl-cluster-prod-jh-opsdx-io" {
  name                 = "etl.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.etl-cluster-prod-jh-opsdx-io.id}"
  desired_capacity     = 1
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "ETL Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "etl.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "etl"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "locust-cluster-prod-jh-opsdx-io" {
  name                 = "locust.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.locust-cluster-prod-jh-opsdx-io.id}"
  max_size             = 100
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Locust Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "locust.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "locust"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1a-masters-cluster-prod-jh-opsdx-io" {
  name                 = "master-us-east-1a.masters.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1a-masters-cluster-prod-jh-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-b8b3e1e2"]

  tag = {
    key                 = "Component"
    value               = "Master-1a"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1a.masters.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1c-masters-cluster-prod-jh-opsdx-io" {
  name                 = "master-us-east-1c.masters.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1c-masters-cluster-prod-jh-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-89c49da5"]

  tag = {
    key                 = "Component"
    value               = "Master-1c"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1c.masters.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-prod-jh-opsdx-io" {
  name                 = "master-us-east-1d.masters.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-prod-jh-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Master-1d"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "nodes-cluster-prod-jh-opsdx-io" {
  name                 = "nodes.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.nodes-cluster-prod-jh-opsdx-io.id}"
  desired_capacity     = 1
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "nodes.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "nodes"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "predictor-cluster-prod-jh-opsdx-io" {
  name                 = "predictor.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor-cluster-prod-jh-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "predictor"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "spot-nodes-cluster-prod-jh-opsdx-io" {
  name                 = "spot-nodes.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.spot-nodes-cluster-prod-jh-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Spot Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "spot-nodes.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "spot-nodes"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "spot-predictor-cluster-prod-jh-opsdx-io" {
  name                 = "spot-predictor.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.spot-predictor-cluster-prod-jh-opsdx-io.id}"
  max_size             = 64
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Spot Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "spot-predictor.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "spot-predictor"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "web-cluster-prod-jh-opsdx-io" {
  name                 = "web.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.web-cluster-prod-jh-opsdx-io.id}"
  desired_capacity     = 2
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Webserver Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "web.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "web"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "web2-cluster-prod-jh-opsdx-io" {
  name                 = "web2.cluster-prod.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.web2-cluster-prod-jh-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Webserver Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "web2.cluster-prod.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Production"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "web2"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "a-etcd-events-cluster-prod-jh-opsdx-io" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-prod.jh.opsdx.io"
    Name                 = "a.etcd-events.cluster-prod.jh.opsdx.io"
    "k8s.io/etcd/events" = "a/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "a-etcd-main-cluster-prod-jh-opsdx-io" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-prod.jh.opsdx.io"
    Name                 = "a.etcd-main.cluster-prod.jh.opsdx.io"
    "k8s.io/etcd/main"   = "a/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-events-cluster-prod-jh-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-prod.jh.opsdx.io"
    Name                 = "c.etcd-events.cluster-prod.jh.opsdx.io"
    "k8s.io/etcd/events" = "c/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-main-cluster-prod-jh-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-prod.jh.opsdx.io"
    Name                 = "c.etcd-main.cluster-prod.jh.opsdx.io"
    "k8s.io/etcd/main"   = "c/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-events-cluster-prod-jh-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-prod.jh.opsdx.io"
    Name                 = "d.etcd-events.cluster-prod.jh.opsdx.io"
    "k8s.io/etcd/events" = "d/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-cluster-prod-jh-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-prod.jh.opsdx.io"
    Name                 = "d.etcd-main.cluster-prod.jh.opsdx.io"
    "k8s.io/etcd/main"   = "d/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_elb" "api-cluster-prod-jh-opsdx-io" {
  name = "api-cluster-prod-jh-opsdx-9k6c4r"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-cluster-prod-jh-opsdx-io.id}"]
  subnets         = ["subnet-19c39a35", "subnet-e5beecbf", "subnet-e9a9b6a1"]

  health_check = {
    target              = "TCP:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  idle_timeout = 300

  tags = {
    KubernetesCluster = "cluster-prod.jh.opsdx.io"
    Name              = "api.cluster-prod.jh.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-prod-jh-opsdx-io" {
  name = "masters.cluster-prod.jh.opsdx.io"
  role = "${aws_iam_role.masters-cluster-prod-jh-opsdx-io.name}"
}

resource "aws_iam_instance_profile" "nodes-cluster-prod-jh-opsdx-io" {
  name = "nodes.cluster-prod.jh.opsdx.io"
  role = "${aws_iam_role.nodes-cluster-prod-jh-opsdx-io.name}"
}

resource "aws_iam_role" "masters-cluster-prod-jh-opsdx-io" {
  name               = "masters.cluster-prod.jh.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster-prod.jh.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-cluster-prod-jh-opsdx-io" {
  name               = "nodes.cluster-prod.jh.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster-prod.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "additional-nodes-cluster-prod-jh-opsdx-io" {
  name   = "additional.nodes.cluster-prod.jh.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-prod-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_additional.nodes.cluster-prod.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-prod-jh-opsdx-io" {
  name   = "masters.cluster-prod.jh.opsdx.io"
  role   = "${aws_iam_role.masters-cluster-prod-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster-prod.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-prod-jh-opsdx-io" {
  name   = "nodes.cluster-prod.jh.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-prod-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster-prod.jh.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824" {
  key_name   = "kubernetes.cluster-prod.jh.opsdx.io-55:1d:14:81:20:28:ed:6f:ac:47:5b:92:d6:89:38:24"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster-prod.jh.opsdx.io-551d14812028ed6fac475b92d6893824_public_key")}"
}

resource "aws_launch_configuration" "etl-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "etl.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_etl.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "locust-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "locust.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m3.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_locust.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  ephemeral_block_device = {
    device_name  = "/dev/sdc"
    virtual_name = "ephemeral0"
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.012"
}

resource "aws_launch_configuration" "master-us-east-1a-masters-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1a.masters.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1a.masters.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1c-masters-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1c.masters.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1c.masters.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "nodes-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "nodes.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "t2.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "predictor.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "spot-nodes-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "spot-nodes.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_spot-nodes.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.1330"
}

resource "aws_launch_configuration" "spot-predictor-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "spot-predictor.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_spot-predictor.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.2"
}

resource "aws_launch_configuration" "web-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "web.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_web.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "web2-cluster-prod-jh-opsdx-io" {
  name_prefix                 = "web2.cluster-prod.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-prod-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-prod-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_web2.cluster-prod.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "api-cluster-prod-jh-opsdx-io" {
  name = "api.cluster-prod.jh.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-cluster-prod-jh-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-cluster-prod-jh-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z216PFCPPMYV7T"
}

resource "aws_security_group" "api-elb-cluster-prod-jh-opsdx-io" {
  name        = "api-elb.cluster-prod.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "cluster-prod.jh.opsdx.io"
    Name              = "api-elb.cluster-prod.jh.opsdx.io"
  }
}

resource "aws_security_group" "masters-cluster-prod-jh-opsdx-io" {
  name        = "masters.cluster-prod.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "cluster-prod.jh.opsdx.io"
    Name              = "masters.cluster-prod.jh.opsdx.io"
  }
}

resource "aws_security_group" "nodes-cluster-prod-jh-opsdx-io" {
  name        = "nodes.cluster-prod.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "cluster-prod.jh.opsdx.io"
    Name              = "nodes.cluster-prod.jh.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-prod-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-prod-jh-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-prod-jh-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  from_port                = 1
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-10-0-0-0--16" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-prod-jh-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/16"]
}

resource "aws_security_group_rule" "ssh-external-to-node-10-0-0-0--16" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-prod-jh-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/16"]
}

terraform = {
  required_version = ">= 0.9.3"
}
