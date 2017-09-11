output "cluster_name" {
  value = "cluster-dev-ml.jh.opsdx.io"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"]
}

output "masters_role_arn" {
  value = "${aws_iam_role.masters-cluster-dev-ml-jh-opsdx-io.arn}"
}

output "masters_role_name" {
  value = "${aws_iam_role.masters-cluster-dev-ml-jh-opsdx-io.name}"
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
}

output "node_subnet_ids" {
  value = ["subnet-52acb31a"]
}

output "nodes_role_arn" {
  value = "${aws_iam_role.nodes-cluster-dev-ml-jh-opsdx-io.arn}"
}

output "nodes_role_name" {
  value = "${aws_iam_role.nodes-cluster-dev-ml-jh-opsdx-io.name}"
}

output "region" {
  value = "us-east-1"
}

output "subnet_ids" {
  value = ["subnet-52acb31a", "subnet-e9a9b6a1"]
}

output "vpc_id" {
  value = "vpc-0234067b"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-dev-ml-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-ml-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-dev-ml-jh-opsdx-io.id}"
}

resource "aws_autoscaling_group" "c2dw-etl-cluster-dev-ml-jh-opsdx-io" {
  name                 = "c2dw-etl.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.c2dw-etl-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 5
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "C2DW ETL"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "c2dw-etl.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "c2dw-etl"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-dev-ml-jh-opsdx-io" {
  name                 = "master-us-east-1d.masters.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf1-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf1.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf1-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf1.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf1"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf10-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf10.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf10-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf10.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf10"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf11-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf11.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf11-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf11.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf11"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf12-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf12.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf12-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf12.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf12"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf2-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf2.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf2-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf2.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf2"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf3-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf3.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf3-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf3.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf3"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf4-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf4.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf4-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf4.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf4"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf5-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf5.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf5-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf5.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf5"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf6-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf6.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf6-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf6.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf6"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf7-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf7.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf7-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf7.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf7"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf8-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf8.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf8-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf8.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf8"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "tf9-cluster-dev-ml-jh-opsdx-io" {
  name                 = "tf9.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf9-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 65
  min_size             = 0
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf9.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "tf9"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "utility-cluster-dev-ml-jh-opsdx-io" {
  name                 = "utility.cluster-dev-ml.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.utility-cluster-dev-ml-jh-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Cluster Utility Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "utility.cluster-dev-ml.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Dev-ML-TF"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "utility"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "d-etcd-events-cluster-dev-ml-jh-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev-ml.jh.opsdx.io"
    Name                 = "d.etcd-events.cluster-dev-ml.jh.opsdx.io"
    "k8s.io/etcd/events" = "d/d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-cluster-dev-ml-jh-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev-ml.jh.opsdx.io"
    Name                 = "d.etcd-main.cluster-dev-ml.jh.opsdx.io"
    "k8s.io/etcd/main"   = "d/d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_elb" "api-cluster-dev-ml-jh-opsdx-io" {
  name = "api-cluster-dev-ml-jh-ops-ltcg3n"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-cluster-dev-ml-jh-opsdx-io.id}"]
  subnets         = ["subnet-e9a9b6a1"]

  health_check = {
    target              = "TCP:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  idle_timeout = 300

  tags = {
    KubernetesCluster = "cluster-dev-ml.jh.opsdx.io"
    Name              = "api.cluster-dev-ml.jh.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-dev-ml-jh-opsdx-io" {
  name = "masters.cluster-dev-ml.jh.opsdx.io"
  role = "${aws_iam_role.masters-cluster-dev-ml-jh-opsdx-io.name}"
}

resource "aws_iam_instance_profile" "nodes-cluster-dev-ml-jh-opsdx-io" {
  name = "nodes.cluster-dev-ml.jh.opsdx.io"
  role = "${aws_iam_role.nodes-cluster-dev-ml-jh-opsdx-io.name}"
}

resource "aws_iam_role" "masters-cluster-dev-ml-jh-opsdx-io" {
  name               = "masters.cluster-dev-ml.jh.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster-dev-ml.jh.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-cluster-dev-ml-jh-opsdx-io" {
  name               = "nodes.cluster-dev-ml.jh.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster-dev-ml.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "additional-nodes-cluster-dev-ml-jh-opsdx-io" {
  name   = "additional.nodes.cluster-dev-ml.jh.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-dev-ml-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_additional.nodes.cluster-dev-ml.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-dev-ml-jh-opsdx-io" {
  name   = "masters.cluster-dev-ml.jh.opsdx.io"
  role   = "${aws_iam_role.masters-cluster-dev-ml-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster-dev-ml.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-dev-ml-jh-opsdx-io" {
  name   = "nodes.cluster-dev-ml.jh.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-dev-ml-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster-dev-ml.jh.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824" {
  key_name   = "kubernetes.cluster-dev-ml.jh.opsdx.io-55:1d:14:81:20:28:ed:6f:ac:47:5b:92:d6:89:38:24"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster-dev-ml.jh.opsdx.io-551d14812028ed6fac475b92d6893824_public_key")}"
}

resource "aws_launch_configuration" "c2dw-etl-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "c2dw-etl.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_c2dw-etl.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster-dev-ml.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf1-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf1.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf1.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf10-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf10.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf10.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf11-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf11.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf11.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf12-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf12.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf12.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf2-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf2.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf2.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf3-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf3.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf3.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf4-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf4.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf4.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf5-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf5.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf5.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf6-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf6.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf6.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf7-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf7.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf7.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf8-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf8.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf8.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "tf9-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "tf9.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf9.cluster-dev-ml.jh.opsdx.io_user_data")}"

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

resource "aws_launch_configuration" "utility-cluster-dev-ml-jh-opsdx-io" {
  name_prefix                 = "utility.cluster-dev-ml.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "t2.micro"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-ml-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_utility.cluster-dev-ml.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "api-cluster-dev-ml-jh-opsdx-io" {
  name = "api.cluster-dev-ml.jh.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-cluster-dev-ml-jh-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-cluster-dev-ml-jh-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z216PFCPPMYV7T"
}

resource "aws_security_group" "api-elb-cluster-dev-ml-jh-opsdx-io" {
  name        = "api-elb.cluster-dev-ml.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "cluster-dev-ml.jh.opsdx.io"
    Name              = "api-elb.cluster-dev-ml.jh.opsdx.io"
  }
}

resource "aws_security_group" "masters-cluster-dev-ml-jh-opsdx-io" {
  name        = "masters.cluster-dev-ml.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "cluster-dev-ml.jh.opsdx.io"
    Name              = "masters.cluster-dev-ml.jh.opsdx.io"
  }
}

resource "aws_security_group" "nodes-cluster-dev-ml-jh-opsdx-io" {
  name        = "nodes.cluster-dev-ml.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "cluster-dev-ml.jh.opsdx.io"
    Name              = "nodes.cluster-dev-ml.jh.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-ml-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-ml-jh-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 1
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-10-0-0-0--16" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-dev-ml-jh-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/16"]
}

resource "aws_security_group_rule" "ssh-external-to-node-10-0-0-0--16" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-ml-jh-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/16"]
}

terraform = {
  required_version = ">= 0.9.3"
}
