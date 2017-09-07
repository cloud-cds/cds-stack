output "cluster_name" {
  value = "cluster-dev.jh.opsdx.io"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"]
}

output "masters_role_arn" {
  value = "${aws_iam_role.masters-cluster-dev-jh-opsdx-io.arn}"
}

output "masters_role_name" {
  value = "${aws_iam_role.masters-cluster-dev-jh-opsdx-io.name}"
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
}

output "node_subnet_ids" {
  value = ["subnet-52acb31a", "subnet-89c49da5", "subnet-b8b3e1e2"]
}

output "nodes_role_arn" {
  value = "${aws_iam_role.nodes-cluster-dev-jh-opsdx-io.arn}"
}

output "nodes_role_name" {
  value = "${aws_iam_role.nodes-cluster-dev-jh-opsdx-io.name}"
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

resource "aws_autoscaling_attachment" "master-us-east-1a-masters-cluster-dev-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1a-masters-cluster-dev-jh-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1c-masters-cluster-dev-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1c-masters-cluster-dev-jh-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-dev-jh-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-jh-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-dev-jh-opsdx-io.id}"
}

resource "aws_autoscaling_group" "etl-cluster-dev-jh-opsdx-io" {
  name                 = "etl.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.etl-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "etl.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "locust-cluster-dev-jh-opsdx-io" {
  name                 = "locust.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.locust-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "locust.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "master-us-east-1a-masters-cluster-dev-jh-opsdx-io" {
  name                 = "master-us-east-1a.masters.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1a-masters-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1a.masters.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1c-masters-cluster-dev-jh-opsdx-io" {
  name                 = "master-us-east-1c.masters.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1c-masters-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1c.masters.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-dev-jh-opsdx-io" {
  name                 = "master-us-east-1d.masters.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/master"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "nodes-cluster-dev-jh-opsdx-io" {
  name                 = "nodes.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.nodes-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "nodes.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "predictor-cluster-dev-jh-opsdx-io" {
  name                 = "predictor.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor-cluster-dev-jh-opsdx-io.id}"
  max_size             = 32
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "predictor2-cluster-dev-jh-opsdx-io" {
  name                 = "predictor2.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor2-cluster-dev-jh-opsdx-io.id}"
  max_size             = 32
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "C4.L Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor2.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "predictor2"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "predictor3-cluster-dev-jh-opsdx-io" {
  name                 = "predictor3.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor3-cluster-dev-jh-opsdx-io.id}"
  max_size             = 32
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "C4.XL Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor3.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "predictor3"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "predictor4-cluster-dev-jh-opsdx-io" {
  name                 = "predictor4.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor4-cluster-dev-jh-opsdx-io.id}"
  max_size             = 32
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "C4.2XL Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor4.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "predictor4"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "predictor5-cluster-dev-jh-opsdx-io" {
  name                 = "predictor5.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor5-cluster-dev-jh-opsdx-io.id}"
  max_size             = 32
  min_size             = 0
  vpc_zone_identifier  = ["subnet-b8b3e1e2", "subnet-89c49da5", "subnet-52acb31a"]

  tag = {
    key                 = "Component"
    value               = "C4.4XL Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor5.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/cluster-autoscaler/node-template/label/opsdx_nodegroup"
    value               = "predictor5"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "spot-nodes-cluster-dev-jh-opsdx-io" {
  name                 = "spot-nodes.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.spot-nodes-cluster-dev-jh-opsdx-io.id}"
  desired_capacity     = 6
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "spot-nodes.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "spot-predictor-cluster-dev-jh-opsdx-io" {
  name                 = "spot-predictor.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.spot-predictor-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "spot-predictor.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "web-cluster-dev-jh-opsdx-io" {
  name                 = "web.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.web-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "web.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_autoscaling_group" "web2-cluster-dev-jh-opsdx-io" {
  name                 = "web2.cluster-dev.jh.opsdx.io"
  launch_configuration = "${aws_launch_configuration.web2-cluster-dev-jh-opsdx-io.id}"
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
    value               = "cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "web2.cluster-dev.jh.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
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

resource "aws_ebs_volume" "a-etcd-events-cluster-dev-jh-opsdx-io" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev.jh.opsdx.io"
    Name                 = "a.etcd-events.cluster-dev.jh.opsdx.io"
    "k8s.io/etcd/events" = "a/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "a-etcd-main-cluster-dev-jh-opsdx-io" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev.jh.opsdx.io"
    Name                 = "a.etcd-main.cluster-dev.jh.opsdx.io"
    "k8s.io/etcd/main"   = "a/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-events-cluster-dev-jh-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev.jh.opsdx.io"
    Name                 = "c.etcd-events.cluster-dev.jh.opsdx.io"
    "k8s.io/etcd/events" = "c/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-main-cluster-dev-jh-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev.jh.opsdx.io"
    Name                 = "c.etcd-main.cluster-dev.jh.opsdx.io"
    "k8s.io/etcd/main"   = "c/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-events-cluster-dev-jh-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev.jh.opsdx.io"
    Name                 = "d.etcd-events.cluster-dev.jh.opsdx.io"
    "k8s.io/etcd/events" = "d/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-cluster-dev-jh-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = true

  tags = {
    KubernetesCluster    = "cluster-dev.jh.opsdx.io"
    Name                 = "d.etcd-main.cluster-dev.jh.opsdx.io"
    "k8s.io/etcd/main"   = "d/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_elb" "api-cluster-dev-jh-opsdx-io" {
  name = "api-cluster-dev-jh-opsdx--b0kecl"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-cluster-dev-jh-opsdx-io.id}"]
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
    KubernetesCluster = "cluster-dev.jh.opsdx.io"
    Name              = "api.cluster-dev.jh.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-dev-jh-opsdx-io" {
  name = "masters.cluster-dev.jh.opsdx.io"
  role = "${aws_iam_role.masters-cluster-dev-jh-opsdx-io.name}"
}

resource "aws_iam_instance_profile" "nodes-cluster-dev-jh-opsdx-io" {
  name = "nodes.cluster-dev.jh.opsdx.io"
  role = "${aws_iam_role.nodes-cluster-dev-jh-opsdx-io.name}"
}

resource "aws_iam_role" "masters-cluster-dev-jh-opsdx-io" {
  name               = "masters.cluster-dev.jh.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster-dev.jh.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-cluster-dev-jh-opsdx-io" {
  name               = "nodes.cluster-dev.jh.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster-dev.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "additional-nodes-cluster-dev-jh-opsdx-io" {
  name   = "additional.nodes.cluster-dev.jh.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-dev-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_additional.nodes.cluster-dev.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-dev-jh-opsdx-io" {
  name   = "masters.cluster-dev.jh.opsdx.io"
  role   = "${aws_iam_role.masters-cluster-dev-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster-dev.jh.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-dev-jh-opsdx-io" {
  name   = "nodes.cluster-dev.jh.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-dev-jh-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster-dev.jh.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824" {
  key_name   = "kubernetes.cluster-dev.jh.opsdx.io-55:1d:14:81:20:28:ed:6f:ac:47:5b:92:d6:89:38:24"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster-dev.jh.opsdx.io-551d14812028ed6fac475b92d6893824_public_key")}"
}

resource "aws_launch_configuration" "etl-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "etl.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_etl.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_launch_configuration" "locust-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "locust.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m3.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_locust.cluster-dev.jh.opsdx.io_user_data")}"

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

  spot_price = "0.067"
}

resource "aws_launch_configuration" "master-us-east-1a-masters-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1a.masters.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1a.masters.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_launch_configuration" "master-us-east-1c-masters-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1c.masters.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1c.masters.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 64
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_launch_configuration" "nodes-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "nodes.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "predictor.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor2-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "predictor2.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor2.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor3-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "predictor3.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor3.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor4-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "predictor4.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor4.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor5-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "predictor5.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor5.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "spot-nodes-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "spot-nodes.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_spot-nodes.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_launch_configuration" "spot-predictor-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "spot-predictor.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "c4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_spot-predictor.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.097"
}

resource "aws_launch_configuration" "web-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "web.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_web.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_launch_configuration" "web2-cluster-dev-jh-opsdx-io" {
  name_prefix                 = "web2.cluster-dev.jh.opsdx.io-"
  image_id                    = "ami-08431d73"
  instance_type               = "r4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-jh-opsdx-io-551d14812028ed6fac475b92d6893824.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-jh-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_web2.cluster-dev.jh.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 128
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.133"
}

resource "aws_route53_record" "api-cluster-dev-jh-opsdx-io" {
  name = "api.cluster-dev.jh.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-cluster-dev-jh-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-cluster-dev-jh-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z216PFCPPMYV7T"
}

resource "aws_security_group" "api-elb-cluster-dev-jh-opsdx-io" {
  name        = "api-elb.cluster-dev.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "cluster-dev.jh.opsdx.io"
    Name              = "api-elb.cluster-dev.jh.opsdx.io"
  }
}

resource "aws_security_group" "masters-cluster-dev-jh-opsdx-io" {
  name        = "masters.cluster-dev.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "cluster-dev.jh.opsdx.io"
    Name              = "masters.cluster-dev.jh.opsdx.io"
  }
}

resource "aws_security_group" "nodes-cluster-dev-jh-opsdx-io" {
  name        = "nodes.cluster-dev.jh.opsdx.io"
  vpc_id      = "vpc-0234067b"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "cluster-dev.jh.opsdx.io"
    Name              = "nodes.cluster-dev.jh.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-jh-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-dev-jh-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  from_port                = 1
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-10-0-0-0--16" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-dev-jh-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/16"]
}

resource "aws_security_group_rule" "ssh-external-to-node-10-0-0-0--16" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-jh-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["10.0.0.0/16"]
}

terraform = {
  required_version = ">= 0.9.3"
}
