output "cluster_name" {
  value = "cluster.dev.opsdx.io"
}

output "master_security_group_ids" {
  value = ["${aws_security_group.masters-cluster-dev-opsdx-io.id}"]
}

output "node_security_group_ids" {
  value = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
}

output "node_subnet_ids" {
  value = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]
}

output "region" {
  value = "us-east-1"
}

output "vpc_id" {
  value = "vpc-6fd4b409"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_autoscaling_attachment" "master-us-east-1a-masters-cluster-dev-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1a-masters-cluster-dev-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1c-masters-cluster-dev-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1c-masters-cluster-dev-opsdx-io.id}"
}

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-cluster-dev-opsdx-io" {
  elb                    = "${aws_elb.api-cluster-dev-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-cluster-dev-opsdx-io.id}"
}

resource "aws_autoscaling_group" "etl-cluster-dev-opsdx-io" {
  name                 = "etl.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.etl-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "ETL Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "etl.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "locust-cluster-dev-opsdx-io" {
  name                 = "locust.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.locust-cluster-dev-opsdx-io.id}"
  max_size             = 100
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Locust Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "locust.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "master-us-east-1a-masters-cluster-dev-opsdx-io" {
  name                 = "master-us-east-1a.masters.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1a-masters-cluster-dev-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Master-1a"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1a.masters.cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "master-us-east-1c-masters-cluster-dev-opsdx-io" {
  name                 = "master-us-east-1c.masters.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1c-masters-cluster-dev-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Master-1c"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1c.masters.cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "master-us-east-1d-masters-cluster-dev-opsdx-io" {
  name                 = "master-us-east-1d.masters.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-cluster-dev-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Master-1d"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "master-us-east-1d.masters.cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "nodes-cluster-dev-opsdx-io" {
  name                 = "nodes.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.nodes-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "nodes.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "predictor-cluster-dev-opsdx-io" {
  name                 = "predictor.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Predictor Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "predictor-spot-cluster-dev-opsdx-io" {
  name                 = "predictor-spot.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.predictor-spot-cluster-dev-opsdx-io.id}"
  max_size             = 64
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Predictor Spot Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "predictor-spot.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "web-cluster-dev-opsdx-io" {
  name                 = "web.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.web-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Webserver Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "web.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_group" "web2-cluster-dev-opsdx-io" {
  name                 = "web2.cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.web2-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Webserver Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "web2.cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Stack"
    value               = "Development"
    propagate_at_launch = true
  }

  tag = {
    key                 = "k8s.io/role/node"
    value               = "1"
    propagate_at_launch = true
  }
}

resource "aws_ebs_volume" "a-etcd-events-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.dev.opsdx.io"
    Name                 = "a.etcd-events.cluster.dev.opsdx.io"
    "k8s.io/etcd/events" = "a/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "a-etcd-main-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1a"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.dev.opsdx.io"
    Name                 = "a.etcd-main.cluster.dev.opsdx.io"
    "k8s.io/etcd/main"   = "a/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-events-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.dev.opsdx.io"
    Name                 = "c.etcd-events.cluster.dev.opsdx.io"
    "k8s.io/etcd/events" = "c/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "c-etcd-main-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1c"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.dev.opsdx.io"
    Name                 = "c.etcd-main.cluster.dev.opsdx.io"
    "k8s.io/etcd/main"   = "c/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-events-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.dev.opsdx.io"
    Name                 = "d.etcd-events.cluster.dev.opsdx.io"
    "k8s.io/etcd/events" = "d/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_ebs_volume" "d-etcd-main-cluster-dev-opsdx-io" {
  availability_zone = "us-east-1d"
  size              = 20
  type              = "gp2"
  encrypted         = false

  tags = {
    KubernetesCluster    = "cluster.dev.opsdx.io"
    Name                 = "d.etcd-main.cluster.dev.opsdx.io"
    "k8s.io/etcd/main"   = "d/a,c,d"
    "k8s.io/role/master" = "1"
  }
}

resource "aws_eip" "us-east-1a-cluster-dev-opsdx-io" {
  vpc = true
}

resource "aws_eip" "us-east-1c-cluster-dev-opsdx-io" {
  vpc = true
}

resource "aws_eip" "us-east-1d-cluster-dev-opsdx-io" {
  vpc = true
}

resource "aws_elb" "api-cluster-dev-opsdx-io" {
  name = "api-cluster-dev-opsdx-io-8np59q"

  listener = {
    instance_port     = 443
    instance_protocol = "TCP"
    lb_port           = 443
    lb_protocol       = "TCP"
  }

  security_groups = ["${aws_security_group.api-elb-cluster-dev-opsdx-io.id}"]
  subnets         = ["${aws_subnet.utility-us-east-1a-cluster-dev-opsdx-io.id}", "${aws_subnet.utility-us-east-1c-cluster-dev-opsdx-io.id}", "${aws_subnet.utility-us-east-1d-cluster-dev-opsdx-io.id}"]

  health_check = {
    target              = "TCP:443"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    timeout             = 5
  }

  idle_timeout = 300

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "api.cluster.dev.opsdx.io"
  }
}

resource "aws_iam_instance_profile" "masters-cluster-dev-opsdx-io" {
  name  = "masters.cluster.dev.opsdx.io"
  roles = ["${aws_iam_role.masters-cluster-dev-opsdx-io.name}"]
}

resource "aws_iam_instance_profile" "nodes-cluster-dev-opsdx-io" {
  name  = "nodes.cluster.dev.opsdx.io"
  roles = ["${aws_iam_role.nodes-cluster-dev-opsdx-io.name}"]
}

resource "aws_iam_role" "masters-cluster-dev-opsdx-io" {
  name               = "masters.cluster.dev.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_masters.cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role" "nodes-cluster-dev-opsdx-io" {
  name               = "nodes.cluster.dev.opsdx.io"
  assume_role_policy = "${file("${path.module}/data/aws_iam_role_nodes.cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "additional-nodes-cluster-dev-opsdx-io" {
  name   = "additional.nodes.cluster.dev.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-dev-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_additional.nodes.cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "masters-cluster-dev-opsdx-io" {
  name   = "masters.cluster.dev.opsdx.io"
  role   = "${aws_iam_role.masters-cluster-dev-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_masters.cluster.dev.opsdx.io_policy")}"
}

resource "aws_iam_role_policy" "nodes-cluster-dev-opsdx-io" {
  name   = "nodes.cluster.dev.opsdx.io"
  role   = "${aws_iam_role.nodes-cluster-dev-opsdx-io.name}"
  policy = "${file("${path.module}/data/aws_iam_role_policy_nodes.cluster.dev.opsdx.io_policy")}"
}

resource "aws_key_pair" "kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23" {
  key_name   = "kubernetes.cluster.dev.opsdx.io-94:a2:2f:95:b3:cc:fd:3f:4d:a6:d2:15:22:59:2b:23"
  public_key = "${file("${path.module}/data/aws_key_pair_kubernetes.cluster.dev.opsdx.io-94a22f95b3ccfd3f4da6d21522592b23_public_key")}"
}

resource "aws_launch_configuration" "etl-cluster-dev-opsdx-io" {
  name_prefix                 = "etl.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_etl.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "locust-cluster-dev-opsdx-io" {
  name_prefix                 = "locust.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m3.medium"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_locust.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
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

resource "aws_launch_configuration" "master-us-east-1a-masters-cluster-dev-opsdx-io" {
  name_prefix                 = "master-us-east-1a.masters.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1a.masters.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1c-masters-cluster-dev-opsdx-io" {
  name_prefix                 = "master-us-east-1c.masters.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1c.masters.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "master-us-east-1d-masters-cluster-dev-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.masters-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.masters-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_master-us-east-1d.masters.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "nodes-cluster-dev-opsdx-io" {
  name_prefix                 = "nodes.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor-cluster-dev-opsdx-io" {
  name_prefix                 = "predictor.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "predictor-spot-cluster-dev-opsdx-io" {
  name_prefix                 = "predictor-spot.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "c4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_predictor-spot.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }

  spot_price = "0.097"
}

resource "aws_launch_configuration" "web-cluster-dev-opsdx-io" {
  name_prefix                 = "web.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_web.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "web2-cluster-dev-opsdx-io" {
  name_prefix                 = "web2.cluster.dev.opsdx.io-"
  image_id                    = "ami-03690415"
  instance_type               = "m4.large"
  key_name                    = "${aws_key_pair.kubernetes-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_web2.cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_nat_gateway" "us-east-1a-cluster-dev-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1a-cluster-dev-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1a-cluster-dev-opsdx-io.id}"
}

resource "aws_nat_gateway" "us-east-1c-cluster-dev-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1c-cluster-dev-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1c-cluster-dev-opsdx-io.id}"
}

resource "aws_nat_gateway" "us-east-1d-cluster-dev-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1d-cluster-dev-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1d-cluster-dev-opsdx-io.id}"
}

resource "aws_route" "0-0-0-0--0" {
  route_table_id         = "${aws_route_table.cluster-dev-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "igw-e2153385"
}

resource "aws_route" "private-us-east-1a-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1a-cluster-dev-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1a-cluster-dev-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1c-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1c-cluster-dev-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1c-cluster-dev-opsdx-io.id}"
}

resource "aws_route" "private-us-east-1d-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1d-cluster-dev-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1d-cluster-dev-opsdx-io.id}"
}

resource "aws_route53_record" "api-cluster-dev-opsdx-io" {
  name = "api.cluster.dev.opsdx.io"
  type = "A"

  alias = {
    name                   = "${aws_elb.api-cluster-dev-opsdx-io.dns_name}"
    zone_id                = "${aws_elb.api-cluster-dev-opsdx-io.zone_id}"
    evaluate_target_health = false
  }

  zone_id = "/hostedzone/Z2GCTM75P01WXX"
}

resource "aws_route_table" "cluster-dev-opsdx-io" {
  vpc_id = "vpc-6fd4b409"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "cluster.dev.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1a-cluster-dev-opsdx-io" {
  vpc_id = "vpc-6fd4b409"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "private-us-east-1a.cluster.dev.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1c-cluster-dev-opsdx-io" {
  vpc_id = "vpc-6fd4b409"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "private-us-east-1c.cluster.dev.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1d-cluster-dev-opsdx-io" {
  vpc_id = "vpc-6fd4b409"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "private-us-east-1d.cluster.dev.opsdx.io"
  }
}

resource "aws_route_table_association" "private-us-east-1a-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1a-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1a-cluster-dev-opsdx-io.id}"
}

resource "aws_route_table_association" "private-us-east-1c-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1c-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1c-cluster-dev-opsdx-io.id}"
}

resource "aws_route_table_association" "private-us-east-1d-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1d-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1d-cluster-dev-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1a-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1a-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-dev-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1c-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1c-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-dev-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1d-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1d-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.cluster-dev-opsdx-io.id}"
}

resource "aws_security_group" "api-elb-cluster-dev-opsdx-io" {
  name        = "api-elb.cluster.dev.opsdx.io"
  vpc_id      = "vpc-6fd4b409"
  description = "Security group for api ELB"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "api-elb.cluster.dev.opsdx.io"
  }
}

resource "aws_security_group" "masters-cluster-dev-opsdx-io" {
  name        = "masters.cluster.dev.opsdx.io"
  vpc_id      = "vpc-6fd4b409"
  description = "Security group for masters"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "masters.cluster.dev.opsdx.io"
  }
}

resource "aws_security_group" "nodes-cluster-dev-opsdx-io" {
  name        = "nodes.cluster.dev.opsdx.io"
  vpc_id      = "vpc-6fd4b409"
  description = "Security group for nodes"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "nodes.cluster.dev.opsdx.io"
  }
}

resource "aws_security_group_rule" "all-master-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-master-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-node" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "api-elb-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-api-elb-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.api-elb-cluster-dev-opsdx-io.id}"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "https-elb-to-master" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.api-elb-cluster-dev-opsdx-io.id}"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "master-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-egress" {
  type              = "egress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "node-to-master-tcp-1-4000" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  from_port                = 1
  to_port                  = 4000
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-tcp-4003-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  from_port                = 4003
  to_port                  = 65535
  protocol                 = "tcp"
}

resource "aws_security_group_rule" "node-to-master-udp-1-65535" {
  type                     = "ingress"
  security_group_id        = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  source_security_group_id = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  from_port                = 1
  to_port                  = 65535
  protocol                 = "udp"
}

resource "aws_security_group_rule" "ssh-external-to-master-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.masters-cluster-dev-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "ssh-external-to-node-0-0-0-0--0" {
  type              = "ingress"
  security_group_id = "${aws_security_group.nodes-cluster-dev-opsdx-io.id}"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_subnet" "us-east-1a-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.32.0/19"
  availability_zone = "us-east-1a"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "us-east-1a.cluster.dev.opsdx.io"
  }
}

resource "aws_subnet" "us-east-1c-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.64.0/19"
  availability_zone = "us-east-1c"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "us-east-1c.cluster.dev.opsdx.io"
  }
}

resource "aws_subnet" "us-east-1d-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.96.0/19"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "us-east-1d.cluster.dev.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1a-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.0.0/22"
  availability_zone = "us-east-1a"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "utility-us-east-1a.cluster.dev.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1c-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.4.0/22"
  availability_zone = "us-east-1c"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "utility-us-east-1c.cluster.dev.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1d-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.8.0/22"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster = "cluster.dev.opsdx.io"
    Name              = "utility-us-east-1d.cluster.dev.opsdx.io"
  }
}
