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
  value = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]
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

resource "aws_autoscaling_attachment" "master-us-east-1d-masters-ml-cluster-dev-opsdx-io" {
  elb                    = "${aws_elb.api-ml-cluster-dev-opsdx-io.id}"
  autoscaling_group_name = "${aws_autoscaling_group.master-us-east-1d-masters-ml-cluster-dev-opsdx-io.id}"
}

resource "aws_autoscaling_group" "master-us-east-1d-masters-ml-cluster-dev-opsdx-io" {
  name                 = "master-us-east-1d.masters.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.master-us-east-1d-masters-ml-cluster-dev-opsdx-io.id}"
  max_size             = 1
  min_size             = 1
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]

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

resource "aws_autoscaling_group" "tf1-ml-cluster-dev-opsdx-io" {
  name                 = "tf1.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf1-ml-cluster-dev-opsdx-io.id}"
  max_size             = 33
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf1.ml-cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "tf2-ml-cluster-dev-opsdx-io" {
  name                 = "tf2.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf2-ml-cluster-dev-opsdx-io.id}"
  max_size             = 33
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf2.ml-cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "tf3-ml-cluster-dev-opsdx-io" {
  name                 = "tf3.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf3-ml-cluster-dev-opsdx-io.id}"
  max_size             = 33
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf3.ml-cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "tf4-ml-cluster-dev-opsdx-io" {
  name                 = "tf4.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.tf4-ml-cluster-dev-opsdx-io.id}"
  max_size             = 33
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]

  tag = {
    key                 = "Component"
    value               = "Tensorflow Node"
    propagate_at_launch = true
  }

  tag = {
    key                 = "KubernetesCluster"
    value               = "ml-cluster.dev.opsdx.io"
    propagate_at_launch = true
  }

  tag = {
    key                 = "Name"
    value               = "tf4.ml-cluster.dev.opsdx.io"
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

resource "aws_autoscaling_group" "train-ml-cluster-dev-opsdx-io" {
  name                 = "train.ml-cluster.dev.opsdx.io"
  launch_configuration = "${aws_launch_configuration.train-ml-cluster-dev-opsdx-io.id}"
  max_size             = 10
  min_size             = 0
  vpc_zone_identifier  = ["${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"]

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

resource "aws_eip" "us-east-1d-ml-cluster-dev-opsdx-io" {
  vpc = true
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
  subnets         = ["${aws_subnet.utility-us-east-1d-ml-cluster-dev-opsdx-io.id}"]

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

resource "aws_launch_configuration" "master-us-east-1d-masters-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "master-us-east-1d.masters.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.xlarge"
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

resource "aws_launch_configuration" "tf1-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf1.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.large"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf1.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf1-c4xl-od-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf1.c4-xl.od.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf1.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf1-t2l-sir-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf1.t2-lrg.sir.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "t2.large"
  spot_price                  = "0.09"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf1.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf1-c4xl-sir-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf1.c4-xl.sir.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.xlarge"
  spot_price                  = "0.18"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf1.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}



resource "aws_launch_configuration" "tf2-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf2.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.2xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf2.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf2-c42xl-sir-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf2.c4-2xl.sir.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.2xlarge"
  spot_price                  = "0.38"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf2.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf3-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf3.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.4xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf3.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}

resource "aws_launch_configuration" "tf3-c44xl-sir-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf3.c4-4xl.sir.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.4xlarge"
  spot_price                  = "0.75"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf3.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}



resource "aws_launch_configuration" "tf4-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf4.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.8xlarge"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf4.ml-cluster.dev.opsdx.io_user_data")}"

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 20
    delete_on_termination = true
  }

  lifecycle = {
    create_before_destroy = true
  }
}


resource "aws_launch_configuration" "tf4-c48xl-sir-ml-cluster-dev-opsdx-io" {
  name_prefix                 = "tf4.c4-8xl.sir.ml-cluster.dev.opsdx.io-"
  image_id                    = "ami-5f1afc49"
  instance_type               = "c4.8xlarge"
  spot_price                  = "1.25"
  key_name                    = "${aws_key_pair.kubernetes-ml-cluster-dev-opsdx-io-94a22f95b3ccfd3f4da6d21522592b23.id}"
  iam_instance_profile        = "${aws_iam_instance_profile.nodes-ml-cluster-dev-opsdx-io.id}"
  security_groups             = ["${aws_security_group.nodes-ml-cluster-dev-opsdx-io.id}"]
  associate_public_ip_address = false
  user_data                   = "${file("${path.module}/data/aws_launch_configuration_tf4.ml-cluster.dev.opsdx.io_user_data")}"

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
  instance_type               = "t2.2xlarge"
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



resource "aws_nat_gateway" "us-east-1d-ml-cluster-dev-opsdx-io" {
  allocation_id = "${aws_eip.us-east-1d-ml-cluster-dev-opsdx-io.id}"
  subnet_id     = "${aws_subnet.utility-us-east-1d-ml-cluster-dev-opsdx-io.id}"
}

resource "aws_route" "0-0-0-0--0" {
  route_table_id         = "${aws_route_table.ml-cluster-dev-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "igw-e2153385"
}

resource "aws_route" "private-us-east-1d-0-0-0-0--0" {
  route_table_id         = "${aws_route_table.private-us-east-1d-ml-cluster-dev-opsdx-io.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.us-east-1d-ml-cluster-dev-opsdx-io.id}"
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

resource "aws_route_table" "ml-cluster-dev-opsdx-io" {
  vpc_id = "vpc-6fd4b409"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "ml-cluster.dev.opsdx.io"
  }
}

resource "aws_route_table" "private-us-east-1d-ml-cluster-dev-opsdx-io" {
  vpc_id = "vpc-6fd4b409"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "private-us-east-1d.ml-cluster.dev.opsdx.io"
  }
}

resource "aws_route_table_association" "private-us-east-1d-ml-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.us-east-1d-ml-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.private-us-east-1d-ml-cluster-dev-opsdx-io.id}"
}

resource "aws_route_table_association" "utility-us-east-1d-ml-cluster-dev-opsdx-io" {
  subnet_id      = "${aws_subnet.utility-us-east-1d-ml-cluster-dev-opsdx-io.id}"
  route_table_id = "${aws_route_table.ml-cluster-dev-opsdx-io.id}"
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

resource "aws_subnet" "us-east-1d-ml-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.248.0/22"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "us-east-1d.ml-cluster.dev.opsdx.io"
  }
}

resource "aws_subnet" "utility-us-east-1d-ml-cluster-dev-opsdx-io" {
  vpc_id            = "vpc-6fd4b409"
  cidr_block        = "10.0.255.0/24"
  availability_zone = "us-east-1d"

  tags = {
    KubernetesCluster = "ml-cluster.dev.opsdx.io"
    Name              = "utility-us-east-1d.ml-cluster.dev.opsdx.io"
  }
}
