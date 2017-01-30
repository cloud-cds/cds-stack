variable "local_shell"  {}
variable "web_instance" {}
variable "gpu_instance" {}
variable "cpu_instance" {}
variable "jnb_instance" {}

variable "enable_nodesets" {}

resource "null_resource" "key_name_extractor" {
  provisioner "local-exec" {
    command = "${var.local_shell} k8s/key_name_extractor.sh"
  }
}

## Webservice nodes ASG
resource "aws_autoscaling_group" "web-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  name = "web-nodes.cluster.opsdx.daiware.io"
  launch_configuration = "${aws_launch_configuration.web-nodes-cluster-opsdx-daiware-io.id}"
  min_size = 0
  desired_capacity = 0
  max_size = 5
  vpc_zone_identifier = ["${aws_autoscaling_group.nodes-cluster-opsdx-daiware-io.vpc_zone_identifier}"]
  tag = {
    key = "KubernetesCluster"
    value = "cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "web-nodes.cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/node"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_launch_configuration" "web-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  depends_on = ["null_resource.key_name_extractor"]
  name_prefix = "web-nodes.cluster.opsdx.daiware.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "${var.web_instance}"
  key_name = "${trimspace(file("${path.module}/kubernetes_key_name"))}"
  iam_instance_profile = "${aws_iam_instance_profile.nodes-cluster-opsdx-daiware-io.id}"
  security_groups = ["${aws_security_group.nodes-cluster-opsdx-daiware-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.opsdx.daiware.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}


## GPU nodes ASG
resource "aws_autoscaling_group" "gpu-train-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  name = "gpu-train-nodes.cluster.opsdx.daiware.io"
  launch_configuration = "${aws_launch_configuration.gpu-train-nodes-cluster-opsdx-daiware-io.id}"
  min_size = 0
  desired_capacity = 0
  max_size = 5
  vpc_zone_identifier = ["${aws_autoscaling_group.nodes-cluster-opsdx-daiware-io.vpc_zone_identifier}"]
  tag = {
    key = "KubernetesCluster"
    value = "cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "gpu-train-nodes.cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/node"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_launch_configuration" "gpu-train-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  depends_on = ["null_resource.key_name_extractor"]
  name_prefix = "gpu-train-nodes.cluster.opsdx.daiware.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "${var.gpu_instance}"
  key_name = "${trimspace(file("${path.module}/kubernetes_key_name"))}"
  iam_instance_profile = "${aws_iam_instance_profile.nodes-cluster-opsdx-daiware-io.id}"
  security_groups = ["${aws_security_group.nodes-cluster-opsdx-daiware-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.opsdx.daiware.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}


## CPU nodes ASG
resource "aws_autoscaling_group" "cpu-train-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  name = "cpu-train-nodes.cluster.opsdx.daiware.io"
  launch_configuration = "${aws_launch_configuration.cpu-train-nodes-cluster-opsdx-daiware-io.id}"
  min_size = 0
  desired_capacity = 0
  max_size = 5
  vpc_zone_identifier = ["${aws_autoscaling_group.nodes-cluster-opsdx-daiware-io.vpc_zone_identifier}"]
  tag = {
    key = "KubernetesCluster"
    value = "cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "cpu-train-nodes.cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/node"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_launch_configuration" "cpu-train-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  depends_on = ["null_resource.key_name_extractor"]
  name_prefix = "cpu-train-nodes.cluster.opsdx.daiware.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "${var.cpu_instance}"
  key_name = "${trimspace(file("${path.module}/kubernetes_key_name"))}"
  iam_instance_profile = "${aws_iam_instance_profile.nodes-cluster-opsdx-daiware-io.id}"
  security_groups = ["${aws_security_group.nodes-cluster-opsdx-daiware-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.opsdx.daiware.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}

## Notebook nodes ASG
resource "aws_autoscaling_group" "notebook-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  name = "notebook-nodes.cluster.opsdx.daiware.io"
  launch_configuration = "${aws_launch_configuration.notebook-nodes-cluster-opsdx-daiware-io.id}"
  min_size = 0
  desired_capacity = 0
  max_size = 5
  vpc_zone_identifier = ["${aws_autoscaling_group.nodes-cluster-opsdx-daiware-io.vpc_zone_identifier}"]
  tag = {
    key = "KubernetesCluster"
    value = "cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "Name"
    value = "notebook-nodes.cluster.opsdx.daiware.io"
    propagate_at_launch = true
  }
  tag = {
    key = "k8s.io/role/node"
    value = "1"
    propagate_at_launch = true
  }
}

resource "aws_launch_configuration" "notebook-nodes-cluster-opsdx-daiware-io" {
  count = "${var.enable_nodesets}"
  depends_on = ["null_resource.key_name_extractor"]
  name_prefix = "notebook-nodes.cluster.opsdx.daiware.io-"
  image_id = "ami-4bb3e05c"
  instance_type = "${var.jnb_instance}"
  key_name = "${trimspace(file("${path.module}/kubernetes_key_name"))}"
  iam_instance_profile = "${aws_iam_instance_profile.nodes-cluster-opsdx-daiware-io.id}"
  security_groups = ["${aws_security_group.nodes-cluster-opsdx-daiware-io.id}"]
  associate_public_ip_address = false
  user_data = "${file("${path.module}/data/aws_launch_configuration_nodes.cluster.opsdx.daiware.io_user_data")}"
  root_block_device = {
    volume_type = "gp2"
    volume_size = 20
    delete_on_termination = true
  }
  lifecycle = {
    create_before_destroy = true
  }
}
