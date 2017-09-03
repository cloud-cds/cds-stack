variable "controller_sg_id" {}
variable "node_sg_id" {}

resource "aws_security_group_rule" "all-controller-to-node" {
  type                     = "ingress"
  security_group_id        = "${var.node_sg_id}"
  source_security_group_id = "${var.controller_sg_id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}

resource "aws_security_group_rule" "all-node-to-controller" {
  type                     = "ingress"
  security_group_id        = "${var.controller_sg_id}"
  source_security_group_id = "${var.node_sg_id}"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
}
