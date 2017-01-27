resource "aws_route53_zone" "primary" {
   name = "${var.k8s_domain}"
   comment = "Test zone for OpsDX"
}
