variable "k8s_domain" {}
variable "opsdx_domain" {}

resource "aws_route53_zone" "primary" {
   name = "${var.k8s_domain}"
   comment = "Test zone for OpsDX"
}

resource "aws_route53_zone" "opsdx" {
   name = "${var.opsdx_domain}"
   comment = "Production domain zone for OpsDX"
}

output "zone_id" {
  value = "${aws_route53_zone.primary.zone_id}"
}
