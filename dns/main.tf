variable "k8s_domain" {}

resource "aws_route53_zone" "primary" {
   name = "${var.k8s_domain}"
   comment = "Test zone for OpsDX"
}

output "zone_id" {
  value = "${aws_route53_zone.primary.zone_id}"
}
