variable "domain" {}

resource "aws_route53_zone" "production" {
   name = "${var.domain}"
   comment = "Production domain for OpsDX"
}

output "zone_id" {
  value = "${aws_route53_zone.production.zone_id}"
}
