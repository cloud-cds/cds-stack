variable "domain" {}

resource "aws_route53_zone" "main" {
   name = "${var.domain}"
   comment = "DNS Zone for OpsDX"
}

output "zone_id" {
  value = "${aws_route53_zone.main.zone_id}"
}
