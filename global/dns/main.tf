variable "domain" {
  default = "opsdx.io"
}

variable "spf" {}
variable "dkim" {}
variable "dkim_selector" {}

resource "aws_route53_zone" "main" {
   name = "${var.domain}"
   comment = "DNS Zone for OpsDX"
}

resource "aws_route53_record" "zoho_mx" {
    zone_id = "${aws_route53_zone.main.zone_id}"
    name    = "${var.domain}"
    type    = "MX"
    ttl     = "300"
    records = [
      "10 mx.zoho.com.",
      "20 mx2.zoho.com.",
      "50 mx3.zoho.com.",
    ]
}

resource "aws_route53_record" "spf" {
    zone_id = "${aws_route53_zone.main.zone_id}"
    name    = "${var.domain}"
    type    = "TXT"
    ttl     = "300"
    records = ["${var.spf}"]
}

resource "aws_route53_record" "dkim" {
    zone_id = "${aws_route53_zone.main.zone_id}"
    name    = "${var.dkim_selector}._domainkey.${var.domain}"
    type    = "TXT"
    ttl     = "300"
    records = ["${var.dkim}"]
}

output "zone_id" {
  value = "${aws_route53_zone.main.zone_id}"
}
