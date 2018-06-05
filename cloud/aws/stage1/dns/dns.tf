variable "root_domain" {}
variable "domain" {}

data "aws_route53_zone" "root" {
  name = "${var.root_domain}."
}

resource "aws_route53_zone" "main" {
   name = "${var.domain}"
   comment = "Metabolic Compass DNS"
}

resource "aws_route53_record" "main-ns" {
  zone_id = "${data.aws_route53_zone.root.zone_id}"
  name = "${var.domain}"
  type = "NS"
  ttl = "30"
  records = [
      "${aws_route53_zone.main.name_servers.0}",
      "${aws_route53_zone.main.name_servers.1}",
      "${aws_route53_zone.main.name_servers.2}",
      "${aws_route53_zone.main.name_servers.3}"
  ]
}

output "zone_id" {
  value = "${aws_route53_zone.main.zone_id}"
}
