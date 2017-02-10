variable "local_shell" {}
variable "domain_zone_id" {}
variable "web_dns_name" {}
variable "web_hostname_file" {}

## Up
resource "null_resource" "start-web-service-prod" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/web/start.sh app=nginx"
  }
}

## Down
resource "null_resource" "stop-web-service-prod" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/web/stop.sh"
  }
}

# DNS
resource "aws_route53_record" "prod_rest_api" {
  zone_id = "${var.domain_zone_id}"
  name    = "${var.web_dns_name}"
  type    = "CNAME"
  ttl     = "300"
  records = ["${trimspace(file("${var.web_hostname_file}"))}"]
}
