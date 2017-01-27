variable "local_shell" {}

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