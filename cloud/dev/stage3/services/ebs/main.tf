variable "local_shell" {}

## Up
resource "null_resource" "start-ebs" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/ebs/start.sh"
  }
}

## Down
resource "null_resource" "stop-ebs" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/ebs/stop.sh"
  }
}