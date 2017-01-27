variable "local_shell" {}

## Up
resource "null_resource" "start-jupyter" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/jupyter/start.sh"
  }
}

## Down
resource "null_resource" "stop-jupyter" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/jupyter/stop.sh"
  }
}