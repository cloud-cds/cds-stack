variable "local_shell" {}

## Up
resource "null_resource" "start-message-bus" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/confluent/start.sh"
  }
}

## Down
resource "null_resource" "stop-message-bus" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/confluent/stop.sh"
  }
}