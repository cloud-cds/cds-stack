variable "local_shell" {}

## Up
resource "null_resource" "start-zookeeper" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/zookeeper/start.sh"
  }
}

## Down
resource "null_resource" "stop-zookeeper" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/zookeeper/stop.sh"
  }
}