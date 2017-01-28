resource "aws_s3_bucket" "kops-state-store" {
    bucket = "opsdx-kops-state-store"
    acl = "private"

    versioning {
        enabled = true
    }
}

# Tensorlow container registry.
resource "aws_ecr_repository" "tensorflow" {
  name = "tensorflow"
}

output "tensorflow_registry_url" {
  value = "${aws_ecr_repository.tensorflow.repository_url}"
}