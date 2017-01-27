resource "aws_s3_bucket" "kops-state-store" {
    bucket = "opsdx-kops-state-store"
    acl = "private"

    versioning {
        enabled = true
    }
}