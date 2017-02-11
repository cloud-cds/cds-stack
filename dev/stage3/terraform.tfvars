terragrunt = {
  include {
    path = "${find_in_parent_folders()}"
  }

  dependencies {
    paths = ["../../dns", "../stage1", "../stage2"]
  }
}
