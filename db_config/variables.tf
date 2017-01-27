######################################
# RDS DB

variable "db_ip" {
  description = "DB hostname/ip"
}

variable "db_name_dev" {
  default = "opsdx_dev"
  description = "Development DB name"
}

variable "db_username" {
  default = "opsdx_root"
  description = "User name"
}

variable "db_password" {
  description = "DB Password"
}
