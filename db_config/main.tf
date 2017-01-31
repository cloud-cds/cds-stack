###########################################
# RDS database setup and configuration

# API Dev DB
provider "postgresql" {
  alias    = "api"
  host     = "${var.db_ip}"
  username = "${var.db_username}"
  password = "${var.db_password}"
  sslmode  = "require"
}

resource "postgresql_database" "db_api_dev" {
  provider = "postgresql.api"
  name     = "${var.db_name_dev}"
  owner    = "${var.db_username}"
}

resource "postgresql_database" "db_deis" {
  provider = "postgresql.api"
  name     = "${var.db_name_deis}"
  owner    = "${var.db_username}"
}
