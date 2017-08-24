resource "aws_db_parameter_group" "pgstats" {
  name   = "${var.deploy_prefix}-pgstats-pg"
  family = "postgres9.5"

  parameter {
    name = "shared_preload_libraries"
    value = "pg_stat_statements"
    apply_method = "pending-reboot"
  }

  parameter {
    name = "track_activity_query_size"
    value = "2048"
    apply_method = "pending-reboot"
  }

  parameter {
    name = "pg_stat_statements.track"
    value = "ALL"
    apply_method = "pending-reboot"
  }
}
