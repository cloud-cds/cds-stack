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

resource "aws_db_parameter_group" "pgstats96" {
  name   = "${var.deploy_prefix}-pgstats-pg96"
  family = "postgres9.6"

  # Parallelism
  parameter {
    name  = "max_parallel_workers_per_gather"
    value = "4"
  }

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

resource "aws_db_parameter_group" "pg-etl-96" {
  name   = "${var.deploy_prefix}-pgetl-pg96"
  family = "postgres9.6"

  # Autovacuum and checkpoints
  parameter {
    name  = "autovacuum"
    value = "0"
  }

  parameter {
    name  = "synchronous_commit"
    value = "off"
  }

  parameter {
    name  = "checkpoint_timeout"
    value = "3600"
  }

  parameter {
    name  = "max_wal_size"
    value = "16384"
  }

  # Logging
  parameter {
    name  = "rds.log_retention_period"
    value = "1440"
  }

  parameter {
    name  = "log_error_verbosity"
    value = "TERSE"
  }

  parameter {
    name  = "log_min_error_statement"
    value = "panic"
  }

  # Parallelism
  parameter {
    name  = "max_parallel_workers_per_gather"
    value = "4"
  }

  # Statistics
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

resource "aws_db_parameter_group" "pgbadger" {
  name   = "${var.deploy_prefix}-pgbadger-pg"
  family = "postgres9.5"

  parameter {
    name  = "lc_messages"
    value = "C"
  }

  parameter {
    name  = "log_autovacuum_min_duration"
    value = "0"
  }

  parameter {
    name  = "log_connections"
    value = "1"
  }

  parameter {
    name  = "log_disconnections"
    value = "1"
  }

  parameter {
    name  = "log_duration"
    value = "0"
  }

  parameter {
    name  = "log_error_verbosity"
    value = "default"
  }

  parameter {
    name  = "log_filename"
    value = "postgresql.log.%Y-%m-%d"
  }

  parameter {
    name  = "log_lock_waits"
    value = "1"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "5"
  }

  parameter {
    name  = "log_rotation_age"
    value = "1440"
  }

  parameter {
    name  = "log_rotation_size"
    value = "2097151"
  }

  parameter {
    name  = "log_statement"
    value = "none"
  }

  parameter {
    name  = "log_temp_files"
    value = "0"
  }

  parameter {
    name  = "rds.log_retention_period"
    value = "10080"
  }
}
