######################################
# Stream demo lambda.

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package" {}
variable "aws_klaunch_lambda_role_arn" {}

variable "k8s_server_host" {}
variable "k8s_server_port" {}

variable "k8s_name" {}
variable "k8s_server" {}
variable "k8s_user" {}
variable "k8s_pass" {}
variable "k8s_cert_auth" {}
variable "k8s_cert" {}
variable "k8s_key" {}
variable "k8s_token" {}
variable "k8s_image" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}
variable "jhapi_client_id" {}
variable "jhapi_client_secret" {}

variable "TREWS_ETL_SERVER" {}
variable "TREWS_ETL_HOSPITAL" {}
variable "TREWS_ETL_HOURS" {}
variable "TREWS_ETL_ARCHIVE" {}
variable "TREWS_ETL_MODE" {}
variable "TREWS_ETL_DEMO_MODE" {}
variable "TREWS_ETL_STREAM_HOURS" {}
variable "TREWS_ETL_STREAM_SLICES" {}
variable "TREWS_ETL_STREAM_SLEEP_SECS" {}
variable "TREWS_ETL_EPIC_NOTIFICATIONS" {}

resource "aws_lambda_function" "etl_lambda_demo" {

    function_name    = "${var.deploy_prefix}-etl-lambda-demo"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_server_port}"

        kube_job_name  = "epic2op-dev-replay"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_image}"
        #kube_cert      = "${var.k8s_cert}"
        #kube_key       = "${var.k8s_key}"
        #kube_token     = "${var.k8s_token}"

        k8s_job_db_host     = "${var.db_host}"
        k8s_job_db_port     = "${var.db_port}"
        k8s_job_db_name     = "${var.db_name}"
        k8s_job_db_user     = "${var.db_username}"
        k8s_job_db_password = "${var.db_password}"

        k8s_job_jhapi_client_id     = "${var.jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.jhapi_client_secret}"

        k8s_job_TREWS_ETL_SERVER            = "${var.TREWS_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL          = "${var.TREWS_ETL_HOSPITAL}"
        k8s_job_TREWS_ETL_HOURS             = "${var.TREWS_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE           = "${var.TREWS_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE              = "${var.TREWS_ETL_DEMO_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS      = "${var.TREWS_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES     = "${var.TREWS_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS = "${var.TREWS_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.TREWS_ETL_EPIC_NOTIFICATIONS}"
      }
    }
}