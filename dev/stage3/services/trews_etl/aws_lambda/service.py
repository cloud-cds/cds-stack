# -*- coding: utf-8 -*-

import os, pykube, yaml


def handler(event, context):
  kube_config = {
    "name": os.environ["kube_name"],
    "server": os.environ["kube_server"],
    "certificate-authority-data": os.environ["kube_cert_auth"].rstrip(),
    "user": os.environ["kube_user"],
    "pass": os.environ["kube_pass"]
  }

  kube_doc = """
apiVersion: v1
kind: Config
preferences: {{}}
clusters:
- cluster:
    certificate-authority-data: {certificate-authority-data}
    server: {server}
  name: {name}
contexts:
- context:
    cluster: {name}
    user: {name}
  name: {name}
current-context: {name}
users:
- name: {name}
  user:
    username: {user}
    password: {pass}
""".format(**kube_config)

  with open('/tmp/kube_config', 'w') as kube_config_file:
    kube_config_file.write(kube_doc)
    kube_config_file.close()

  config = pykube.KubeConfig.from_file("/tmp/kube_config")
  api = pykube.HTTPClient(config)
  job = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
      "name": "trews-etl"
    },
    "spec": {
      "template": {
        "metadata": {
          "name": "trews-etl"
        },
        "spec": {
          "containers": [{
            "name": "trews-etl",
            "image": os.environ["kube_image"],
            "env": [
              {"name": "db_host",                      "value": os.environ["db_host"]},
              {"name": "db_port",                      "value": os.environ["db_port"]},
              {"name": "db_name",                      "value": os.environ["db_name"]},
              {"name": "db_user",                      "value": os.environ["db_user"]},
              {"name": "db_password",                  "value": os.environ["db_password"]},
              {"name": "jhapi_client_id",              "value": os.environ["jhapi_client_id"]},
              {"name": "jhapi_client_secret",          "value": os.environ["jhapi_client_secret"]},
              {"name": "TREWS_ETL_SERVER",             "value": os.environ["TREWS_ETL_SERVER"]},
              {"name": "TREWS_ETL_HOSPITAL",           "value": os.environ["TREWS_ETL_HOSPITAL"]},
              {"name": "TREWS_ETL_HOURS",              "value": os.environ["TREWS_ETL_HOURS"]},
              {"name": "TREWS_ETL_ARCHIVE",            "value": os.environ["TREWS_ETL_ARCHIVE"]},
              {"name": "TREWS_ETL_MODE",               "value": os.environ["TREWS_ETL_MODE"]},
              {"name": "TREWS_ETL_STREAM_HOURS" ,      "value": os.environ["TREWS_ETL_STREAM_HOURS"]},
              {"name": "TREWS_ETL_STREAM_SLICES",      "value": os.environ["TREWS_ETL_STREAM_SLICES"]},
              {"name": "TREWS_ETL_STREAM_SLEEP_SECS",  "value": os.environ["TREWS_ETL_STREAM_SLEEP_SECS"]},
              {"name": "TREWS_ETL_EPIC_NOTIFICATIONS", "value": os.environ["TREWS_ETL_EPIC_NOTIFICATIONS"]}
            ]
          }],
          "restartPolicy": "Never"
        }
      }
    }
  }
  job = pykube.Job(api, job)
  if job.exists():
    print("Reloading job for DB: " + os.environ["db_name"] + "@" + os.environ["db_host"])
    pods = pykube.Pod.objects(api).filter(namespace="default", selector={"job-name": "trews-etl"})
    for pod in pods:
      print("Deleting " + pod.name + " for " + os.environ["db_name"] + "@" + os.environ["db_host"])
      pod.delete()
    job.delete()
    job.create()
  else:
    print("Creating job for DB: " + os.environ["db_name"] + "@" + os.environ["db_host"])
    job.create()

