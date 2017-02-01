# -*- coding: utf-8 -*-

import pykube

def handler(event, context):
  config = pykube.KubeConfig.from_file('opsdx-kube-config')
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
            "image": "810056373490.dkr.ecr.us-east-1.amazonaws.com/trews-etl:0.0.1",
          }],
          "restartPolicy": "Never"
        }
      }
    }
  }
  job = pykube.Job(api, job)
  if job.exists():
    print("Reloading job")
    pods = pykube.Pod.objects(api).filter(namespace="default", selector={"job-name": "trews-etl"})
    for pod in pods:
      print("Deleting " + pod.name)
      pod.delete()
    job.delete()
    job.create()
  else:
    print("Creating job")
    job.create()

