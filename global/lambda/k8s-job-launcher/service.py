# -*- coding: utf-8 -*-

import os, pykube, yaml, json, copy
from datetime import datetime, timedelta
import dateutil.parser

# REQUIRED env vars:
# kube_job_name
# kube_name
# kube_server
# kube_cert_auth
# kube_user
# kube_pass
# kube_image
#
# OPTIONAL:
# kube_cmd_*
#
# REQUIRED k8s secrets:
# aws-secrets
#
# ENV VAR FORWARD PREFIX: k8s_job_*
#
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

  cmd_prefix = 'kube_cmd_'
  cmd_array = [ v for k, v in sorted(os.environ.items()) \
                if k.startswith(cmd_prefix) and len(k) > len(cmd_prefix)]

  default_env = [
    {"name": "AWS_ACCESS_KEY_ID",     "valueFrom": { "secretKeyRef": { "name": "aws-secrets", "key": "access_key_id"     } } },
    {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": { "secretKeyRef": { "name": "aws-secrets", "key": "secret_access_key" } } },
    {"name": "AWS_DEFAULT_REGION",    "valueFrom": { "secretKeyRef": { "name": "aws-secrets", "key": "default_region"    } } }
  ]

  env_forward_prefix = 'k8s_job_'
  forward_env = [ {'name': k[len(env_forward_prefix):], 'value' : v} \
                for k,v in os.environ.items() \
                  if k.startswith(env_forward_prefix) and len(k) > len(env_forward_prefix) ]

  job_env = default_env + forward_env

  job_name = os.environ["kube_job_name"]
  job_container = {
    "name"  : job_name,
    "image" : os.environ["kube_image"],
    "env"   : job_env,
  }

  if "kube_privileged" in os.environ:
    p_mode = os.environ['kube_privileged']
    print('Job {} privileged mode: {}'.format(job_name, p_mode))
    job_container['securityContext'] = {
      'privileged': bool(p_mode)
    }

  if "kube_cpu_resources" in os.environ:
    cpu_resources = os.environ['kube_cpu_resources']
    job_container['requests'] = {
      'cpu': str(cpu_resources)
    }

  if len(cmd_array) > 0:
    job_container['command'] = cmd_array

  pod_spec = {
    "containers": [job_container],
    "restartPolicy": "Never"
  }

  if "kube_nodegroup" in os.environ and os.environ['kube_nodegroup'] is not None:
    node_group = os.environ['kube_nodegroup']
    print('Job {} nodeGroup: {}'.format(job_name, node_group))
    pod_spec['nodeSelector'] = { 'opsdx_nodegroup': node_group }

  batch_job_spec = {
    "template": {
      "metadata": { "name": job_name },
      "spec": pod_spec
    }
  }

  if "kube_active_deadline_seconds" in os.environ:
    try:
      deadline = int(os.environ['kube_active_deadline_seconds'])
      print('Job {} activeDeadlineSeconds: {}'.format(job_name, deadline))
      batch_job_spec['activeDeadlineSeconds'] = deadline
    except ValueError:
      print('Invalid kube_active_deadline_seconds environment variable, skipping deadline.')

  job_spec = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": { "name": job_name },
    "spec": batch_job_spec
  }

  print("job spec:")
  print(job_spec)

  job = pykube.Job(api, job_spec)
  if job.exists():
    # Refresh the job execution metadata.
    checkJob = copy.deepcopy(job)
    checkJob.reload()

    reloadJob = False
    print('Current {} job status : {}'.format(job_name, json.dumps(checkJob.obj['status'])))

    if 'active' in checkJob.obj['status']:
      # jobStart = dateutil.parser.parse(j.obj['status']['startTime']).replace(tzinfo=None)
      # jobExpiry = datetime.utcnow() - timedelta(minutes=expiryMinutes)
      # reloadJob = jobStart <= jobExpiry:
      reloadJob = False
    else:
      reloadJob = True

    if reloadJob:
      print('Reloading {} job'.format(job_name))

      # Clean up pods, leaving at most 10 stale containers on k8s.
      pods = pykube.Pod.objects(api).filter(namespace="default", selector={"job-name": job_name})
      if len(pods) > 10:
        sortedPods = sorted(list(pods), key=lambda pod: pod.obj['status']['startTime'])
        while len(sortedPods) > 10:
          pod = sortedPods.pop(0)
          print('Deleting {} pod {}'.format(job_name, pod.name))
          pod.delete()

      job.delete()
      job.create()

  else:
    print('Creating {} job'.format(job_name))
    job.create()
