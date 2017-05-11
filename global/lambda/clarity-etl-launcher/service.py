# -*- coding: utf-8 -*-

import os, pykube, yaml, json, copy
from datetime import datetime, timedelta
import dateutil.parser
import boto3, botocore

s3 = boto3.resource('s3')

def check_bucket(bucket_name):
  '''
  Returns whether a bucket with the given name exists on S3.
  '''
  bucket_exists = True
  try:
      s3.meta.client.head_bucket(Bucket=bucket_name)
  except botocore.exceptions.ClientError as e:
      # If a client error is thrown, then check that it was a 404 error.
      # If it was a 404 error, then the bucket does not exist.
      error_code = int(e.response['Error']['Code'])
      if error_code == 404:
          bucket_exists = False

  return bucket_exists

def check_object(bucket_name, obj_key):
  '''
  Returns whether an object exists on S3 in the given bucket.
  '''
  obj_exists = True
  try:
      s3.meta.client.head_object(Bucket=bucket_name, Key=obj_key)
  except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
          obj_exists = False

  return obj_exists


# REQUIRED environment vars:
#   bucket
#   upload_prefix
#   upload_delim
#
def archive_if_ready(event, bucket):
  '''
  Archive when all daily upload files are available on the bucket.
  Args:
    event: the triggering event for the lambda
    bucket: an boto S3 bucket object for the uploads

  Returns: a boolean indicating whether archiving occurred.
  '''
  bucket_name = os.environ['bucket'] if 'bucket' in os.environ else None
  upload_prefix = os.environ['upload_prefix'] if 'upload_prefix' in os.environ else None
  upload_delim = os.environ['upload_delim'] if 'upload_delim' in os.environ else None

  # Files found uploaded today.
  uploaded_files = []

  # File list to match against.
  expected_files = []

  archived = False
  date_today = datetime.datetime.today().date()

  for record in event['Records']:
    event_bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    if event_bucket == bucket_name:
      # Get all top-level csvs under the ssis/ prefix
      objects = s3.meta.client.list_objects(Bucket=bucket.name, Prefix=upload_prefix, Delimiter=upload_delim)
      objects_to_copy = []
      for cp in objects.get('CommonPrefixes', []):
        o_key = cp.get('Prefix')
        o = bucket.Object(o_key) if o_key else None
        o_exists = check_object(bucket.name, o_key) if o else False
        modified_today = o.last_modified.date() == date_today if o_exists else False
        if o_exists and modified_today:
          uploaded_files.append(o_key)
          objects_to_copy.append(o)


      if sorted(uploaded_files) == sorted(expected_files):
        # Upload complete; Archive files.
        archive_prefix = 'clarity-%s' % date_today.isoformat()
        for (upload_key, upload_object) in zip(uploaded_files, objects_to_copy):
          archive_key = '/'.join([upload_prefix, archive_prefix, upload_key])

          print('Copying %s to %s' % (upload_key, archive_key))
          bucket.Object(archive_key).copy_from(CopySource=upload_key)

          print('Deleting %s' % upload_key)
          upload_object.delete()

        archived = True
        break

    else:
      print('Invalid lambda event (bucket mismatch), expected: %s event: %s' % (bucket_name, event_bucket))

  return archived


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
def launch_k8s():
  '''
  Launch a k8s job as specified by the above environment variables.
  '''
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
    "env"   : job_env
  }

  if len(cmd_array) > 0:
    job_container['command'] = cmd_array

  job_spec = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
      "name": job_name
    },
    "spec": {
      "template": {
        "metadata": {
          "name": job_name
        },
        "spec": {
          "containers": [job_container],
          "restartPolicy": "Never"
        }
      }
    }
  }

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


def handler(event, context):

  if bucket_name and upload_prefix:
    bucket = s3.Bucket(bucket_name)
    bucket_exists = check_bucket(bucket_name)
    if bucket_exists:
      archived = archive_if_ready(event, bucket)

      if archived:
        # Launch the C2DW ETL job on Kubernetes.
        launch_k8s()

    else:
      print('Invalid bucket %s (No such bucket found)' % bucket_name)

  else:
    print('Invalid environment variables')
    print('    bucket_name: %s' % bucket_name)
    print('    upload_prefix: %s' % upload_prefix)
    print('    upload_delim: %s' % upload_delim)

