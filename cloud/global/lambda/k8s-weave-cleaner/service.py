# -*- coding: utf-8 -*-

import os, re
from kubernetes import config
from kubernetes.client import configuration
from kubernetes.client.apis import core_v1_api
from kubernetes.client.rest import ApiException

def handler(event, context):
  if 'local_config' in os.environ and os.environ['local_config'].lower() == 'true':
    config.load_kube_config()

  else:
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

    config.load_kube_config(config_file="/tmp/kube_config")

  print('Loaded kube config')

  configuration.assert_hostname = False
  api = core_v1_api.CoreV1Api()

  ##########################################
  # Reclaim weave IPs.

  # Get master
  node_selector = 'kubernetes.io/role=master'
  master_name = None
  try:
    nodes = api.list_node(label_selector=node_selector, watch=False)
    for n in nodes.items:
      master_name = n.metadata.name
  except ApiException as e:
    if e.status != 404:
        print("Unknown error: %s" % e)
        exit(1)

  if master_name is None:
    print('No master node found in the cluster!')
    exit(1)

  else:
    print('Found master node: %s' % master_name)

  # Get the weave pod running on the master
  pod_name = None
  pod_ns = 'kube-system'
  pod_container = 'weave'
  pod_fselector = 'spec.nodeName=%s' % master_name
  pod_lselector = 'name=weave-net'
  try:
    pods = api.list_namespaced_pod(namespace=pod_ns, field_selector=pod_fselector, label_selector=pod_lselector, watch=False)
    for p in pods.items:
      pod_name = p.metadata.name
  except ApiException as e:
    if e.status != 404:
        print("Unknown error: %s" % e)
        exit(1)

  if pod_name is None:
    print("Cannot find pod on master %s" % master_name)
    exit(1)

  else:
    print('Found pod matching %s: %s' % (pod_lselector, pod_name))


  # calling exec and wait for response.
  ipam_status_cmd = [
    '/home/weave/weave',
    '--local',
    'status',
    'ipam'
  ]

  mac_addrs = None
  try:
    ipam = api.connect_get_namespaced_pod_exec(pod_name, pod_ns, command=ipam_status_cmd, container=pod_container,
                                               stderr=True, stdin=False, stdout=True, tty=False)

    if ipam is not None:
      ipam_lines = ipam.split('\n')
      mac_addrs = [re.sub('\(.*', '', l) for l in ipam_lines if re.search('unreachable', l)]

  except ApiException as e:
    if e.status != 404:
        print("Unknown error: %s" % e)
        exit(1)

  if mac_addrs:
    for mac in mac_addrs:
      rmpeer_cmd = [
        '/home/weave/weave',
        '--local',
        'status',
        'ipam',
        mac
      ]

      try:
        print('Reclaiming from mac addr: %s' % mac)
        ipam = api.connect_get_namespaced_pod_exec(pod_name, pod_ns, command=rmpeer_cmd, container=pod_container,
                                                   stderr=True, stdin=False, stdout=True, tty=False)

        if ipam is not None:
          ipam_lines = ipam.split('\n')
          mac_addrs = [re.sub('\(.*', '', l) for l in ipam_lines]

      except ApiException as e:
        if e.status != 404:
          print("Unknown error while remove mac addr %s: %s" % (mac, e))
          break


if __name__ == "__main__":
  handler(None, None)