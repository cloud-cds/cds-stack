* [ETL](#etl)
* [Webservice](#webservice)
* [RDS](#rds)
* [Services](#services)
* [TensorFlow](#tensorflow)
* [Kubernetes](#kubernetes)
* [Terraform](#terraform)
* [Networking](#vpc--networking )
* [Controllers](#controllers)
* [Load testing](#locust-load-testing)
* [Fire drills](#fire-drills)

TODO: Component Architecture Figure

***

## ETL

### Common **Terraform** commands
**Launch/stop**
```
cd [dev|prod]/stage3
terragrunt [plan|apply|destroy] -var-file=[path]/secrets.tfvars -target=[module name]
```

### Common **k8s** commands
**Status**
```
kubectl get jobs/<job name>
kubectl get pods --show-all -l job-name=[job name] --sort-by='{.metadata.creationTimestamp}'
kubectl describe pods -l job-name=[job name]
kubectl logs [pod-name]
kubectl exec [pod-name] -it -- bash
```

### :fire: epic2op
#### Launch, Shutdown & Status
* Host: Terraform host
* Config: Terraform (including secrets file)
  - Environment vars: Terraform > Lambda > k8s passthrough
  - Python config: dashan-emr/dashan-etl
* Repo: yanif/opsdx-cloud
* Command:
  - launch lambda: **terraform** (module name: `module.trews_etl`)
  - status: **k8s** (job name: `epic2op-[dev|prod]`)

Config highlights:
* Lambda:
  - execution frequency: `15 mins`
* YAML spec:
  - opsdx_nodegroup=etl
  - image=[aws access key id].dkr.ecr.us-east-1.amazonaws.com/trews-etl:<tag>
  - TODO: resources: {cpu: XXX, memory: XXX}

#### Logging & Analysis
* Execution logs:
  - (Python) Cloudwatch > opsdx-dev-k8s-logs > kubernetes/default/epic2op/dev
  - (Lambda) Cloudwatch > /aws/lambda/opsdx-dev-etl-lambda
* Common filter patterns:
  - TODO
* Scripts:
  - Log retrieval:
    `awslogs get opsdx-dev-k8s-logs kubernetes/default/epic2op/dev -q log [-f] [-s] [-e]`

* TODO: Save task graph on S3 with Goofys

#### Metrics & Instrumentation
* Grafana: [OpsDX Dev ETL dashboard](http://grafana.dev.opsdx.io/dashboard/db/opsdx-dev-etl)
* Cloudwatch Metrics: OpsDX > ETL


### :fire: c2dw-daily (Lead: Andong)
#### Launch, Shutdown & Status
* Host: Terraform host
* Config: Terraform (including secrets file)
  - Environment vars: Terraform > Lambda > k8s passthrough
  - Python config: dashan-emr/dashan-etl
* Repo: yanif/opsdx-cloud
* Command:
  - launch lambda: **terraform** (module name: `module.c2dw_daily_etl`)
  - status: **k8s** (job name: `c2dw_daily_etl`)

Config highlights:
* Lambda:
  - execution frequency: 9:00 AM daily
* YAML spec:
  - opsdx_nodegroup=etl
  - image=[aws access key id].dkr.ecr.us-east-1.amazonaws.com/trews-etl:<tag>
  - TODO: resources: {cpu: XXX, memory: XXX}

#### Logging & Analysis
* Execution logs:
  - (Python) Cloudwatch > opsdx-dev-k8s-logs > kubernetes/default/c2dw/dev/daily
  - (Lambda) Cloudwatch > /aws/lambda/opsdx-dev-c2dw-etl-lambda
* Common filter patterns:
  - TODO
* Scripts:
  - Log retrieval:
    `awslogs get opsdx-dev-k8s-logs kubernetes/default/c2dw/dev/daily -q log [-f] [-s] [-e]`
  - S3 Log: Amazon > S3 > opsdx-clarity-etl-stagec2dw
    + c2dw-etl-daily.pdf: the engine execution graph

* TODO: Save task graph on S3 with Goofys

#### Metrics & Instrumentation
* Grafana: [OpsDX Dev ETL dashboard](http://grafana.dev.opsdx.io/dashboard/db/opsdx-dev-etl)
* Cloudwatch Metrics: OpsDX > ETL

### :fire: op2dw (Lead: Yanif)
#### Launch

#### Shutdown

#### Logging

#### Log Analysis

#### Metrics & Instrumentation


### :fire: c2dw (Lead: Andong)
#### Launch, Shutdown & Status
* Host: Terraform host
* Config: Terraform (including secrets file)
  - Environment vars: Terraform > Lambda > k8s passthrough
  - Python config: dashan-emr/dashan-etl
* Repo: yanif/opsdx-cloud
* Command:
  - launch job: `kubectl create -f /dev/stage3/services/c2dw_etl/c2dw-etl-1m.yaml -n c2dw-etl`
  - status: `kubectl get pods -n c2dw-etl`

Config highlights:
* YAML spec: under `/dev/stage3/services/c2dw-etl/`

#### Logging & Analysis
* Execution logs:
  - (Python) Cloudwatch > opsdx-ml-dev-k8s-logs > kubernetes/c2dw/etl/c2dw/etl/*
* Common filter patterns:
  - TODO
* Scripts:
  - Log retrieval:
    `awslogs get opsdx-ml-dev-k8s-logs kubernetes/c2dw/etl/c2dw/etl/* -q log [-f] [-s] [-e]`
  - S3 Log: Amazon > S3 > opsdx-clarity-etl-stagec2dw
    + c2dw-etl-*.pdf: the engine execution graph
    + c2dw-etl-*.log: the ETL log in text format

#### Metrics & Instrumentation
* Grafana: TODO
* Cloudwatch Metrics: TODO


***

## Webservice  
(Lead: Yanif)
#### Launch, Stop
```
cd dev/stage3/services/web
kubectl apply -f trews-rest-api.yml
kubectl apply -f trews-autoscaler.yaml   ## Starts pod autoscaler
cd ../cluster-autoscaler
helm install stable/aws-cluster-autoscaler -f values.yaml -n devscale   ## Starts cluster autoscaler
```

#### Status
```
kubectl get pods --show-all -l app=trews-rest-api
kubectl top pod -l app=trews-rest-api
kubectl get hpa/trews-rest-api
kubectl get nodes -l opsdx_nodegroup=web
```

#### Logging & Analysis
* (Access) Cloudwatch Logs > opsdx-web-logs-[dev|prod] > monitoring
* (Pod) Cloudwatch Logs > opsdx-[dev|prod]-k8s-logs > kubernetes/default/trews/rest/api
* (ELB) S3 > opsdx-elb-access-logs
* Scripts:
  - Log retrieval:
    `awslogs get opsdx-dev-k8s-logs kubernetes/default/trews/rest/api -q log [-f] [-s] [-e] > web-pod.log`

  - Server latency: TODO: grep on pod logs
  - User latency: TODO: grep on pod logs
  - Cache hits: TODO: grep on pod logs 

#### Metrics & Instrumentation
* Server: Cloudwatch Metrics > [API | Route | API, MetricStreamId, Route]
* User/Browser: Cloudwatch Metrics > [Browser | Browser, MetricStreamId]
* External pings: Cloudwatch Metrics > PingSource, PingStack > damsl + rambo
  - Expected rate: 60 data samples every 5 mins
* Alarms:
  - opsdx-[dev|prod]-ping-up: ensure # external pings > 45 every 5 mins
  - opsdx-[dev|prod]-web-nodes-inservice
  - opsdx-[dev|prod]-elb-connections-failed-<availability-zone> = # of requests reaching webserver

#### Autoscaling
* YAML spec highlights:
  - opsdx_nodegroup=web
  - image=[aws access key id].dkr.ecr.us-east-1.amazonaws.com/trews-rest-api:<tag>
  - resources:
    - pod: {cpu: 1200m, memory: 2816MiB}
    - node: m4.large

***

## RDS 
DB + DW (Lead: Andong, Yanif)
#### Launch
* Command: **terraform**
* Config: `[dev|prod]/stage1/db` (for both db + dw)
  - DB configured for backup and multi-AZ
  - DW disabled backup and multi-AZ for fast loading
    - Must manually backup

#### Status & Maintenance
* Increase RDS space
* Change RDS instance type to increase/decrease cores/memory
* Change high availability
* Change backup window
* Edit and change RDS parameter groups
* Upgrade engine version

#### Logging & Log Analysis
* RDS logs

#### Metrics & Instrumentation
* Grafana: OpsDX Performance dashboard
* Cloudwatch Metrics: Metrics > RDS
* TODO: Alarms


***

## Services

### Common **kubectl** commands
```
cd dev/stage3/services/[name]
kubectl apply -f [service].yml
kubectl get pods --show-all -l [service selector]
kubectl delete -f [service].yml
```

### Common **helm** commands
```
cd dev/stage3/services/[name]
helm install [chart name] -f [values.yaml] -n [service name] --namespace [service namespace]
helm status [service name]
helm delete [service name] --purge
```

### :fire: Concourse CI (Lead: Mike)
#### Launch, Shutdown, Status
* Commands: **helm**
* Values file: values.yaml
* Chart name: concourse (local chart implementation)
* Service name: concourse
* Service namespace: concourse
* Post-setup configuration:
```
fly -t concourse login http://concourse.dev.opsdx.io
fly -t concourse set-pipeline -p main-pipeline -c [dashan-emr/dashan-etl/ci/pipeline.yml] -l concourse-vars.yml
```

#### Logging & Analysis


### :fire: Grafana (Metrics Dashboard, Lead: Yanif)
#### Launch, Shutdown, Status
* Commands: **helm**
* Values file: values.yaml
* Chart name: grafana (local chart implementation)
* Service name: grafana
* Service namespace: monitoring
* Post-setup configuration: import dev/stage3/services/grafana/dashboards
* Access admin password: `kubectl get secret -n monitoring grafana-grafana -o jsonpath="{.data.grafana-admin-password}" | base64 -D`

#### Logging & Analysis


### :fire: Redash (Data Dashboard, Lead: Yanif, Andong)
#### Launch, Shutdown, Status
* Commands: **kubectl**
* Service file: redash.yaml
* Service name: redash
* Full deletion/reinstall must remove the database from RDS (and create redash_root user as needed):
```
psql -h $db_host -U $db_user -d $db_name -p $db_port
drop database redash;
create database redash;
```

#### Logging & Analysis


### :fire: Prometheus
#### Launch, Shutdown, Status
* Commands: **helm**
* Values file: values.yaml
* Chart name: stable/prometheus
* Service name: prometheus
* Service namespace: monitoring

#### Logging & Analysis


### :fire: TODO: NFS (Lead: Yanif)
#### Launch, Shutdown, Status
* Commands: **kubectl**
* Service file: redash.yaml
* Service name: redash
* TODO: mounting on controller
* TODO: mounting on pod



### :fire: Behavior monitors (Lead: Peter)
#### Launch, shutdown
* Command: **terraform**

#### Logging & Analysis

#### Metrics & instrumentation


***

## TODO: TensorFlow (Lead: Ben Ring)
#### Launch, Shutdown, Status
* Command: **kubectl**

#### Logging & Analysis

#### Metrics & Instrumentation


***

## Kubernetes (Lead: Yanif)
#### Launch: **kops**

See kops docs at: [https://github.com/kubernetes/kops/tree/master/docs](https://github.com/kubernetes/kops/tree/master/docs)

```
# Load environment vars for kops.
source env/env-dev.sh # or env-prod.sh, env-dev-ml.sh etc.

# Check kops and kubectl version
kops version 
Version 1.5.3 (git-46364f6)
kubectl version
Client Version: version.Info{Major:"1", Minor:"6", GitVersion:"v1.6.0", GitCommit:"fff5156092b56e6bd60fff75aad4dc9de6b6ef37", GitTreeState:"clean", BuildDate:"2017-03-28T16:36:33Z", GoVersion:"go1.7.5", Compiler:"gc", Platform:"darwin/amd64"}

# TODO: create S3 bucket for kops state store via terraform
cd global/storage
vi main.tf    # Add s3 bucket resource to terraform file
terragrunt [plan|apply] -var-file=secrets.tfvars -target=[bucket resource name]

# kops setup
cd ../..
kops create cluster \
    --node-count 5 \
    --zones us-east-1a,us-east-1c,us-east-1d \
    --master-zones us-east-1a,us-east-1c,us-east-1d \
    --dns-zone dev.opsdx.io \
    --node-size t2.large \
    --master-size t2.medium \
    --ssh-public-key [path/to/key] \
    --topology=private \
    --networking=weave \
    --network-cidr="10.0.0.0/16" \
    --vpc=${VPC_ID} \
    ${NAME}

kops edit cluster ${NAME}
kops create ig [ig-name] --name ${NAME}
kops edit ig [ig-name] --name ${NAME}

# Creates a terraform spec for the k8s cluster
kops update cluster ${NAME} --out=k8s --target=terraform

# Deploy the k8s cluster
# mv k8s dev/stage2 (best practice: use diff to check updated files and only replace those files)
# cd dev/stage2
terragrunt [plan|apply] -var-file=secrets.tfvars -target=module.k8s
```

Post-setup configuration:
* Load kops dashboard addon
```
kubectl create -f https://raw.githubusercontent.com/kubernetes/kops/master/addons/kubernetes-dashboard/v1.5.0.yaml
```

* Load kops heapster addon
```
kubectl create -f https://raw.githubusercontent.com/kubernetes/kops/master/addons/monitoring-standalone/v1.3.0.yaml
```

* Launch the cloudwatch logging daemonset in the new k8s cluster:
```
cd opsdx-cloud/global/k8s/logging
kubectl config use-context [cluster]
kubectl apply -f fluent.[cluster].daemonset.yaml
```

#### Status & Maintenance
* TODO: weave overlay IP garbage collection one-liner
* TODO: alarms

#### Logging & Analysis
* Manually get pod logs from a k8s node:
```
ssh -i [path/to/key] admin@[node ip]
sudo docker ps 
cd /var/lib/docker/container/[container id]
sudo ls -lat # Should list hourly json.log.gz files
sudo cp [log file] /tmp/
sudo chown admin:admin /tmp/[log file]
```

And on the controller:
```
scp -i [path/to/key] admin@[node ip]/tmp/[log file] /path/on/controller
```

Don't forget to clean up the duplicate log file on the k8s node!


#### Metrics & Instrumentation


***

## Terraform (Lead: Yanif)
#### AWS component layout
```
github:yanif/opsdx-cloud
`+  global/
 |
 |- [dev|prod]/stage1
 | `+ core
 |  |- audit
 |  |- db
 |  `- dns
 |  
 |- [dev|prod]/stage2
 |`+ k8s
 | |- k8s-ml
 | `- k8s-ml-tf
 |
 `- [dev|prod]/stage3
   `+ services
     `+ ...
```


***

## VPC & Networking  (Lead: Yanif)
#### Configuration
* TODO: VPCs
  - dev
  - prod
* TODO: subnets
  - by availability zone
  - by node role: master, node, utility
  - public, private
* internet gateways
* nat gateways
* security groups

***

## Controllers
(Lead: Andong)
#### Launch

#### Shutdown

#### Logging

#### Log Analysis

#### Metrics & Instrumentation

***

## Locust: Load Testing

#### Launch, Shutdown, Status
```
cd [dev|prod]/stage3/locust
helm install locust -f values.yaml --set master.config.trews_pats="xxx",master.config.trews_admin_key="xxx",worker.config.trews_pats="xxx",worker.config.trews_admin_key="xxx" -n dev-locust

kubectl port-forward dev-locust-master-xxx [locust-port] [local-port]  

helm del [dev|prod]-locust --purge
```

#### Analysis

***

## Fire Drills

#### Launch, Shutdown, Status
* Chaoskube
```
cd [dev|prod]/stage3/services/chaoskube
helm delete web-chaos --purge
helm install stable/chaoskube -f value-web.yaml
```

#### Analysis

***

[[Support Template|Support-Template]]