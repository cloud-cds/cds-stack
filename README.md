### Cheetsheet
#### Switch betwee dev and prod
You need `kops`, `kubectl` installed and the bash commands to switch between dev and prod are below

```bash
  cd opsdx-cloud/env/
  source env-prod.sh
  cd ..
  source aws-env.sh
  kops export kubecfg $NAME
  kubectl config get-contexts
  kubectl config use-context cluster.dev.opsdx.io
  kubectl config get-contexts
  kubectl get pods
  kubectl config use-context cluster.prod.opsdx.io
  kubectl get pods
```

### Config dev and prod controller		
#### set the limit of files to 65536 (the same as the ETL pod)		
Add the following lines to the file: `/etc/security/limits.conf`		
		
```bash		
* soft  nofile 40000		
* hard  nofile 40000		
```		
		
And then add following line in the file: /etc/pam.d/common-session		
```bash		
session required pam_limits.so
