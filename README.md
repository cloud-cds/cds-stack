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
