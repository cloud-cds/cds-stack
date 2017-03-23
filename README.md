# trews_rest_api
The REST API to serve TREWS front-end

## Set up standalone
- Create a virtualenv called venv
- Pip Install `pip install -r requirements.txt`
- Set up some environment variables, contact Andong about credentials and which variables
- Run server! `gunicorn -b localhost:8000 trews:app`

## Development
- ssh to dev controller and git pull the latest version of trews_rest_api
- deploy to deis: `git push deis master`
- we can find the trews-web pods by using kubectl: `kubectl get pods --show-all --all-namespaces`
- we can use kubectl to view the logs: `kubectl logs trews-dev-web-xxx -n trews-dev --since=1m`

## Troubleshooting
### deis push failed
You can try
- push again
- rollback and make a empty commit: `deis rollback` and `git commit --allow-empty`
- or scale the deis workers: `deis scale -a trews-dev web=0 && deis scale -a trews-dev web=2`
- or delete the trews-web pod: `kubectl delete pods/trews-web-xxx -n trews-web`
- or scale the deis builder: `kubectl scale --replicas=0 deploy/deis-builder -n deis` and then `kubectl scale --replicas=1 deploy/deis-builder -n deis`