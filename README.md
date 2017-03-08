# trews_rest_api
The REST API to serve TREWS front-end

### Set up standalone
- Create a virtualenv called venv
- Pip Install `pip install -r requirements.txt`
- Set up some environment variables, contact Andong about credentials and which variables
- Run server! `gunicorn -b localhost:8000 trews:app`

### Development
- ssh to dev controller and git pull the latest version of trews_rest_api
- deploy to deis: `git push deis master`
- we can find the trews-web pods by using kubectl: `kubectl get pods --show-all --all-namespaces`
- we can use kubectl to view the logs: `kubectl logs trews-dev-web-xxx -n trews-dev --since=1m`
