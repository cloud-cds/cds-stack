# Deploying Concourse for continuous integration

[Concourse](https://concourse.ci/) is a simple and scalable CI system; it builds,
runs tests, and pushes Docker containers to AWS Elastic Container Service.

(For generic info see [concourse/README.md](concourse/README.md).)

Our concourse web UI is at [https://concourse-dev.jh.opsdx.io](https://concourse-dev.jh.opsdx.io).  The system occasionally
breaks -- for example, it may fail to query its git resources (the resource elements in the UI will
indicate "errored" state.)  If this happens, restart it:

1. __Set up__. Install the [helm tool](https://helm.sh/).  (You may have to specify kubernetes config location,
    e.g., `helm init --kube-context ~/.kube/config`.)

2. __Delete concourse__ with `helm delete --purge concourse`

3. __Clear the concourse database__: access the "dev" db with `psql` and run:
    ```
    drop database concourse;
    create database concourse;
    ```

4. __Launch concourse__.  You will need to obtain the secrets file
`cloud/aws/stage3/dev-services/concourse/values.yaml` (note this is different from the template
`values.yaml` in `cloud/aws/stage3/dev-services/concourse/concourse`).  From the
`cloud/aws/stage3/dev-services/concourse/` dir, do
    ```
    helm install concourse/ -f values.yaml -n concourse --namespace concourse
    kubectl get pods -n concourse
    ```
    The last command should give you something like
    ```
    NAME                            READY     STATUS    RESTARTS   AGE
    concourse-web-486896661-21tsq   1/1       Running   0          13s
    concourse-worker-0              1/1       Running   0          13s
    concourse-worker-1              1/1       Running   0          11s
    ```

5. Check the [web UI](https://concourse-dev.jh.opsdx.io) - you should see an empty concourse
   system, ready for your pipeline.

6. Set up the CI pipeline by following the directions under [/ci/](../../../../../ci/README.md).
