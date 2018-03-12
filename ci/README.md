# :arrows_clockwise: Continuous Integration :arrows_clockwise:

Yay. We now have continuous integration using Concourse. ([website](https://concourse.ci/), [github](https://github.com/concourse/concourse))

This README describes operations related to setting up the concourse pipeline;
for instructions on how to deploy concourse,
see `cloud/aws/stage3/dev-services/concourse/README.md`

---

## Getting started

Create a `credentials.yml` in this folder file with your github private key.

_Example `credentials.yml`:_
```
aws-repository-uri: someurl.amazonaws.com/trews-etl
aws-access-key-id: ABC123ACCESSKEYID
aws-secret-access-key: asdffdsakljfd432lfdsFDSAjh324lk234lkj
github-access-token: 795fc3lkjafdslkj234fdas32d217e48432fdsac44
github-private-key: |
  -----BEGIN RSA PRIVATE KEY-----
  MIIEpQIBAAKCAQEAuvUl9YUlDHWBMVcuu0FH9u2gSi83PkL4o9TS+F185qDTlfUY
  fGLxDo/bn8ws8B88oNbRKBZR6yig9anIB4Hym2mSwuMOUAg5qsA9zm5ArXQBGoAr
  ...
  iSHcGbKdWqpObR7oau3ILOVEYANIFvUXNu80XNy+jaXltqo7MSSBYJjbnLTmdUFwp
  HBstYQubAQy4oAEHu8osRhH1VX8AR/atewdHHTm48DN74M/FX3/HeJo=
  -----END RSA PRIVATE KEY-----
```
_don't forget the pipe -->_  |

---

## How to use:

It should be all set to run for any commit to a pull-request. 
However if changes need to be made to the pipeline you will need to set them 
with [fly-cli](https://concourse.ci/fly-cli.html).

For example, to set up the pipeline specified in `pipeline.yml`, you'll
need to obtain `concourse-dev-vars.yaml` and run the following commands
to authenticate, set up the pipeline, and unpause it
(when prompted, choose 1. Github and go to the provided URL to authenticate.)

```
fly -t opsdx_dev login https://concourse-dev.jh.opsdx.io/
fly -t opsdx_dev sp -p main -c pipeline.yml -l concourse-dev-vars.yaml
fly -t opsdx_dev up -p main
``` 

Other useful commands:

`fly -t opsdx_dev destroy-pipeline -p main-pipeline`

`fly -t opsdx_dev expose-pipeline --pipeline main-pipeline`
