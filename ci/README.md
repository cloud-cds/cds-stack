# :arrows_clockwise: Continuous Integration :arrows_clockwise:

Yay. We now have continuous integration using Concourse. ([website](https://concourse.ci/), [github](https://github.com/concourse/concourse))

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

It should be all set to run for any commit to a pull-request. However if changes need to be made to the pipeline you will need to set them with [fly-cli](https://concourse.ci/fly-cli.html).

`fly -t opsdx_dev login -c http://concourse.dev.opsdx.io:8080`

`fly -t opsdx_dev destroy-pipeline -p main-pipeline`

`fly -t opsdx_dev set-pipeline -p main-pipeline -c pipeline.yml -l credentials.yml`

`fly -t opsdx_dev unpause-pipeline -p main-pipeline`
