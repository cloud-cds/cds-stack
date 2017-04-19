#!/usr/bin/python3

import requests, json

passed_tests = True

# Test getting index.html
r = requests.get('https://trews.dev.opsdx.io')
if r.status_code != 200:
    print("Response from index.html = {}".format(r.status_code))
    passed_tests = False

# Test getting /api
post_body = json.dumps({
    "q":            "3132",
    "u":            None,
    "depid":        None,
    "csn":          None,
    "loc":          None,
    "actionType":   None,
    "action":       None
})
r = requests.post('https://trews.dev.opsdx.io/api', data=post_body)
if r.status_code != 200:
    print("Response from /api = {}".format(r.status_code))
    passed_tests = False

# Return correct status code
if passed_tests:
    print("All tests passed!")
    exit(0)
else:
    exit(1)
