#!/usr/bin/python3

import requests, json
import os, time

passed_tests = True

# Start up the server

cwd = os.path.dirname(os.path.realpath(__file__))
work_dir = cwd.replace("/test", "/")
os.chdir(work_dir)
os.system("gunicorn -b 0.0.0.0:8000 trews:app --worker-class aiohttp.GunicornUVLoopWebWorker -c gunicorn_conf.py &")
time.sleep(5)

# Test getting index.html
r = requests.get('http://0.0.0.0:8000')
if r.status_code != 200:
    print("\n\n\tFAILED getting index.html")
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
r = requests.post('http://0.0.0.0:8000/api', data=post_body)
if r.status_code != 200:
    print("\n\n\tFAILED getting response from /api")
    print("Response from /api = {}".format(r.status_code))
    passed_tests = False

# Return correct status code
if passed_tests:
    print("All tests passed!")
    exit(0)
else:
    exit(1)
