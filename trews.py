# trews.py

# Let's get this party started!
import os
import falcon
from Crypto.Cipher import AES
import api
from jinja2 import Environment, FileSystemLoader
import os
STATIC_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(STATIC_DIR, 'static')
import logging
import json
import boto3

URL = '/'
URL_STATIC = URL
URL_API = URL + "api"
URL_LOG = URL + "log"
URL_FEEDBACK = URL + "feedback"
INDEX_FILENAME = 'index.html'

# default keys for JHH
KEYS = {
    'lactate': '2',
    'blood_culture': '4',
    'antibiotics': '5',
    'fluid': '1',
    "vasopressors": '7'
}


class TREWSStaticResource(object):
    def on_get(self, req, resp):
        global URL_STATIC, STATIC_DIR, INDEX_FILENAME
        resp.status = falcon.HTTP_200
        if req.path.endswith('.css'):
            resp.content_type = 'text/css'
        elif req.path.endswith('json'):
            resp.content_type = 'application/json'
        elif req.path.endswith('js'):
            resp.content_type = 'application/javascript'
        else:
            resp.content_type = 'text/html'

        abspath = req.path
        if abspath == '' or abspath == '/':
            abspath = INDEX_FILENAME
            logging.info("index request uri:" + req.relative_uri)
        elif abspath.startswith('/'):
            abspath = abspath[1:]
        filename = os.path.join(STATIC_DIR, abspath)
        if filename.endswith(INDEX_FILENAME):
            # TODO: customize order keys based on LOC
            # TODO parse parameters from query string
            parameters = req.params
            hospital = 'JHH'
            if 'LOC' in parameters:
                loc = parameters['LOC']
                if len(loc) == 6:
                    if loc.startswith("1101"):
                        loc = 'JHH'
                    elif loc.startswith("1102"):
                        loc = 'BMC'
                        KEYS['antibiotics'] = '6'
                        KEYS['vasopressors'] = '13'
                    elif loc.startswith("1103"):
                        loc = 'HCGH'
                        KEYS['antibiotics'] = '3'
                    elif loc.startswith("1104"):
                        loc = 'Sibley'
                    elif loc.startswith("1105"):
                        loc = 'Suburban'
                    elif loc.startswith("1107"):
                        loc = 'KKI'
                else:
                    logging.error("LOC parsing error:" + loc)
            else:
                logging.error("No LOC in query string. Use JHH as default hospital")
            j2_env = Environment(loader=FileSystemLoader(STATIC_DIR),
                                                 trim_blocks=True)
            resp.body = j2_env.get_template(INDEX_FILENAME).render(
                    keys=KEYS
                )
            logging.info("falcon logging example: user request on index.html")
        else:
            with open(filename, 'r') as f:
                resp.body = f.read()

class TREWSLog(object):
    def on_post(self, req, resp):
        try:
            log_json = req.stream.read()
            logging.error(json.dumps(log_json, indent=4))
        except Exception as ex:
            # logger.info(json.dumps(ex, default=lambda o: o.__dict__))
            raise falcon.HTTPError(falcon.HTTP_400,
                'Error',
                ex.message)

class TREWSFeedback(object):
    def on_post(self, req, resp):
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

        try:
            result_json = json.loads(raw_json, encoding='utf-8')
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_400, 'Malformed JSON',
                'Could not decode the request body. The JSON was incorrect.')

        try:
            subject = 'Feedback - {}'.format(str(result_json['u']))
            html_text = [
                ("Physician", str(result_json['u'])),
                ("Current patient in view", str(result_json['q'])),
                ("Department", str(result_json['depid'])),
                ("Feedback", str(result_json['feedback'])),
            ]
            body = "".join(["<h4>{}</h4><p>{}</p>".format(x, y) for x,y in html_text])
            client = boto3.client('ses')
            client.send_email(
                Source      = 'trews-jhu@opsdx.io',
                Destination = {
                    'ToAddresses': [ 'trews-jhu@opsdx.io' ],
                },
                Message     = {
                    'Subject': { 'Data': subject, },
                    'Body': {
                        'Html': { 'Data': body, },
                    },
                }
            )
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(result_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error sending email', ex.message)


app = falcon.API()

# Resources are represented by long-lived class instances

trews_www = TREWSStaticResource()
trews_api = api.TREWSAPI()
trews_log = TREWSLog()
trews_feedback = TREWSFeedback()
handler = TREWSStaticResource().on_get
app.add_route(URL_API, trews_api)
app.add_route(URL_LOG, trews_log)
app.add_route(URL_FEEDBACK, trews_feedback)
app.add_sink(handler, prefix=URL_STATIC)
# app.add_route('/trews-api/', trews_www)

