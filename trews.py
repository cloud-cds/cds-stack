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

URL = '/'
URL_STATIC = URL
URL_API = URL + "api"
INDEX_FILENAME = 'index.html'

KEYS = {
    'initial_lactate': '1',
    'blood_culture': '2',
    'antibiotics': '3',
    'fluid': '4'
}


class TREWSStaticResource(object):
    def on_get(self, req, resp):
        global URL_STATIC, STATIC_DIR, INDEX_FILENAME
        resp.status = falcon.HTTP_200
        if req.path.endswith('css'):
            resp.content_type = 'text/css'
        elif req.path.endswith('js'):
            resp.content_type = 'application/javascript'
        else:    
            resp.content_type = 'text/html'

        abspath = req.path
        if abspath == '' or abspath == '/':
            abspath = INDEX_FILENAME
        elif abspath.startswith('/'):
            abspath = abspath[1:]
        filename = os.path.join(STATIC_DIR, abspath)
        if filename.endswith(INDEX_FILENAME):
            j2_env = Environment(loader=FileSystemLoader(STATIC_DIR),
                                                 trim_blocks=True)
            resp.body = j2_env.get_template(INDEX_FILENAME).render(
                    keys=KEYS
                )
        else:
            with open(filename, 'r') as f:
                resp.body = f.read() 


app = falcon.API()



# Resources are represented by long-lived class instances

trews_www = TREWSStaticResource()
trews_api = api.TREWSAPI()

handler = TREWSStaticResource().on_get
app.add_route(URL_API, trews_api)
app.add_sink(handler, prefix=URL_STATIC)
# app.add_route('/trews-api/', trews_www)

