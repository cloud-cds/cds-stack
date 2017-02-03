# trews.py

# Let's get this party started!
import falcon
from Crypto.Cipher import AES
import decrypt

# Falcon follows the REST architectural style, meaning (among
# other things) that you think in terms of resources and state
# transitions, which map to HTTP verbs.
class TREWSResource(object):
    def on_get(self, req, resp):
        """Handles GET requests"""
        resp.content_type = 'text/html'
        resp.status = falcon.HTTP_200  # This is the default status

        resp.body = ("""\nTREWS -- Targeted Real-time Early Warning Score\n""")


    def decrypt(self, ciphertext):
        key = "shhh-this is a secret!"
        IV = bytes(key[0:16], 'utf-8')
        decryptor = AES.new(key, AES.MODE_CBC, IV=IV)
        plain = decryptor.decrypt(ciphertext)
        return plain

# falcon.API instances are callable WSGI apps
app = falcon.API()

# Resources are represented by long-lived class instances
trews = TREWSResource()
trews_decrypt = decrypt.TREWSDecrypt()
# things will handle all requests to the '/things' URL path
app.add_route('/trews-api', trews)
app.add_route('/trews-api/decrypt/', trews_decrypt)
