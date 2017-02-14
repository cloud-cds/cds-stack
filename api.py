import falcon
from Crypto.Cipher import AES
from pkcs7 import PKCS7Encoder
import base64
import hashlib
import binascii
import urllib
import data_example
import ujson as json
import dashan_query as query
# import watchtower, logging
import json
#hashed_key = 'C8ED911A8907EFE4C1DE24CA67DF5FA2'
#hashed_key = '\xC8\xED\x91\x1A\x89\x07\xEF\xE4\xC1\xDE\x24\xCA\x67\xDF\x5F\xA2'
#hashed_key = 'e7cde81226f1d5e03c2681035692964d'
hashed_key = '\xe7\xcd\xe8\x12\x26\xf1\xd5\xe0\x3c\x26\x81\x03\x56\x92\x96\x4d'
IV = '\x00' * 16
MODE = AES.MODE_CBC

DECRYPTED = False

class TREWSAPI(object):
    def on_get(self, req, resp):
        """
        See example in test_decrpyt.py
        """
        encrypted_text = urllib.unquote(req.query_string)
        resp.content_type = 'text/html'
        resp.status = falcon.HTTP_200  # This is the default status

        body = "query_string: " + encrypted_text
        
        decodetext =  base64.b64decode(encrypted_text)
        aes = AES.new(hashed_key, MODE, IV)
        cipher = aes.decrypt(decodetext)
        encoder = PKCS7Encoder()
        pad_text = encoder.decode(cipher)
        
        body += "</br></br>plain_text: " + pad_text
        resp.body = (body)
        
    def decrypt(self, encrypted_text):
        encrypted_text = urllib.unquote(encrypted_text)
        decodetext =  base64.b64decode(encrypted_text)
        aes = AES.new(hashed_key, MODE, IV)
        cipher = aes.decrypt(decodetext)
        encoder = PKCS7Encoder()
        pad_text = encoder.decode(cipher)
        return pad_text

    def update_response_json(self, data, eid):
        """
        TODO: update other part
        update chart data
        """
        data['pat_id'] = eid
        df = query.get_trews(eid)
        twf = get_twf(eid)
        data['chart_data']['chart_values']['timestamp'] = df.tsp 
        data['chart_data']['chart_values']['trewscore'] = df.trewscore
        df_data = df.drop(['enc_id','trewscore','tsp'],1)
        df_rank = df_data.rank(axis=1, method='max', ascending=False)
        top1 = df_rank.as_matrix() < 1.5
        data['chart_data']['chart_values']['tf_1_name'] \
            = [df_rank.columns.values[t][0] for t in top1]
        data['chart_data']['chart_values']['tf_1_value'] \
             = [row[top1_cols[i]] for i, row in twf.iterrows()]
        top2 = (df_rank.as_matrix() < 2.5) & (df_rank.as_matrix() > 1.5)
        data['chart_data']['chart_values']['tf_2_name'] \
            = [df_rank.columns.values[t][0] for t in top2]
        data['chart_data']['chart_values']['tf_2_value'] \
             = [row[top2_cols[i]] for i, row in twf.iterrows()]
        top3 = (df_rank.as_matrix() < 3.5) & (df_rank.as_matrix() > 2.5)
        data['chart_data']['chart_values']['tf_3_name'] \
            = [df_rank.columns.values[t][0] for t in top3]
        data['chart_data']['chart_values']['tf_3_value'] \
             = [row[top3_cols[i]] for i, row in twf.iterrows()]

    def on_post(self, req, resp):
        # logging.basicConfig(level=logging.INFO)
        # logger = logging.getLogger(__name__)
        # logger.addHandler(watchtower.CloudWatchLogHandler(log_group="opsdx-web-logs-prod", create_log_group=False))
        
        # logger.info(
        #     {
        #         'req': {
        #             'protocol': req.protocol,
        #             'method': req.method.
        #             'host': req.host,
        #             'subdomain': req.subdomain,
        #             'app': req.app,
        #             'access_route': req.access_route,
        #             'remote_addr': req.remote_addr
        #         }
        #     }
        #     )
        try:
            raw_json = req.stream.read()
            # logger.info(raw_json)
        except Exception as ex:
            # logger.info(json.dumps(ex, default=lambda o: o.__dict__))
            raise falcon.HTTPError(falcon.HTTP_400,
                'Error',
                ex.message)
 
        try:
            req_body = json.loads(raw_json)
        except ValueError as ex:
            # logger.info(json.dumps(ex, default=lambda o: o.__dict__))
            raise falcon.HTTPError(falcon.HTTP_400,
                'Malformed JSON',
                'Could not decode the request body. The '
                'JSON was incorrect. request body = %s' % raw_json)
        
        eid = req_body['q']
        resp.status = falcon.HTTP_202
        data = data_example.patient_data_example
        
        if eid:
            if DECRYPTED:
                eid = self.decrypt(eid)
            if query.eid_exist(eid):
                self.update_response_json(data, eid)
        resp.body = json.dumps(data)
        resp.status = falcon.HTTP_200
        # logger.info(
        #     json.dumps({
        #         'resp.body': data ,
        #         'resp.status': falcon.HTTP_200
        #     })
        # )



    def hash_password(key):
        """
        example in test_hash.py
        Note: not sure do we need to run this code in runtime
        """
        hash_object = hashlib.sha1(key.encode('utf-8'))
        dig = bytearray(hash_object.digest())
        hex_dig = hash_object.hexdigest()
        key_size = 128
        hashed_key_1 = bytearray('\x00' * 64)
        hashed_key_2 = bytearray('\x00' * 64)
        
        for i in range(64):
            if i < len(dig):
                hashed_key_1[i] = dig[i] ^ 0x36
            else:
                hashed_key_1[i] = 0x36

        for i in range(64):
            if i < len(dig):
                hashed_key_2[i] = dig[i] ^ 0x5c
            else:
                hashed_key_2[i] = 0x5c

        hash_object = hashlib.sha1(hashed_key_1)
        hashed_key_1 = bytearray(hash_object.digest())

        hash_object = hashlib.sha1(hashed_key_2)
        hashed_key_2 = bytearray(hash_object.digest())


        hashed_key = hashed_key_1 + hashed_key_2
        return binascii.hexlify(hashed_key[:16])