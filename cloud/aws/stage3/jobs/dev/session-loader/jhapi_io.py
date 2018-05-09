import grequests
import json
import datetime as dt
import logging
import pytz


class JHAPI:
    def __init__(self, server, client_id, client_secret):
        if server not in SERVERS:
            raise ValueError('Invalid server name provided. Must be one of: {}.'
                             .format(', '.join(SERVERS.keys())))
        else:
            self.server = SERVERS[server]
        self.headers = {
            'client_id': client_id,
            'client_secret': client_secret,
            'User-Agent': ''
        }

    def load_flowsheet(self, patients, flowsheet_id, load_tz='US/Eastern'):
        # NOTE: need to turn timestamp to US/Eastern for Epic
        if patients is None or len(patients) == 0:
            logging.warn('No patients passed in')
            return None
        url = self.server + '/patients/addflowsheetvalue'
        t_utc = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
        now = str(t_utc.astimezone(pytz.timezone(load_tz)))
        payloads = [{
            'PatientID':            pat['pat_id'],
            'PatientIDType':        'EMRN',
            'ContactID':            pat['visit_id'],
            'ContactIDType':        'CSN',
            'UserID':               'WSEPSIS',
            'FlowsheetID':          flowsheet_id,
            'Value':                pat['value'],
            'InstantValueTaken':    str(pat['tsp'].astimezone(pytz.timezone((load_tz)))) if 'tsp' in pat else now,
            'FlowsheetTemplateID':  '304700006',
        } for pat in patients]
        for payload in payloads:
            logging.info('load_flowsheet %s %s %s %s %s' % (flowsheet_id, payload['InstantValueTaken'], payload['PatientID'], payload['ContactID'], payload['Value']))
        reqs = [grequests.post(url, json=payload, timeout=10.0, headers=self.headers) for payload in payloads]
        responses = grequests.map(reqs)
        return responses


    def extract_orders(self, pat_id, csn, hospital):
        lab_url = '{}/facilities/hospital/{}/orders/activeprocedures'.format(self.server, hospital)
        lab_params = {'csn': csn, 'ordermode': 2}
        med_url = '{}/patients/medications'.format(self.server)
        med_params = {'id': pat_id, 'searchtype': 'IP', 'dayslookback': '3'}
        reqs = [grequests.get(lab_url, params=lab_params, timeout=10, headers=self.headers),
                grequests.get(med_url, params=med_params, timeout=10, headers=self.headers)]
        responses = grequests.map(reqs)
        logging.info("Got responses: {}".format(responses))
        lab_orders = responses[0].json() if responses[0] else []
        med_orders = responses[1].json() if responses[1] else {}
        return lab_orders, med_orders

