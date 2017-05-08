import grequests
import datetime as dt
import logging
import pytz

SERVERS = {
    'test': 'https://api-test.jh.edu/internal/v2/clinical',
    'stage': 'https://api-stage.jh.edu/internal/v2/clinical',
    'prod': 'https://api.jh.edu/internal/v2/clinical',
}

class Loader:
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

    def load_notifications(self, patients, load_tz='US/Eastern'):
        if patients is None or len(patients) == 0:
            logging.warn('No patients passed in')
            return None
        url = self.server + '/patients/addflowsheetvalue'
        t_utc = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
        current_time = str(t_utc.astimezone(pytz.timezone(load_tz)))
        payloads = [{
            'PatientID':            pat['pat_id'],
            'ContactID':            pat['visit_id'],
            'UserID':               'WSEPSIS',
            'FlowsheetID':          '9490',
            'Value':                pat['notifications'],
            'InstantValueTaken':    current_time,
            'FlowsheetTemplateID':  '304700006',
        } for pat in patients]
        for payload in payloads:
            logging.info('%s NOTIFY %s %s %s' % (payload['InstantValueTaken'], payload['PatientID'], payload['ContactID'], payload['Value']))
        reqs = [grequests.post(url, json=payload, timeout=10.0, headers=self.headers) for payload in payloads]
        responses = grequests.map(reqs)
        return responses
