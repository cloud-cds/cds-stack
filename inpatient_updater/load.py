from inpatient_updater import http_utils
from inpatient_updater.config import app_config
import datetime as dt
import pandas as pd
import logging

class Loader:
    def __init__(self, server, client_id, client_secret):
        self.server = server
        self.headers = {'client_id': client_id, 'client_secret': client_secret}

    def load_notifications(self, patients):
        if patients is None or len(patients) == 0:
            logging.warn('No patients passed in')
            return None
        resource = '/patients/addflowsheetvalue'
        current_time = str(dt.datetime.utcnow())
        payloads = [{
            'PatientID':            pat['pat_id'],
            'ContactID':            pat['visit_id'],
            'UserID':               'WSEPSIS',
            'FlowsheetID':          '9490',
            'Value':                pat['notifications'],
            'InstantValueTaken':    current_time,
            'FlowsheetTemplateID':  '304700006',
        } for pat in patients]
        return http_utils.make_nonblocking_requests(self.server, resource, 'POST', payloads)

    def load_notifications_single(self, notifications, pat_id, visit_id, comment=''):
        df = pd.DataFrame([{
            'notifications':    notifications,
            'pat_id':           pat_id,
            'visit_id':         visit_id,
            'comment':          comment,
        }])
        return self.load_notifications(df)[0].json()



