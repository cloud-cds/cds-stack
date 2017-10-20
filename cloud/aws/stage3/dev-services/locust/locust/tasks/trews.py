#!/usr/bin/env python3

from locust import HttpLocust, TaskSet, events
import sys, os, random
import logging
import datetime
import copy
import json
import pytz
import requests
import sqlalchemy
from sqlalchemy import text
sys.path.append(os.getcwd())

###########################
# Constants & Utilities

epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

load_test_user = 'LOADTESTUSER'
location = os.environ['TREWS_LOCATION'] if 'TREWS_LOCATION' in os.environ else '1103701'
adminkey = os.environ['TREWS_ADMIN_KEY']

global_pat_ids = [
    "3121",
    "3122",
]

logging.basicConfig()

if 'TREWS_PATS' in os.environ:
    global_pat_ids = os.environ['TREWS_PATS'].split(',')
    logging.info('Using pat_ids: {}'.format(global_pat_ids))

logging.info('Creating DB Engine... ')

host     = os.environ['DB_HOST']
port     = os.environ['DB_PORT']
db       = os.environ['DB_NAME']
user     = os.environ['DB_USER']
pw       = os.environ['DB_PASSWORD']
conn_str = 'postgresql://{}:{}@{}:{}/{}'.format(user, pw, host, port, db)
engine   = sqlalchemy.create_engine(conn_str)

logging.info('Created DB engine.')


def get_request_body(user, action_type=None, action=None):
    return json.dumps({
        "q":            str(user['pat_id']),
        "u":            str(user['user_id']),
        "depid":        "test_department",
        "csn":          "123",
        "loc":          str(user['location']),
        "actionType":   action_type,
        "action":       action
    })

override_action = {
    "actionName": "heart_rate",
    "value": [
        {
            "range": "max",
            "lower": None,
            "upper": 142
        }
    ],
    "clear": False
}

suspicion_action = {
    "actionName": "sus-edit",
    "value": "Infections"
}

notification_action = {
    "id": 1,
    "read": True
}

place_order_action = {
    "actionName": "initial_lactate_order"
}

complete_order_action = {
    "actionName": "initial_lactate_order"
}

order_not_indicated_action = {
    "actionName": "initial_lactate_order"
}

reset_patient_action = {
    "value": None
}

deactivate_action = {
    "value": None
}

set_deterioration_feedback_action = {
    "value": None,
    "other": None
}

json_header = {'content-type': 'application/json'}

def jsonify(json_dict):
    return json.dumps(json_dict)

###########################
# API requests

# Read
def index(l):
    global adminkey
    l.client.get("/?PATID={}&USERID={}&LOC={}&adminkey={}".format(l.user['pat_id'], l.user['user_id'], l.user['location'], adminkey))

# Read
def general_poll(l):
    l.client.post("/api", data = get_request_body(l.user))

# Read
def poll_notifications(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "pollNotifications"
    ))

# Read
def poll_history(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "pollAuditlist"
    ))

# Write
def override(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "override",
        action = override_action
    ))

# Write
def suspicion_of_infection(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "suspicion_of_infection",
        action = suspicion_action
    ))

# Write
def notification(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "notification",
        action = notification_action
    ))

# Write
def place_order(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "place_order",
        action = place_order_action
    ))

# Write
def complete_order(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "complete_order",
        action = complete_order_action
    ))

# Write
def order_not_indicated(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "order_not_indicated",
        action = order_not_indicated_action
    ))

# Write
def reset_patient(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "reset_patient",
        action = reset_patient_action
    ))

# Write
def deactivate(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "deactivate",
        action = deactivate_action
    ))

# Write
def set_deterioration_feedback(l):
    l.client.post("/api", data = get_request_body(
        user=l.user,
        action_type = "set_deterioration_feedback",
        action = set_deterioration_feedback_action
    ))

class IdleBehavior(TaskSet):
    tasks = {
        index: 1,
        general_poll: 1,
        poll_notifications: 1,
        poll_history: 1,
        # override: 1,
        # suspicion_of_infection: 1,
        # notification: 1,
        # place_order: 1,
        # complete_order: 1,
        # order_not_indicated: 1,
        # reset_patient: 1,
        # deactivate: 1,
        # set_deterioration_feedback: 1,
    }
    def on_start(self):
        self.user = {
            'pat_id': random.choice(global_pat_ids),
            'user_id': load_test_user,
            'location': location
        }
        index(self)

# class ActiveBehavior(TaskSet):
#     tasks = {poll_notifications: 1}
#     def on_start(self):
#         index(self)

class IdleUser(HttpLocust):
    weight = 1
    task_set = IdleBehavior
    min_wait = 1000
    max_wait = 1000

# class ActiveUser(HttpLocust):
#     weight = 1
#     task_set = ActiveBehavior
#     min_wait = 28000
#     max_wait = 32000



#############################
# Event handlers.

job_id = int(os.environ['JOB_ID']) if 'JOB_ID' in os.environ else -1
table_name = os.environ['LOCUST_TABLE'] if 'LOCUST_TABLE' in os.environ else 'locust_stats'
batch_size = int(os.environ['BATCH_SZ']) if 'BATCH_SZ' in os.environ else 100

prometheus_url = os.environ['PROM_URL'] if 'PROM_URL' in os.environ else 'http://prometheus-prometheus-server.monitoring.svc.cluster.local'
prometheus_load_query = 'label_replace(sum(rate(container_cpu_usage_seconds_total{pod_name=~".*trews-rest-api-[0-9].*", image=~".*universe.*"}[2m])) by (pod_name),"pod_lbl","trews-api-$1","pod_name","trews-rest-api-[0-9a-zA-Z]*-(.*)")'

# Per run statistics
batch_start = None
batch_end = None

latencies = []

def flush_stats():
    global table_name, job_id, batch_size
    global prometheus_url, prometheus_load_query
    global epoch, batch_start, batch_end
    global latencies

    batch_end = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    t_start = (batch_start - epoch).total_seconds()
    t_end = (batch_end - epoch).total_seconds()

    batch_start = batch_end

    # Get CPU load from prometheus
    pod_cpus = []
    try:
        r = requests.get('%s/api/v1/query' % prometheus_url, params={'query': prometheus_load_query})
        r.raise_for_status()

        result = r.json()
        pod_cpus = [{ 'pod': s['metric']['pod_lbl'], 'tsp': s['value'][0], 'load_2m': s['value'][1] } \
                        for s in result['data']['result']]


    except requests.exceptions.RequestException as e:
        logging.error("Error: {}".format(e))


    # Write to DB
    params = {
        'job_id'  : job_id,
        't_start' : t_start,
        't_end'   : t_end,
        'latency' : json.dumps(latencies),
        'load'    : json.dumps(pod_cpus)
    }

    sql = \
    '''
    insert into %(tbl)s (job_id, t_start, t_end, latencies, load)
        values (
            :job_id,
            'epoch'::timestamptz + :t_start * interval '1 second',
            'epoch'::timestamptz + :t_end * interval '1 second',
            :latency, :load)
    ''' % {'tbl': table_name}

    conn = engine.connect()
    conn.execute(text(sql), params)

    logging.info('Pushed stats to DB for %s to %s' % (batch_start, batch_end))
    latencies.clear()
    conn.close()

def on_request_success(request_type, name, response_time, response_length):
    """
    Event handler that get triggered on every successful request
    """
    global latencies
    latencies.append(response_time)

def on_report_to_master(client_id, data):
    """
    This event is triggered on the slave instances every time a stats report is
    to be sent to the locust master. It will allow us to add our extra content-length
    data to the dict that is being sent, and then we clear the local stats in the slave.
    """
    global latencies
    data['latencies'] = copy.deepcopy(latencies)
    latencies.clear()

def on_slave_report(client_id, data):
    """
    This event is triggered on the master instance when a new stats report arrives
    from a slave. Here we just add the content-length to the master's aggregated
    stats dict.
    """
    global latencies, batch_size
    latencies += data['latencies']
    if len(latencies) > batch_size:
        flush_stats()

def on_master_start_hatching():
    global latencies, job_id, batch_start, batch_end
    job_id += 1
    batch_start = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    batch_end = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    latencies.clear()

def on_master_stop_hatching():
    # Flush remaining stats.
    flush_stats()

def on_locust_start_hatching():
    global latencies, batch_start, batch_end
    batch_start = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    batch_end = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    latencies.clear()


def on_locust_stop_hatching():
    pass

# Hook up the event listeners
events.request_success += on_request_success
events.report_to_master += on_report_to_master
events.slave_report += on_slave_report

events.master_start_hatching += on_master_start_hatching
events.master_stop_hatching += on_master_stop_hatching
events.locust_start_hatching += on_locust_start_hatching
#events.locust_stop_hatching += on_locust_stop_hatching