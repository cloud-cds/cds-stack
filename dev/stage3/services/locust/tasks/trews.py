#!/usr/bin/env python3

from locust import HttpLocust, TaskSet
import json
import sys, os, random
sys.path.append(os.getcwd())

###########################
# Constants & Utilities

load_test_user = 'LOADTESTUSER'
location = os.environ['trews_location'] if 'trews_location' in os.environ else '1103701'
adminkey = os.environ['trews_admin_key']

global_pat_ids = [
    "3121",
    "3122",
]

if 'trews_pats' in os.environ:
    global_pat_ids = os.environ['trews_pats'].split(',')
    print('Using pat_ids: {}'.format(global_pat_ids))

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
        #index: 1,
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
