import requests
from inpatient_updater.config import app_config
import grequests
import ujson as json

def make_request(server_name, resource_name, http_method='GET', payload=None):
    validate_server_name(server_name)

    base_url = app_config.SERVERS[server_name]
    url = base_url + resource_name

    request_settings = generate_request_settings(http_method, url, payload)

    response = requests.request(**request_settings)
    response.raise_for_status()
    return response.json()
 
def make_nonblocking_requests(server_name, resource_name, http_method='GET', payloads=None, timeout=5.0):
    validate_server_name(server_name)

    base_url = app_config.SERVERS[server_name]
    url = base_url + resource_name
    if http_method == 'GET':
        reqs = [grequests.get(url, params=payload, timeout=timeout, headers=app_config.HEADERS)  for payload in payloads]    
    elif http_method == 'POST':
        reqs = [grequests.post(url, json=payload, timeout=timeout, headers=app_config.HEADERS) for payload in payloads]

    responses = grequests.map(reqs)
    # Note: it returns responses instead of json
    return responses

def validate_server_name(server_name):
    if server_name not in app_config.SERVERS:
        raise ValueError('Invalid server name provided. Must be one of: {}.'
                         .format(', '.join(app_config.SERVERS.keys())))

def generate_request_settings(http_method, url, payload=None):
    request_settings = {
        'method': http_method,
        'url': url,
        'headers': app_config.HEADERS,
    }

    if payload is not None:
        key = 'params' if http_method == 'GET' else 'json'
        request_settings[key] = payload

    return request_settings

