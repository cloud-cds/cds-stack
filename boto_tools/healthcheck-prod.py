import boto3
import requests
import time
import traceback
from datetime import datetime

url = 'https://trews.prod.opsdx.io/'
counter = 1000000
interval = 5

num_failures = 0

# CW client.
client = boto3.client('cloudwatch')

epoch = datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

# Upload latency to cloudwatch
for i in range(counter):
    print('%(ts)s Ping %(iter)s of %(total)s, %(tf)s failures' % {'ts': datetime.utcnow().isoformat(), 'iter': str(i), 'total': str(counter), 'tf': str(num_failures)})

    # HTTPS request.
    request_start = datetime.utcnow()
    try:
        #ping = requests.post(url_prefix + str(i), json={'id': str(i)})
        ping = requests.get(url, params={'PATID': '3155', 'LOC': '110300', 'SEQID': str(i)})

        print('%(ts)s Ping time %(elapsed)s' % {'ts': datetime.utcnow().isoformat(), 'elapsed': str(ping.elapsed.total_seconds()) })

        metric_name = 'ExternalLatency'
        if ping.status_code != 200:
            metric_name = 'ExternalLatencyFailure'
            num_failures += 1
            print('%(ts)s Ping Status %(status)s, %(nf)s total failures' % {'ts': datetime.utcnow().isoformat(), 'status': ping.status_code, 'nf': str(num_failures)})

        metric_value = float(ping.elapsed.total_seconds())

    except requests.exceptions.RequestException as e:
        metric_name = 'ExternalLatencyFailure'
        metric_value = float((datetime.utcnow() - request_start).total_seconds())
        num_failures += 1
        print('%(ts)s Ping Exception (iter %(i)s) %(elapsed)s, %(nf)s total failures' % {'ts': datetime.utcnow().isoformat(), 'i': str(i), 'elapsed': metric_value, 'nf': str(num_failures)})
        traceback.print_exc()

    put_status = client.put_metric_data(
      Namespace='OpsDX',
      MetricData=[{
        'MetricName': metric_name,
        'Dimensions': [{'Name': 'Source', 'Value': 'damsl.cs.jhu.edu'}, {'Name': 'Stack', 'Value': 'Prod'}],
        'Timestamp': datetime.utcnow(),
        'Value': metric_value,
        'Unit': 'Seconds'
      }])

    if put_status['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('CWM Status %(status)s' % {'status': put_status['ResponseMetadata']['HTTPStatusCode'] })

    time.sleep(interval)

print('Report: %(tf)s total failures' % { 'tf': str(num_failures) })
