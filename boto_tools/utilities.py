import boto3
import time
from datetime import datetime, timedelta
import sys

# sys.path.append('../../dashan_core/src/ews_server/')
# from dashan import Dashan


def time2epoch(tsp):
    return int(((tsp - datetime(1970, 1, 1)).total_seconds() )*(10**3))

def epoch2time(epoch):
    return datetime.utcfromtimestamp(epoch / 1000)

def getfiltLogEvent(firstTime, lasTime, client,
                    logGroup,logStreamNames,filterPattern,
                    callLimit=None):

    print(time.strftime("%H:%M:%S") + " Started Filtered Search")

    res = client.filter_log_events(logGroupName=logGroup, logStreamNames=logStreamNames,
                                   startTime=firstTime, endTime=lasTime, filterPattern=filterPattern)

    resList = [res]

    if 'nextToken' in res:
        nt = res['nextToken']
    else:
        nt = False

    loops = 0
    while nt:
        loops += 1
        if callLimit is not None:
            if loops > callLimit:
                break

        print("We are on call {} of filter log events".format(loops))

        res = client.filter_log_events(logGroupName=logGroup, logStreamNames=logStreamNames,
                                 startTime=firstTime, endTime=lasTime, filterPattern=filterPattern,
                                 nextToken=nt)

        resList += [res]

        if 'nextToken' in res:
            nt = res['nextToken']
        else:
            nt = False

    print(time.strftime("%H:%M:%S") + " Filtered Search Complete ")

    return resList

def getLogs(logStart, logEnd, client,
            logGroup, logStreamNames):

    stack = client.get_log_events(logGroupName=logGroup, logStreamName=logStreamNames,
                                  startTime=logStart, endTime=logEnd, startFromHead=True)
    stackList = [stack]

    loops = 0
    if 'nextToken' in stack:
        nt = stack['nextToken']
    else:
        nt = False

    while nt:
        loops += 1
        stack = client.get_log_events(logGroupName='opsdx-dev-k8s-logs', logStreamName='kubernetes/default/trews/etl',
                                      startTime=logStart, endTime=logEnd, nextToken = nt, startFromHead=True)
        stackList += [stack]

        if 'nextToken' in stack:
            nt = stack['nextToken']
        else:
            nt = False

    return stackList

def metricHistoryLoop(firstTime,LastTime,time_delta,
                      func,
                      metric_name, metric_unit, dimList,
                      namespace='OpsDX',doPush=True):

    metric = list()
    time = list()

    intervalStart = firstTime
    intervalEnd = firstTime + time_delta

    client = boto3.client('cloudwatch')

    while intervalEnd <= LastTime:
        value = func(intervalStart, intervalEnd)

        metric.append(value)
        time.append(intervalEnd)

        if doPush:
            put_status = client.put_metric_data(Namespace=namespace,
                                                MetricData=[{
                                                    'MetricName': metric_name,
                                                    'Dimensions': dimList,
                                                    'Timestamp': intervalEnd,
                                                    'Value': value,
                                                    'Unit': metric_unit}])

            # print (put_status)

        intervalStart += time_delta
        intervalEnd += time_delta

    return metric, time

def addDFtoDB(frame,dashan_id, datalink_id):
    dashan_id = 'opsdx_dev'
    datalink_id = 'jhapi_test'
    dashan = Dashan(dashan_id)

    # etl_job_id + 'table' + current_time = dt.datetime.now().strftime('%m%d%H%M%S')


    # def db_load_task(data_type, data, dtypes=None):
    #     dashan.log.info("%s: starting db_load_task for data %s" % (etl_job_id, data_type))
    #     dashan.copy_to(etl_job_id, data_type, data, dtypes, schema="workspace")
    #     dashan.log.info("%s: exiting db_load_task for data %s" % (etl_job_id, data_type))
    # schema = "workspace"
