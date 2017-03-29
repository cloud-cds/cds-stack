from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import pickle as pickle
import time
import boto3
import utilities as au


def getUsersInInterval(firstTime, lastTime):
    lastTime = au.time2epoch(lastTime)
    firstTime = au.time2epoch(firstTime)

    client = boto3.client('logs')

    resList = au.getfiltLogEvent(firstTime, lastTime, client,
                                    'opsdx-web-logs-prod', ['trews'], '{$.req.url=*USERID*}')


    allDicts = []
    for res in resList:
        for event in res['events']:
            ts = au.epoch2time(event['timestamp'])

            urlStrRaw = json.loads(event['message'])['req']['url']

            try:
                _, urlStr=urlStrRaw.split('?') #cleans website stuff
            except:
                print('URL did not match pattern')
                print(urlStrRaw)
                continue



            keyValList = urlStr.split('&')

            doAppend = True
            thisDict = {}
            for keyValStr in keyValList:
                key, value = keyValStr.split('=')
                thisDict[key] = value
                #Additional Filtering
                if 'key' == 'PATID':
                    if value[0] != 'E':
                        doAppend = False

            thisDict['tsp'] = ts

            if doAppend:
                allDicts.append(thisDict)

    results = pd.DataFrame(allDicts)
    return results


# lastTime = datetime.now() - timedelta(days=6) # last time to look for exceptions
# firstTime = datetime.now() - timedelta(days=6) - timedelta(hours=5) # First time to look for exceptions

# ----------------
## User Metrics Metrics
# ----------------
def numUniqueUsers(out):

    if 'USERID' in out:
        num = len(set(out['USERID']))
    else:
        num = 0
    return num

lastTime = datetime.utcnow() # last time to look for exceptions
firstTime = datetime.utcnow() - timedelta(days=13.9)

out = getUsersInInterval(firstTime, lastTime)


dimList = [{'Name': 'Source', 'Value': 'opsdx-web-logs-prod'},
           {'Name': 'Stack', 'Value': 'Prod'}]

# func = lambda x, y: numUniqueUsers(getUsersInInterval(x,y))
#
# metric, time = au.metricHistoryLoop(firstTime, lastTime, timedelta(hours=3),
#                                      func,
#                                      'NumUniqueUsrs','Count',dimList,
#                                      doPush=False)
