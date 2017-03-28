from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import pickle as pickle
import time
import boto3
import devops.awsMetrics.utilities as au

#----------------
## Utilities
#----------------
def filterLogList(logList):

    strFilt = ''
    sucesss = True


    try:
        isTraceback = np.array([line.find('Traceback') != -1 for line in logList])
        traceBackLine = np.where(isTraceback)[0]
        isFileLine = np.array([line.find('File') != -1 for line in logList])
        fileLines = np.where(isFileLine)[0]
    except ValueError:
        return '','',False

    strFilt = [line for line,isfile in zip(logList,isFileLine) if isfile]
    strFilt = ''.join(strFilt)

    lineBuffer = 10

    strLong  = [line for idx, line in enumerate(logList) if idx >= (min(fileLines) -lineBuffer) and idx <= (max(fileLines)+lineBuffer)]
    strLong = ''.join(strLong)

    return strFilt, strLong, sucesss


def getErrorsInInterval(firstExceptionTime, lastExceptionTime,logWindowSize=timedelta(seconds=5)):

    client = boto3.client('logs')

    lastExceptionTime = au.time2epoch(lastExceptionTime)
    firstExceptionTime = au.time2epoch(firstExceptionTime)

    resList = au.getfiltLogEvent(firstExceptionTime, lastExceptionTime, client,
                                    'opsdx-dev-k8s-logs',['kubernetes/default/trews/etl'],'Traceback')

    exceptions = list()
    for res in resList:
        exceptions += res['events']

    numExceptions = len(exceptions)
    print("{} exceptions found".format(numExceptions))

    # with open('exceptionList.pkl', 'wb') as f:
    #     pickle.dump(exceptions, f)

    #----------------
    ## Exception Loop
    #----------------

    print(time.strftime("%H:%M:%S") + " Begining exception Processing")
    # for iExcept in range(1,3):
    uniqueExceptions = list()
    exceptionLog = list()
    numTimes = list()
    logsWithNoTraceback = list()
    noTracebackCount = 0

    for iExcept in range(0, numExceptions):

        exceptTsp = au.epoch2time(exceptions[iExcept]['timestamp'])

        logStart = au.time2epoch(exceptTsp)
        logEnd = au.time2epoch(exceptTsp + logWindowSize)

        stackList = au.getLogs(logStart, logEnd, client,
                             'opsdx-dev-k8s-logs', 'kubernetes/default/trews/etl')

        logList  = []
        for stack in stackList:
            logList = [json.loads(event['message'])['log'] for event in stack['events']]


        strFilt, strLong, sucesss = filterLogList(logList)

        if not sucesss:
            print("No Traceback found for index {}, at time {}, {} lines of log returned".format(iExcept, exceptTsp, len(logList)))
            noTracebackCount += 1
            logsWithNoTraceback.append(''.join(logList))


        try:
            thisIdx = uniqueExceptions.index(strFilt)
            numTimes[thisIdx] += 1
        except ValueError:
            uniqueExceptions.append(strFilt)
            exceptionLog.append(strLong)
            numTimes.append(1)


    print('{} tracebacks lost'.format(noTracebackCount))
    print('{} unique exceptions found'.format(len(uniqueExceptions)))


    out = pd.DataFrame({'numOccurances':numTimes,
                        'Location':uniqueExceptions,
                        'LogSelection':exceptionLog})
    return out

# ----------------
## Error Metrics
# ----------------
def uniqueErrors(out):
    return out.shape[0]

def numErrors(out):
    return sum(out['numOccurances'])

lastExceptionTime = datetime.utcnow() # last time to look for exceptions
firstExceptionTime = datetime.utcnow() - timedelta(days=13.9) # First time to look for exceptions

# out = getErrorsInInterval(firstExceptionTime,lastExceptionTime)

dimList = [{'Name': 'Source', 'Value': 'opsdx-dev-k8s-logs'},
           {'Name': 'Stack', 'Value': 'Prod'}]

func = lambda x, y: numErrors(getErrorsInInterval(x,y))

au.metricHistoryLoop(firstExceptionTime, lastExceptionTime, timedelta(hours=3),
                     func,
                     'NumTracebacks','Count',dimList)

# out.to_csv('etlErrors_320.csv')

