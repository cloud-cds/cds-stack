# evaluate.py
"""
$1: frame_id 
$2: lambda
"""

import time
import numpy as np
import pandas as pd
try:
    import cPickle as pickle
except:
    import pickle
from dashan_core.src.ews_client.client import DataFrameFactory
from dashan_app_sepsis import sepsis_functions as func
from dashan_app_sepsis.DashanInput import InputParamFactory
from dashan_app_sepsis.offLine.InitialProcessing import getAdverseEventName
import sys
from sqlalchemy import create_engine, text
import preProcScore
#import dashan_app_sepsis.offLine.preProcScore
#dialect+driver://username:password@host:port/database



def packagePatientInfo(enc_id, adverseEventTimes, censorTimes):
    """return a pandas dataframe, with information about which patients were postives and not censored"""
    pop_df = pd.DataFrame({'enc_id' : enc_id,
                           'adverseEventTimes' : adverseEventTimes,
                           'censorTimes':  censorTimes})

    pop_df = pop_df.groupby('enc_id')['adverseEventTimes','censorTimes'].agg(lambda x: not np.isnan(x.iloc[0]))
    pop_df.columns = ['is_positive','is_intervention']

    num_pos_patients = np.sum(pop_df['is_positive'])
    num_neg_patients = np.sum(np.logical_and(np.logical_not(pop_df['is_positive']),
                                    np.logical_not(pop_df['is_intervention'])))
    idx_cens = np.logical_and(np.logical_not(pop_df['is_positive']),pop_df['is_intervention'])
    idx_not_cens = np.logical_not(idx_cens)

    pop_df = pop_df[idx_not_cens]

    print 'number of positive encounters:', num_pos_patients
    print 'number of negative encounters:', num_neg_patients
    print 'number of censored encounters:', np.sum(idx_cens)
    return pop_df

def computeDetailedMetrics(results_df,name):
    df = pd.DataFrame()

    # ML Population Metrics
    df['name'] = pd.Series(name)
    df['PopSize'] = pd.Series(results_df.shape[0])

    # ML Metrics
    resultsSeries = results_df['results']
    results_valCounts = resultsSeries.value_counts()
    theseCategories = resultsSeries.cat.categories.tolist()

    if 'TP' in theseCategories:
        TP = results_valCounts.loc['TP']
    else:
        TP = 0

    if 'FN' in theseCategories:
        FN = results_valCounts.loc['FN']
    else:
        FN = 0

    if 'FP' in theseCategories:
        FP = results_valCounts.loc['FP']  # This doesn't account for one encounter with multiple false postivies
    else:
        FP = 0

    if 'TN' in theseCategories:
        TN = results_valCounts.loc['TN']
    else:
        TN = 0

    df['TP'] = pd.Series(TP)
    df['FN'] = pd.Series(FN)
    df['FP'] = pd.Series(FP)
    df['TN'] = pd.Series(TN)

    sensitivity = np.true_divide(TP, TP + FN)
    specificity = np.true_divide(TN, TN + FP)
    PPV = np.true_divide(TP, TP + FP)
    NPV = np.true_divide(TN, TN + FN)

    df['sensitivity'] = pd.Series(sensitivity)
    df['specificity'] = pd.Series(specificity)
    df['PPV'] = pd.Series(PPV)
    df['NPV'] = pd.Series(NPV)

    # Hours before metric depend on their being True postivitives, and the event happening
    beforeEvent = results_df[results_df['results'] == 'TP']['maxMinutesFromDet2Event']
    beforeEvent = beforeEvent / 60.0

    try:
        df['hrsBeforeEvent'] = pd.Series(np.percentile(beforeEvent, 50))
    except:
        df['hrsBeforeEvent'] = pd.Series(np.nan)

    try:
        df['any_antibiotics_medianHrB4'] = pd.Series(
            np.percentile(results_df['any_antibiotics_first_rel'].dropna(), 50))
    except:
        df['any_antibiotics_medianHrB4'] = pd.Series(np.nan)

    try:
        df['any_antibiotics_order_medianHrB4'] = pd.Series(
            np.percentile(results_df['any_antibiotics_order_first_rel'].dropna(), 50))
    except:
        df['any_antibiotics_order_medianHrB4'] = pd.Series(np.nan)

    try:
        df['Culture_median_medianHrB4'] = pd.Series(np.percentile(results_df['firstculturesent_rel'].dropna(), 50))
    except:
        df['Culture_median_medianHrB4'] = pd.Series(np.nan)

    try:
        df['pressor_median_medianHrB4'] = pd.Series(np.percentile(results_df['any_pressor_first_rel'].dropna(), 50))
    except:
        df['pressor_median_medianHrB4'] = pd.Series(np.nan)

    try:
        df['organFail_medianHrB4'] = pd.Series(np.percentile(results_df['any_organ_failure_first_rel'].dropna(), 50))
    except:
        df['organFail_medianHrB4'] = pd.Series(np.nan)

    try:
        df['organFail_medianHrB4'] = pd.Series(np.percentile(results_df['any_organ_failure_first_rel'].dropna(), 50))
    except:
        df['organFail_medianHrB4'] = pd.Series(np.nan)

    try:
        df['death_medianHrB4'] = pd.Series(np.percentile(results_df['death_time_rel'].dropna(), 50))
    except:
        df['death_medianHrB4'] = pd.Series(np.nan)


    return df

def evaluate(inputPassedIn, lambda_input='file', targetSensitivity='file', thresholds='', SubTypes='file'):
    #===========================================================================
    # Parse inputs
    #===========================================================================
    inputFact = InputParamFactory()
    inputValues = inputFact.parseInput(inputPassedIn)

    data_id = inputValues.data_id
    model_id = inputValues.model_id
    score_id = inputValues.score_id
    eval_id = inputValues.eval_id

    print time.strftime("%H:%M:%S") + " Evaluating Data ID {}".format(data_id)
    print time.strftime("%H:%M:%S") + " Evaluating Model ID {}".format(model_id)
    print time.strftime("%H:%M:%S") + " Evaluating Score ID {}".format(score_id)
    print time.strftime("%H:%M:%S") + " To Generate {}".format(score_id)


    useSensitivity = False

    if targetSensitivity == 'file':
        targetSensitivity = inputValues.sensitivityTargets

    if thresholds == 'file':
        thresholds = inputValues.thresholds

    if (targetSensitivity == '') and (thresholds == ''):
        ValueError('A sensitivity target or threshold must be passed in, not neither ')
    elif(len(targetSensitivity)>0) and (len(thresholds) >0):
        print 'A sensitivity target and a threshold was passed in, using targetSensitivity'
        useSensitivity = True
    elif (len(targetSensitivity)>0) and (thresholds == ''):
        useSensitivity = True
    elif (targetSensitivity == '') and (len(thresholds)>0):
        pass
    else:
        ValueError('Unexpected has occured')

    if SubTypes == 'file':
        SubTypes = inputValues.subtypes
    else:
        SubTypes = []


    # ============================
    # clean lambdas
    # ============================
    lostLambdas = list()
    lambda_indexes = list()

    if lambda_input == 'file':
        lambda_input = list(inputValues.evaluationLambdas)

    for lambda_in in lambda_input:
        try:
            idx = list(inputValues.lambda_list).index(lambda_in)
            lambda_indexes.append(idx)
        except:
            lostLambdas.append(lambda_in)

    if len(lostLambdas) > 0:
        raise ValueError('The following lambdas passed in not found lambda list used in training' + str(lostLambdas))
    if len(lambda_indexes) == 0:
        raise ValueError('No Valid Lambdas')

    del(lostLambdas)

    lambda_clean = [inputValues.lambda_list[idx] for idx in lambda_indexes]

    del(lambda_input)
    print 'Evaluating for lambdas' + str(lambda_clean)
    # ============================
    # clean lambdas
    # ============================

    #===========================================================================
    # Load Data
    #===========================================================================
    print 'Loading Event Data: ' + (time.strftime("%H:%M:%S"))
    engine = create_engine('postgresql:///hcgh_1608') #@peter, use more of session
    eventTbl = pd.read_sql_query(text('select * from eventTable'), con=engine, index_col='enc_id')
    engine.dispose()

    eventList = eventTbl.columns.tolist()
    eventList = [i.encode('ascii', 'ignore') for i in eventList]

    for col in eventList:
        eventTbl[col] = pd.to_datetime(eventTbl[col])
    eventTbl.columns = eventList


    print 'Loading Data Train: ' + (time.strftime("%H:%M:%S"))

    train_score = np.loadtxt('%s.train_score.txt' % score_id, delimiter=',') #generated by predict
    test_score = np.loadtxt('%s.test_score.txt' % score_id, delimiter=',') #generated by predict

    # left = left_edge[is_train]
    # right = right_edge[is_train]
    # np.savetxt('%s.left.txt' % model_id, left, delimiter='\n', fmt='%f')  # used in evaluate
    # np.savetxt('%s.right.txt' % model_id, right, delimiter='\n', fmt='%f')  # used in evaluate



    is_train = np.genfromtxt('%s.is_train.txt' % model_id, delimiter='\n', dtype=bool)
    is_test = np.genfromtxt('%s.is_test.txt' % model_id, delimiter='\n', dtype=bool)

    dataAdversePairName = getAdverseEventName(inputValues)

    if data_id in ['nomFeats9','condFeats9']:
        print "Hack to use old Data"
        dataAdversePairName = model_id


    right_edge = np.loadtxt('%s.right_edge.txt' % dataAdversePairName, delimiter='\n')

    right = right_edge[is_train]

    valid_idx = right >= 0 # samples before shock onset

    factory = DataFrameFactory()
    data_frame_processed = factory.load(dataAdversePairName + "_processed")

    min_to_censor = np.loadtxt('%s.min_to_censorEvent.txt' % dataAdversePairName, delimiter='\n')
    min_to_adverseEvent = np.loadtxt('%s.min_to_adverseEvent.txt' % dataAdversePairName, delimiter='\n')  # I may be able to get positives by changing this value, these come from the jupyter notebook

    #===========================================================================
    # Get Population information
    #===========================================================================
    pop_df = pd.DataFrame(data=np.asmatrix(data_frame_processed[valid_idx]), columns=data_frame_processed.colnames())
    # need to add is positive, and is CMI
    #===========================================================================
    # evaluating Training
    #===========================================================================
    print 'Evaluating Training Set: ' + (time.strftime("%H:%M:%S"))

    train_info = data_frame_processed[is_train, ['enc_id', 'tsp']]
    train_info_valid = train_info[valid_idx]
    min_to_censor_train_valid = min_to_censor[is_train][valid_idx]
    min_to_adverseEvent_train_valid = min_to_adverseEvent[is_train][valid_idx]

    print "train_score.shape", train_score.shape
    print "valid_idx.shape", valid_idx.shape
    train_score_valid = train_score[valid_idx]
    print 'train_score_valid.shape:', train_score_valid.shape

    TrainPop_df = packagePatientInfo(train_info_valid[:,0], min_to_adverseEvent_train_valid, min_to_censor_train_valid)

    trainOutList = list()
    IdxOut = list()

    def makeScoreDf(score, data_info):
        score_df = pd.DataFrame({'enc_id': data_info[:, 0],
                                 'tsp': data_info[:, 1],
                                 'raw_score': score})
        return score_df

    for thisLambda, thisIdx in zip(lambda_clean, lambda_indexes):
        # try:
        #======================
        # Get ROC Information
        #======================
        thisScore_df = makeScoreDf(train_score_valid[:, thisIdx], train_info_valid)

        if thisIdx >= train_score_valid.shape[1]:
            # TODO understand why sometime i may be larger than the length
            print "Unexpected Index missing in Test, continuing"
            continue

        proc_score, thisPreProcInfo, valid = preProcScore.process(thisScore_df, PreProcName=inputValues.preProcStrat)

        if not valid:
            print 'Scores invalid for lambda ', thisLambda, ', no ROC returned'
            continue

        thisRocInfo = func.getROCInformation(proc_score, 'Train', thisLambda, TrainPop_df, thisPreProcInfo)

        print thisRocInfo
        trainOutList.append(thisRocInfo)
        IdxOut.append(thisIdx)

        with open('%s.trainEvalOut.pkl' % model_id, 'wb') as f:
            pickle.dump(trainOutList, f)

        #======================
        # Get Specific Threshold Info
        #======================
        if useSensitivity:
            thresholds = [thisRocInfo.selectThreshold(sens) for sens in targetSensitivity]
        else:
            targetSensitivity = [thisRocInfo.predictSensitivity(thresh) for thresh in thresholds]

        for threshold, psens in zip(thresholds, targetSensitivity):
            # try:
            print 'Threshold: ' + str(threshold) + ', Predicted Sensitivity: ' + str(psens)
            thisThreshInfo = func.getThresholdInformation(proc_score, TrainPop_df, eventTbl, threshold, min_to_adverseEvent_train_valid)
            print thisThreshInfo
            thisRocInfo.thresholdInfoList.append(thisThreshInfo)
        #     except:
        #         print 'Threshold: ' + str(threshold) + ', Predicted Sensitivity: ' + str(psens) + ' Failed \n'
        # except:
        #     print '==========================================================================='
        #     print 'lambda ' + str(thisLambda) + ' with index ' + str(thisIdx) +' failed Error:'
        #     print sys.exc_info()[0]

    with open('%s.trainEvalOut.pkl' % model_id, 'wb') as f:
        pickle.dump(trainOutList, f)

    #===========================================================================
    # evaluating Test Set
    #===========================================================================
    print 'Evaluating Test Set: ' + (time.strftime("%H:%M:%S"))

    right_test = right_edge[is_test]
    valid_idx_test = right_test > 0
    test_score_valid = test_score[valid_idx_test]
    min_to_censor_test_valid = min_to_censor[is_test][valid_idx_test]
    min_to_adverse_test_valid = min_to_adverseEvent[is_test][valid_idx_test]

    test_info = data_frame_processed[is_test, ['enc_id','tsp']]
    test_info_valid = test_info[valid_idx_test]

    TestPop_df = packagePatientInfo(test_info_valid[:,0], min_to_adverse_test_valid, min_to_censor_test_valid)

    testOutList = list()
    for trainOut, thisIdx in zip(trainOutList, IdxOut):
        # try:

        if thisIdx >= test_score_valid.shape[1]:
            # TODO understand why sometime i may be larger than the length
            print "Unexpected Index missing in Test, continuing"
            continue

        thisLambda = trainOut.lambdaThis
        thisPreProcInfo = trainOut.preProcInfo

        thisScore_df = makeScoreDf(test_score_valid[:, thisIdx], test_info_valid)

        proc_score, thisPreProcInfo, valid = preProcScore.process(thisScore_df, thisPreProcInfo=thisPreProcInfo)

        thisRocInfo = func.getROCInformation(proc_score, 'Test', thisLambda, TestPop_df, thisPreProcInfo)

        print thisRocInfo
        testOutList.append(thisRocInfo)
        with open('%s.testEvalOut.pkl' % model_id, 'wb') as f:
            pickle.dump(testOutList, f)

        for trainThreshInfo in trainOut.thresholdInfoList:
            thisThresh = trainThreshInfo.threshold
            predSens   = trainThreshInfo.sensitivity
            print 'Threshold: ' + str(thisThresh) + ', Predicted Sensitivity: ' + str(predSens)

            thisThreshInfo = func.getThresholdInformation(proc_score, TestPop_df, eventTbl, thisThresh, min_to_adverse_test_valid)

            print thisThreshInfo
            thisRocInfo.thresholdInfoList.append(thisThreshInfo)


    with open('%s.testEvalOut.pkl' % model_id, 'wb') as f:
        pickle.dump(testOutList, f)

    # ===================================================================
    # Compute High Level Metics
    # ===================================================================
    train_AUCs = [thisRocInfo.AUC for thisRocInfo in trainOutList]

    bestTrainROC_idx = np.argmax(train_AUCs)
    bestRoc = testOutList[bestTrainROC_idx]
    print 'Test of Highest Preforming Train'
    print bestRoc

    nomResults = bestRoc.thresholdInfoList[0].results_df

    metricsTable = computeDetailedMetrics(nomResults, 'nominal')
    print metricsTable
    # ===================================================================
    # Additional Analysis Code
    # ===================================================================
    if len(SubTypes) > 0:
        nomResults['enc_id'] = nomResults.index

        print time.strftime("%H:%M:%S") + " Downloading Subtypes"
        engine = create_engine('postgresql:///hcgh_1608')  # peter, use more of session

        for group, specificList in SubTypes.iteritems():
            groupTbl = pd.DataFrame()
            for specific in specificList:
                query = text("select * from {}_enc_ids;".format(specific))
                specific_tbl = pd.read_sql_query(query, con=engine)
                groupTbl = groupTbl.append(specific_tbl)
            groupTbl.drop_duplicates(inplace=True)

            thisSubtypeResTbl = nomResults[nomResults['enc_id'].isin(list(groupTbl['enc_id']))]
            if thisSubtypeResTbl.empty:
                print "No data for subtype {}".format(group)
            else:
                theseMetrics = computeDetailedMetrics(thisSubtypeResTbl, group)
                print theseMetrics
                metricsTable = metricsTable.append(theseMetrics)

        print 'Final Metrics Table'
        print metricsTable
        metricsTable.to_csv('{}.highLevelMetricsTable.csv'.format(eval_id))
        np.savetxt('%s_evalComplete.txt' % eval_id, [], delimiter=',', fmt='%f')

if __name__ == "__main__":
    evaluate(sys.argv[1])

