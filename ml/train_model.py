# train_model.py

import cPickle as pickle
import time

import dashan_core.src.ews_client.utility as util
import numpy as np
from dashan_app_sepsis.DashanInput import InputParamFactory
from dashan_app_sepsis import sepsis_functions as func
from dashan_core.src.ews_client.client import DataFrameFactory, DataFrame
from sklearn import cross_validation
from dashan_app_sepsis.offLine.InitialProcessing import getAdverseEventName

def splitTestAndTrainData(inputValues, data_frame_p, left_edge, right_edge):
    model_id = inputValues.model_id
    # ==============================================================
    ## Split train and test data
    # ==============================================================
    valid_enc_ids = np.unique(data_frame_p[:, 'enc_id'])
    valid_enc_ids = np.array([int(x) for x in valid_enc_ids])
    valid_enc_ids = valid_enc_ids[~np.isnan(valid_enc_ids)]

    rs = cross_validation.ShuffleSplit(len(valid_enc_ids), n_iter=1, test_size=inputValues.testFraction,
                                       random_state=0)
    train_ids = []
    test_ids = []
    for train_idx, test_idx in rs:
        train_ids = valid_enc_ids[train_idx]
        test_ids = valid_enc_ids[test_idx]

    is_train = np.in1d(data_frame_p[:, 'enc_id'], train_ids)
    is_test = np.in1d(data_frame_p[:, 'enc_id'], test_ids)

    np.savetxt('%s.is_train.txt' % model_id, is_train, delimiter='\n', fmt='%s')  # used in evaluate
    np.savetxt('%s.is_test.txt' % model_id, is_test, delimiter='\n', fmt='%s')  # used in evaluate and evaluate all

    # ==============================================================
    ## Split and normalize train and test data
    # ==============================================================

    # ## 5. Generate train and test sets
    # Remove all non-features in the future
    data_frame_pr = data_frame_p.remove_cols(['enc_id', 'tsp', 'cmi', 'septic_shock', 'severe_sepsis'])
    prColNamesList = data_frame_pr.colnames()

    data_train = data_frame_pr[is_train, :]
    data_test = data_frame_pr[is_test, :]

    print "start to standardize"
    std_scale, data_train_sd, data_test_sd = util.standardize(data_train, data_test, ColNamesList=prColNamesList,
                                                              handleMinutesSinceFeats=True)

    print "complete"

    print "save std_scale to file"
    with open('%s.std_scale.pkl' % model_id, 'wb') as output:
        # Pickle dictionary using protocol 0.
        pickle.dump(std_scale, output)

    print "save standardized datasets"
    data_train_sd = DataFrame(data_train_sd, data_frame_pr.colnames())
    data_train_sd.save(model_id + "_train_sd")  # predict and train
    data_test_sd = DataFrame(data_test_sd, data_frame_pr.colnames())
    data_test_sd.save(model_id + "_test_sd")  # predict



    left_train = left_edge[is_train]
    right_train = right_edge[is_train]
    return data_train_sd, left_train, right_train

def loadData(inputValues):
    name = getAdverseEventName(inputValues)

    factory = DataFrameFactory()

    data_frame_p = factory.load('%s_processed' % name)

    right_edge = np.loadtxt('%s.right_edge.txt' % name, delimiter='\n')
    left_edge = np.loadtxt('%s.left_edge.txt' % name, delimiter='\n')

    return data_frame_p, left_edge, right_edge

def train_model(inputPassedIn):
    inputFact = InputParamFactory()
    inputValues = inputFact.parseInput(inputPassedIn)

    data_id = inputValues.data_id
    model_id = inputValues.model_id

    print "----------------------------------------------------"
    print time.strftime("%H:%M:%S") + " Training {} on dataset {}".format(model_id, data_id)
    print "----------------------------------------------------"
    print time.strftime("%H:%M:%S") + " Begin Data Preprocessing"
    getAdverseEventName(inputPassedIn)
    data_frame_p, left_edge, right_edge = loadData(inputValues)

    data_train_sd, left_train, right_train  = splitTestAndTrainData(inputValues, data_frame_p, left_edge, right_edge)

    print time.strftime("%H:%M:%S") + " Data Preprocessing Complete"

    print time.strftime("%H:%M:%S") + " Training"

    # print 'shapes', data_train.shape, left.shape, right.shape
    print time.strftime("%H:%M:%S"), " generate mi_train_data"
    mi_train_data = np.asarray(np.concatenate( \
        (data_train_sd, np.vstack((left_train, right_train)).T), axis=1), dtype=float)
    print time.strftime("%H:%M:%S"), " complete mi_train_data"
    print "mi_train_data.shape", mi_train_data.shape

    print time.strftime("%H:%M:%S"), " start mi_surv_R"
    sub_idx = range(0, mi_train_data.shape[0], inputValues.downSampleFactor)
    sv_probs = func.mi_surv_R(mi_train_data[sub_idx])

    np.savetxt('%s.sv_probs.txt' % model_id, sv_probs, delimiter=',', fmt='%f')
    print time.strftime("%H:%M:%S"), " complete mi_surv_R"
    print "sv_probs.shape:", sv_probs.shape

    # interval censored rows
    interval_idx = np.logical_and(left_train < right_train, np.isfinite(right_train))

    ub = left_train[interval_idx]
    vb = right_train[interval_idx]
    NUM_OF_MIICD_ITER = inputValues.numberOfIterations

    imputed_event_time = func.impute_event_time(sv_probs, ub, vb, NUM_OF_MIICD_ITER)
    np.savetxt('%s.imputed_event_time.txt' % model_id, imputed_event_time, \
               delimiter=',', fmt='%f')

    miicd_event_time = np.copy(left_train)
    miicd_is_right_cens = np.isinf(right_train)
    print time.strftime("%H:%M:%S"), " start training "

    lambdaList = inputValues.lambda_list

    feature_names = data_train_sd.colname_lst

    models, weights_avg, num_iter = func.train_R_in_para(model_id, NUM_OF_MIICD_ITER, data_train_sd,
                                                         miicd_event_time, imputed_event_time,
                                                         miicd_is_right_cens, lambdaList,
                                                         interval_idx, feature_names, inputValues.ncpus,inputValues.featureConstraints)

    np.savetxt('%s.lambdasTrained.csv' % model_id, lambdaList, \
        delimiter=',', fmt='%f', comments='')

    weights_avg = np.true_divide(weights_avg, num_iter)
    np.savetxt('%s.feature_weights.csv' % model_id, weights_avg, \
        delimiter=',', fmt='%f', header=','.join(feature_names), comments='')

    with open('%s.mdl.pkl' % model_id, 'wb') as mdls_f:
        pickle.dump(models, mdls_f)

if __name__ == "__main__":
    import sys
    train_model(sys.argv[1])