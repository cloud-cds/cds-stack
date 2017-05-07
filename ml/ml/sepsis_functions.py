# sepsis_functions.py
import rpy2.robjects as robjects
from rpy2.robjects import numpy2ri
from rpy2.robjects.packages import importr

try:
    import cPickle as pickle
except:
    import pickle
import time
import csv
import numpy as np
import pandas as pd
import ml.evaluationDataset as eData
from sqlalchemy import create_engine, text
import logging
import pytz
from dateutil import tz

#dialect+driver://username:password@host:port/database
engine = create_engine('postgresql:///hcgh_1608')

def combine_diag_and_hist_features(data_frame):
    # combine hist feature with diag features
    feature_list = [f for f in data_frame.get_feature_names() \
        if f.endswith('_diag') or f.endswith('_hist')]
    for feature in feature_list:
        data_frame.replace_none(feature, False)
    for feature in feature_list:
        if feature.endswith('_hist'):
            diag = feature[:-5] + '_diag'
            if diag in feature_list:
                # not exist
                data_frame[:,diag] = np.logical_or(data_frame[:,diag],
                                                   data_frame[:,feature])
            else:
                data_frame.rename_col(feature, diag)
    data_frame = data_frame.remove_cols([feature for feature in feature_list \
        if feature.endswith('_hist')])
    return data_frame

def combine_dhp_features(data_frame):
    # combine diag, hist, and prob features
    feature_list = [f for f in data_frame.get_feature_names() \
        if f.endswith('_diag') or f.endswith('_hist') or f.endswith('_prob')]
    for feature in feature_list:
        data_frame.replace_none(feature, False)
    for feature in feature_list:
        if feature.endswith('_hist') or feature.endswith('_prob'):
            diag = feature[:-5] + '_diag'
            if diag in feature_list:
                # not exist
                data_frame[:,diag] = np.logical_or(data_frame[:,diag],
                                                   data_frame[:,feature])
            else:
                data_frame.rename_col(feature, diag)
    data_frame = data_frame.remove_cols([feature for feature in feature_list \
        if feature.endswith('_hist') or feature.endswith('_prob')])
    return data_frame

def minutes_to_feature(data_frame, fid, allowNegative = False):
    """
    update on 2/3/2016
    input colunmns are enc_id, tsp, fid
    For encounters has never had fid, all rows are NAN
    For encounters has fid,
        return NAN for rows after the first fid;
    otherwise,
        return min(t) - tsp,
    here min(t) stands for the first tsp when fid is True
    """
    nrows = data_frame.nrows()
    min_to_fid = np.empty(nrows)
    min_to_fid[:] = np.NAN
    cur_enc_id = -1
    fid_dt = None
    for i in range(nrows):
        if data_frame[i,'enc_id'] != cur_enc_id:
            # new encounter
            if cur_enc_id >=0 and fid_dt is not None:
                # calculate min_to_fid
                for j in range(start_i, i):
                    dt = fid_dt - data_frame[j, 'tsp']
                    if allowNegative:
                        min_to_fid[j] = dt.total_seconds() / 60.0
                    else:
                        if fid_dt > data_frame[j, 'tsp']:
                            min_to_fid[j] = dt.total_seconds()/60.0
                        else:
                            min_to_fid[j] = np.nan
            start_i = i
            cur_enc_id = data_frame[i,'enc_id']
            fid_dt = None
        if fid_dt is None and data_frame[i, fid] == True:
            fid_dt = data_frame[i,'tsp']
    if cur_enc_id >=0 and fid_dt is not None:
        # calculate min_to_fid
        for j in range(start_i, nrows):
            dt = fid_dt - data_frame[j, 'tsp']

            if allowNegative:
                min_to_fid[j] = dt.total_seconds() / 60.0
            else:
                if fid_dt > data_frame[j, 'tsp']:
                    min_to_fid[j] = dt.total_seconds()/60.0
                else:
                    min_to_fid[j] = np.nan

    return min_to_fid

def minutes_to_cmi(data_frame):
    """
    update on 2/1/2016
    input colunmns are enc_id, tsp, cmi
    For encounters has never had cmi, all rows are NAN
    For encounters has cmi, return NAN for rows after the first cmi; otherwise,
        return min(t) - tsp,
    here min(t) stands for the first tsp when cmi is True
    """
    nrows = data_frame.nrows()
    min_to_cmi = np.empty(nrows)
    min_to_cmi[:] = np.NAN
    cur_enc_id = -1
    cmi_dt = None
    for i in range(nrows):
        if data_frame[i,'enc_id'] != cur_enc_id:
            # new encounter
            if cur_enc_id >=0 and cmi_dt is not None:
                # calculate min_to_cmi
                for j in range(start_i, i):
                    dt = cmi_dt - data_frame[j, 'tsp']
                    if cmi_dt > data_frame[j, 'tsp']:
                        min_to_cmi[j] = dt.total_seconds()/60.0
                    else:
                        min_to_cmi[j] = np.nan
            start_i = i
            cur_enc_id = data_frame[i,'enc_id']
            cmi_dt = None
        if cmi_dt is None and data_frame[i, 'cmi'] == True:
            cmi_dt = data_frame[i,'tsp']
    if cur_enc_id >=0 and cmi_dt is not None:
        # calculate min_to_cmi
        for j in range(start_i, nrows):
            dt = cmi_dt - data_frame[j, 'tsp']
            if cmi_dt > data_frame[j, 'tsp']:
                min_to_cmi[j] = dt.total_seconds()/60.0
            else:
                min_to_cmi[j] = np.nan
    return min_to_cmi

def minutes_to_shock_onset(data_frame):
    """
    input colunmns are enc_id, tsp, septic_shock
    For encounters has never had septic_shock, all rows are NAN
    For encounters has septic_shock, return min(t) - tsp,
    here min(t) stands for the first tsp when septic_shock is True
    """
    nrows = data_frame.nrows()
    min_to_shock = np.empty(nrows)
    min_to_shock[:] = np.NAN
    cur_enc_id = -1
    septic_shock_dt = None
    for i in range(nrows):
        if data_frame[i, 'enc_id'] != cur_enc_id:
            # new encounter
            if cur_enc_id >=0 and septic_shock_dt is not None:
                # calculate min_to_shock
                for j in range(start_i, i):
                    dt = septic_shock_dt - data_frame[j, 'tsp']
                    min_to_shock[j] = dt.total_seconds()/60.0
            start_i = i
            cur_enc_id = data_frame[i, 'enc_id']
            septic_shock_dt = None
        if septic_shock_dt is None and \
            data_frame[i, 'septic_shock'] == True:
            septic_shock_dt = data_frame[i, 'tsp']
    if cur_enc_id >=0 and septic_shock_dt is not None:
        # calculate min_to_shock
        for j in range(start_i, nrows):
            dt = septic_shock_dt - data_frame[j, 'tsp']
            min_to_shock[j] = dt.total_seconds()/60.0

    return min_to_shock

def minutes_to_censor(min_to_cmi, min_to_shock):
    """
    return:
    @min_to_event: the tsp for cmi or shock
    @is_event_cmi: is current row the tsp for cmi
    """
    n = len(min_to_cmi)
    min_to_event = np.empty(n)
    is_event_cmi = np.empty(n)
    cmi_isnan_idx = np.isnan(min_to_cmi)
    shock_isnan_idx = np.isnan(min_to_shock)

    both_not_nan_idx = ~ cmi_isnan_idx & ~ shock_isnan_idx

    # min_to_event[both_not_nan_idx] = np.min(min_to_cmi[both_not_nan_idx],
    #                                         min_to_shock[both_not_nan_idx])
    cmi_before_shock_idx = both_not_nan_idx & (min_to_cmi < min_to_shock)
    is_event_cmi = (~ cmi_isnan_idx & shock_isnan_idx) | cmi_before_shock_idx
    min_to_event[is_event_cmi] = min_to_cmi[is_event_cmi]
    min_to_event[~is_event_cmi] = min_to_shock[~is_event_cmi]

    return min_to_event


def generate_edges(min_to_cens, min_to_shock, min_to_max_hosp_time):
    # update: 2/3/2016
    """
    R code:
    # Left edge is (1) time of CMI (2) time of shock if no CMI (3) time of discharge if no shock or CMI
    left_edge <- apply(cbind( minToCens,minToShock), FUN= function(x) min(x, na.rm=T), MARGIN=1)
    left_edge[is.na(minToCens)] = max_hospital_time[is.na(minToCens)]
    # Right edge is (1) time of shock (2) NA if no shock
    right_edge=minToShock
    right_edge[is.na(right_edge)]<-Inf
    """
    # R: left_edge <- apply(cbind( minToCens,minToShock), FUN= function(x) min(x, na.rm=T), MARGIN=1)
    left_edge = np.empty(len(min_to_cens))
    right_edge = np.empty(len(min_to_cens))
    is_nan_idx = np.isnan(min_to_cens)
    left_edge[~is_nan_idx] = min_to_cens[~is_nan_idx]
    # R: left_edge[is.na(minToCens)] = max_hospital_time[is.na(minToCens)]
    left_edge[is_nan_idx] = min_to_max_hosp_time[is_nan_idx]
    # R: right_edge=minToShock
    # R: right_edge[is.na(right_edge)]<-Inf
    right_edge = np.copy(min_to_shock)
    is_nan_idx = np.isnan(right_edge)
    right_edge[is_nan_idx] = np.inf
    return (left_edge, right_edge)

def minutes_to_maximum_hospital_time(data_frame):
    """
    return minutes to the last tsp for each enc_id
    XXX: should we return the minutes to transfer or discharge time?
    """
    enc_id = -1
    start_i = -1
    nrows = data_frame.shape[0]
    min_to_max_hosp_time = np.empty(nrows)
    for i, row in enumerate(data_frame):
        if enc_id != row[0]:
            # new encounter
            # calculate min_to_max_hosp_time for current enc_id
            if enc_id >= 0:
                for j in range(start_i, i):
                    dt = data_frame[i-1,1] - data_frame[j,1]
                    min_to_max_hosp_time[j] = dt.total_seconds()/60.0
            # update current status
            enc_id = row[0]
            start_i = i
    for j in range(start_i, nrows):
        dt = data_frame[nrows-1,1] - data_frame[j,1]
        min_to_max_hosp_time[j] = dt.total_seconds()/60.0
    return min_to_max_hosp_time

def mi_surv_R(train_data):
    numpy2ri.activate()
    miicd = importr('MIICD')
    ncols = train_data.shape[1]
    print("mi_surv_R train_data shape: " + str(train_data.shape) )
    mi_train_data = robjects.r['as.data.frame'](train_data)
    mi_train_data.colnames[ncols-2:ncols] = robjects.StrVector(['left', 'right'])
    # mi_train_data = robjects.r["subset"](mi_train_data, select=robjects.r('c("left", "right")'))
    robjects.globalenv['mi_train_data'] = mi_train_data
    print("mi_surv_R mi_train_data shape: " + str(robjects.r("dim(mi_train_data)")) )
    # model_survival = miicd.MI_surv(k=5, m = 5, data = bcos)
    model_survival = robjects.r("MI.surv(m = 5, data = mi_train_data)")
    # print(model_survival)
    # use the first two column: time, surv
    sv_probs = np.array(model_survival[0][0:2]).T
    numpy2ri.deactivate()
    return sv_probs

def impute_event_time(surv_probs, ub, vb, niters):
    num_input_events = len(ub)
    imputed_event_time = np.empty((num_input_events, niters))
    print("enter function impute_event_time")
    for event_i in range(num_input_events):
        # print "interval:", ub[event_i], vb[event_i]
        sample_probs = \
            surv_probs[np.logical_and(surv_probs[:,0]>=ub[event_i],
                                      surv_probs[:,0]<=vb[event_i])] #surv pops in the interval
        # print "sample_probs in the interval:", sample_probs
        if sample_probs.shape[0] < 2:
            sample_probs = surv_probs[surv_probs[:,0]>=ub[event_i]]
            sampled_time = [sample_probs[0,0]]*niters
        else:
            # normalize probabilities
            prob_sum = sum(sample_probs[:,1])
            sample_probs[:,1] = \
                np.true_divide(sample_probs[:,1], prob_sum)
            #print 'choices:', sample_probs[:,0]
            sampled_time = np.random.choice(sample_probs[:,0], niters,\
                replace=True, p=sample_probs[:,1])
        # print sampled_time
        imputed_event_time[event_i, :] = sampled_time
    return imputed_event_time

def train_R(frame_id, iter, data_as_frame, curr_event_time, miicd_is_right_cens,
            lambda_list):
    numpy2ri.activate()
    gl = importr('glmnet')
    surv = importr('survival')
    # remove time points that are past the event
    # for that particular set of imputed values
    # For interval censored data, the status indicator
    # is 0=right censored, 1=event at time
    surv_times = surv.Surv(curr_event_time, 1-miicd_is_right_cens, type='right')
    np.savetxt('%s.surv_times.%s.txt' % (frame_id, iter), surv_times, \
        delimiter=',', fmt='%f')
    # print 'surv_times.shape:', surv_times.shape
    # surv_times_mat = robjects.r.matrix(surv_times, ncol=2)
    # surv_times_mat.colnames = robjects.StrVector(['time', 'status'])
    robjects.globalenv['surv_times_all'] = surv_times
    robjects.r("not_neg_times <- which(!(surv_times_all[,'time'] <=0))")

    robjects.globalenv['x'] = data_as_frame
    robjects.globalenv['y'] = surv_times
    robjects.globalenv['lambda_list'] = robjects.FloatVector(lambda_list)
    robjects.r("mdl = glmnet(x[not_neg_times,], y[not_neg_times,], family='cox', lambda=lambda_list, alpha=1)")
    feature_weights = robjects.r("as.matrix(mdl$beta)")
    print(feature_weights)
    numpy2ri.deactivate()
    return robjects.globalenv['mdl']

def train_R_in_loop(frame_id, num_iter, data_as_frame, miicd_event_time,
                    imputed_event_time, miicd_is_right_cens, lambda_list,
                    feature_names, interval_idx):
    numpy2ri.activate()
    print(time.strftime('%X') + " import library and globalenv")
    gl = importr('glmnet')
    surv = importr('survival')
    robjects.globalenv['miicd_evTime'] = miicd_event_time
    robjects.globalenv['imputed_event_time'] = imputed_event_time
    # print robjects.r('imputed_event_time[,0]')
    robjects.globalenv['miicd_isCens'] = miicd_is_right_cens
    robjects.globalenv['data_as_frame'] = data_as_frame
    robjects.globalenv['lambda_list'] = lambda_list
    robjects.globalenv['miicd_isRand'] = interval_idx
    models = []
    for i in range(num_iter):
        print(time.strftime('%X') + " start iteration " + i)
        r_code = '''
            cntI = %s
            curr_evTime = miicd_evTime
            curr_evTime[which(miicd_isRand)] = imputed_event_time[,cntI]
            surv_times_all <- Surv(curr_evTime, 1-miicd_isCens)
            not_neg_times <- which(!(surv_times_all[,'time'] <=0))
            data_as_frame_tmp <- as.matrix(data_as_frame[not_neg_times,])
            surv_times <- surv_times_all[not_neg_times,]
            mdl <- glmnet(data_as_frame_tmp, surv_times, family='cox', lambda=lambda_list, alpha=1)
        ''' % (i+1)
        print("r code: " + r_code)
        robjects.r(r_code)
        print(time.strftime('%X') + "complete iteration " + i)
        mdl = robjects.globalenv['mdl']
        models.append(mdl)
        save_feature_weights(frame_id, i, mdl, feature_names)
    numpy2ri.deactivate()
    return models

def resolveFeatureConstraints(data_as_frame, feature_names, featureConstraints):
    print("Applying Feature Constraints")

    if all in featureConstraints:
        minConArray = featureConstraints['all'][0]*np.ones((len(feature_names)))
        maxConArray = featureConstraints['all'][1]*np.ones((len(feature_names)))
    else:
        minConArray = np.ones((len(feature_names)))*np.inf*-1.0
        maxConArray = np.ones((len(feature_names)))*np.inf

        for key, value in featureConstraints.items():
            feat_idx = feature_names.index(key)
            minConArray[feat_idx] = value[0]
            maxConArray[feat_idx] = value[1]


    return minConArray,maxConArray


def train_R_in_para(frame_id, num_iter, data_as_frame, miicd_event_time,
                    imputed_event_time, miicd_is_right_cens, lambda_list,
                    interval_idx, feature_names, n_cpu,featureConstraints):


    numpy2ri.activate()
    print(time.strftime('%X') + " import library and globalenv")
    gl = importr('glmnet')
    surv = importr('survival')
    importr('foreach')
    importr('doMC')
    robjects.globalenv['miicd_evTime'] = miicd_event_time
    robjects.globalenv['imputed_event_time'] = imputed_event_time
    # print robjects.r('imputed_event_time[,0]')
    robjects.globalenv['miicd_isCens'] = miicd_is_right_cens
    robjects.globalenv['data_as_frame'] = data_as_frame
    robjects.globalenv['lambda_list'] = lambda_list
    robjects.globalenv['miicd_isRand'] = interval_idx
    print("null values in data frame:")
    print(robjects.r("sum(is.nan(data_as_frame))"))
    print(robjects.r("sum(is.na(data_as_frame))"))
    print(robjects.r("sum(!is.numeric(data_as_frame))"))
    minConArray, maxConArray = resolveFeatureConstraints(data_as_frame, feature_names, featureConstraints)
    robjects.globalenv['minCon'] = minConArray
    robjects.globalenv['maxCon'] = maxConArray
    models = []

    # robjects.r('''
    #     curr_evTime = miicd_evTime
    #     curr_evTime[which(miicd_isRand)] = imputed_event_time[,1]
    #     surv_times_all <- Surv(curr_evTime, 1-miicd_isCens)
    #     not_neg_times <- which(!(surv_times_all[,'time'] <=0))
    #     data_as_frame_tmp <- as.matrix(data_as_frame[not_neg_times,])
    #     surv_times <- surv_times_all[not_neg_times,]
    #     save(data_as_frame_tmp,file="data_as_frame_tmp.Rda")
    #     save(surv_times,file="surv_times.Rda")
    #     save(minCon,file="minCon.Rda")
    #     save(maxCon,file="maxCon.Rda")
    #     save(lambda_list,file="lambda_list.Rda")
    #     mdl <- glmnet(data_as_frame_tmp, surv_times, family='cox', lambda=lambda_list, alpha=1,lower.limits=minCon,upper.limits=maxCon)
    #     cat(format(Sys.time(), "%%X"), "complete iteration:", 1, "\n")
    #     '''
    #     )

    r_code = '''
    registerDoMC(%s)
    models <- foreach(cntI=1:%s) %%dopar%% {
        curr_evTime = miicd_evTime
        curr_evTime[which(miicd_isRand)] = imputed_event_time[,cntI]
        surv_times_all <- Surv(curr_evTime, 1-miicd_isCens)
        not_neg_times <- which(!(surv_times_all[,'time'] <=0))
        data_as_frame_tmp <- as.matrix(data_as_frame[not_neg_times,])
        surv_times <- surv_times_all[not_neg_times,]
        mdl <- glmnet(data_as_frame_tmp, surv_times, family='cox', lambda=lambda_list, alpha=1,lower.limits=minCon,upper.limits=maxCon)
        cat(format(Sys.time(), "%%X"), "complete iteration:", cntI, "\n")
        mdl
    }
    ''' % (n_cpu, num_iter)
    print("r code: " + r_code)
    robjects.r(r_code)
    print("models:")
    print(robjects.globalenv['models'])
    models = robjects.r('models')
    weights_avg = None
    # @peter the R is doing some weird stuff guys, it sometimes returns different sized arrays
    for i in range(num_iter):
        print("save feature weight iter {}".format(i))
        weights = robjects.r('as.matrix(models[[%s]]$beta)' % (i+1))
        np.savetxt('%s.feature_weights.%s.csv' % (frame_id, i), weights.T, \
            delimiter=',', fmt='%f', header=','.join(feature_names), comments='')

        weightArray = weights.T

        if weightArray.shape[0] == 17:
            print("Odd behavior from R lib, assuming default behavior !!!! ")
            weightArray = np.concatenate([weightArray, np.zeros([1, weights.T.shape[1]])], axis=0)

        if weights_avg is None:
            weights_avg = weightArray
        else:
            weights_avg += weightArray

    numpy2ri.deactivate()
    return models, weights_avg, num_iter



def predict_R_in_parallel(models, test_data, n_cpu):
    numpy2ri.activate()
    importr('glmnet')
    importr('foreach')
    importr('doMC')
    robjects.globalenv['models'] = models
    robjects.globalenv['test_data'] = test_data
    print("predicting for testing set")
    print("testing set shape " + str(test_data.shape))

    r_code = '''
    num_iter = length(models)
    cat(format(Sys.time(), "%%X"), "num_iter:", num_iter, "\n")
    registerDoMC(%s)
    scores <- foreach(cntI=1:num_iter, .combine='+') %%dopar%% {
        cat(format(Sys.time(), "%%X"), "start iteration:", cntI, "\n")
        score = predict(models[[cntI]], test_data)
        cat(format(Sys.time(), "%%X"), "complete iteration:", cntI, "\n")
        score
    }
    scores = scores/num_iter
    ''' % n_cpu
    robjects.r(r_code)
    print("complete prediction")
    numpy2ri.deactivate()
    r_scores = robjects.r('scores')

    # looks like this works. All predict is doing is a dot product
    # featWeights = pd.read_csv('test.feature_weights.csv')
    # ilambda = 7
    # thisRScores = r_scores[:, ilambda]
    # thisWeights = np.asarray(featWeights.iloc[ilambda, :])
    # sum(np.abs(np.dot(test_data, thisWeights) - thisRScores))

    return r_scores

def save_feature_weights(frame_id, iter, model, colnames):
    numpy2ri.activate()
    weights = robjects.r('as.matrix(mdl$beta)')
    numpy2ri.deactivate()
    weights = np.asarray(weights).T
    feature_names = ','.join(colnames)
    np.savetxt('%s.feature_weights.%s.csv' % (frame_id, iter), weights, \
        delimiter=',', fmt='%f', header=feature_names, comments='')


def predict_R(mdl, test_data):
    numpy2ri.activate()
    importr('glmnet')
    robjects.globalenv['mdl'] = mdl
    robjects.globalenv['test_data'] = test_data
    print("predicting for testing set")
    print("testing set shape " + test_data.shape)
    result = robjects.r("predict(mdl, test_data)")
    print("result shape " + result.shape)
    print(result)
    numpy2ri.deactivate()
    return result

def roc_ci_R(is_positive, max_score):
    numpy2ri.activate()
    proc = importr('pROC')
    roc_res_list = []
    ci_auc_res_list = []
    if len(max_score.shape) == 1:
        num_lambda = 1
        # print len(is_positive), len(max_score)
        roc_res = proc.roc(robjects.IntVector(is_positive),
                           robjects.FloatVector(max_score))
        ci_auc_res = proc.ci(roc_res)
        roc_res_list.append(roc_res)
        ci_auc_res_list.append(ci_auc_res)
    else:
        num_lambda = max_score.shape[1]
        for i in range(num_lambda):
            print(len(is_positive) + ' ' + len(max_score[:,i]))
            roc_res = proc.roc(robjects.IntVector(is_positive),
                               robjects.FloatVector(max_score[:,i]))
            ci_auc_res = proc.ci(roc_res)
            roc_res_list.append(roc_res)
            ci_auc_res_list.append(ci_auc_res)
    numpy2ri.deactivate()
    return (roc_res_list, ci_auc_res_list)

def get_roc(roc_res, val):
    numpy2ri.activate()
    proc = importr('pROC')
    robjects.globalenv['roc'] = roc_res
    value = robjects.r('roc$%s' % val)
    numpy2ri.deactivate()
    return value

def getDetailedDetectionInformationOnPatient(wasDetected, times):
    # this accepts information from one patient, and computes detection related information
    # assumes that the input is np arrays
    wasDetected = np.asarray(wasDetected)
    times = np.asarray(times)

    # pad
    wasDetected = np.insert(wasDetected, 0, False, axis=0)
    wasDetected = np.insert(wasDetected, len(wasDetected), False, axis=0)

    # find edges
    isDetLeftEdge = np.diff(wasDetected.astype(int)) > 0.1
    isDetRightEdgeOrig = np.diff(wasDetected.astype(int)) < -0.1

    isDetLeftEdge = np.delete(isDetLeftEdge, -1)

    if isDetRightEdgeOrig[-1] == True:
        # can lead to detections of duration zero,
        # but this seems a sensible case to do that
        isDetRightEdge = np.delete(isDetRightEdgeOrig, -1)
        isDetRightEdge[-1] = True
    else:
        isDetRightEdge = np.delete(isDetRightEdgeOrig, -1)

    # get edge information
    leftEdges = times[isDetLeftEdge]
    rightEdges = times[isDetRightEdge]
    lengths = np.true_divide((rightEdges - leftEdges), np.timedelta64(1, 'h'))

    if not(list(lengths)):
        numDets = 0.0
        shortestDet = 0.0
        longestDet = 0.0
    else:
        numDets = len(lengths)
        shortestDet = np.min(lengths)
        longestDet = np.max(lengths)
    return numDets, shortestDet, longestDet

def getDetailedDetectionInformation(det_df):
    enc_ids = np.unique(det_df['enc_id'])
    numDets = list()
    shortestDet = list()
    longestDet = list()

    for enc_id in enc_ids:
        thisDetInfo = det_df[det_df['enc_id'] == enc_id]
        thisNumDets, thisShortestDet, thisLongestDet = getDetailedDetectionInformationOnPatient(thisDetInfo['IsDetected'], thisDetInfo['tsp'])
        numDets.append(thisNumDets)
        shortestDet.append(thisShortestDet)
        longestDet.append(thisLongestDet)

    out_df = pd.DataFrame()
    out_df['enc_id'] = pd.Series(enc_ids)
    out_df['numDets'] = pd.Series(numDets)
    out_df['shortestDet'] = pd.Series(shortestDet)
    out_df['longestDet'] = pd.Series(longestDet)
    out_df.set_index('enc_id',inplace=True)
    return out_df

def getRelativeTime(in_df, t0Event, events2MakeRelativeList, units='h', whereT0NotNull=True, post='_rel'):

    if whereT0NotNull:
        nullIdxs = in_df['first_det_tsp'].isnull()
        if np.any(nullIdxs):
            print('Removing null values when computing relative time')
            in_df = in_df[~nullIdxs]

    out_df = in_df.copy() # maybe very important line

    t0Series = in_df[t0Event]

    for event in events2MakeRelativeList:
        if post is not None:
            print(out_df.head())
            out_df.drop(event, 1, inplace=True)
            print(t0Series)
            print(event)
            print(in_df[event])
            print(in_df[event].replace(tzinfo=tz.tzutc()) - t0Series.replace(tzinfo=tz.tzutc()))
            out_df[event+post] = np.true_divide(in_df[event].replace(tzinfo=tz.tzutc()) - t0Series.replace(tzinfo=tz.tzutc()), np.timedelta64(1, units))
        else:
            out_df[event] = np.true_divide((in_df[event] - t0Series), np.timedelta64(1, units))

    return out_df

def getThresholdInformation(score_df, pop_df, event_df, threshold, min2AdverseEvent):
    #---------------------------------------
    # Get detection table
    #---------------------------------------
    score_df['min2AdverseEvent'] = min2AdverseEvent
    score_df['IsDetected'] = score_df['score']>=threshold
    det_df = score_df[score_df['IsDetected'] == True]
    det_df.reset_index(inplace=True)
    wasdet_df = det_df.groupby('enc_id').agg({'score':[np.max, np.min],
                                              'tsp':np.min, # first absolute time detected
                                              'min2AdverseEvent':np.max,
                                              'IsDetected':np.max}) #most time till event

    wasdet_df.columns = [' '.join(col).strip() for col in wasdet_df.columns.values]
    wasdet_df.rename(columns={'IsDetected amax':'IsDetected',
                              'min2AdverseEvent amax':'maxMinutesFromDet2Event',
                              'tsp amin':'first_det_tsp'},inplace=True)

    detailedDet_df = getDetailedDetectionInformation(det_df)

    # outer shouldn't matter, because they should have the same set of enc_idx
    wasdet_df = pd.merge(wasdet_df, detailedDet_df, how='outer', left_index=True, right_index=True)
    wasdet_df = pd.merge(wasdet_df, event_df, how='left', left_index=True, right_index=True)

    eventList = event_df.columns.tolist()
    # eventList = [i.encode('ascii', 'ignore') for i in eventList]
    print(wasdet_df)
    # reldet_df = getRelativeTime(wasdet_df, 'first_det_tsp', eventList)
    reldet_df = wasdet_df

    #---------------------------------------
    # Compute Results
    #---------------------------------------
    # print(pop_df)
    # print(reldet_df)
    results_df = pd.merge(pop_df, reldet_df, suffixes=['pop','det'],
                          how='left', left_index=True, right_index=True) #inner join to remove censored people


    results_df['IsDetected'] = (results_df['IsDetected'] == True) # sets nans to false
    results_df['is_positive'] = (results_df['is_positive'] == True) # converts from int to bool

    TP = (results_df['is_positive'])&(results_df['IsDetected'])
    FN = (results_df['is_positive'])&(~results_df['IsDetected'])
    FP = (~results_df['is_positive'])&(results_df['IsDetected'])
    TN = (~results_df['is_positive'])&(~results_df['IsDetected'])

    resultSeries = pd.Series(1*TP + 2*FN + 3*FP+ 4*TN,dtype="category")
    key = {1:'TP',2:'FN',3:'FP',4:'TN'}
    resultSeries.cat.categories = [key[cat] for cat in resultSeries.cat.categories]

    results_df['results'] = resultSeries

    #---------------------------------------
    # Package and return results
    #---------------------------------------
    threshInfo = eData.threshInformation(threshold, results_df)

    return threshInfo

def getROCInformation(score_dfIn, evaluationType, lambdaIn, pop_df, thisPreProcInfo):

    #===========================================================================
    # Get max response for each encounter
    #===========================================================================
    score_df = score_dfIn.groupby('enc_id')['score'].agg('max').to_frame()

    join_df = pd.merge(pop_df, score_df, suffixes=['pop','score'], how='inner', left_index=True, right_index=True) # inner to remove censored people
    #===========================================================================
    # get ROC information
    #===========================================================================
    roc_res_list, ci_auc_res_list = roc_ci_R(np.asarray(join_df['is_positive']),
                                             np.asarray(join_df['score']))


    sensitivities = get_roc(roc_res_list[0], 'sensitivities')
    specificities = get_roc(roc_res_list[0], 'specificities')
    thresholds = get_roc(roc_res_list[0], 'thresholds')
    auc = get_roc(roc_res_list[0], 'auc')[0]

    rocInfo = eData.rocInformation(lambdaIn, score_dfIn, evaluationType,
                                   sensitivities,  specificities, thresholds,
                                   auc, [ci_auc_res_list[0][0], ci_auc_res_list[0][2]],
                                   thisPreProcInfo)
    return rocInfo





def write_metrics_to_csv(metrics, frame_id, lambda_val, metrics_cols):
    try:
        csv_file = '%s.%s.operating_point_metrics.csv' % (frame_id, lambda_val)
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=metrics_cols)
            writer.writeheader()
            for data in metrics:
                writer.writerow(data)
    except IOError as error:
            print("I/O error: ({})".format(error))
    return

def create_events_table(engine, dataset_id):
    event_list = ['any_pressor','any_organ_failure','severe_sepsis','septic_shock',
                  'any_antibiotics_order','any_antibiotics', 'discharge','qsofa','sirs_intp']
    agg_func = ['' for event in event_list]
    agg_func[-1] = 'max'

    force_list = [False for event in event_list]
    force_list[-1] = True

    #special case event tables, which are still event times by enc_id
    specialCase = [];

    enter = """SELECT enc_id, min(tsp) as entranceTime
            FROM cdm_t
            WHERE fid like 'care_unit' and dataset_id = {}
            GROUP BY ENC_ID""".format(dataset_id)
    specialCase.append(enter)

    deathSql = """with
                dischargeTbl as (
                select enc_id, tsp, cast(value AS json)->> 'disposition' as disp from cdm_t where fid = 'discharge' and dataset_id = {}
                )
                select enc_id, tsp as death_Time
                from dischargeTbl
                where disp like '%Expired%'""".format(dataset_id)
    specialCase.append(deathSql)

    lastEmger = """select enc_id, max(leave_time) as last_emerg_leave_time from (
                        SELECT enc_id, tsp, fid, value,
                        lead(tsp,1) OVER (PARTITION BY enc_id ORDER BY tsp) as leave_time
                        FROM cdm_t
                        WHERE fid like 'care_unit' and dataset_id = {}
                        ORDER BY enc_id, tsp) as winT
                    WHERE value like 'HCGH EMERGENCY%'
                    GROUP BY enc_id""".format(dataset_id)
    specialCase.append(lastEmger)

    cultureSentTbl = """with cultStatTbl as (select enc_id, tsp, cast(value AS json)->>'status' as status from cdm_t where fid like 'culture_order' and dataset_id = {})
    select enc_id, min(tsp) as firstCultureSent
    from cultStatTbl
    where status like 'Sent'
    group by enc_id""".format(dataset_id)

    specialCase.append(cultureSentTbl)


    # # Get Feature Table Information
    #handle basic boolean features
    sql_prefix = 'SELECT fid, category, data_type from cdm_feature where \n'
    eventListProc = ['FID like \'{}\''.format(event) for event in event_list]
    event_sql = ' or \n'.join(eventListProc)
    event_sql = sql_prefix + '(' + event_sql + ') and dataset_id = {};'.format(dataset_id)
    print("event sql:")
    print(event_sql)
    featureTypeTbl = pd.read_sql_query(text(event_sql),con=engine)
    featureTypeTbl.set_index('fid',inplace=True)
    print(featureTypeTbl.head())

    agg_func = [func if len(func)>0 else 'min' for func in agg_func]
    func2suffix= {'min':'_first','max':'_last'}
    cdm_t_queryTemplate = 'select enc_id, {}(tsp) as {} from cdm_t where fid = \'{}\' and dataset_id = {} group by enc_id'
    cdm_twf_queryTemplate = 'select enc_id, {}(tsp) as {} from cdm_twf where {}::int = 1 and dataset_id = {} group by enc_id'
    fid = 'any_organ_failure'


    subQueries = [];
    for agg, fid, force in zip(agg_func, event_list,force_list):
        ft = featureTypeTbl.loc[fid]['data_type'].lower()
        if ft=='boolean'or ft=='integer' or force:
            suff = func2suffix[agg]
            name = fid + suff
            if featureTypeTbl.loc[fid]['category'].lower() == 't':
                subQueries.append(cdm_t_queryTemplate.format(agg,name,fid, dataset_id))
            elif featureTypeTbl.loc[fid]['category'].lower() == 'twf':
                subQueries.append(cdm_twf_queryTemplate.format(agg,name,fid, dataset_id))
            else:
                raise ValueError('Unknown category {}'.format(featureTypeTbl[fid]['category']))
        else:
            print("Unclear if automatic query constructor can handle feature type {}, skipping {}".format(ft, fid))
    print(subQueries)


    subQueries = specialCase + subQueries

    asStatement = ''
    fromStatement = ''
    for idx, subQuery in enumerate(subQueries):
       asStatement += 'tbl_{} as ({}), \n'.format(idx,subQuery)
       if idx == 0:
           fromStatement += 'tbl_{}'.format(idx)
       else:
           fromStatement += '\nFULL OUTER JOIN\n tbl_{} USING (enc_id)'.format(idx)

    asStatement = asStatement[:-3]

    joinQuery = 'with \n{} \n select * from \n{}'.format(asStatement, fromStatement)
    print("join query:")
    print(joinQuery)
    return pd.read_sql_query(text(joinQuery), con=engine, index_col='enc_id')