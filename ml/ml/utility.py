# utility.py
# utilities for machine learning and ews client
from sklearn import preprocessing
import numpy as np
from scipy import stats
import logging

def standardize(train_matrix, test_matrix=None, ColNamesList=None, handleMinutesSinceFeats = True):
    # Standardization
    train_matrix = np.asmatrix(train_matrix, dtype='float')
    std_scale = preprocessing.StandardScaler().fit(train_matrix)

    if ColNamesList is None and handleMinutesSinceFeats == True:
        raise ValueError('A colnamelist must be supplied to handle the minutes since features')
    elif handleMinutesSinceFeats == True:
        logging.info("Treating minutes features as a special case")

        if "minutes_since_any_antibiotics" in ColNamesList:
            std_scale.mean_[ColNamesList.index('minutes_since_any_antibiotics')] = 0.0
            vec = train_matrix[:, ColNamesList.index('minutes_since_any_antibiotics')]
            std_scale.scale_[ColNamesList.index('minutes_since_any_antibiotics')] = np.sqrt(np.var(vec[vec != 0]))

        if "minutes_since_any_organ_fail" in ColNamesList:
            std_scale.mean_[ColNamesList.index('minutes_since_any_organ_fail')] = 0.0
            vec = train_matrix[:, ColNamesList.index('minutes_since_any_organ_fail')]
            std_scale.scale_[ColNamesList.index('minutes_since_any_organ_fail')] = np.sqrt(np.var(vec[vec != 0]))


    else:
        logging.info("Not treating minutes features as a special case")

    logging.info("std_scale:" + str(std_scale))
    train_matrix_std = std_scale.transform(train_matrix)
    if test_matrix is not None:
        test_matrix = np.asmatrix(test_matrix, dtype='float')
        test_matrix_std = std_scale.transform(test_matrix)
        return (std_scale, train_matrix_std, test_matrix_std)
    else:
        return (std_scale, train_matrix_std)

def fillin_popmean(data_frame, colname):
    is_none = data_frame[:,colname] == np.array(None)
    popmean = np.mean(data_frame[np.logical_not(is_none),colname])
    data_frame[is_none, colname] = popmean

def summary(data_frame):
    logging.info("Number of features %s" % data_frame.shape[1])
    logging.info("feature columns" + data_frame.colname_lst)
    for col in range(2, data_frame.shape[1]):
        colname = data_frame.colname_lst[col]
        logging.info("Summary of colunm %s:" % colname)
        logging.info("--------------------------------")
        coldata = data_frame[:, col]
        try:
            n, min_max, mean, var, skew, kurt = stats.describe(coldata)
            logging.info("Number of elements: {0:d}".format(n))
            logging.info("Minimum: {0:8.6f} Maximum: {1:8.6f}".format(min_max[0], min_max[1]))
            logging.info("Mean: {0:8.6f}".format(mean))
            logging.info("Variance: {0:8.6f}".format(var))
            logging.info("Skew : {0:8.6f}".format(skew))
            logging.info("Kurtosis: {0:8.6f}".format(kurt))
            logging.info("")
        except Exception as e:
            logging.info(e)
            logging.info("")

