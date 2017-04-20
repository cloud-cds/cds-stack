# predict_datasets.py
import time

import numpy as np
from ml import sepsis_functions as func
from ml.client import DataFrameFactory


import pickle
from ml.dashan_input import InputParamFactory

def predict(models, data_frame, n_cpu, filename=None):

    score = func.predict_R_in_parallel(models, data_frame, n_cpu)

    if filename is not None:
        np.savetxt('%s.txt' % filename, score, delimiter=',', fmt='%f')
    return score

def predict_test_train(inputPassedIn):
    # for use with lambdas in the lambda list
    inputFact = InputParamFactory()
    inputValues = inputFact.parseInput(inputPassedIn)

    data_id = inputValues.data_id
    model_id = inputValues.model_id
    score_id = inputValues.score_id

    print(time.strftime("%H:%M:%S") + " Predicting test/train for model ID {} with data ID {}".format(model_id,data_id))

    print(time.strftime("%H:%M:%S"))
    print(time.strftime("%H:%M:%S")) + " loading data id:".format(data_id)

    factory = DataFrameFactory()
    data_train_sd = factory.load(model_id + "_train_sd")
    data_test_sd = factory.load(model_id + "_test_sd")

    print("loading model data id:" + model_id)

    mdls_f = open('%s.mdl.pkl' % model_id, 'rb')
    models = pickle.load(mdls_f)
    mdls_f.close()

    print(" Generating scores for {}: Predicting:" + score_id)

    n_cpu = inputValues.ncpus

    predict(models, data_train_sd, n_cpu, filename='{}.train_score'.format(score_id))
    predict(models, data_test_sd, n_cpu, filename='{}.test_score'.format(score_id))

if __name__ == "__main__":
    import sys
    predict_test_train(sys.argv[1])