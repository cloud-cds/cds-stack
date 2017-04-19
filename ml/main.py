import os
import sys
from dashan_ml import InitialProcessing, train_model, predict_datasets, evaluate
from dashan_ml.input import InputParamFactory

def full_pipe(input_arg):
    #---------------------------
    # Parse Inputs
    #---------------------------
    inputFact = InputParamFactory()
    inputValues = inputFact.parseInput(input_arg)
    forceRedo = inputValues.forceRedo

    #---------------------------
    # Generate Frame
    #---------------------------
    if forceRedo:
        getData = 1
    else:
        data_id = inputValues.data_id
        if not os.path.isfile('%s_processed_full.npy' % data_id):
            getData = 1
        else:
            print "Existing Dataset found with id {}".format(data_id)
            getData = 0

    if getData:
        InitialProcessing.GenerateDataFrame(inputValues)
        InitialProcessing.adverseEventProcessing(inputValues)


    #---------------------------
    # Train Model
    #---------------------------
    if forceRedo:
        doTrain = True
    else:
        model_id = inputValues.model_id
        if not os.path.isfile('%s.mdl.pkl' % model_id):
            doTrain = True
        else:
            print "Existing Model found with id {}".format(model_id)
            doTrain = False

    if doTrain:
        train_model.train_model(inputValues)

    #---------------------------
    # Predict Datasets
    #---------------------------
    if forceRedo:
        doPredict = True
    else:
        score_id = inputValues.score_id
        if not os.path.isfile('{}.test_score.txt'.format(score_id)):
            doPredict = True
        else:
            print "Existing predictions found with id {}".format(score_id)
            doPredict = False

    if doPredict:
        predict_datasets.predict_test_train(inputValues)

    #---------------------------
    # Predict Datasets
    #---------------------------
    if forceRedo:
        doEval = True
    else:
        eval_id = inputValues.eval_id
        if not os.path.isfile('%s_evalComplete.txt' % eval_id):
            doEval = True
        else:
            print "Existing Evaluations found with id {}".format(eval_id)
            doEval = False

    if doEval:
        evaluate.evaluate(inputValues)

    #---------------------------
    # Add a package for real time function here
    #---------------------------

if __name__ == "__main__":
    input_arg = sys.argv[1]
    full_pipe(input_arg)
