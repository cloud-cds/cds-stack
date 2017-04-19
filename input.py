import cPickle as Pickle

import numpy as np


class inputParams(object):
    def __init__(self, dashan_id='hcgh_1608', data_id='', model_id='', score_id='', eval_id='',
                 forceRedo=False,
                 maxNumRows = None, feature_list=None, featureConstraints=None,
                 numberOfIterations=100, ncpus=25, lambda_list=None, downSampleFactor=50,
                 testFraction=0.4, adverse_event='septic_shock', censoring_event='cmi',
                 preProcStrat='simple', sensitivityTargets=[0.85], thresholds='',subtypes='', evaluationLambdas=None):

        #===============================
        # Documentation Parameters
        #===============================
        self.dashan_id = dashan_id #name associated with the database

        if data_id is '':
            self.data_id = self.dashan_id
        else:
            self.data_id = data_id #name associated with the data from the database

        if model_id is '':
            self.model_id = self.data_id
        else:
            self.model_id = model_id #name associated with the model, and choices made by the model

        if score_id is '':
            self.score_id = self.model_id
        else:
            self.score_id = score_id #name associated with the score, and choices made by the score

        if eval_id is '':
            self.eval_id = self.score_id
        else:
            self.eval_id = eval_id #name associated with the score, and choices made by the score

        #===============================
        # Full Pipe Parameters
        #===============================
        self.forceRedo = forceRedo #Tells the model to redo all computations, even if the intermediate results appear to exist

        #===============================
        # Data Download Parameters
        #===============================
        self.maxNumRows = maxNumRows # Max number of rows to download from the database, max of None means get all

        # List of Features To use for these Runs
        if feature_list is None:
            feature_list = np.loadtxt('sepsis_features.csv', delimiter=',', dtype=str, usecols=(0,))
            feature_list = list(feature_list)
        self.feature_list = feature_list

        #===============================
        # Train Parameters
        #===============================
        # List of Lambda values to use in training

        if featureConstraints is None:
            featureConstraints = {'all':np.array([-1.0*np.inf,1.0*np.inf])}

        self.featureConstraints = featureConstraints

        if lambda_list is None:
            lambda_list = np.loadtxt('scripts/hcgh_1608_general.lambda_list.txt', delimiter=',')
        self.lambda_list = lambda_list

        self.ncpus = ncpus #number of CPU's available to use

        self.downSampleFactor = downSampleFactor #amount to downsample the training data, integer higher is more downsampleing

        self.numberOfIterations = numberOfIterations # number of iterations to computer the imputations in train

        self.testFraction = testFraction #faction of the data to be held for testing

        self.adverse_event = adverse_event

        self.censoring_event = censoring_event
        #===============================
        # Score Parameters
        #===============================

        #===============================
        # Evaluate Parameters
        #===============================
        self.sensitivityTargets = sensitivityTargets #first is assumed to be nominal

        self.preProcStrat = preProcStrat

        self.thresholds = thresholds

        self.subtypes = subtypes

        if evaluationLambdas is None:
            self.evaluationLambdas = lambda_list
        else:
            self.evaluationLambdas = evaluationLambdas



    def toPickle(self, outputFile=None):
        if outputFile is None:
            outputFile = 'scripts/' + self.model_id + '.pkl'

        with open(outputFile,'w') as output:
            # Pickle dictionary using protocol 0.
            Pickle.dump(self, output)

        print 'Wrote :' + outputFile

    def __str__(self):
        outStr = ''
        outStr += 'dashan_id: ' + self.dashan_id + '\n'
        outStr += 'data_id: ' + self.data_id + '\n'
        outStr += 'model_id: ' + self.model_id + '\n'
        outStr += 'adverseEvent:  ' + self.adverse_event + '\n'
        outStr += 'censoring_event ' + self.censoring_event + '\n'
        return outStr


class InputParamFactory(object):
    def __init__(self):
        pass

    def getInputsFromPkl(self, pklPath):
        with open(pklPath, 'rb') as f:
            params = Pickle.load(f)
        return params

    def parseInput(self, datin):
        if isinstance(datin, basestring):
            if datin.endswith('.pkl'):
                return self.getInputsFromPkl(datin)
            elif datin.endswith('.json'):
                return self.getInputsFromJson(datin)
            else:
                raise ValueError('Unknown how to return input params from this file extension. ')
        elif (type(datin)==inputParams):
            return datin
        else:
            raise ValueError('Unknown how to return input parms from supplied object')

if __name__ == "__main__":
    import sys
    dataID = sys.argv[1]
    thisinput = inputParams(data_id=dataID)
    thisinput.toPickle()
