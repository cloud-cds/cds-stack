import numpy as np


class threshInformation(object):
    def __init__(self,threshold, results_df):
        # Information regarding specific threshold
        self.threshold = threshold
        self.results_df = results_df


        # error checking to handle case where there are none of these
        resultsSeries = results_df['results']
        results_valCounts = resultsSeries.value_counts()

        theseCategories = resultsSeries.cat.categories.tolist()
        if 'TP' in theseCategories:
            self.TP = results_valCounts.loc['TP']
        else:
            self.TP = 0

        if 'FN' in theseCategories:
            self.FN = results_valCounts.loc['FN']
        else:
            self.FN = 0

        if 'FP' in theseCategories:
            self.FP = results_valCounts.loc['FP'] # This doesn't account for one encounter with multiple false postivies
        else:
            self.FP = 0

        if 'TN' in theseCategories:
            self.TN = results_valCounts.loc['TN']
        else:
            self.TN = 0


        self.sensitivity = np.true_divide(self.TP,self.TP+self.FN)
        self.specificity = np.true_divide(self.TN,self.TN+self.FP)
        self.PPV = np.true_divide(self.TP,self.TP+self.FP)
        self.NPV = np.true_divide(self.TN,self.TN+self.FN)

        beforeEvent = results_df[results_df['results'] == 'TP']['maxMinutesFromDet2Event']
        beforeEvent = beforeEvent / 60.0

        stats = beforeEvent.describe()
        self.hour2Onset_mean = stats['mean']
        self.hour2Onset_median = stats['50%']
        self.hour2Onset_std = stats['std']
        self.hour2Onset_min = stats['min']
        self.hour2Onset_max = stats['max']
        self.hour2Onset_25p = stats['25%']
        self.hour2Onset_75p = stats['75%']
        self.hour2Onset_IQR = stats['75%'] - stats['25%']

        # Add a detections be hour concept here


    def __str__(self):
        thisStr = ''
        thisStr += 'sensitivity: {:.3f}'.format(self.sensitivity) + '\n'
        thisStr += 'specificity: {:.3f}'.format(self.specificity) + '\n'
        thisStr += 'PPV: {:.3f}'.format(self.PPV) + '\n'
        thisStr += 'NPV: {:.3f}'.format(self.NPV) + '\n'
        thisStr += 'Median Hours 2 Onset: {:.3f}'.format(self.hour2Onset_median) + '\n'


        return thisStr

class rocInformation(object):
    def __init__(self,lambdaThis, score_dfIn, evaluationType, sensitivities,  specificities, thresholds, AUC, AUC_interval, thisPreProcInfo):
        # information regarding a ROC, regardless of selected threshold.
        self.lambdaThis = lambdaThis

        self.score_df = score_dfIn
        self.preProcInfo = thisPreProcInfo

        self.evaluationType = evaluationType

        self.ROC = {'sensitivities': sensitivities,
                    'specificities': specificities,
                    'thresholds': thresholds} # dictionary of values returned by R
        self.AUC = AUC
        self.AUC_interval = AUC_interval

        self.thresholdInfoList = []

    def __str__(self):
        thisStr = 'Evaluation Type: {}\n'.format(self.evaluationType)
        thisStr += 'Pre-Processing Function: {}\n'.format(self.preProcInfo.funcName)
        thisStr += 'lambda: {:.3f}\n'.format(self.lambdaThis)
        thisStr += 'model_level_metrics: {:.3f}\n'.format(self.AUC)
        thisStr += 'model_level_metrics 95% CI: ({:.3f},{:.3f})\n'.format(self.AUC_interval[0],self.AUC_interval[1])
        thisStr += 'Num Associated Thresholds:({})\n'.format(len(self.thresholdInfoList))
        return thisStr

    def selectThreshold(self, sensitivity_target):
        threshPicked = self.ROC['thresholds'][np.argmax(self.ROC['sensitivities'] <= sensitivity_target)]
        return threshPicked
    def predictSensitivity(self,threshold):
        sens = self.ROC['sensitivities'][np.argmax(self.ROC['thresholds'] <= threshold)]
        return sens




