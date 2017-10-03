# ======================================================
## Support Code
#=======================================================
import numpy as np
import pandas as pd


class preProcInfo(object):
    def __init__(self, funcName, params):
        self.funcName = funcName
        self.params = params

def applyFuncToLagWindowOnEnc(times, raw_score, func, windowSize, units):
    score_trans = np.empty([0])
    for idx, time in enumerate(times):
        relTime = times - time

        inWindow = (relTime < np.timedelta64(0, 'h')) & (relTime > -np.timedelta64(windowSize, units))
        wind_idx = [i for i, x in enumerate(inWindow) if x]
        wind_idx.append(idx)

        if wind_idx[0] != 0:
            wind_idx.insert(0, wind_idx[0] - 1)

        scoresInWind = np.interp(range(-windowSize, 1, 1),
                                 np.true_divide(relTime[wind_idx], np.timedelta64(windowSize, units)), raw_score[wind_idx])

        score_trans = np.append(score_trans, func(scoresInWind))

    return score_trans

def applyFuncToLagWindow(score_df, func, name, thisPreProcInfo=None, windowSize=200, units='h'):
    #------------------------------------
    # Input parsing
    #------------------------------------
    if (thisPreProcInfo is not None) and ('func' in thisPreProcInfo.params):
        func = thisPreProcInfo.params['func']
        windowSize = thisPreProcInfo.params['windowSize']
        units = thisPreProcInfo.params['units']
    else:
        params = dict()
        params['func'] = func
        params['windowSize'] = windowSize
        params['units'] = units
        thisPreProcInfo = preProcInfo(name, params)

    #------------------------------------
    # apply function to lagging window
    #------------------------------------
    enc_ids = np.unique(score_df['enc_id'])
    score_trans = list()

    for enc_id in enc_ids:
        thisEncInfo = score_df[score_df['enc_id'] == enc_id]
        score_trans.append(applyFuncToLagWindowOnEnc(thisEncInfo['tsp'], thisEncInfo['raw_score'], func, windowSize, units))

    score_df['trans_score'] = pd.Series(score_trans)
    #------------------------------------
    # Do simple
    #------------------------------------
    score_df, thisPreProcInfo, valid = normScore(score_df, toNorm='trans_score', thisPreProcInfo=thisPreProcInfo)

    return score_df, thisPreProcInfo, valid

def normScore(score_df, toNorm='raw_score', normed='score', thisPreProcInfo=None):
    #--------------
    # Input Parsing
    #--------------
    toNormscore = score_df[toNorm]

    if (thisPreProcInfo is not None)and('score_scale' in thisPreProcInfo.params):
        print("Applying old Normalization Factors")
        score_scale = thisPreProcInfo.params['score_scale']
    else:
        score_scale = None

    if score_scale is None:
        print("Recalculating Normalization Factors")
        score_scale = [0,0]
        # score_scale[0] = np.min(toNormscore, axis=0)
        # score_scale[1] = np.max(toNormscore, axis=0)
        # less outlier rejection means that the data has a small dynamic range.
        score_scale[0] = np.percentile(toNormscore, 1) # more  outlier regection runs risk that meaningful operating points will be out of 0/1
        score_scale[1] = np.percentile(toNormscore, 99) # more outlier regection runs risk that meaningful operating points will be out of 0/1
    if score_scale[1] <= 10 ** -6:
        return score_df, None, False

    if thisPreProcInfo is None:
        params = dict()
        params['score_scale'] = score_scale
        thisPreProcInfo = preProcInfo('simple',params)
    #--------------
    # Normalize
    #--------------

    score_norm = np.true_divide((toNormscore - score_scale[0]), (score_scale[1] - score_scale[0]))
    score_df[normed] = score_norm

    return score_df, thisPreProcInfo, True

# ======================================================
## Main
# =======================================================
def process(score_df, PreProcName=None, thisPreProcInfo=None):
    #===================================================================
    #  Parse inputs
    #===================================================================
    if PreProcName is None and thisPreProcInfo is None:
        raise ValueError('Either a Name or a preProcInfo must be passed in')
    elif PreProcName is None:
        PreProcName = thisPreProcInfo.funcName

    PreProcType = PreProcName.lower()
    #===================================================================
    # Do pre-processing by type
    #===================================================================
    doSimple = False
    if PreProcType == 'simple':
        doSimple = True
    elif PreProcType == 'lagMean':
        score_df, thisPreProcInfo, valid = applyFuncToLagWindow(score_df, np.mean, 'lagMean', thisPreProcInfo=thisPreProcInfo)
    else:
        print('unknown pre-proc name, defaulting to simple')
        doSimple = True
    #===================================================================
    #  Simple Pre-processing
    #===================================================================
    if doSimple:
        score_df, thisPreProcInfo, valid = normScore(score_df, toNorm='raw_score', thisPreProcInfo=thisPreProcInfo)


    #===================================================================
    #  Return
    #===================================================================

    return score_df, thisPreProcInfo, valid

