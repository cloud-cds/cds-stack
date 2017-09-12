import numpy as np
import pandas as pd
import re, pickle, pdb
import matplotlib.pylab as plt
import logging
plt.style.use('bmh')
import matplotlib as mpl
colors = mpl.rcParams['axes.prop_cycle']
import os
from collections import defaultdict
from pytz import timezone
def __locallize_tz(x):
    y = x.tz_localize(timezone('utc'))
    return y

MODEL        = os.environ['model']
MNT          = os.environ['mnt']
INPUT        = 'input'
OUTPUT       = 'output'
ALL_FEATURES = os.path.join(MNT, MODEL, INPUT, 'all_features.csv')
TRAINS_IDS   = os.path.join(MNT, MODEL, INPUT, 'train_ids.npy')
TEST_IDS     = os.path.join(MNT, MODEL, INPUT, 'test_ids.npy')
CDM_TWF      = os.path.join(MNT, MODEL, INPUT, '1yr_cdm_twf.csv')
CDM_S        = os.path.join(MNT, MODEL, INPUT, '1yr_cdm_s.csv')
LABEL        = os.path.join(MNT, MODEL, INPUT, 'final_sepsis3_hcgh1.csv')
COEFS        = os.path.join(MNT, MODEL, OUTPUT, 'coefs.csv')
FINAL_COEFS        = os.path.join(MNT, MODEL, OUTPUT, 'final_coefs.csv')
FIG_AUC      = os.path.join(MNT, MODEL, OUTPUT, 'fig_auc.png')
FIG_PPV      = os.path.join(MNT, MODEL, OUTPUT, 'fig_ppv.png')

# load involved features
logging.info("loading feature config")
all_features = pd.read_csv(ALL_FEATURES)
twf_feats = ['enc_id', 'tsp']
twf_feats.extend(all_features.loc[all_features['table']=='TWF', 'name'].tolist())
s_feats = all_features.loc[all_features['table']=='S', 'name'].tolist()
s_feats.extend(['enc_id'])
s_feats.remove('heart_failure_diag')
s_feats.remove('heart_failure_hist')

# load training and testing enc_id cohort
logging.info("loading enc_id cohort")
train_ids = np.load(TRAINS_IDS)
test_ids  = np.load(TEST_IDS)

# load data: cdm_twf and cdm_s
logging.info("loading cdm_twf")
cdm_twf = pd.read_csv(CDM_TWF)
test_cdm_twf = cdm_twf.loc[cdm_twf['enc_id'].isin(test_ids), twf_feats].copy()
test_cdm_twf['tsp'] = pd.to_datetime(test_cdm_twf['tsp'])
test_cdm_twf['tsp'] = test_cdm_twf['tsp'].apply(__locallize_tz)
cdm_twf = cdm_twf.loc[cdm_twf['enc_id'].isin(train_ids), twf_feats]
cdm_twf['tsp'] = pd.to_datetime(cdm_twf['tsp'])
cdm_twf['tsp'] = cdm_twf['tsp'].apply(__locallize_tz)

logging.info("loading cdm_s")
cdm_s = pd.read_csv(CDM_S)
cdm_s = cdm_s.loc[cdm_s['enc_id'].isin(train_ids), :]
test_cdm_s = cdm_s.loc[cdm_s['enc_id'].isin(test_ids), :]

# load labels
sep3 = pd.read_csv(LABEL)[['enc_id', 'tsp']]
adverse_event_tbl = sep3.copy()

adverse_event_tbl['tsp'] = pd.to_datetime(adverse_event_tbl['tsp'])
adverse_event_tbl['tsp'] = adverse_event_tbl['tsp'].apply(__locallize_tz)

adverse_event_tbl = adverse_event_tbl.groupby('enc_id').agg({'tsp':'min'})
adverse_event_tbl.reset_index(level=0, inplace=True)
adverse_event_tbl.rename(columns={'tsp':'adverse_event_time'}, inplace=True)
adverse_event_tbl['adverse_event'] = True

first_event_time_dict = defaultdict()
for i0, row in adverse_event_tbl.iterrows():
    first_event_time_dict[row['enc_id']] = row['adverse_event_time']

# max_tsp for right_censored
def get_max_tsp(main_df):
    max_tsp_df = main_df.groupby('enc_id').agg({'tsp':'max'}).copy()
    max_tsp_df.reset_index(level=0, inplace=True)
    max_tsp_df = max_tsp_df.loc[~(max_tsp_df['enc_id'].isin(adverse_event_tbl['enc_id'])), :]
    max_tsp_df.rename(columns={'tsp':'adverse_event_time'}, inplace=True)
    max_tsp_df['adverse_event'] = False
    new_label_df = pd.concat([adverse_event_tbl, max_tsp_df])
    return new_label_df

new_label_df = get_max_tsp(cdm_twf)
test_new_label_df = get_max_tsp(test_cdm_twf)

def assign_label_and_convert_tsp(main_df, new_label_df):

    data_df = pd.merge(main_df, new_label_df, how='left', on='enc_id').copy()
    upto_event_data_df = data_df.loc[data_df['tsp'] < data_df['adverse_event_time'], :].copy()
    upto_event_data_df['censored'] = 0 # censored
    upto_event_data_df.loc[upto_event_data_df['adverse_event']==True, 'censored'] = 1
    # convert tsp to hosp_time
    upto_event_data_df['tsp'] = (upto_event_data_df['adverse_event_time'] - upto_event_data_df['tsp'])\
                            /pd.to_timedelta(1, unit='m')
    return upto_event_data_df


upto_event_data_df = assign_label_and_convert_tsp(cdm_twf, new_label_df)
test_upto_event_data_df = assign_label_and_convert_tsp(test_cdm_twf, test_new_label_df)

logging.info("upto_event_data_df.shape:{}, test_upto_event_data_df.shape:{}".format(upto_event_data_df.shape, test_upto_event_data_df.shape))


# merge with cdm_s
def merge_with_cdms(cdms, main_df):
    for feat in s_feats:
        if feat in ['enc_id', 'admit_weight']:
            continue
        elif feat in ['gender', 'age']:
            df = cdms.loc[cdms['fid']==feat, ['enc_id', 'value']].copy()
            df.rename(columns={'value':feat}, inplace=True)
            main_df = pd.merge(main_df, df, how='left', on='enc_id')
        else:
            enc_ids = np.unique(cdms.loc[cdms['fid']==feat, 'enc_id'])
            main_df[feat] = False
            main_df.loc[main_df['enc_id'].isin(enc_ids), feat] = True
    return main_df

upto_event_data_df = merge_with_cdms(cdm_s, upto_event_data_df)
test_upto_event_data_df = merge_with_cdms(test_cdm_s, test_upto_event_data_df)
logging.info("after merge_with_cdms: upto_event_data_df.shape:{}, test_upto_event_data_df.shape:{}".format(upto_event_data_df.shape, test_upto_event_data_df.shape))


feature_list = list(twf_feats)
feature_list.remove('enc_id')
feature_list.remove('tsp')
feature_list.remove('severe_sepsis')
feature_list.remove('septic_shock')
feature_list.remove('cmi')
feature_list.extend(s_feats)
feature_list.remove('enc_id')
feature_list.remove('age')
feature_list.remove('admit_weight')
logging.info('Number of features'.format(len(feature_list)))
logging.info(feature_list)

x_data = upto_event_data_df[feature_list].copy()
test_x_data = test_upto_event_data_df[feature_list].copy()

pop_mean = defaultdict()
pop_std = defaultdict()
for feat in feature_list:
    if all_features.loc[all_features['name']==feat, 'val'].tolist()[0] == 'Boolean'\
            and all_features.loc[all_features['name']==feat, 'table'].tolist()[0] == 'TWF':
        x_data[feat] = x_data[feat].map({'f':0, 't':1})
        test_x_data[feat] = test_x_data[feat].map({'f':0, 't':1})

def return_matrices(x_data, event_df):
    x_mat = x_data.as_matrix().astype(float)
    y_mat = event_df['tsp'].as_matrix().astype(float)
    c_mat = event_df['censored'].as_matrix().astype(float)
    return x_mat, y_mat, c_mat

x_data_mat, y_data_mat, c_data_mat = return_matrices(x_data, upto_event_data_df)
test_x_data_mat, test_y_data_mat, test_c_data_mat = return_matrices(test_x_data, test_upto_event_data_df)

pop_mean = np.nanmean(x_data_mat, 0)
pop_std = np.nanstd(x_data_mat, 0)
pop_std[pop_std == 0] = 1.0

logging.info("shapes of x_data_mat, y_data_mat: {} {}".format(x_data_mat.shape, y_data_mat.shape))
logging.info("shapes of test_x_data_mat, test_y_data_mat: {} {}".format(test_x_data_mat.shape, test_y_data_mat.shape))

# repalce nan values
for f0,feat in enumerate(feature_list):
    logging.info('replacing nan values for {}'.format(feat))
    x_data_mat[:,f0] = np.where(np.isfinite(x_data_mat[:,f0]), x_data_mat[:,f0], pop_mean[f0])
    x_data_mat[:,f0] = (x_data_mat[:,f0] - pop_mean[f0]) / pop_std[f0]
    test_x_data_mat[:,f0] = np.where(np.isfinite(test_x_data_mat[:,f0]), test_x_data_mat[:,f0], pop_mean[f0])
    test_x_data_mat[:,f0] = (test_x_data_mat[:,f0] - pop_mean[f0]) / pop_std[f0]

np.random.seed(10001)
pos_inds = np.where(c_data_mat==1)[0]
neg_inds = np.where(c_data_mat==0)[0]
tr_pos_inds = np.random.choice(pos_inds, int(np.floor(0.7*len(pos_inds))), replace=False)
val_pos_inds = np.setdiff1d(pos_inds, tr_pos_inds)
tr_neg_inds = np.random.choice(neg_inds, int(np.floor(0.7*len(neg_inds))), replace=False)
val_neg_inds = np.setdiff1d(neg_inds, tr_neg_inds)

val_inds = np.random.permutation(np.union1d(val_neg_inds, val_pos_inds))
tr_inds = np.random.permutation(np.union1d(tr_neg_inds, tr_pos_inds))
logging.info('training rows: {} validation rows: {}'.format(len(tr_inds), len(val_inds)))

x_matrix = os.path.join(MNT, MODEL, OUTPUT, 'x_matrix.csv')
y_matrix = os.path.join(MNT, MODEL, OUTPUT, 'y_matrix.csv')
c_matrix = os.path.join(MNT, MODEL, OUTPUT, 'c_matrix.csv')

np.savetxt(x_matrix, x_data_mat[tr_inds,:], delimiter=',', header=','.join(feature_list))
np.savetxt(y_matrix, y_data_mat[tr_inds], delimiter=',')
np.savetxt(c_matrix, c_data_mat[tr_inds], delimiter=',', fmt='%d')

logging.info("Start training in R")

import rpy2.robjects as robjects
from rpy2.robjects import numpy2ri
from rpy2.robjects.packages import importr

# ### R Code
r_code = '''
library(survival)
library(glmnet)
x = read.csv("{x_matrix}", header = TRUE)
y = read.csv("{y_matrix}")
c = read.csv("{c_matrix}")
yy = Surv(as.numeric(y[,1]), as.numeric(c[,1]))
fit=glmnet(as.matrix(x[-1,]),yy,family="cox", thresh=1e-3, maxit=1000, nlambda=20)
write.table(as.matrix(fit$beta), file='{coefs}', sep=",")
'''.format(x_matrix=x_matrix, y_matrix=y_matrix, c_matrix=c_matrix, coefs=COEFS)
logging.info("r code: {}".format(r_code))
robjects.r(r_code)

logging.info("validating...")
weights_df = pd.read_csv(COEFS)
weights_matrix = weights_df.as_matrix()[:, 1:].astype(float)
logging.info("weights_matrix.shape: {}, len_of_features: {}".format(weights_matrix.shape, len(feature_list)))
pred_matrix = np.dot(x_data_mat[val_inds,:], weights_matrix)

from sklearn import metrics
from sklearn.metrics import precision_recall_curve
num_lambdas = weights_matrix.shape[1]
val_auc = np.zeros(num_lambdas)
val_ppv = np.zeros(num_lambdas)

for i in range(num_lambdas):
    fpr, tpr, thresholds = metrics.roc_curve(c_data_mat[val_inds], pred_matrix[:,i])
    val_auc[i] = metrics.auc(fpr, tpr)
    precision, recall, _ = precision_recall_curve(c_data_mat[val_inds], pred_matrix[:,i])
    val_ppv[i] = metrics.auc(recall, precision)

fig = plt.figure()
plt.plot(val_auc)
fig.savefig(FIG_AUC)
fig = plt.figure()
plt.plot(val_ppv)
fig.savefig(FIG_PPV)

best_ind = np.argmax(val_auc)
logging.info("Saving best coefs")
final_coefs = weights_df.loc[:, ['X', 's'+str(best_ind-1)]]
final_coefs.to_csv(FINAL_COEFS, index=False)