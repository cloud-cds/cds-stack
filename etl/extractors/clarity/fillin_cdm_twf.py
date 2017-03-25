"""
fillin_cdm_twf.py
fillin all measured twf features
"""

from resources import CDM
from dashan_config import Config
from multiprocessing import Pool, Process
import traceback

def fillin_cdm_twf(dbname, fid_list=None, nproc=1, recalculate_popmean=True):
  config = Config(dbname)
  cdm = CDM(config)
  cdm.connect()
  features = cdm.get_all_measured_twf_features()
  cdm.disconnect()

  if fid_list is not None:
    features = [feature for feature in features \
      if feature['fid'] in fid_list]

  if nproc == 1:
    for feature in features:
      # select feature values from dblink
      fillin_single_cdm_twf(dbname, feature)
  else:
    pool = Pool(processes=nproc)
    for fid in fid_list:
      print 'new process', fid
      pool.apply_async(fillin_single_cdm_twf, args=(dbname, feature, recalculate_popmean))
    pool.close()
    pool.join()
    print 'finish all processes'

    # jobs = []
    # for fid in fid_list:
    #   p = Process(target=fillin_single_cdm_twf, args=(fid,))
    #   jobs.append(p)
    #   p.start()
    # for proc in jobs:
    #   proc.join()


def fillin_single_cdm_twf(dbname, feature, recalculate_popmean=True):
  fid = feature['fid']
  try:
    config = Config(dbname)
    cdm = CDM(config)
    cdm.connect()
    cdm.fillin(fid, recalculate_popmean)
    cdm.disconnect()
  except Exception as e:
    print(traceback.format_exc())
    raise

if __name__ == '__main__':
  # fid_list = ['abp_dias', 'creatinine']
  # fillin_cdm_twf(fid_list, nproc=2)
  fillin_cdm_twf(nproc=1)