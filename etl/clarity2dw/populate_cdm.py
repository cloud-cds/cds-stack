"""
populate_cdm.py
"""
from resources import CDM, DBLink
from dashan_config import Config
from multiprocessing import Pool
from multiprocessing import current_process
import traceback
import csv
import os
import sys
from sqlalchemy import create_engine


def print_populate_sql_statements(dblink_id):
  config = Config(dblink_id)

  dblink = DBLink(config)
  dblink.connect()
  # import patient encounters
  # create visit_id to pat_enc mapping
  visit_id_to_enc_id = dblink.get_visit_id_to_enc_id_mapping(dblink_id)

  # print sql statement one by one
  mappings = dblink.select_feature_mapping(dblink_id)
  for mapping in mappings:
    # select feature values from dblink
    fid = mapping['fid']
    sql = dblink.get_extract_sql(mapping, visit_id_to_enc_id)
    print "feature %s sql: %s" % (fid, sql)
  dblink.disconnect()

def _load_fids_from_csv(csv_file, all_fids):
  with open(csv_file, mode='rb') as infile:
    reader = csv.reader(infile, delimiter=',')
    return [line[0] for line in reader \
      if int(line[1]) == 1 and line[0] in all_fids]

def populate_cdm_in_parallel(dblink_id, debug, log_folder, nprocs, plan=False):
  # get list of measured features
  config = Config(dblink_id, debug=debug)
  cdm = CDM(config)
  cdm.connect()
  all_features = cdm.get_all_cdm_features()
  populate_fids = [feature['fid'] for feature in all_features \
    if not feature['is_deprecated'] and feature['is_measured']]
  print "populating fids", populate_fids
  # get shared variables
  dblink = DBLink(config)
  dblink.connect()
  # create visit_id to pat_enc mapping
  visit_id_to_enc_id = dblink.get_visit_id_to_enc_id_mapping(dblink_id)
  pat_id_to_enc_ids = dblink.get_pat_id_to_enc_id_mapping(dblink_id)
  print 'number of visit_ids', len(visit_id_to_enc_id)
  #print visit_id_to_enc_id
  mappings = dblink.select_feature_mapping(dblink_id)
  dblink.disconnect()


  # populate feature in parallel
  cdm.log.info("debug: " + str(debug))
  cdm.log.info("plan: " + str(plan))

  pool = Pool(processes=nprocs)

  for fid in populate_fids:
    print "populating %s" % fid
    logfile = os.path.join(log_folder, fid + ".log")
    result = pool.apply_async(populate_cdm_process, args=(dblink_id, \
      fid, mappings, visit_id_to_enc_id, pat_id_to_enc_ids, debug, \
      logfile, plan))
  pool.close()
  pool.join()

def populate_cdm_process(dblink_id, fid, mappings, visit_id_to_enc_id,
             pat_id_to_enc_ids, debug, logfile, plan):
  logname = current_process().name + ": " + fid
  config = Config(dblink_id, log=logname, debug=debug, logfile=logfile)
  try:
    fid_mappings = [mapping for mapping in mappings \
      if mapping['fid'] == fid]

    if len(fid_mappings) > 0:
      cdm = CDM(config)
      cdm.connect()
      cdm.log.debug("connect to cdm for fid %s" % fid)
      dblink = DBLink(config)
      dblink.connect()
      for mapping in fid_mappings:
        if "." in mapping['transform_func_id']:
          transform_extension(dblink, cdm, mapping)
        else:
          dblink.populate_dblink_feature_to_cdm(mapping, cdm, \
              visit_id_to_enc_id, pat_id_to_enc_ids, plan=plan)
      cdm.disconnect()
      cdm.log.debug("disconnect to cdm for fid %s" % fid)
      dblink.disconnect()
  except Exception as e:
    cdm.log.error(traceback.format_exc())

def populate_cdm(dashan_id, datalink_id, nproc=1, fid_input=None, debug=False, plan=False):
  from dashan import Dashan
  dashan_instance = Dashan(dashan_id)
  dashan_instance.connect()
  all_features = dashan_instance.get_all_cdm_features()
  meas_fids = [feature['fid'] for feature in all_features \
    if not feature['is_deprecated'] and feature['is_measured']]

  populate_fids = []
  if fid_input is None:
    populate_fids = meas_fids
  elif fid_input.endswith(".csv"):
    populate_fids = _load_fids_from_csv(fid_input, meas_fids)
  elif isinstance(fid_input, str):
    populate_fids = [fid_input]
  else:
    populate_fids = fid_input

  print "populate fids:", populate_fids

  datalink = dashan_instance.get_datalink(datalink_id)
  # create visit_id to pat_enc mapping
  visit_id_to_enc_id = dashan_instance.get_visit_id_to_enc_id_mapping()
  pat_id_to_enc_ids = dashan_instance.get_pat_id_to_enc_id_mapping()
  print 'number of visit_ids', len(visit_id_to_enc_id)
  #print visit_id_to_enc_id
  # populate feature one by one
  mappings = datalink.select_feature_mapping()
  if nproc == 1:
    for fid in populate_fids:
        print "populating %s" % fid
        # delete existing values first
        if not plan:
          dashan_instance.delete_feature_values(fid)
        dashan_instance.log.debug("empty values for %s" % fid)
        fid_mappings = [mapping for mapping in mappings \
          if mapping['fid'] == fid]
        for mapping in fid_mappings:
          if "." in mapping['transform_func_id']:
            transform_extension(datalink, dashan_instance, mapping)
          else:
            datalink.populate_datalink_feature_to_cdm(mapping, \
              visit_id_to_enc_id, pat_id_to_enc_ids, plan=plan)

  else:
    # deprecated!
    print "deprecated!"
    # pool = Pool(processes=nproc)
    # for mapping in mappings:
    #     pool.apply_async(populate_cdm_feature, \
    #         args=(mapping, visit_id_to_enc_id, pat_id_to_enc_ids))
    # pool.close()
    # pool.join()
  #datalink.disconnect()
  dashan_instance.disconnect()

def transform_extension(src, dist, mapping):
  transform_func = mapping['transform_func_id'].split(".")
  transform_func_pkg = transform_func[0]
  transform_func_id = transform_func[1]
  src.log.info("use 3rd party transform function: package: %s  function: %s" % (transform_func_pkg, transform_func_id))
  if transform_func_pkg == "inpatient-updater":
    src.log.info("TODO import inpatient-updater functions")
    from inpatient_updater.clarityExtractLoad import transforms
    func = getattr(trasforms, transform_func_id)
    engine_src = create_engine(src.config.get_db_conn_string_sqlalchemy())
    engine_dist = create_engine(dist.config.get_db_conn_string_sqlalchemy())
    func(engine_src, engine_dist)
    src.log.info("run function: %s" % transform_func_id)

def populate_cdm_feature(mapping, visit_id_to_enc_id):
  # deprecated!
  try:
    config = Config(mapping['fid'])

    dblink = DBLink(config)
    dblink.connect()
    dblink.import_feature_values(mapping, visit_id_to_enc_id)
    dblink.disconnect()
  except Exception as e:
    print(traceback.format_exc())
    raise

