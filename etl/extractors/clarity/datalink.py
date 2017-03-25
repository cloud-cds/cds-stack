"""
datalink.py
"""
import os
from resources import Resource, PatEnc
import ConfigParser
import csv
import importlib
import sys
import timeit
# import function.transform as transform
from transforms.primitives.row import transform
# import function.fillin as fillin
import json
from populate_cdm import populate_cdm
from fillin_cdm_twf import fillin_cdm_twf
from derive_main import derive_main
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
import criteria

class DataLink(Resource):
  def __init__(self, dashan, datalink_id):
    self.dashan = dashan
    self.datalink_id = datalink_id
    self.log = self.dashan.log
    self.datalink_folder = os.path.join(self.dashan.config.DATALINK_DIR, \
      self.datalink_id)
    self.load_config()
    datalink_type = self.get_datalink_attribute('datalink_type')
    if datalink_type == "WSLink":
      self.load_online_prediction_parameters()

  def load_online_prediction_parameters(self):
    self.log.info("load online_prediction_features")
    self.feature_weights = {}
    # load features weights from database
    server_cursor = self.dashan.select_with_sql("select * from trews_feature_weights")
    trews_feature_weights = server_cursor.fetchall()
    server_cursor.close()
    for weight in trews_feature_weights:
      self.feature_weights[weight['fid']] = weight['weight']
      self.log.info("feature: %s\t weight: %s" % (weight['fid'], weight['weight']))
    server_cursor = self.dashan.select_with_sql("select * from trews_parameters")
    trews_parameters = server_cursor.fetchall()
    server_cursor.close()
    for parameter in trews_parameters:
      if parameter['name'] == 'max_score':
        self.max_score = parameter['value']
      if parameter['name'] == 'min_score':
        self.min_score = parameter['value']
    self.log.info('set max_score to %s and min_score to %s' % (self.max_score, self.min_score))

  def _load_score_range(self, range_type, lambda_idx):
    range_type_file = os.path.join(self.datalink_folder, \
      self.config['DATALINK_FEATURE'][range_type])
    with open(range_type_file) as rtf:
      rows = list(csv.reader(rtf))
      return float(rows[lambda_idx][0])

  def create(self):
    self.log.info("create datalink %s" % self.datalink_id)
    self.dashan.connect()
    # configure extension for this dblink
    # function extension
    if self.config['DATALINK_EX']['function_extension']:
      self.extend_function(os.path.join(self.datalink_folder,\
        self.config['DATALINK_EX']['function_extension']))
    # feature extension
    if self.config['DATALINK_EX']['feature_extension']:
      self.extend_feature(os.path.join(self.datalink_folder,\
        self.config['DATALINK_EX']['feature_extension']))


    # insert datalink into database
    self.dashan.insert_datalink(self.config, commit=False)

    # insert dblink features
    feature_mapping_file = self.config['DATALINK_FEATURE']['feature_mapping']
    if len(feature_mapping_file) > 0:
      self.dashan.insert_datalink_feature_mapping(self.datalink_id, \
        feature_mapping_file, commit=False)
    self.dashan.conn.commit()
    self.dashan.disconnect()

  def load_config(self):
    config_file_path = os.path.join(self.datalink_folder, 'datalink.conf')
    # load data link config file
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config_file_path)
    self.config = {}
    self.config['DATALINK'] = \
      self._config_section_map(config_parser, 'DATALINK')
    self.config['DATALINK_CONNECTION_SETTING'] = \
      self._config_section_map(config_parser, 'DATALINK_CONNECTION_SETTING')
    self.config['DATALINK_FEATURE'] = \
      self._config_section_map(config_parser, 'DATALINK_FEATURE')
    self.config['DATALINK_EX'] = \
      self._config_section_map(config_parser, 'DATALINK_EXTENSIONS')

    if self.config['DATALINK']['datalink_type'] == 'DBLink':
      self.config['DATALINK_SQL'] = \
        self._config_section_map(config_parser, 'DATALINK_SQL')
    print self.config['DATALINK_FEATURE']
    if 'online_prediction_feature_weights' in self.config['DATALINK_FEATURE']:
      self.online_prediction_feature_weights = \
        self.config['DATALINK_FEATURE']['online_prediction_feature_weights']
    if 'cdm_g_csv' in self.config['DATALINK_FEATURE']:
      self.cdm_g_csv = self.config['DATALINK_FEATURE']['cdm_g_csv']

  def call(self, cmd, args=None):
    methodToCall = getattr(self, cmd)
    if methodToCall:
      if args:
        return methodToCall(args=args)
      else:
        return methodToCall()
    else:
      datalink_type = self.get_datalink_attribute('datalink_type')
      if datalink_type == "WSLink":
        self.call_wslink(cmd, args)

  def _get_feature_description_report(self, features, dictionary):
    num = len(features)
    features_description = {}
    for fid in features:
      description = dictionary[fid]['description'].lower()
      if description in features_description:
        features_description[description].append(fid)
      else:
        features_description[description] = [fid]

    report = "All features: %s \n Total number of features:%s\n" % (features, num)
    for desc in features_description:
      report += desc + ":\n" + " ".join(features_description[desc]) + "\n"
    return report

  def calculate_trewscore(self, job_id):
    datalink_type = self.get_datalink_attribute('datalink_type')
    cdm_feature_dict = self.dashan.get_cdm_feature_dictionary()
    # list the measured features for online prediction
    features_with_intermediates = self.get_features_with_intermediates(\
      self.feature_weights.keys(), cdm_feature_dict)
    measured_features = [fid for fid in features_with_intermediates if\
      cdm_feature_dict[fid]["is_measured"] ]
    self.log.info("The measured features in online prediction: %s" \
      % self._get_feature_description_report(measured_features, cdm_feature_dict))

    fillin_features = [fid for fid in features_with_intermediates if\
      cdm_feature_dict[fid]["is_measured"] and \
      cdm_feature_dict[fid]["category"] == "TWF"]
    self.log.info("The fillin features in online prediction: %s" % fillin_features)

    # list the derive features for online prediction
    derive_features = [fid for fid in features_with_intermediates if\
      not cdm_feature_dict[fid]["is_measured"]]
    self.log.info("The derive features in online prediction: %s" % derive_features)

    target = 'workspace.'+job_id
    # call fill-in function
    self.log.info("start fillin")
    self.dashan.fillin(fillin_features, recalculate_popmean=False,
               target=target)
    self.log.info("complish fillin")

    # call derive functions
    self.log.info("start derive")
    self.dashan.derive(derive_features, cdm_feature_dict, target=target)
    self.log.info("complish derive")
    # calcualte prediction in database

    self.log.info("start prediction")
    self.dashan.predict(self.feature_weights, self.max_score, self.min_score, \
      cdm_feature_dict, target=target)
    self.log.info("complish prediction")

  def calculate_online_trewscore(self, results, args=None):
    # DEPRECATED
    # args: 't', 'f', 'd', 'p'
    # stands for transform, fillin, derive, predict
    # extraction, transformation, and loading
    datalink_type = self.get_datalink_attribute('datalink_type')
    cdm_feature_dict = self.dashan.get_cdm_feature_dictionary()
    do_transform = do_fillin = do_derive = do_predict = False
    if args:
      if 't' in args:
        do_transform = True
      if 'f' in args:
        do_fillin = True
      if 'd' in args:
        do_derive = True
      if 'p' in args:
        do_predict = True
    else:
      do_transform = do_fillin = do_derive = do_predict = True
    if datalink_type == 'WSLink':
      # list the measured features for online prediction
      features_with_intermediates = self.get_features_with_intermediates(\
        self.feature_weights.keys(), cdm_feature_dict)
      measured_features = [fid for fid in features_with_intermediates if\
        cdm_feature_dict[fid]["is_measured"] ]
      self.log.info("The measured features in online prediction: %s" \
        % self._get_feature_description_report(measured_features, cdm_feature_dict))

      fillin_features = [fid for fid in features_with_intermediates if\
        cdm_feature_dict[fid]["is_measured"] and \
        cdm_feature_dict[fid]["category"] == "TWF"]
      self.log.info("The fillin features in online prediction: %s" % fillin_features)

      # list the derive features for online prediction
      derive_features = [fid for fid in features_with_intermediates if\
        not cdm_feature_dict[fid]["is_measured"]]
      self.log.info("The derive features in online prediction: %s" % derive_features)

      # create workspace database tables for this batch process
      self.create_batch_workspace(hours=12)

      # all batch processes run inside the workspace
      # call transform method
      if do_transform:
        self.log.info("start transformation")
        self.wslink_transform(cdm_feature_dict, target='workspace', results=results)
        self.log.info("complish transformation")
      if do_fillin:
        # call fill-in function
        self.log.info("start fillin")
        self.dashan.fillin(fillin_features, recalculate_popmean=False,
                   target='workspace')
        self.log.info("complish fillin")
      if do_derive:
        # call derive functions
        self.log.info("start derive")
        self.dashan.derive(derive_features, cdm_feature_dict, target='workspace')
        self.log.info("complish derive")
      # calcualte prediction in database
      if do_predict:
        self.log.info("start prediction")
        self.dashan.predict(self.feature_weights, self.max_score, self.min_score, \
          cdm_feature_dict, target='workspace')
        self.log.info("complish prediction")
        # update workspace to the main space
        self.load_batch_workspace()
    else:
      self.error('this datalink is not an online datalink')

  def etl(self, args=None):
    self.log.info("enter ETL process")
    # extraction, transformation, and loading
    datalink_type = self.get_datalink_attribute('datalink_type')
    cdm_feature_dict = self.dashan.get_cdm_feature_dictionary()
    do_transform = do_fillin = do_derive = False
    if args:
      if 't' in args:
        do_transform = True
      if 'f' in args:
        do_fillin = True
      if 'd' in args:
        do_derive = True
    else:
      do_transform = do_fillin = do_derive = True
    if datalink_type == 'WSLink':
      # list the measured features for online prediction
      features_with_intermediates = self.get_features_with_intermediates(\
        self.feature_weights.keys(), cdm_feature_dict)
      measured_features = [fid for fid in features_with_intermediates if\
        cdm_feature_dict[fid]["is_measured"] ]
      self.log.info("The measured features in online prediction: %s" \
        % self._get_feature_description_report(measured_features, cdm_feature_dict))

      fillin_features = [fid for fid in features_with_intermediates if\
        cdm_feature_dict[fid]["is_measured"] and \
        cdm_feature_dict[fid]["category"] == "TWF"]
      self.log.info("The fillin features in online prediction: %s" % fillin_features)

      # list the derive features for online prediction
      derive_features = [fid for fid in features_with_intermediates if\
        not cdm_feature_dict[fid]["is_measured"]]
      self.log.info("The derive features in online prediction: %s" % derive_features)

      # create workspace database tables for this batch process
      self.create_batch_workspace(hours=12)

      # all batch processes run inside the workspace
      # call transform method
      if do_transform:
        self.log.info("start transformation")
        self.wslink_transform(cdm_feature_dict, target='workspace')
        self.log.info("complish transformation")
      if do_fillin:
        # call fill-in function
        self.log.info("start fillin")
        self.dashan.fillin(fillin_features, recalculate_popmean=False,
                   target='workspace')
        self.log.info("complish fillin")
      if do_derive:
        # call derive functions
        self.log.info("start derive")
        self.dashan.derive(derive_features, cdm_feature_dict, target='workspace')
        self.log.info("complish derive")
      # calcualte prediction in database
      if args and 'p' in args:
        self.log.info("start prediction")
        self.dashan.predict(self.feature_weights, self.max_score, self.min_score, \
          cdm_feature_dict, target='workspace')
        self.log.info("complish prediction")
        # update workspace to the main space
        self.load_batch_workspace()


    elif datalink_type == 'DBLink':
      # import patients
      if do_transform:
        self.log.info("start import patients")
        self.import_patients()
        self.log.info("complete import patients")
        # populate features into database
        self.log.info("start populating raw features")
        populate_cdm(self.dashan.name, self.datalink_id)
        self.log.info("complete populating raw features")
      if do_fillin:
        # fill-in missing values
        self.log.info("start filling in missing values")
        fillin_cdm_twf(self.dashan.name)
        self.log.info("complete filling in missing values")
      if do_derive:
        # derive new features
        self.log.info("start deriving features")
        derive_main(self.dashan)
        self.log.info("complete deriving features")

  def create_batch_workspace(self, hours=24):
    create_workspace_sql = """
    drop table IF EXISTS cdm_twf_workspace;
    create table cdm_twf_workspace as
    select * from cdm_twf where now() - tsp  < interval '%s hours';
    alter table cdm_twf_workspace add primary key (enc_id, tsp);
    """ % hours
    #self.dashan.connect()
    server_cursor = self.dashan.query_with_sql(create_workspace_sql)
    server_cursor.close()
    #self.dashan.disconnect()

  def load_batch_workspace(self):
    load_sql = """
    delete from cdm_twf
    where tsp >= (select min(tsp) from cdm_twf_workspace);
    insert into cdm_twf
    (
      select * from cdm_twf_workspace
      );

    create table if not exists trews (like trews_workspace);

    delete from trews
    where tsp >= (select min(tsp) from trews_workspace);
    insert into trews
    (
      select * from trews_workspace
      );
    """
    self.dashan.connect()
    server_cursor = self.dashan.query_with_sql(load_sql)
    server_cursor.close()
    self.dashan.disconnect()

  def get_features_with_intermediates(self, features, dictionary):
    output = set()
    feature_set = set(features)
    while len(feature_set) > 0:
      for fid in feature_set.copy():
        if not dictionary[fid]['is_measured']:
          fid_input_str = dictionary[fid]['derive_func_input']
          fid_input = [item.strip() for item in fid_input_str.split(',')]
          for fid_in in fid_input:
            feature_set.add(fid_in)
        output.add(fid)
        feature_set.remove(fid)
    return output



  def wslink_transform(self, cdm_feature, target='main', results=None):
    """
    transform the measure features for online prediction using jhapi main function
    """
    if not results:
      conn_settings = self.config['DATALINK_CONNECTION_SETTING']
      api_module = conn_settings['api_module']
      api_method = conn_settings['api_method']
      api_args = conn_settings['api_args']
      cmd = "%s %s %s" % (api_module, api_method, api_args)
      results = self.call_wslink(cmd)

    # save results to CDM
    # insert patients to PAT_ENC
    self.log.info("update PAT_ENC")
    patients = [pt_result[0] for pt_result in results if len(pt_result) > 0]
    patients_with_enc_ids = self.update_pat_enc(patients)
    for patient in patients_with_enc_ids['patients']:
      for fid in patient:
        if fid in cdm_feature:
          self.transform_feature(patient['enc_id'], fid, patient[fid], cdm_feature)
    # insert measurements
    self.log.info("insert measurements")
    for pt_results in results:
      for result in pt_results:
        tsp = result['tsp']
        visit_id = result['visit_id']
        self.log.info("insert measurements for %s " % visit_id)
        for meas in result['measurements']:
          try:
            enc_id = patients_with_enc_ids['visit_id_to_enc_id'][visit_id]
            self.transform_feature(enc_id, meas['fid'], meas['value'],
                         cdm_feature, tsp, target=target)
          except Exception as e:
            self.log.error(str(e) + str(meas))


  def transform_feature(self, enc_id, fid, value, cdm_feature, tsp=None, target='main'):
    """
    pt : patient encounter info
    raw: extracted raw value
    cdm_feature: CDM feature dictionary
    """
    data_type = cdm_feature[fid]['data_type']
    is_no_add = True
    # TODO enable is_add for medications and I/O features
    # if 'is_no_add' in mapping:
    #     is_no_add = mapping['is_no_add']
    category = cdm_feature[fid]['category']
    transform_func_id = None
    row = None
    if category == "S":
      row = [value]
    else:
      row = [tsp, value]
    if row:
      self.save_row_to_cdm(fid, category, data_type, enc_id, row,
                 self.dashan, is_no_add, target=target)

  def save_row_to_cdm(self, fid, category, data_type, enc_id,
            row, cdm, is_no_add, confidence=0, target='main'):
    '''
    We assume all values' is_no_add is true, i.e., the API has already merged them.
    '''

    value = row[-1]
    if category == 'S':
      cdm.upsert_s([enc_id, fid, str(value), confidence])
    elif category == 'T':
      tsp = row[0]
      cdm.upsert_t([enc_id, tsp, fid, str(value), confidence])
    elif category == 'TWF':
      tsp = row[0]
      if data_type == 'Real':
        value = float(value)
      elif data_type == 'Integer':
        value = int(value)
      if target=='main':
        cdm.upsert_twf([enc_id, tsp, fid, value, confidence])
      elif target=='workspace':
        cdm.upsert_twf_workspace([enc_id, tsp, fid, value, confidence])

  # def save_row_to_cdm(self, fid, category, data_type, enc_id, row, cdm, is_no_add, confidence=0):
  #     value = row[-1]
  #     if category == 'S':
  #         if is_no_add:
  #             cdm.upsert_s([enc_id, fid, str(value), confidence])
  #         else:
  #             cdm.add_s([enc_id, fid, str(value), confidence])
  #     elif category == 'T':
  #         tsp = row[0]
  #         if is_no_add:
  #             cdm.upsert_t([enc_id, tsp, fid, str(value), confidence])
  #         else:
  #             cdm.add_t([enc_id, tsp, fid, str(value), confidence])
  #     elif category == 'TWF':
  #         tsp = row[0]
  #         if data_type == 'Real':
  #             value = float(value)
  #         elif data_type == 'Integer':
  #             value = int(value)
  #         if is_no_add:
  #             cdm.upsert_twf([enc_id, tsp, fid, value, confidence])
  #         else:
  #             cdm.add_twf([enc_id, tsp, fid, value, confidence])

  def extract(self, api, pat_id=None, visit_id=None):
    """
    extract data from data source
    """
    api_method = api['method']
    method_args = api['method_args']
    self.call_wslink("%s %s %s" % (api_method))

  def insert_pat_enc(self, patients):
    """
    insert patients into pat_enc assuming pat_enc is empty
    return current list of enc_ids
    """
    # map external patient id to enc_id
    colnames_clause = "pat_id, visit_id"
    pat_enc = PatEnc(self.dashan.config)
    pat_enc.connect()
    new_pts = 0
    for pat in patients:
      pat_enc.insert(colnames_clause, [pat['pat_id'], pat['visit_id']])
      new_pts += 1
    visit_id_to_enc_id = pat_enc.get_visit_id_to_enc_id_mapping()
    for pat in patients:
      pat['enc_id'] = visit_id_to_enc_id[pat['visit_id']]
    pat_enc.disconnect()
    self.log.info("Patients: total new: %s" % new_pts)
    return patients

  def update_pat_enc(self, patients):
    """
    upsert patients into pat_enc
    return current list of enc_ids
    """
    # map external patient id to enc_id
    colnames_clause = "pat_id, visit_id"
    pat_enc = PatEnc(self.dashan.config)
    pat_enc.connect()
    visit_id_to_enc_id = pat_enc.get_visit_id_to_enc_id_mapping()
    exist_pts = 0
    new_pts = 0
    for pat in patients:
      if pat['visit_id'] in visit_id_to_enc_id:
        exist_pts += 1
      else:
        pat_enc.insert(colnames_clause, [pat['pat_id'], pat['visit_id']])
        new_pts += 1
    visit_id_to_enc_id = pat_enc.get_visit_id_to_enc_id_mapping()
    for pat in patients:
      pat['enc_id'] = visit_id_to_enc_id[pat['visit_id']]
    pat_enc.disconnect()
    self.log.info("Patients: total: %s, exist: %s, new: %s" \
      % (len(patients), exist_pts, new_pts))
    return {'patients':patients, 'visit_id_to_enc_id':visit_id_to_enc_id}

  # def transform_feature(self, pt, raw, mapping, cdm_feature):
  #     """
  #     pt : patient encounter info
  #     raw: extracted raw value
  #     mapping: feature mapping info
  #     cdm_feature: CDM feature dictionary
  #     """
  #     fid = mapping['fid']
  #     transform_func_id = None
  #     if 'transform_func_id' in mapping:
  #         transform_func_id = mapping['transform_func_id']
  #     data_type = cdm_feature[fid]['data_type']
  #     is_no_add = True
  #     if 'is_no_add' in mapping:
  #         is_no_add = mapping['is_no_add']
  #     category = cdm_feature[fid]['category']
  #     enc_id = pt['enc_id']

  #     if not isinstance(raw, list):
  #         raw_list = [raw]
  #     else:
  #         raw_list = raw
  #     for raw in raw_list:
  #         if category == "S":
  #             if fid in raw:
  #                 raw_entry = [raw[fid]]
  #             else:
  #                 # fid may not exist, e.g., diagnosis
  #                 raw_entry = None
  #         else:
  #             raw_entry = [raw['fid'], raw['tsp'], raw['value']]
  #         if raw_entry:
  #             result = transform.transform(fid, transform_func_id, raw_entry,
  #                                          data_type, self.log)
  #             if result is not None:
  #                 self.save_result_to_cdm(fid, category, enc_id, raw_entry, result,
  #                                         self.dashan, is_no_add)


  # def etl_features(self, pt):
  #     flowsheets_values = self.call_wslink("flowsheet post %s JHHMRN %s CSN" %\
  #         (pt['pat_id'], pt['visit_id']))
  #     for flowsheet_row in flowsheets_values:
  #         # test spo2
  #         if flowsheet_row['measurement'] == "SpO2":
  #             fid = "spo2"
  #             transform_func_id = None
  #             data_type = 'Real'
  #             category = "TWF"
  #             enc_id = pt['enc_id']
  #             is_no_add = True
  #             result = transform.transform(fid, \
  #                 transform_func_id, flowsheet_row, data_type, self.log)
  #             if result is not None:
  #                 self.save_result_to_cdm(fid, category, enc_id, \
  #                     flowsheet_row, result, self.dashan, is_no_add)

  def import_patients(self, dblink_id=None, num_ids=100000):
    # insert patient encounters to pat_enc
    if not dblink_id:
      dblink_id = self.datalink_id
    pat_enc = PatEnc(self.dashan.config)
    pat_enc.connect()
    # connect to dblink database
    dblink_conn = self.connect_dblink()
    # all select methods return a server-side cursor
    dblink_cursor = self.select_patients(dblink_conn, dblink_id)
    rows = dblink_cursor.fetchmany(num_ids)
    colnames = [desc[0] for desc in dblink_cursor.description]
    colnames_clause = ','.join(colnames)
    sum_ids = 0
    while rows:
      pat_enc.insert(colnames_clause, rows)
      sum_ids = sum_ids + len(rows)
      self.log.info('inserted %s patient encounters' % sum_ids)
      rows = dblink_cursor.fetchmany(num_ids)
    # the server-side cursor need to be closed afterwards.
    dblink_cursor.close()
    dblink_conn.close()
    pat_enc.disconnect()

  def call_wslink(self, cmd):
    self.log.info("call_wslink:" + cmd)
    cmd_args = cmd.split(" ")
    api = cmd_args[0]
    api_method = cmd_args[1]
    api_method_args = None

    conn_settings = self.config['DATALINK_CONNECTION_SETTING']
    api_dir = conn_settings['api_dir']
    api_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),\
      api_dir)
    # self.log.info("load api path:%s" % api_path)
    sys.path.append(api_path)

    mod = importlib.import_module(api)
    method = getattr(mod, api_method)
    results = None
    if len(cmd_args) == 2:
      results = method()
    elif len(cmd_args) == 3:
      api_method_args = cmd_args[2]
      results = method(api_method_args)
    elif len(cmd_args) > 3:
      api_method_args = cmd_args[2:]
      print api_method_args
      results = method(*api_method_args)
    return results


  def _config_section_map(self, config_parser, section):
    dict1 = {}
    options = config_parser.options(section)
    for option in options:
      try:
        dict1[option] = config_parser.get(section, option)
        if dict1[option] == -1:
          self.log.warning(\
            "Config DataSource warning: skip option %s" % option)
      except:
        self.log.error("Config DataSource Exception on %s" % option)
    return dict1


  def select_feature_mapping(self):
    sql = """
    SELECT * FROM datalink_feature_mapping
    INNER JOIN cdm_feature ON datalink_feature_mapping.fid = cdm_feature.fid
    WHERE datalink_id = '%s' AND cdm_feature.is_deprecated = 'f'
    AND cdm_feature.is_measured = 't'
    ORDER BY cdm_feature.fid
    """ % self.datalink_id
    server_cursor = self.dashan.select_with_sql(sql)
    mapping = server_cursor.fetchall()
    server_cursor.close()
    return mapping

  def populate_datalink_feature_to_cdm(self, mapping, visit_id_dict,
                     pat_id_dict, plan=False):
    cdm = self.dashan
    datalink_id = mapping['datalink_id']
    data_type = mapping['data_type']
    datalink_conn = self.connect_dblink()
    fid = mapping['fid']
    self.log.info('importing feature value fid %s' % fid)
    transform_func_id = mapping['transform_func_id']
    if transform_func_id:
      self.log.info("transform func: %s" % transform_func_id)
    attributes = cdm.get_feature_attributes(fid)
    category = attributes['category']
    is_no_add = mapping['is_no_add']
    is_med_action = mapping['is_med_action']
    self.log.info("is_no_add: %s, is_med_action: %s" \
      % (is_no_add, is_med_action))
    # print fid, category, transform_func_id
    visit_id_name = self.get_visit_id_naming()
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    start = timeit.default_timer()
    line = 0
    if is_med_action and fid != 'vent':
      # process medication action input for HC_EPIC
      # order by csn_id, medication id, and timeActionTaken
      orderby = """ "CSN_ID", "MEDICATION_ID", "TimeActionTaken" """
      dblink_cursor = self.select_dblink_feature_values(datalink_conn, \
        mapping, visit_id_name, orderby=orderby, plan=plan)
      if not plan:
        cur_enc_id = None
        cur_med_id = None
        cur_med_events = []
        for row in dblink_cursor:
          if str(row['visit_id']) in visit_id_dict:
            enc_id = visit_id_dict[str(row['visit_id'])]
            med_id = row['MEDICATION_ID']
            # print "cur", row
            if cur_enc_id is None \
              or cur_enc_id != enc_id or med_id != cur_med_id:
              # new enc_id, med_id pair
              if cur_enc_id is not None and cur_med_id is not None:
                # process cur_med_events
                len_of_events = len(cur_med_events)
                if len_of_events > 0:
                  line += len_of_events
                  self.process_med_events(cur_enc_id, \
                    cur_med_id, cur_med_events, fid_info, \
                    mapping, cdm)

              cur_enc_id = enc_id
              cur_med_id = med_id
              cur_med_events = []
            # print "cur events temp:", cur_med_events
            cur_med_events.append(row)
            # print "cur events:", cur_med_events
        dblink_cursor.close()
        if cur_enc_id is not None and cur_med_id is not None:
          # process cur_med_events
          len_of_events = len(cur_med_events)
          if len_of_events > 0:
            line += len_of_events
            self.process_med_events(cur_enc_id, cur_med_id,
                        cur_med_events, fid_info, mapping,
                        cdm)
    elif is_med_action and fid == 'vent':
      orderby = " icustay_id, realtime"
      dblink_cursor = self.select_dblink_feature_values(datalink_conn, \
        mapping, visit_id_name, orderby=orderby, plan=plan)
      if not plan:
        cur_enc_id = None
        cur_vent_events = []
        for row in dblink_cursor:
          if str(row['visit_id']) in visit_id_dict:
            enc_id = visit_id_dict[str(row['visit_id'])]
            if enc_id:
              if cur_enc_id is None or cur_enc_id != enc_id:
                # new enc_id, med_id pair
                if cur_enc_id:
                  # process cur_med_events
                  len_of_events = len(cur_vent_events)
                  if len_of_events > 0:
                    line += len_of_events
                    self.process_vent_events(cur_enc_id, \
                      cur_vent_events, fid_info, mapping, cdm)
                cur_enc_id = enc_id
                cur_vent_events = []
              # print "cur events temp:", cur_med_events
              cur_vent_events.append(row)
              # print "cur events:", cur_med_events
        dblink_cursor.close()
        if cur_enc_id and len_of_events > 0:
          line += len_of_events
          self.process_vent_events(cur_enc_id, cur_vent_events, \
            fid_info, mapping, cdm)
    else:
      # process features unrelated to med actions
      dblink_cursor = self.select_dblink_feature_values(\
        datalink_conn, mapping, visit_id_name, plan=plan)
      if not plan:
        line = 0
        start = timeit.default_timer()
        pat_id_based = 'pat_id' in mapping['select_cols']
        for row in dblink_cursor:
          if pat_id_based:
            if str(row['pat_id']) in pat_id_dict:
              enc_ids = pat_id_dict[str(row['pat_id'])]
              for enc_id in enc_ids:
                result = transform.transform(fid, \
                  transform_func_id, row, data_type, self.log)
                if result is not None:
                  line += 1
                  self.save_result_to_cdm(fid, category, enc_id, \
                    row, result, cdm, is_no_add)
          elif str(row['visit_id']) in visit_id_dict:
            enc_id = visit_id_dict[str(row['visit_id'])]
            if enc_id:
              # transform return a result containing both value and
              # confidence flag
              result = transform.transform(fid, transform_func_id, \
                row, data_type, self.log)
              # print row, result
              if result is not None:
                line += 1
                self.save_result_to_cdm(fid, category, enc_id, \
                  row, result, cdm, is_no_add)
          if line > 0 and line % 10000 == 0:
            self.log.info('import rows %s', line)
        dblink_cursor.close()
    if not plan:
      duration = timeit.default_timer() - start
      if line == 0:
        self.log.warn(\
          'stats: Zero line found in dblink for fid %s %s s' \
            % (fid, duration))
      else:
        self.log.info(\
          'stats: %s valid lines found in dblink for fid %s %s s' \
            % (line, fid, duration))
    datalink_conn.close()

  def _get_datalink_conn_string(self):
    connection_setting_json = self.config['DATALINK_CONNECTION_SETTING']
    datalink_conn_settings = []

    for key in connection_setting_json:
      datalink_conn_settings.append("%s=%s" \
        % (key, connection_setting_json[key]))
    if connection_setting_json:
      return ', '.join(datalink_conn_settings)
    else:
      return None

  def get_visit_id_naming(self):
    sql_file_path = os.path.join(self.datalink_folder, \
      self.config['DATALINK_SQL']['sql_query_patients'])
    with open(sql_file_path, 'r') as f:
      sql_import_patients = f.read()
    # get select clause
    select_clause = sql_import_patients[6:sql_import_patients.index('FROM')]
    cols = select_clause.split(',')
    for col in cols:
      if 'visit_id' in col:
        return col.strip()
    return None

  def connect_dblink(self):
    datalink_conn_string = self._get_datalink_conn_string()
    return self.connect(db_conn_string=datalink_conn_string)



  def get_datalink_attribute(self, attribute):
    return self.config['DATALINK'][attribute]



  """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
  below is legacy code
  """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""






  def extend_feature(self, feature_ex_file):
    with open(feature_ex_file) as f:
      reader = csv.reader(f, delimiter=',')
      header = None
      for row in reader:
        if not header:
          header = ",".join(row)
        else:
          self.dashan.upsert_cdm_feature(header, row)

  def extend_function(self, func_ex_file):
    with open(func_ex_file) as f:
      reader = csv.reader(f, delimiter=',')
      header = None
      for row in reader:
        if not header:
          header = ",".join(row)
        else:
          self.dashan.upsert_cdm_function(header, row)











  def delete(self, dblink_id):
    # ambiguous name
    where_conditions = "dblink_id='%s'" % dblink_id
    Resource.delete(self, self.conn, 'dblink_feature_mapping', where_conditions)
    cdm = CDM(self.config)
    cdm.set_conn(self.conn)
    cdm_where_conditions = \
      "enc_id in (SELECT enc_id FROM pat_enc WHERE dblink_id='%s')" \
        % dblink_id
    cdm.delete(cdm_where_conditions)
    pat_enc = PatEnc(self.config)
    pat_enc.set_conn(self.conn)
    pat_enc.delete(where_conditions)
    Resource.delete(self, self.conn, 'dblink', where_conditions)



  def select_patients(self, dblink_conn, dblink_id, sql_filter=None):
    dblink_folder = os.path.join(self.dashan.config.DATALINK_DIR, dblink_id)
    sql_file_path = os.path.join(dblink_folder, \
      self.config['DATALINK_SQL']['sql_query_patients'])
    with open(sql_file_path, 'r') as f:
      sql_import_patients = f.read()
    if sql_filter:
      sql_import_patients += " " + sql_filter
    return self.select_with_sql(sql_import_patients, dblink_conn)





  def get_extract_sql(self, mapping, visit_id_dict):
    dblink_id = mapping['dblink_id']

    visit_id_name = self.get_visit_id_naming(dblink_id)
    select_clause = "%s," % visit_id_name + mapping['select_cols']
    dbtable = mapping['dbtable']
    where_clause = mapping['where_conditions']
    if where_clause is None:
      where_clause = ''
    sql = "SELECT %s FROM %s %s" % (select_clause, dbtable, where_clause)
    return sql

  # def process_med_events(self, enc_id, med_name, med_events,
  #                        fid_info, mapping, cdm):
  #     self.log.debug("\nentries from HC_EPIC:")
  #     for row in med_events:
  #         self.log.debug(row)
  #     fid = fid_info['fid']
  #     self.log.debug("transformed entries:")
  #     self.log.debug("transform function: %s" % mapping['transform_func_id'])
  #     dose_intakes = transform.transform(fid, mapping['transform_func_id'], \
  #         med_events, fid_info['data_type'])
  #     if dose_intakes is not None and len(dose_intakes) > 0:
  #         for intake in dose_intakes:
  #             if intake is None:
  #                 self.log.warn("transform function returns None!")
  #             else:
  #                 self.log.debug(intake)
  #                 # dose intake are T category
  #                 tsp = intake[0]
  #                 volume = str(intake[1])
  #                 confidence = intake[2]
  #                 if fid_info['is_no_add']:
  #                     cdm.upsert_t([enc_id, tsp, fid, volume, confidence])
  #                 else:
  #                     cdm.add_t([enc_id, tsp, fid, volume, confidence])

  def _transform_med_action(self, fid, mapping, event, fid_info, cdm, enc_id):
    dose_entry = transform.transform(fid, mapping['transform_func_id'], \
      event, fid_info['data_type'], self.log)
    if dose_entry:
      self.log.debug(dose_entry)
      tsp = dose_entry[0]
      volume = str(dose_entry[1])
      confidence = dose_entry[2]
      if fid_info['is_no_add']:
        cdm.upsert_t([enc_id, tsp, fid, volume, confidence])
      else:
        cdm.add_t([enc_id, tsp, fid, volume, confidence])



  def process_med_events(self, enc_id, med_id, med_events,
               fid_info, mapping, cdm):
    med_route = med_events[0]['MedRoute']
    med_name = med_events[0]['display_name']
    self.log.debug("\nentries from med_id %s, med_route %s:" \
      % (med_id, med_route))
    for row in med_events:
      self.log.debug(row)
    fid = fid_info['fid']
    self.log.debug("transformed entries:")
    self.log.debug("transform function: %s" % mapping['transform_func_id'])
    dose_intakes = transform.transform(fid, mapping['transform_func_id'], \
      med_events, fid_info['data_type'], self.log)
    if dose_intakes is not None and len(dose_intakes) > 0:
      for intake in dose_intakes:
        if intake is None:
          self.log.warn("transform function returns None!")
        else:
          self.log.debug(intake)
          # dose intake are T category
          tsp = intake[0]
          volume = str(intake[1])
          confidence = intake[2]

          if fid_info['is_no_add']:
            cdm.upsert_t([enc_id, tsp, fid, volume, confidence])
          else:
            cdm.add_t([enc_id, tsp, fid, volume, confidence])

  def process_vent_events(self, enc_id, vent_events, fid_info, mapping, cdm):
    self.log.debug("\nentries from enc_id %s:" % enc_id)
    for row in vent_events:
      self.log.debug(row)
    fid = fid_info['fid']
    self.log.debug("transformed entries:")
    self.log.debug("transform function: %s" % mapping['transform_func_id'])
    vent_results = transform.transform(fid, mapping['transform_func_id'], \
      vent_events, fid_info['data_type'], self.log)
    if vent_results is not None and len(vent_results) > 0:
      for result in vent_results:
        if result is None:
          self.log.warn("transform function returns None!")
        else:
          self.log.debug(result)
          # dose result are T category
          tsp = result[0]
          on_off = str(result[1])
          confidence = result[2]

          if fid_info['is_no_add']:
            cdm.upsert_t([enc_id, tsp, fid, on_off, confidence])
          else:
            cdm.add_t([enc_id, tsp, fid, on_off, confidence])



  def save_result_to_cdm(self, fid, category, enc_id, row, results, cdm, \
    is_no_add):
    if not isinstance(results[0], list):
      results = [results]
    for result in results:
      tsp = None
      if len(result) == 3:
        # contain tsp, value, and confidence
        tsp = result[0]
        result.pop(0)
      value = result[0]
      confidence = result[1]
      if category == 'S':
        if is_no_add:
          cdm.upsert_s([enc_id, fid, str(value), confidence])
        else:
          cdm.add_s([enc_id, fid, str(value), confidence])
      elif category == 'M':
        if is_no_add:
          cdm.upsert_m([enc_id, fid, row[1], str(value), confidence])
        else:
          cdm.add_m([enc_id, fid, row[1], str(value), confidence])
      elif category == 'T':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if tsp is not None:
          if is_no_add:
            cdm.upsert_t([enc_id, tsp, fid, str(value), confidence])
          else:
            cdm.add_t([enc_id, tsp, fid, str(value), confidence])
      elif category == 'TWF':
        if tsp is None:
          if isinstance(row, dict):
            tsp = row['tsp']
          else:
            if len(row) >= 4:
              tsp = row[2]
            else:
              tsp = row[1]
        if is_no_add:
          cdm.upsert_twf([enc_id, tsp, fid, value, confidence])
        else:
          cdm.add_twf([enc_id, tsp, fid, value, confidence])







  def select_dblink_feature_values(self, dblink_conn, mapping,
                   visit_id_name, orderby=None, plan=False):
    sql = self.get_dblink_feature_sql_query(mapping, visit_id_name, orderby)
    if plan:
      sql += " limit 100"
      for row in self.select_with_sql(sql, dblink_conn):
        self.log.info(row)
    else:
      return self.select_with_sql(sql, dblink_conn)

  def get_dblink_feature_sql_query(self, mapping, visit_id_name, orderby=None):
    if 'subject_id' in mapping['select_cols'] \
      and 'pat_id' not in mapping['select_cols']:
      # hist features in mimic
      select_clause = \
        mapping['select_cols'].replace("subject_id", "subject_id pat_id")
    elif 'visit_id' in mapping['select_cols'] \
      or 'pat_id' in mapping['select_cols']:
      select_clause = mapping['select_cols']
    else:
      select_clause = ("%s," % visit_id_name) + mapping['select_cols']
    dbtable = mapping['dbtable']
    where_clause = mapping['where_conditions']
    if where_clause is None:
      where_clause = ''
    sql = "SELECT %s FROM %s %s" % (select_clause, dbtable, where_clause)
    if orderby:
      sql += " order by " + orderby
    self.log.info("sql: %s" % sql)
    return sql

