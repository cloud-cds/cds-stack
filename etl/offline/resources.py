"""
resources.py
"""
import ConfigParser
import os
import dbapi_postgresql as dbapi
import json
import csv
import psycopg2
from psycopg2.extras import DictRow
import datetime
# import function.transform as transform
# import function.fillin as fillin
from transforms.primitives.row import transform
import timeit
from create_database import create_cdm_twf

class Resource:
  def __init__(self, config):
    self.config = config
    self.db_conn_string = self.config.get_db_conn_string()
    self.log = self.config.log

  def query_with_sql(self, sql):
    return dbapi.query(self.conn, sql)

  def insert(self, conn, dbtable, cols, values, \
    commit=True, close_cursor=True):
    return dbapi.insert(conn, dbtable=dbtable, cols=cols, values=values,\
      commit=commit, close_cursor=close_cursor)

  def update(self, conn, dbtable, cols, values, where_conditions, \
    commit=True, close_cursor=True):
    return dbapi.update(conn, dbtable, cols, values,\
      where_conditions, commit=commit, close_cursor=close_cursor)

  def upsert(self, conn, proc, values, \
    commit=True, close_cursor=True):
    if proc == 'merge_cdm_twf' or proc == 'merge_cdm_twf_workspace':
      if type(values[0]) is list or type(values[0]) is DictRow:
        pass
      else:
        values = [values]
      new_values = []
      for value in values:
        # value = [enc_id, tsp, fid, value, confidence]
        update_set_cols = "%s,%s_c" \
        % (value[2], value[2])
        update_set_values = ','.join(self._map_str(value[3:]))
        update_where = "enc_id = %s AND tsp = '%s'" % (value[0], value[1])
        insert_cols = "enc_id, tsp, " + update_set_cols
        del value[2]
        insert_values = ','.join(self._map_str(value))
        new_value = [update_set_cols, update_set_values, update_where,\
          insert_cols, insert_values]
        new_values.append(new_value)
      return dbapi.callproc(conn, proc, new_values, \
        commit=commit, close_cursor=close_cursor)
    else:
      return dbapi.callproc(conn, proc, values, \
        commit=commit, close_cursor=close_cursor)

  def add(self, conn, proc, values, \
    commit=True, close_cursor=True):
    if proc == 'add_cdm_s':
      return dbapi.callproc(conn, proc, values, \
        commit=commit, close_cursor=close_cursor)
    elif proc == 'add_cdm_m':
      return dbapi.callproc(conn, proc, values, \
        commit=commit, close_cursor=close_cursor)
    elif proc == 'add_cdm_t':
      return dbapi.callproc(conn, proc, values, \
        commit=commit, close_cursor=close_cursor)
    elif proc == 'merge_cdm_twf':
      if type(values[0]) is list or type(values[0]) is DictRow:
        pass
      else:
        values = [values]
      new_values = []
      for value in values:
        # value = [enc_id, tsp, fid, value, confidence]
        # value should be numeric
        update_set_cols = "%s,%s_c" % (value[2], value[2])
        update_value = "coalease(cast(%s as numeric),0) + %s" \
          % (value[2], value[3])
        update_confidence = "coalease(confidence,0) | " + value[4]
        update_set_values = update_value + " , " + update_confidence
        update_where = "enc_id = %s AND tsp = '%s'" \
          % (value[0], value[1])
        insert_cols = "enc_id, tsp, " + update_set_cols
        del value[2]
        insert_values = ','.join(self._map_str(value))
        new_value = [update_set_cols, update_set_values, update_where,\
          insert_cols, insert_values]
        new_values.append(new_value)
      return dbapi.callproc(conn, proc, new_values, \
        commit=commit, close_cursor=close_cursor)
    else:
      raise UnknownProcError(proc)

  def _map_str(self, value_list):
    n = len(value_list)
    new_list = ['']*n
    for i in range(n):
      if type(value_list[i]) is str or datetime.datetime:
        new_list[i] = "'%s'" % value_list[i]
      else:
        new_list[i] = str(value_list[i])
    return new_list

  def delete(self, conn, dbtable, where_conditions):
    dbapi.delete(conn, dbtable, where_conditions)

  def connect(self, db_conn_string=None, secondary=False):
    if db_conn_string is None:
      # connect to system db
      db_conn_string = self.db_conn_string
      if secondary:
        return dbapi.connect(db_conn_string)
      else:
        self.conn = dbapi.connect(db_conn_string)
        return self.conn
    else:
      # connect to dblink db
      return dbapi.connect(db_conn_string)

  def disconnect(self, conn=None):
    if conn:
      conn.close()
    elif self.conn:
      self.conn.close()

  def get_conn(self):
    return self.conn

  def set_conn(self, conn):
    self.conn = conn

  def select_with_sql(self, sql, conn=None):
    if conn is None:
      conn = self.conn
    return dbapi.select(conn, sql=sql)

  def select(self, conn, dbtable, select_cols=None, \
    where_conditions=None, cursor_name=None, sql_filter=None, itersize=None):
    return dbapi.select(conn, dbtable=dbtable, select_cols=select_cols, \
      where_conditions=where_conditions, sql_filter=sql_filter, \
      cursor_name=cursor_name, itersize=itersize)

  def update_with_sql(self, conn, update_sql, commit=True, close_cursor=True):
    return dbapi.query(conn, update_sql,\
      commit=commit, close_cursor=close_cursor)

  def query_to_csv(self, sql, csv_file):
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)
    cur = self.conn.cursor()
    with open(csv_file, 'w') as f:
      cur.copy_expert(outputquery, f)

class DBLink(Resource):
  def create_dblink(self, dblink_id):
    dblink_folder = os.path.join(self.config.DATALINK_DIR, dblink_id)

    config_file_path = os.path.join(dblink_folder, 'datalink.conf')
    # load config file
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config_file_path)
    dblink_conf = {}
    dblink_conf['DATALINK'] = \
      self._config_section_map(config_parser, 'DATALINK')
    dblink_conf['DATALINK_CONNECTION_SETTING'] = \
      self._config_section_map(config_parser, 'DATALINK_CONNECTION_SETTING')
    dblink_conf['DATALINK_FEATURE'] = \
      self._config_section_map(config_parser, 'DATALINK_FEATURE')
    dblink_conf['DATALINK_SQL'] = \
      self._config_section_map(config_parser, 'DATALINK_SQL')
    dblink_conf['DATALINK_EX'] = \
      self._config_section_map(config_parser, 'DATALINK_EXTENSIONS')

    # configure extension for this dblink
    # function extension
    if dblink_conf['DATALINK_EX']['function_extension']:
      self.extend_function(os.path.join(dblink_folder,\
        dblink_conf['DATALINK_EX']['function_extension']))
    # feature extension
    if dblink_conf['DATALINK_EX']['feature_extension']:
      self.extend_feature(os.path.join(dblink_folder,\
        dblink_conf['DATALINK_EX']['feature_extension']))
    self.insert_dblink(dblink_conf, commit=False)

    # create cdm_twf table
    create_cdm_twf(self.conn)

    # insert dblink features
    feature_mapping_file = dblink_conf['DATALINK_FEATURE']['feature_mapping']
    self.insert_feature_mapping(dblink_id, feature_mapping_file, \
      commit=False)
    self.conn.commit()

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

  def _get_dblink_conn_string(self, dblink_id):
    connection_setting_json = \
      self.get_dblink_attribute(dblink_id, 'connection_setting_json')
    dblink_conn_settings = []

    for key in connection_setting_json:
      dblink_conn_settings.append("%s=%s" \
        % (key, connection_setting_json[key]))
    if connection_setting_json:
      return ', '.join(dblink_conn_settings)
    else:
      return None

  def get_dblink_attribute(self, dblink_id, attribute):
    sql = "SELECT %s FROM datalink WHERE datalink_id='%s'" \
      % (attribute, dblink_id)
    cursor = self.select_with_sql(sql)
    result = cursor.fetchone()[0]
    cursor.close()
    return result

  def extend_feature(self, feature_ex_file):
    with open(feature_ex_file) as f:
      reader = csv.reader(f, delimiter=',')
      header = None
      for row in reader:
        if not header:
          header = ",".join(row)
        else:
          self.upsert_cdm_feature(header, row)

  def extend_function(self, func_ex_file):
    with open(func_ex_file) as f:
      reader = csv.reader(f, delimiter=',')
      header = None
      for row in reader:
        if not header:
          header = ",".join(row)
        else:
          self.upsert_cdm_function(header, row)

  def upsert_cdm_feature(self, cols, values):
    insert_suc = True
    self.log.info("customized feature %s" % values[0])
    for i in range(len(values)):
      if values[i] == '':
        values[i] = None
    try:
      Resource.insert(self, self.conn, 'cdm_feature', cols, values)
    except psycopg2.IntegrityError as e:
      self.conn.rollback()
      insert_suc = False
    if not insert_suc:
      where_conditions = " WHERE fid = '%s'" % values[0]
      Resource.update(self, self.conn, 'cdm_feature', cols, values, \
        where_conditions)

  def upsert_cdm_function(self, cols, values):
    insert_suc = True
    self.log.info("customized function %s" % values[0])
    for i in range(len(values)):
      if values[i] == '':
        values[i] = None
    try:
      Resource.insert(self, self.conn, 'cdm_function', cols, values)
    except psycopg2.IntegrityError as e:
      self.conn.rollback()
      insert_suc = False
    if not insert_suc:
      where_conditions = " WHERE FUNC_ID = '%s'" % values[0]
      Resource.update(self, self.conn, 'cdm_function', cols, values, \
        where_conditions)



  def insert_dblink(self, dblink_conf, commit=True):
    # legacy
    cols = \
    """
    datalink_id,datalink_type,schedule,data_load_type,
    connection_type,connection_setting_json,
    import_patients_sql
    """
    datalink_conn_setting = dblink_conf['DATALINK_CONNECTION_SETTING']
    if datalink_conn_setting['user'] == 'ENV':
      datalink_conn_setting['user'] = os.environ['db_user']

    if datalink_conn_setting['host'] == 'ENV':
      datalink_conn_setting['host'] = os.environ['db_host']

    if datalink_conn_setting['port'] == 'ENV':
      datalink_conn_setting['port'] = os.environ['db_port']

    if datalink_conn_setting['password'] == 'ENV':
      datalink_conn_setting['password'] = os.environ['db_password']

    if datalink_conn_setting['dbname'] == 'ENV':
      datalink_conn_setting['dbname'] = os.environ['db_name']
    values = [\
      dblink_conf['DATALINK']['datalink_id'], \
      dblink_conf['DATALINK']['datalink_type'], \
      dblink_conf['DATALINK']['schedule'], \
      dblink_conf['DATALINK']['data_load_type'], \
      dblink_conf['DATALINK']['connection_type'], \
      json.dumps(dblink_conf['DATALINK_CONNECTION_SETTING']), \
      dblink_conf['DATALINK_SQL']['sql_query_patients']]
    self.insert(self.conn, 'datalink', cols, values, commit=commit)


  def insert_feature_mapping(self, dblink_id, csv_file, commit=True):
    # legacy
    datalink_folder = os.path.join(self.config.DATALINK_DIR, dblink_id)
    file_path = os.path.join(datalink_folder, csv_file)
    with open(file_path, 'rU') as f:
      reader = csv.reader(f)
      feature_mapping_list = list(reader)
      cols = ','.join(feature_mapping_list[0]) + ',datalink_id'
      for fm in feature_mapping_list[1:]:
        for n,i in enumerate(fm):
          if len(i)==0:
            fm[n] = None
        fm.append(dblink_id)
      values = feature_mapping_list[1:]
      self.insert(self.conn, 'datalink_feature_mapping', cols, values, commit=commit)

  def select_feature_mapping(self, dblink_id):
    sql = '''
    SELECT * FROM datalink_feature_mapping
    INNER JOIN cdm_feature ON datalink_feature_mapping.fid = cdm_feature.fid
    WHERE datalink_id = '%s' AND cdm_feature.is_deprecated = 'f'
    AND cdm_feature.is_measured = 't'
    ORDER BY cdm_feature.fid
    ''' % dblink_id
    server_cursor = self.select_with_sql(sql)
    mapping = server_cursor.fetchall()
    server_cursor.close()
    return mapping

  def delete(self, dblink_id):
    # ambiguous name
    where_conditions = "datalink_id='%s'" % dblink_id
    Resource.delete(self, self.conn, 'datalink_feature_mapping', where_conditions)
    cdm = CDM(self.config)
    cdm.set_conn(self.conn)
    cdm_where_conditions = \
      "enc_id in (SELECT enc_id FROM pat_enc WHERE datalink_id='%s')" \
        % dblink_id
    # cdm.delete(cdm_where_conditions)
    cdm.delete('')
    pat_enc = PatEnc(self.config)
    pat_enc.set_conn(self.conn)
    # pat_enc.delete(where_conditions)
    pat_enc.delete('')
    # Resource.delete(self, self.conn, 'datalink', where_conditions)
    Resource.delete(self, self.conn, 'datalink', '')


  def connect_dblink(self, dblink_id):
    dblink_conn_string = self._get_dblink_conn_string(dblink_id)
    return self.connect(db_conn_string=dblink_conn_string)

  def select_patients(self, dblink_conn, dblink_id, sql_filter=None):
    dblink_folder = os.path.join(self.config.DATALINK_DIR, dblink_id)
    sql_file_path = os.path.join(dblink_folder, \
      self.get_dblink_attribute(dblink_id, 'import_patients_sql'))
    with open(sql_file_path, 'r') as f:
      sql_import_patients = f.read()
    if sql_filter:
      sql_import_patients += " " + sql_filter
    return self.select_with_sql(sql_import_patients, dblink_conn)

  def import_patients(self, dblink_id):
    # insert patient encounters to pat_enc
    pat_enc = PatEnc(self.config)
    pat_enc.connect()
    # connect to dblink database
    dblink_conn = self.connect_dblink(dblink_id)
    # all select methods return a server-side cursor
    dblink_cursor = self.select_patients(dblink_conn, dblink_id)
    num_ids = 10000
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

  def get_visit_id_naming(self, dblink_id):
    dblink_folder = os.path.join(self.config.DATALINK_DIR, dblink_id)
    sql_file_path = os.path.join(dblink_folder, \
      self.get_dblink_attribute(dblink_id, 'import_patients_sql'))
    with open(sql_file_path, 'r') as f:
      sql_import_patients = f.read()
    # get select clause
    select_clause = sql_import_patients[6:sql_import_patients.index('FROM')]
    cols = select_clause.split(',')
    for col in cols:
      if 'visit_id' in col:
        return col.strip()
    return None

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
            cdm.upsert_t([enc_id, tsp, str(fid), str(volume), confidence])
          else:
            cdm.add_t([enc_id, tsp, str(fid), str(volume), confidence])

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
        # print tsp, str(value)
        # tsp = "to_timestamp('%s', 'DD-MM-YYYY hh24:mi:ss')" % tsp \
        #     + "::timestamp without time zone"
        # if len(tsp) == 22:
        #     # timestampe with time zone
        if tsp is not None:
          if is_no_add:
            cdm.upsert_t([enc_id, tsp, str(fid), str(value), confidence])
          else:
            cdm.add_t([enc_id, tsp, str(fid), str(value), confidence])
      elif category == 'TWF':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if is_no_add:
          cdm.upsert_twf([enc_id, tsp, fid, value, confidence])
        else:
          cdm.add_twf([enc_id, tsp, fid, value, confidence])

  def populate_dblink_feature_to_cdm(self, mapping, cdm, visit_id_dict,
                     pat_id_dict, plan=False):
    dblink_id = mapping['datalink_id']
    data_type = mapping['data_type']
    dblink_conn = self.connect_dblink(dblink_id)
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
    visit_id_name = self.get_visit_id_naming(dblink_id)
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    start = timeit.default_timer()
    line = 0
    if is_med_action and fid != 'vent':
      # process medication action input for HC_EPIC
      # order by csn_id, medication id, and timeActionTaken
      orderby = """ "CSN_ID", "MEDICATION_ID", "TimeActionTaken" """
      dblink_cursor = self.select_dblink_feature_values(dblink_conn, \
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
      dblink_cursor = self.select_dblink_feature_values(dblink_conn, \
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
        dblink_conn, mapping, visit_id_name, plan=plan)
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
    dblink_conn.close()





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

  def get_visit_id_to_enc_id_mapping(self, dblink_id):
    mapping = {}
    pat_enc = PatEnc(self.config)
    pat_enc.connect()
    # all select methods return a server-side cursor
    server_cursor = pat_enc.select_pat_enc()

    for row in server_cursor:
      mapping[row['visit_id']] = row['enc_id']
    server_cursor.close()

    pat_enc.disconnect()
    return mapping

  def get_pat_id_to_enc_id_mapping(self, dblink_id):
    mapping = {}
    pat_enc = PatEnc(self.config)
    pat_enc.connect()
    # all select methods return a server-side cursor
    condtion = "dblink_id = '%s'" % dblink_id
    server_cursor = pat_enc.select_pat_enc()

    for row in server_cursor:
      if row['pat_id'] in mapping:
        mapping[row['pat_id']].append(row['enc_id'])
      else:
        mapping[row['pat_id']] = [row['enc_id']]
    server_cursor.close()

    pat_enc.disconnect()
    return mapping

class PatEnc(Resource):
  def insert(self, cols, values):
    Resource.insert(self, self.conn, 'pat_enc', cols, values)

  def upsert(self, cols, values):
    Resource.upsert(self, self.conn, 'pat_enc', cols, values)


  def delete(self, where_conditions):
    Resource.delete(self, self.conn, 'pat_enc', where_conditions)

  def select_pat_enc(self, cols='*', where_conditions=None):
    if where_conditions:
      sql = "SELECT %s FROM pat_enc WHERE %s" % (cols, where_conditions)
    else:
      sql = "SELECT %s FROM pat_enc" % cols
    return self.select_with_sql(sql, self.conn)

  def get_visit_id_to_enc_id_mapping(self):
    mapping = {}
    pat_enc = PatEnc(self.config)
    pat_enc.connect()
    # all select methods return a server-side cursor
    server_cursor = pat_enc.select_pat_enc()

    for row in server_cursor:
      mapping[row['visit_id']] = row['enc_id']
    server_cursor.close()

    pat_enc.disconnect()
    return mapping

class CDM(Resource):
  """operations for CDM"""

  def data_clean(self, where_conditions=None):
    # need to change
    Resource.delete(self, self.conn, 'cdm_s', where_conditions)
    Resource.delete(self, self.conn, 'cdm_m', where_conditions)
    Resource.delete(self, self.conn, 'cdm_t', where_conditions)
    Resource.delete(self, self.conn, 'cdm_twf', where_conditions)
    Resource.delete(self, self.conn, 'pat_enc', where_conditions)
    # reset enc_id to start with 1
    self.query_with_sql("ALTER SEQUENCE pat_enc_enc_id_seq RESTART;")

  def delete(self, where_conditions=None):
    # need to change
    Resource.delete(self, self.conn, 'cdm_s', where_conditions)
    Resource.delete(self, self.conn, 'cdm_m', where_conditions)
    Resource.delete(self, self.conn, 'cdm_t', where_conditions)
    Resource.delete(self, self.conn, 'cdm_twf', where_conditions)

  def delete_feature_values(self, fid):
    category = self.get_feature_category(fid)
    if category == 'S':
      Resource.delete(self, self.conn, 'cdm_s', "fid = '%s'" % fid)
    elif category == 'M':
      Resource.delete(self, self.conn, 'cdm_m', "fid = '%s'" % fid)
    elif category == 'G':
      Resource.delete(self, self.conn, 'cdm_g', "fid = '%s'" % fid)
    elif category == 'T':
      Resource.delete(self, self.conn, 'cdm_t', "fid = '%s'" % fid)
    elif category == 'TWF':
      self.update_twf_sql(\
        "update cdm_twf set %(fid)s = null, %(fid)s_c = null" % \
          {'fid': fid})

  def select_s(self, where_clause="", cols="*"):
    sql = "select %s from cdm_s %s" % (cols, where_clause)
    return Resource.select_with_sql(self, sql)

  def select_t(self, where_clause="", cols="*"):
    sql = "select %s from cdm_t %s" % (cols, where_clause)
    return Resource.select_with_sql(self, sql)

  def select_twf(self, where_clause="", cols="*"):
    sql = "select %s from cdm_twf %s" % (cols, where_clause)
    return Resource.select_with_sql(self, sql)

  def insert_g(self, values):
    cols = 'fid,value,confidence'
    Resource.insert(self, self.conn, 'cdm_g', cols, values)

  def insert_s(self, values):
    cols = 'enc_id, fid, value,confidence'
    Resource.insert(self, self.conn, 'cdm_s', cols, values)

  def insert_m(self, values):
    cols = 'enc_id, fid, line, value,confidence'
    Resource.insert(self, self.conn, 'cdm_m', cols, values)

  def insert_t(self, values):
    cols = 'enc_id, tsp, fid, value,confidence'
    Resource.insert(self, self.conn, 'cdm_t', cols, values)

  def insert_twf(self, cols, values):
    Resource.insert(self, self.conn, 'cdm_twf', cols, values)

  def upsert_s(self, values):
    proc = 'merge_cdm_s'
    Resource.upsert(self, self.conn, proc, values)

  def upsert_m(self, values):
    proc = 'merge_cdm_m'
    Resource.upsert(self, self.conn, proc, values)

  def upsert_t(self, values):
    proc = 'merge_cdm_t'
    Resource.upsert(self, self.conn, proc, values)


  def upsert_twf(self, values):
    proc = 'merge_cdm_twf'
    Resource.upsert(self, self.conn, proc, values)

  def upsert_twf_workspace(self, values):
    proc = 'merge_cdm_twf_workspace'
    Resource.upsert(self, self.conn, proc, values)

  def add_s(self, values):
    proc = 'add_cdm_s'
    Resource.add(self, self.conn, proc, values)

  def add_m(self, values):
    proc = 'add_cdm_m'
    Resource.add(self, self.conn, proc, values)

  def add_t(self, values):
    proc = 'add_cdm_t'
    Resource.add(self, self.conn, proc, values)

  def add_twf(self, values):
    proc = 'add_cdm_twf'
    Resource.add(self, self.conn, proc, values)

  def update_twf(self, values):
    # fillin_value = [enc_id, tsp, fid, lastest_value, 0]
    dbtable = 'cdm_twf'
    update_cols = "\"%s\",\"%s_c\"" % (values[2], values[2])
    update_values = "%s, %s" % (values[3], values[4])
    where_conditions = "enc_id=%s AND tsp='%s'" % (values[0],values[1])
    update_sql = "UPDATE %s SET (%s) = (%s) WHERE %s" % \
      (dbtable, update_cols, update_values, where_conditions)
    self.update_with_sql(self.conn, update_sql)

  def update_twf_sql(self, sql):
    self.update_with_sql(self.conn, sql)

  def get_feature_category(self, fid):
    sql = "SELECT category FROM cdm_feature WHERE fid = '%s'" % fid
    server_cursor = self.select_with_sql(sql)
    category = server_cursor.fetchone()['category']
    server_cursor.close()
    return category

  def get_feature_unit(self, fid):
    sql = "SELECT unit FROM cdm_feature WHERE fid = '%s'" % fid
    server_cursor = self.select_with_sql(sql)
    unit = server_cursor.fetchone()['unit']
    server_cursor.close()
    return unit

  def get_feature_attributes(self, fid):
    sql = "SELECT * FROM cdm_feature WHERE fid = '%s'" % fid
    server_cursor = self.select_with_sql(sql)
    row = server_cursor.fetchone()
    server_cursor.close()
    return row

  def fillin(self, fid, recalculate_popmean=True, table='cdm_twf'):
    feature_attributes = self.get_feature_attributes(fid)
    if feature_attributes['category'] == 'TWF':
      select_sql = """
      SELECT count(%(fid)s) from %(table)s
      WHERE %(fid)s_c < 8
      """ % {'fid':fid, 'table':table}
      server_cursor = self.select_with_sql(select_sql)
      cnt = server_cursor.fetchone()[0]
      server_cursor.close()
      if cnt > 0:
        fillin_func_id = feature_attributes['fillin_func_id']
        fillin_func_args = [table,
                  feature_attributes['window_size_in_hours'], 
                  recalculate_popmean]
        self.log.info('start fillin fid %s: %s (%s)' \
          % (fid, fillin_func_id, fillin_func_args))
        fillin.fillin(self, fid, fillin_func_id, fillin_func_args)
        self.log.info('end fillin fid %s' % fid)
      else:
        self.log.warn('no data to fillin fid %s' % fid)
        if not recalculate_popmean:
          self.log.warn('fill in with popmean')
          update_sql = """    
          update %(table)s set %(fid)s = popmean, %(fid)s_c = 24
          from (
            SELECT value::numeric as popmean from cdm_g where fid = '%(fid)s_popmean'
            ) t
          """ % {'table':table, 'fid':fid}
          self.update_with_sql(self.conn, update_sql)
    else:
      self.log.error('This feature is not a TWF feature!')


  def get_all_measured_twf_features(self):
    sql = """
    SELECT fid FROM cdm_feature
    WHERE is_measured = 'T' AND category = 'TWF' AND is_deprecated = 'f'
    """
    server_cursor = self.select_with_sql(sql)
    features = server_cursor.fetchall()
    server_cursor.close()
    return features

  def get_all_derive_features(self):
    sql = """
    SELECT fid FROM cdm_feature
    WHERE is_measured = 'f' AND is_deprecated = 'f'
    """
    server_cursor = self.select_with_sql(sql)
    rows = server_cursor.fetchall()
    server_cursor.close()
    return rows

  def get_all_cdm_features(self):
    sql = """
    SELECT * FROM cdm_feature WHERE is_deprecated = 'f'
    """
    server_cursor = self.select_with_sql(sql)
    rows = server_cursor.fetchall()
    server_cursor.close()
    return rows

  def get_cdm_feature_dictionary(self):
    sql = """
    SELECT * FROM cdm_feature WHERE is_deprecated = 'f'
    """
    server_cursor = self.select_with_sql(sql)
    rows = server_cursor.fetchall()
    server_cursor.close()
    return {r['fid']:r for r in rows}

  def clean_twf(self, fid, value='null', confidence='null', twf_table='cdm_twf'):
    self.twf_set_all_to(fid, value, confidence, twf_table)

  def clean_t(self, fid):
    Resource.delete(self, self.conn, 'cdm_t', "fid = '%s'" % fid)

  def clean_s(self, fid):
    Resource.delete(self, self.conn, 'cdm_s', "fid = '%s'" % fid)

  def twf_set_all_to(self, fid, value, confidence, twf_table):
    """ set a twf feature's value and confidence to the input arguments """
    update_sql = """
    UPDATE %(twf_table)s SET %(fid)s = %(value)s, %(fid)s_c = %(confidence)s
    """ % {'fid':fid, 'value':value, 'confidence':confidence, 
         'twf_table': twf_table}
    self.update_twf_sql(update_sql)

  def upsert_cdm_feature(self, cols, values):
    insert_suc = True
    self.log.info("customized feature %s" % values[0])
    for i in range(len(values)):
      if values[i] == '':
        values[i] = None
    try:
      Resource.insert(self, self.conn, 'cdm_feature', cols, values)
    except psycopg2.IntegrityError as e:
      self.conn.rollback()
      insert_suc = False
    if not insert_suc:
      where_conditions = " WHERE fid = '%s'" % values[0]
      Resource.update(self, self.conn, 'cdm_feature', cols, values, \
        where_conditions)

  def upsert_cdm_function(self, cols, values):
    insert_suc = True
    self.log.info("customized function %s" % values[0])
    for i in range(len(values)):
      if values[i] == '':
        values[i] = None
    try:
      Resource.insert(self, self.conn, 'cdm_function', cols, values)
    except psycopg2.IntegrityError as e:
      self.conn.rollback()
      insert_suc = False
    if not insert_suc:
      where_conditions = " WHERE FUNC_ID = '%s'" % values[0]
      Resource.update(self, self.conn, 'cdm_function', cols, values, \
        where_conditions)

  def insert_datalink(self, datalink_conf, commit=True):
    cols = \
    """
    datalink_id,datalink_type,schedule,data_load_type,
    connection_type,connection_setting_json,
    import_patients_sql
    """
    sql_query_patients = ""
    if 'DATALINK_SQL' in datalink_conf:
      sql_query_patients = datalink_conf['DATALINK_SQL']['sql_query_patients'] 
    values = [\
      datalink_conf['DATALINK']['datalink_id'], \
      datalink_conf['DATALINK']['datalink_type'], \
      datalink_conf['DATALINK']['schedule'], \
      datalink_conf['DATALINK']['data_load_type'], \
      datalink_conf['DATALINK']['connection_type'], \
      json.dumps(datalink_conf['DATALINK_CONNECTION_SETTING']), \
      sql_query_patients]
    self.insert(self.conn, 'datalink', cols, values, commit=commit)

  def insert_datalink_feature_mapping(self, datalink_id, csv_file, commit=True):
    dblink_folder = os.path.join(self.config.DATALINK_DIR, datalink_id)
    file_path = os.path.join(dblink_folder, csv_file)
    with open(file_path, 'rU') as f:
      reader = csv.reader(f)
      feature_mapping_list = list(reader)
      cols = ','.join(feature_mapping_list[0]) + ',datalink_id'
      for fm in feature_mapping_list[1:]:
        for n,i in enumerate(fm):
          if len(i)==0:
            fm[n] = None
        fm.append(datalink_id)
      values = feature_mapping_list[1:]
      self.insert(self.conn, 'datalink_feature_mapping', cols, values, commit=commit)


class UnknownFeatureCategoryError(Exception):
  pass

class UnknownProcError(Exception):
  def __init__(self, proc):
    self.proc = proc 
    print "UnknownProcError", self.proc

