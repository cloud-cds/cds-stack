import pandas as pd
import transforms.primitives.transform
PLAN = True

class Extractor:
  def __init__(self, pool, config):
    self.config = config
    self.pool = pool
    self.log = self.config.log

  async def run(self):
    self.log.info("start to run clarity ETL")
    async with self.pool.acquire() as conn:
      await self.init(conn)
      await self.populate_patients(conn)
      await self.populate_measured_features(conn)


  async def init(self, conn):
    self.log.warn("TODO: delete data from the same etl_id only (currently delete all).")
    init_sql = '''
    delete from pat_enc cascade;
    '''
    self.log.debug("ETL init sql: " + init_sql)
    result = await conn.execute(init_sql)
    self.log.info("ETL Init: " + result)

  async def populate_patients(self, conn):
    sql = '''
    insert into pat_enc (visit_id, pat_id)
    SELECT "CSN_ID" visit_id, "pat_id"
    FROM "Demographics"
    '''
    self.log.debug("ETL populate_patients sql: " + sql)
    result = await conn.execute(sql)
    self.log.info("ETL populate_patients: " + result)


  async def populate_measured_features(self, conn):
    feature_mapping = pd.read_csv(self.config.FEATURE_MAPPING_CSV)
    await cdm_feature_dict = self.get_cdm_feature_dict(conn)
    await pat_mappings = self.get_pat_mapping(conn)
    visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
    pat_id_to_enc_id = pat_mappings['pat_id_to_enc_id']
    self.log.info("load feature mapping")
    self.log.debug(feature_mapping.head())
    for i, mapping in feature_mapping.iterrows():
      fid = mapping['fid']
      if fid in cdm_feature_dict:
        self.populate_feature_to_cdm(mapping, \
                visit_id_to_enc_id, pat_id_to_enc_ids, cdm_feature_dict[fid], plan=PLAN)
      else:
        self.log.warn("feature %s is not in cdm_feature" % fid)

  async def get_pat_mapping(self, conn):
    sql = "select * from pat_enc"
    pats = await conn.fetch(sql)
    visit_id_to_enc_id = {}
    pat_id_to_enc_id = {}
    for pat in pats:
      visit_id_to_enc_id[pat['visit_id']] = pat['enc_id']
      pat_id_to_enc_id[pat['pat_id']] = pat['enc_id']
    return {
      "visit_id_to_enc_id": visit_id_to_enc_id,
      "pat_id_to_enc_id": pat_id_to_enc_id
    }

  async def get_cdm_feature_dict(self, conn):
    sql = "select * from cdm_feature"
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}


  def populate_feature_to_cdm(self, mapping, conn, visit_id_dict,
                     pat_id_dict, cdm_feature_attributes, plan=False):
    dblink_id = mapping['datalink_id']
    data_type = mapping['data_type']
    fid = mapping['fid']
    self.log.info('importing feature value fid %s' % fid)
    transform_func_id = mapping['transform_func_id']
    if transform_func_id:
      self.log.info("transform func: %s" % transform_func_id)
    category = cdm_feature_attributes['category']
    is_no_add = mapping['is_no_add']
    is_med_action = mapping['is_med_action']
    self.log.info("is_no_add: %s, is_med_action: %s" \
      % (is_no_add, is_med_action))
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    start = timeit.default_timer()
    line = 0
    if is_med_action and fid != 'vent':
      # process medication action input for HC_EPIC
      # order by csn_id, medication id, and timeActionTaken
      orderby = """ "CSN_ID", "MEDICATION_ID", "TimeActionTaken" """
      dblink_cursor = self.select_feature_values(mapping, visit_id_name, orderby=orderby, plan=plan)
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
                  # TODO
                  self.process_med_events(cur_enc_id, \
                    cur_med_id, cur_med_events, fid_info, \
                    mapping, cdm)

              cur_enc_id = enc_id
              cur_med_id = med_id
              cur_med_events = []
            # print "cur events temp:", cur_med_events
            cur_med_events.append(row)
            # print "cur events:", cur_med_events
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
      dblink_cursor = self.select_feature_values(mapping, visit_id_name, orderby=orderby, plan=plan)
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
                    # TODO
                    self.process_vent_events(cur_enc_id, \
                      cur_vent_events, fid_info, mapping, cdm)
                cur_enc_id = enc_id
                cur_vent_events = []
              # print "cur events temp:", cur_med_events
              cur_vent_events.append(row)
              # print "cur events:", cur_med_events
        if cur_enc_id and len_of_events > 0:
          line += len_of_events
          # TODO
          self.process_vent_events(cur_enc_id, cur_vent_events, \
            fid_info, mapping, cdm)
    else:
      # process features unrelated to med actions
      dblink_cursor = self.select_feature_values(   dblink_conn, mapping, visit_id_name, plan=plan)
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
                  # TODO
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

  async def select_feature_values(mapping, visit_id_name, orderby=None, plan=False):
    sql = self.get_feature_sql_query(mapping, visit_id_name, orderby)
    if plan:
      sql += " limit 100"
      for row in await self.conn.fetch(sql):
        self.log.info(row)
    else:
      return await self.conn.fetch(sql)

  def get_feature_sql_query(self, mapping, orderby=None):
    if 'subject_id' in mapping['select_cols'] \
      and 'pat_id' not in mapping['select_cols']:
      # hist features in mimic
      select_clause = \
        mapping['select_cols'].replace("subject_id", "subject_id pat_id")
    elif 'visit_id' in mapping['select_cols'] \
      or 'pat_id' in mapping['select_cols']:
      select_clause = mapping['select_cols']
    else:
      select_clause = '"CSN_ID" visit_id,' + mapping['select_cols']
    dbtable = mapping['dbtable']
    where_clause = mapping['where_conditions']
    if where_clause is None:
      where_clause = ''
    sql = "SELECT %s FROM %s %s" % (select_clause, dbtable, where_clause)
    if orderby:
      sql += " order by " + orderby
    self.log.info("sql: %s" % sql)
    return sql