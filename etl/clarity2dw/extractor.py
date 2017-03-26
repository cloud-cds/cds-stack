import pandas as pd
import numpy as np
from etl.transforms.primitives.row import transform
from etl.load.primitives.row import load_row
from etl.load.pipelines.fillin import fillin_pipeline
from etl.load.pipelines.derive import derive_main
import timeit
import importlib

PLAN = False
recalculate_popmean = False # if False, then remember to import cdm_g before extraction
pipeline = [
  # "transform",
  "fillin",
  # "derive",
]

class Extractor:
  def __init__(self, pool, config):
    self.config = config
    self.pool = pool
    self.log = self.config.log

  async def run(self):
    self.log.info("start to run clarity ETL")
    async with self.pool.acquire() as conn:
      if "transform" in pipeline:
        await self.transform(conn)
      if "fillin" in pipeline:
        await self.run_fillin(conn)
      if "derive" in pipeline:
        await self.derive(conn)

  async def transform(self, conn):
    await self.init(conn)
    await self.populate_patients(conn)
    await self.populate_measured_features(conn)

  async def run_fillin(self, conn):
    self.log.info("start fillin pipeline")
    cdm_feature_dict = await self.get_cdm_feature_dict(conn)
    for fid in cdm_feature_dict:
      feature = cdm_feature_dict[fid]
      if feature['category'] == 'TWF' and feature['is_measured']:
        await fillin_pipeline(self.log, conn, feature, recalculate_popmean)
    self.log.info("fillin completed")

  async def derive(self, conn):
    self.log.info("start derive pipeline")
    cdm_feature_dict = await self.get_cdm_feature_dict(conn)
    await derive_main(self.log, conn, cdm_feature_dict)
    self.log.info("derive completed")


##################
# transform pipeline
##################

  async def init(self, conn):
    self.log.warn("TODO: delete data from the same etl_id only (currently delete all).")
    init_sql = '''
    delete from cdm_s;
    delete from cdm_t;
    delete from cdm_twf;
    delete from pat_enc;
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
    cdm_feature_dict = await self.get_cdm_feature_dict(conn)
    pat_mappings = await self.get_pat_mapping(conn)
    visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
    pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
    self.log.info("load feature mapping")

    for i, mapping in feature_mapping.iterrows():
      self.log.debug(mapping)
      fid = mapping['fid']
      transform_func_id = str(mapping['transform_func_id'])
      if fid in cdm_feature_dict:
        if "." in transform_func_id:
          i = len(transform_func_id) - transform_func_id[::-1].index('.')
          package = transform_func_id[:(i-1)]
          transform_func_id = transform_func_id[i:]
          self.log.info("fid: %s using package: %s and transform_func_id: %s" % (fid, package, transform_func_id))
          module = importlib.import_module(package)
          func = getattr(module, transform_func_id)
          await func(conn)
        else:
          await self.populate_feature_to_cdm(mapping, conn,\
                  visit_id_to_enc_id, pat_id_to_enc_ids, cdm_feature_dict[fid], plan=PLAN)
      else:
        self.log.warn("feature %s is not in cdm_feature" % fid)

  async def get_pat_mapping(self, conn):
    sql = "select * from pat_enc"
    pats = await conn.fetch(sql)
    visit_id_to_enc_id = {}
    pat_id_to_enc_ids = {}
    for pat in pats:
      visit_id_to_enc_id[pat['visit_id']] = pat['enc_id']
      if pat['enc_id'] in pat_id_to_enc_ids:
        pat_id_to_enc_ids.append(pat['enc_id'])
      else:
        pat_id_to_enc_ids[pat['pat_id']] = [pat['enc_id']]
    return {
      "visit_id_to_enc_id": visit_id_to_enc_id,
      "pat_id_to_enc_ids": pat_id_to_enc_ids
    }

  async def get_cdm_feature_dict(self, conn):
    sql = "select * from cdm_feature"
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    return cdm_feature_dict


  async def populate_feature_to_cdm(self, mapping, conn, visit_id_dict,
                     pat_id_dict, cdm_feature_attributes, plan=False):
    data_type = cdm_feature_attributes['data_type']
    fid = mapping['fid']
    self.log.info('importing feature value fid %s' % fid)
    if str(mapping['transform_func_id']) == "nan":
      mapping['transform_func_id'] = None
    transform_func_id = mapping['transform_func_id']
    if transform_func_id:
      self.log.info("transform func: %s" % transform_func_id)
    category = cdm_feature_attributes['category']
    is_no_add = bool(mapping['is_no_add'] == "yes")
    is_med_action = bool(mapping['is_med_action'] == "yes")
    self.log.info("is_no_add: %s, is_med_action: %s" \
      % (is_no_add, is_med_action))
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    start = timeit.default_timer()
    line = 0
    if is_med_action and fid != 'vent':
      # process medication action input for HC_EPIC
      # order by csn_id, medication id, and timeActionTaken
      orderby = '"CSN_ID", "MEDICATION_ID", "TimeActionTaken"'
      sql = self.get_feature_sql_query(mapping, orderby=orderby, plan=plan)
      if plan:
        async with conn.transaction():
          async for row in conn.cursor(sql):
            self.log.debug(row)
      else:
        cur_enc_id = None
        cur_med_id = None
        cur_med_events = []
        async with conn.transaction():
          async for row in conn.cursor(sql):
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
                      mapping, conn)

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
                          conn)
    elif is_med_action and fid == 'vent':
      orderby = " icustay_id, realtime"
      sql = self.get_feature_sql_query(mapping, orderby=orderby, plan=plan)
      if plan:
        async with conn.transaction():
          async for row in conn.cursor(sql):
            self.log.debug(row)
      else:
        cur_enc_id = None
        cur_vent_events = []
        async with conn.transaction():
          async for row in conn.cursor(sql):
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
                        cur_vent_events, fid_info, mapping, conn)
                  cur_enc_id = enc_id
                  cur_vent_events = []
                # print "cur events temp:", cur_med_events
                cur_vent_events.append(row)
                # print "cur events:", cur_med_events
          if cur_enc_id and len_of_events > 0:
            line += len_of_events
          self.process_vent_events(cur_enc_id, cur_vent_events, \
            fid_info, mapping, cdm)
    else:
      # process features unrelated to med actions
      sql = self.get_feature_sql_query(mapping, plan=plan)
      if plan:
        async with conn.transaction():
          async for row in conn.cursor(sql):
            self.log.debug(row)
      else:
        line = 0
        start = timeit.default_timer()
        pat_id_based = 'pat_id' in mapping['select_cols']
        async with conn.transaction():
          async for row in conn.cursor(sql):
            if pat_id_based:
              if str(row['pat_id']) in pat_id_dict:
                enc_ids = pat_id_dict[str(row['pat_id'])]
                for enc_id in enc_ids:
                  result = transform.transform(fid, \
                    transform_func_id, row, data_type, self.log)
                  if result is not None:
                    line += 1
                    await self.save_result_to_cdm(fid, category, enc_id, \
                      row, result, conn, is_no_add)
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
                  await self.save_result_to_cdm(fid, category, enc_id, \
                    row, result, conn, is_no_add)
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

  def get_feature_sql_query(self, mapping, orderby=None, plan=False):
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
    where_clause = str(mapping['where_conditions'])
    if where_clause == 'nan':
      where_clause = ''
    sql = "SELECT %s FROM %s %s" % (select_clause, dbtable, where_clause)
    if orderby:
      sql += " order by " + orderby
    if plan:
      sql += " limit 100"
    self.log.info("sql: %s" % sql)
    return sql

  async def _transform_med_action(self, fid, mapping, event, fid_info, conn, enc_id):
    dose_entry = transform.transform(fid, mapping['transform_func_id'], \
      event, fid_info['data_type'], self.log)
    if dose_entry:
      self.log.debug(dose_entry)
      tsp = dose_entry[0]
      volume = str(dose_entry[1])
      confidence = dose_entry[2]
      if fid_info['is_no_add']:
        await load_row.upsert_t(conn, [enc_id, tsp, fid, volume, confidence])
      else:
        await load_row.add_t(conn, [enc_id, tsp, fid, volume, confidence])



  async def process_med_events(self, enc_id, med_id, med_events,
               fid_info, mapping, conn):
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
            await load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(volume), confidence])
          else:
            await load_row.add_t(conn, [enc_id, tsp, str(fid), str(volume), confidence])

  async def process_vent_events(self, enc_id, vent_events, fid_info, mapping, conn):
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
            await load_row.upsert_t(conn, [enc_id, tsp, fid, on_off, confidence])
          else:
            await load_row.add_t(conn, [enc_id, tsp, fid, on_off, confidence])

  async def save_result_to_cdm(self, fid, category, enc_id, row, results, conn, \
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
          await load_row.upsert_s(conn, [enc_id, fid, str(value), confidence])
        else:
          await load_row.add_s(conn, [enc_id, fid, str(value), confidence])
      elif category == 'T':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if tsp is not None:
          if is_no_add:
            await load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(value), confidence])
          else:
            await load_row.add_t(conn, [enc_id, tsp, str(fid), str(value), confidence])
      elif category == 'TWF':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if is_no_add:
          await load_row.upsert_twf(conn, [enc_id, tsp, fid, value, confidence])
        else:
          await load_row.add_twf(conn, [enc_id, tsp, fid, value, confidence])

