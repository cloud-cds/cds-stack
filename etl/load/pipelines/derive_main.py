# derive pipeline for cdm_twf
import etl.load.primitives.tbl.derive as derive_func
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive import with_ds
from etl.load.primitives.tbl.derive_helper import *

import time

async def derive_main(log, conn, cdm_feature_dict, mode=None, fid=None,
                      dataset_id=None, derive_feature_addr=None,
                      incremental=False):
  '''
  mode: "append", run derive functions beginning with @fid sequentially
  mode: "dependent", run derive functions for @fid and other features depends on @fid
  mode: None, run derive functions sequentially for all derive features
  '''
  # generate a sequence to derive
  if mode == 'append':
    append = fid
    log.info("starts from feature %s" % append)
    derive_feature_order = get_derive_seq(cdm_feature_dict)
    log.debug("derive feautre order: " + ", ".join(derive_feature_order))
    idx = derive_feature_order.index(append)
    for i in range(idx, len(derive_feature_order)):
      fid = derive_feature_order[i]
      await derive_feature(log, fid, cdm_feature_dict, conn,
                           dataset_id=dataset_id,
                           derive_feature_addr=derive_feature_addr,
                           incremental=incremental)
  elif mode == 'dependent':
    dependent = fid
    if not cdm_feature_dict[fid]['is_measured']:
      log.info("update feature %s and its dependents" % dependent)
      await derive_feature(log, fid, cdm_feature_dict, conn,
                           dataset_id=dataset_id,
                           derive_feature_addr=derive_feature_addr,
                           incremental=incremental)
    else:
      log.info("update feature %s's dependents" % dependent)
    derive_feature_order = get_dependent_features([dependent],
                                                  cdm_feature_dict)
    for fid in derive_feature_order:
      await derive_feature(log, fid, cdm_feature_dict, conn,
                           dataset_id=dataset_id,
                           derive_feature_addr=derive_feature_addr,
                           incremental=incremental)
  elif mode is None:
    if fid is None:
      log.info("derive features one by one")
      derive_feature_order = get_derive_seq(cdm_feature_dict)
      log.debug("derive feautre order: " + ", ".join(derive_feature_order))
      for fid in derive_feature_order:
        await derive_feature(log, fid, cdm_feature_dict, conn,
                             dataset_id=dataset_id,
                             derive_feature_addr=derive_feature_addr,
                             incremental=incremental)
    else:
      log.info("derive feature: %s" % fid)
      await derive_feature(log, fid, cdm_feature_dict, conn,
                           dataset_id=dataset_id,
                           derive_feature_addr=derive_feature_addr,
                           incremental=incremental)
  else:
    log.error("Unknown mode!")

def get_derive_seq(features=None, input_map=None):
  """
  features is a list of dictionaries
  """
  def rm_measured_dependencies(row, df_list):
    lst = map(str.lstrip, map(str.strip, row.split(',')))
    return [x for x in lst if x in df_list]

  def reduce_dependencies(dependency_lst):
    return [x for x in dependency_lst if x not in order]

  order = []

  if input_map:
    d_map = input_map
  else:
    # create the dependency map
    d_map = {f:features[f]['derive_func_input'] for f in features if not features[f]['is_measured'] and not features[f]['is_deprecated']}
  derived_features = d_map.keys()
  # clear out dependencies on measured features, they should be in CDM already
  d_map = dict((k,rm_measured_dependencies(v, derived_features)) \
    for (k,v) in d_map.items())

  while (len(d_map) != 0):
    ind =  [k for k in d_map if len(d_map[k]) == 0]
    order.extend(ind)
    d_map = dict((k,v) for (k,v) in d_map.items() if k not in order)
    d_map = dict((k, reduce_dependencies(v)) for (k, v) in d_map.items())
  return order

def get_dependent_features(feature_list, cdm_feature_dict):
  # create the dependency map
  d_map = dict((fid, cdm_feature_dict[fid]['derive_func_input']) \
      for fid in cdm_feature_dict if ((not cdm_feature_dict[fid]['is_measured']) \
      and (not cdm_feature_dict[fid]['is_deprecated'])))
  derived_features = d_map.keys()
  get_dependents = feature_list
  dependency_list = []
  first = True

  while len(get_dependents) != 0:
    if first:
      first = False
    else:
      dependency_list.append(get_dependents)
    get_dependents = [fid for fid in derived_features if \
      any(map(lambda x: x in [item.strip() for item \
        in d_map[fid].split(",")], get_dependents))]
    # get_dependents = [fid in derived_features if \
    #     any(map(lambda x: x in d_map[fid], get_dependents))]

  dependent_features = [fid for lst in dependency_list for fid in lst]
  if len(dependent_features) ==  0:
    return dependent_features
  else:
    dic = dict((fid, cdm_feature_dict[fid]['derive_func_input']) \
        for fid in cdm_feature_dict if fid in dependent_features)
    return get_derive_seq(input_map=dic)


async def derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=None, derive_feature_addr=None,incremental=False, cdm_t_target='cdm_t', cdm_t_lookbackhours=None):
  ts = time.time()
  feature = cdm_feature_dict[fid]
  derive_func_id = feature['derive_func_id']
  derive_func_input = feature['derive_func_input']
  fid_category = feature['category']
  log.debug("derive feature %s, function %s, inputs (%s) %s" \
  % (fid, derive_func_id, derive_func_input, 'dataset_id %s' % dataset_id if dataset_id is not None else ''))
  if fid in query_config:
    # =================================
    # Query Coinfiguration Path
    # =================================
    log.debug("Derive Function found in Driver")
    config_entry = query_config[fid]
    fid_input_items = [item.strip() for item in derive_func_input.split(',')]
    clean_sql = ''
    sql = ''
    if fid_input_items == config_entry['fid_input_items']:
      if fid_category == 'TWF':
        twf_table = derive_feature_addr[fid]['twf_table']
        twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
        if 'clean' in config_entry:
          clean_args = config_entry['clean']
          clean_sql = clean_tbl.cdm_twf_clean(fid, twf_table=twf_table_temp, \
            dataset_id = dataset_id, incremental=incremental, **clean_args)
        if config_entry['derive_type'] == 'simple':
          sql = gen_simple_twf_query(config_entry, fid, dataset_id, \
            derive_feature_addr, cdm_feature_dict, incremental)
        elif config_entry['derive_type'] == 'subquery':
          sql = gen_subquery_upsert_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target)
        log.debug(clean_sql + sql)
        await conn.execute(clean_sql + sql)
      elif fid_category == 'T':
        # Note they do not touch TWF table
        sql = gen_cdm_t_upsert_query(config_entry, fid,
                                                dataset_id, incremental, cdm_t_target, cdm_t_lookbackhours)
        log.debug(sql)
        await conn.execute(sql)
      elif fid_category == 'S':
        # Note they do not touch TWF table
        sql = gen_cdm_s_upsert_query(config_entry, fid,
                                                dataset_id, incremental, cdm_s_target)
        log.debug(sql)
        await conn.execute(sql)
      else:
        log.error("Invalid derive fid category: {}".format(fid_category))
    else:
      log.error("Inputs in cdm_feature {} does not equal what's in the query config {}".format(fid_input_items, config_entry['fid_input_items']) )


  else:
    # =================================
    # Custom Derive Function
    # =================================
    log.debug("Derive function is not implemented in driver, so we use legacy derive function")
    await derive_func.derive(fid, derive_func_id, derive_func_input, conn, \
      log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours)
  te = time.time()
  log.info("derive feature %s end. (%2.2f secs)" % (fid, te-ts))



def gen_simple_twf_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
  twf_table = derive_feature_addr[fid]['twf_table']
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  fid_input_items = config_entry['fid_input_items']
  select_table_joins = get_select_table_joins(fid_input_items, \
    derive_feature_addr, cdm_feature_dict, dataset_id, incremental)
  update_expr = config_entry['fid_update_expr']
  update_expr_params = {}
  if '%(twf_table)s' in update_expr:
    update_expr_params['twf_table'] = twf_table
  if '%(with_ds_s)s' in update_expr:
    update_expr_params['with_ds_s'] = with_ds(dataset_id, table_name='cdm_s')
  if '%(with_ds_twf)s' in update_expr:
    update_expr_params['with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf')
  if update_expr_params:
    update_expr = update_expr % update_expr_params
  c_update_expr = config_entry['fid_c_update_expr']
  if '%(twf_table)s' in c_update_expr:
    c_update_expr = c_update_expr % {'twf_table': twf_table}
  sql = """
  INSERT INTO %(twf_table_temp)s (%(dataset_id_key)s enc_id, tsp, %(insert_cols)s)
  SELECT %(dataset_id_key)s enc_id, tsp, %(select_cols)s FROM
  (%(select_table_joins)s) source
  ON CONFLICT (%(dataset_id_key)s enc_id, tsp) DO UPDATE SET
  %(update_cols)s
  """ % {
    'twf_table_temp'     : twf_table_temp,
    'dataset_id_key'     : dataset_id_key(None, dataset_id),
    'select_table_joins' : select_table_joins,
    'select_cols'        : '({update_expr}) as {fid}, ({c_update_expr}) as {fid}_c'.format(\
                                  update_expr=update_expr, fid=fid, c_update_expr=c_update_expr),
    'update_cols'        : '{fid} = excluded.{fid}, {fid}_c = excluded.{fid}_c'.format(fid=fid),
    'insert_cols'        : '{fid}, {fid}_c'.format(fid=fid)
  }
  return sql

def gen_subquery_upsert_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target):
  twf_table = derive_feature_addr[fid]['twf_table']
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  subquery_params = {}
  subquery_params['incremental'] = incremental
  subquery_params['fid'] = fid
  fid_input_items = config_entry['fid_input_items']
  # generate twf_table from selection
  subquery_params['twf_table_join'] = '(' + \
    get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id, incremental) \
    + ')'
  subquery_params['twf_table'] = twf_table
  subquery_params['cdm_t_target'] = cdm_t_target
  subquery_params['dataset_id'] = dataset_id
  subquery_params['with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=False)
  subquery_params['and_with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=True)
  subquery_params['with_ds_t'] = with_ds(dataset_id, table_name=cdm_t_target, conjunctive=True)
  subquery_params['with_ds_ttwf'] = (' AND cdm_t.dataset_id = cdm_twf.dataset_id' if dataset_id else '') + with_ds(dataset_id, table_name='cdm_twf', conjunctive=False)
  subquery_params['dataset_id_key'] = dataset_id_key('cdm_twf', dataset_id)
  for fid_input in config_entry['fid_input_items']:
    if fid_input in derive_feature_addr:
      subquery_params['twf_table_{}'.format(fid_input)] = derive_feature_addr[fid_input]['twf_table']
      subquery_params['twf_table_temp_{}'.format(fid_input)] = derive_feature_addr[fid_input]['twf_table_temp']
  subquery_params['derive_feature_addr'] = derive_feature_addr
  subquery = config_entry['subquery'](subquery_params)
  upsert_clause = '''
  INSERT INTO %(twf_table_temp)s (%(dataset_id_key)s enc_id, tsp,%(fid)s, %(fid)s_c)
  (
    %(subquery)s
  )
  ON CONFLICT (%(dataset_id_key)s enc_id, tsp) DO UPDATE SET
  %(fid)s = excluded.%(fid)s,
  %(fid)s_c = excluded.%(fid)s_c
  ''' % {
    'fid':fid,
    'twf_table_temp': twf_table_temp,
    'subquery': subquery,
    'dataset_id_key': 'dataset_id, ' if dataset_id is not None else ''
  }
  return upsert_clause

def gen_cdm_t_upsert_query(config_entry, fid, dataset_id, incremental, cdm_t_target, cdm_t_lookbackhours):
  fid_select_expr = config_entry['fid_select_expr'] % {
    'cdm_t': cdm_t_target,
    'dataset_col_block': 'cdm_t.dataset_id,' if dataset_id is not None else '',
    'dataset_where_block': (' and cdm_t.dataset_id = %s' % dataset_id) if dataset_id is not None else '',
    'incremental_enc_id_join': incremental_enc_id_join('cdm_t', dataset_id, incremental),
    'incremental_enc_id_match': incremental_enc_id_match(' and ', incremental),
    'lookbackhours': " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else ''
  }
  # print(fid_select_expr)
  delete_clause = ''
  if dataset_id and not incremental:
    # only delete existing data in offline full load mode
    delete_clause = "DELETE FROM %(cdm_t)s where fid = '%(fid)s' %(dataset_where_block)s;\n"

  upsert_clause = (delete_clause + """
  INSERT INTO %(cdm_t)s (%(dataset_col_block)s enc_id,tsp,fid,value,confidence) (%(select_expr)s)
  ON CONFLICT (%(dataset_col_block)s enc_id,tsp,fid) DO UPDATE SET
  value = excluded.value, confidence = excluded.confidence;
  """) % {
    'cdm_t': cdm_t_target,
    'fid':fid,
    'select_expr': fid_select_expr,
    'dataset_col_block': 'dataset_id,' if dataset_id is not None else '',
    'dataset_where_block': (' and dataset_id = %s' % dataset_id) if dataset_id is not None else '',
    'incremental_enc_id_in': incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)
  }
  return upsert_clause

query_config = {
  # 'bun_to_cr': {
  #   'fid_input_items'   : ['bun', 'creatinine'],
  #   'derive_type'       : 'simple',
  #   'fid_update_expr'   : 'bun/creatinine',
  #   'fid_c_update_expr' : 'creatinine_c | bun_c',
  # },
  'pao2_to_fio2': {
    'fid_input_items':  ['pao2', 'fio2'],
    'derive_type': 'simple',
    'fid_update_expr': 'pao2/fio2*100',
    'fid_c_update_expr': 'pao2_c | fio2_c'
  },
  'metabolic_acidosis': {
    'fid_input_items': ['arterial_ph', 'bicarbonate'],
    'derive_type': 'simple',
    'fid_update_expr': '(case when arterial_ph < 7.35 and bicarbonate < 22 then 1 else 0 end)',
    'fid_c_update_expr': 'arterial_ph_c | bicarbonate_c'
  },
  'mi': {
    # TODO add chest pain variable
    'fid_input_items': ['troponin'],
    'derive_type': 'simple',
    'fid_update_expr': '(case when troponin > 0.01 and troponin_c < 8 then 1 else 0 end)',
    'fid_c_update_expr': 'troponin_c',
  },
  'acute_liver_failure': {
    'fid_input_items': ['inr', 'gcs', 'liver_disease_hist'],
    'derive_type': 'simple',
    'fid_update_expr': '''(case when not enc_id in (select enc_id from cdm_s where fid='liver_disease_hist' and value::boolean=TRUE)
      and inr >= 1.5 and gcs <= 13 and inr_c < 24 and gcs_c < 24
      then 1
      else 0 end)''',
    'fid_c_update_expr': 'inr_c | gcs_c',
  },
  'hepatic_sofa': {
    'fid_input_items': ['bilirubin'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                      WHEN (bilirubin > 12.0) THEN 4
                      WHEN (bilirubin >= 6.0) THEN 3
                      WHEN (bilirubin >= 2.0) THEN 2
                      WHEN (bilirubin >= 1.2) THEN 1
                      ELSE 0
                    END)''',
    'fid_c_update_expr': 'bilirubin_c',
  },
  'qsofa': {
    'fid_input_items': ['resp_rate', 'sbpm', 'gcs'],
    'derive_type': 'simple',
    'fid_update_expr': ''' (resp_rate >= 22 and based_on_popmean(resp_rate_c) = 0)::int
                    + (sbpm <= 100 and based_on_popmean(sbpm_c) = 0)::int
                    + (gcs < 13 and based_on_popmean(gcs_c) = 0)::int''',
    'fid_c_update_expr': 'coalesce(resp_rate_c,0) | coalesce(sbpm_c,0) | coalesce(gcs_c,0)',
  },
  'neurologic_sofa': {
    'fid_input_items': ['gcs'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                      WHEN (gcs < 6) THEN 4
                      WHEN (gcs < 10) THEN 3
                      WHEN (gcs < 13) THEN 2
                      WHEN (gcs < 15) THEN 1
                      ELSE 0
                    END)''',
    'fid_c_update_expr': 'gcs_c',
  },
  'hematologic_sofa': {
    'fid_input_items': ['platelets'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                      WHEN (platelets < 20) THEN 4
                      WHEN (platelets < 50) THEN 3
                      WHEN (platelets < 100) THEN 2
                      WHEN (platelets < 150) THEN 1
                      ELSE 0
                    END)''',
    'fid_c_update_expr': 'platelets_c',
  },
  'nbp_mean': {
    'fid_input_items': ['nbp_dias', 'nbp_sys'],
    'derive_type': 'simple',
    'fid_update_expr': 'nbp_sys/3 + nbp_dias/3*2',
    'fid_c_update_expr': 'nbp_sys_c | nbp_dias_c',
  },
  'obstructive_pe_shock': {
    'fid_input_items': ['lactate', 'ddimer', 'spo2', 'heart_rate'],
    'derive_type': 'simple',
    'fid_update_expr': '''(case when (lactate > 2
                        and ddimer > 0.5
                        and spo2 < 95
                        and heart_rate > 100) then 1
                    else 0 end
                    )
                   ''',
    'fid_c_update_expr': 'lactate_c | ddimer_c | spo2_c | heart_rate_c',
  },
  'sirs_hr_oor': {
    'fid_input_items': ['heart_rate'],
    'derive_type': 'simple',
    'fid_update_expr': 'heart_rate > 90',
    'fid_c_update_expr': 'heart_rate_c',
  },
  'sirs_resp_oor': {
    'fid_input_items': ['resp_rate'],
    'derive_type': 'simple',
    'fid_update_expr': 'resp_rate > 20',
    'fid_c_update_expr': 'resp_rate_c',
  },
  'sirs_temperature_oor': {
    'fid_input_items': ['temperature'],
    'derive_type': 'simple',
    'fid_update_expr': 'temperature < 96.8 or temperature > 100.9',
    'fid_c_update_expr': 'temperature_c',
  },
  'shock_idx': {
    'fid_input_items': ['heart_rate', 'sbpm'],
    'derive_type': 'simple',
    'fid_update_expr': 'heart_rate/sbpm',
    'fid_c_update_expr': 'heart_rate_c | sbpm_c',
  },
  'acute_pancreatitis': {
    'fid_input_items': ['lipase', 'amylase'],
    'derive_type': 'simple',
    'fid_update_expr': '(lipase > 400 and amylase > 450)::int',
    'fid_c_update_expr': 'lipase_c | amylase_c',
  },
  'hypotension_raw': {
    'fid_input_items': ['sbpm', 'map'],
    'derive_type': 'simple',
    'fid_update_expr': '(sbpm < 90 and based_on_popmean(sbpm_c) != 1) OR (map < 65 and based_on_popmean(map_c) != 1)',
    'fid_c_update_expr': 'coalesce(sbpm_c,0) | coalesce(map_c,0)',
  },
  'sepsis_related_organ_failure': {
    'fid_input_items': ['inr', 'platelets', 'creatinine', 'bilirubin', 'urine_output_24hr', 'lactate', 'pao2_to_fio2', 'hypotension_intp'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (inr > 1.5 and based_on_popmean(inr_c) != 1)
                    OR (platelets < 100 and based_on_popmean(platelets_c) != 1)
                    OR (lactate < 4.46 and based_on_popmean(lactate_c) != 1)
                    OR (lactate > 13.39 and based_on_popmean(lactate_c) != 1)
                    OR hypotension_intp is TRUE
                    OR (creatinine > 2 and based_on_popmean(creatinine_c) != 1 and
                      coalesce((select cast(value as boolean) from cdm_s
                      where cdm_s.enc_id = cdm_twf.enc_id
                        and cdm_s.fid = 'chronic_kidney_hist' limit 1)
                      , False) = False)
                    OR (bilirubin > 2 and based_on_popmean(bilirubin_c) != 1 and
                      coalesce((select cast(value as boolean) from cdm_s
                      where cdm_s.enc_id = cdm_twf.enc_id
                        and cdm_s.fid = 'liver_disease_hist' limit 1)
                      , False) = False)
                    OR (pao2_to_fio2 < 100 and based_on_popmean(pao2_to_fio2_c) != 1 and
                      coalesce((select cast(value as boolean) from cdm_s
                      where cdm_s.enc_id = cdm_twf.enc_id
                        and cdm_s.fid = 'chronic_pulmonary_hist' limit 1)
                      , False) = False)
                    OR (urine_output_24hr < 500 AND
                        tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf_table)s cdm_twf_2
                           WHERE cdm_twf_2.enc_id = cdm_twf.enc_id)
                        >= interval '24 hours')
                      ''',
    'fid_c_update_expr': '''
                      coalesce(inr_c,0)
                      | coalesce(platelets_c,0) | coalesce(creatinine_c,0)
                      | coalesce(bilirubin_c,0)
                      | coalesce(lactate_c,0) | coalesce(pao2_to_fio2_c,0)
                      | coalesce(hypotension_intp_c,0)
                      | coalesce(urine_output_24hr_c,0)
                     ''',
  },
  'acute_organ_failure': {
    'fid_input_items': ['inr', 'platelets', 'creatinine', 'chronic_kidney_hist', 'bilirubin', 'liver_disease_hist', 'urine_output_24hr', 'lactate', 'pao2_to_fio2', 'chronic_pulmonary_hist', 'hypotension_intp'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
      WITH S as (
      SELECT %(dataset_id_key)s cdm_twf.enc_id, min(cdm_twf.tsp) min_tsp
      FROM %(twf_table)s cdm_twf %(incremental_enc_id_join)s
          %(incremental_enc_id_match)s
          %(sub_dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id)
      SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
      coalesce(
        (inr > 1.5 and based_on_popmean(inr_c) != 1)
        OR (platelets < 100 and based_on_popmean(platelets_c) != 1)
        OR (lactate > 4.0 and based_on_popmean(lactate_c) != 1)
        OR hypotension_intp is TRUE
        OR (creatinine > 2 and based_on_popmean(creatinine_c) != 1 and
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' %(with_ds_s)s limit 1)
          , False) = False)
        OR (bilirubin > 2 and based_on_popmean(bilirubin_c) != 1 and
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'liver_disease_hist' %(with_ds_s)s limit 1)
          , False) = False)
        OR (pao2_to_fio2 < 100 and based_on_popmean(pao2_to_fio2_c) != 1 and
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_pulmonary_hist' %(with_ds_s)s limit 1)
          , False) = False)
        OR (
        urine_output_24hr < 500
        AND
          tsp - S.min_tsp
          >= interval '24 hours'
        AND
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' %(with_ds_s)s limit 1)
          , False) = False
        ), false) as acute_organ_failure,
      coalesce(inr_c,0)
      | coalesce(platelets_c,0) | coalesce(creatinine_c,0)
      | coalesce(bilirubin_c,0)
      | coalesce(lactate_c,0) | coalesce(pao2_to_fio2_c,0)
      | coalesce(hypotension_intp_c,0)
      | coalesce(urine_output_24hr_c,0) as acute_organ_failure_c
      FROM %(twf_table_join)s cdm_twf inner join S on
        cdm_twf.enc_id = S.enc_id %(dataset_id_match)s
        %(dataset_id_equal)s
    ''' % {
      'twf_table'           : para.get("twf_table"),
      'twf_table_join'      : para.get("twf_table_join"),
      'dataset_id_key'      : para.get("dataset_id_key"),
      'dataset_id_match'    : dataset_id_match(" and ", "cdm_twf", "S", para.get("dataset_id")),
      'dataset_id_equal'    : dataset_id_equal(" WHERE ", "cdm_twf", para.get("dataset_id")),
      'sub_dataset_id_equal': dataset_id_equal(" and " if para.get("incremental") else "WHERE " , "cdm_twf", para.get("dataset_id")),
      'with_ds_s'           : dataset_id_equal(" and ", "cdm_s", para.get("dataset_id")),
      'incremental_enc_id_join'         : incremental_enc_id_join('cdm_twf', para.get("dataset_id"), para.get("incremental")),
      'incremental_enc_id_match': incremental_enc_id_match(' and ', para.get("incremental"))
    },
    'clean': {'value': False, 'confidence': 0},
  },
  'severe_sepsis': {
    'fid_input_items': ['sirs_intp', 'acute_organ_failure', 'sepsis_note','infections_angus_diag', 'infections_angus_hist'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    sirs_intp is true
                    AND acute_organ_failure is true
                    AND enc_id in
                    (
                    SELECT distinct enc_id FROM cdm_s WHERE
                    (fid = 'infections_angus_diag' AND value like 'True')
                    OR (fid = 'infections_angus_hist' AND value like 'True')
                    OR (fid = 'sepsis_note' AND value like 'True')
                    )
                    ''',
    'fid_c_update_expr': 'coalesce(acute_organ_failure_c,0) | coalesce(sirs_intp_c, 0)',
  },
  'worst_sofa': {
    'fid_input_items': ['resp_sofa', 'hepatic_sofa', 'hematologic_sofa', 'cardio_sofa', 'neurologic_sofa', 'renal_sofa'],
    'derive_type': 'simple',
    'fid_update_expr': '''GREATEST(resp_sofa * (1-based_on_popmean(resp_sofa_c)),
                              hepatic_sofa * (1-based_on_popmean(hepatic_sofa_c)),
                              hematologic_sofa * (1-based_on_popmean(hematologic_sofa_c)),
                              cardio_sofa * (1-based_on_popmean(cardio_sofa_c)),
                              neurologic_sofa * (1-based_on_popmean(neurologic_sofa_c)),
                              renal_sofa * (1-based_on_popmean(renal_sofa_c)))''',
    'fid_c_update_expr': '''
                      coalesce(resp_sofa_c, 0) |
                      coalesce(hepatic_sofa_c, 0) |
                      coalesce(hematologic_sofa_c, 0) |
                      coalesce(cardio_sofa_c, 0) |
                      coalesce(neurologic_sofa_c, 0) |
                      coalesce(renal_sofa_c, 0)
                      ''',
  },
  'any_organ_failure': {
    'fid_input_items': ['worst_sofa'],
    'derive_type': 'simple',
    'fid_update_expr': '(worst_sofa = 4)',
    'fid_c_update_expr': 'worst_sofa_c',
    },
  'sirs_raw': {
    'fid_input_items': ['sirs_temperature_oor', 'sirs_hr_oor', 'sirs_resp_oor', 'sirs_wbc_oor'],
    'derive_type': 'simple',
    'fid_update_expr': '''(cast(sirs_temperature_oor as int)
                      * (1-based_on_popmean(sirs_temperature_oor_c))
                    + cast(sirs_hr_oor as int) * (1-based_on_popmean(sirs_hr_oor_c))
                    + cast(sirs_resp_oor as int) * (1-based_on_popmean(sirs_resp_oor_c))
                    + cast(sirs_wbc_oor as int) * (1-based_on_popmean(sirs_wbc_oor_c))
                    >= 2)''',
    'fid_c_update_expr': 'sirs_temperature_oor_c | sirs_hr_oor_c | sirs_resp_oor_c | sirs_wbc_oor_c',
  },
  'mapm': {
    'fid_input_items': ['abp_mean','nbp_mean'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                   case
                    when based_on_popmean(abp_mean_c) = 0 and abp_mean is not null
                    then abp_mean
                    when based_on_popmean(nbp_mean_c) = 0 and nbp_mean is not null
                    then nbp_mean
                    else coalesce(abp_mean, nbp_mean)
                  end
                   ''',
    'fid_c_update_expr': '''
                      case
                      when based_on_popmean(abp_mean_c) = 0 and abp_mean is not null
                        then abp_mean_c
                      when based_on_popmean(nbp_mean_c) = 0 and nbp_mean is not null
                        then nbp_mean_c
                      else coalesce(abp_mean_c, 0) | coalesce(nbp_mean_c, 0)
                      end
                     ''',
  },
  'dbpm': {
    'fid_input_items': ['abp_dias', 'nbp_dias'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (CASE
                    WHEN based_on_popmean(abp_dias_c) = 1 or abp_dias is null THEN nbp_dias
                    WHEN (abp_dias > 0.15 * nbp_dias)
                      and based_on_popmean(abp_dias_c) = 0
                      and based_on_popmean(nbp_dias_c) = 0 THEN nbp_dias
                    ELSE abp_dias
                    END)
                   ''',
    'fid_c_update_expr': '''
                      (CASE
                      WHEN based_on_popmean(abp_dias_c) = 1 or abp_dias is null THEN nbp_dias_c
                      WHEN (abp_dias > 0.15 * nbp_dias)
                        and based_on_popmean(abp_dias_c) = 0
                        and based_on_popmean(nbp_dias_c) = 0 THEN nbp_dias_c | abp_dias_c
                      ELSE abp_dias_c
                      END)
                     ''',
  },
  'sbpm': {
    'fid_input_items': ['abp_sys', 'nbp_sys'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (CASE
                    WHEN based_on_popmean(abp_sys_c) = 1 or abp_sys is null THEN nbp_sys
                    WHEN (abp_sys < 0.15 * nbp_sys)
                      and based_on_popmean(abp_sys_c) = 0
                      and based_on_popmean(nbp_sys_c) = 0 THEN nbp_sys
                    ELSE abp_sys
                    END)
                   ''',
    'fid_c_update_expr': '''
                      (CASE
                      WHEN based_on_popmean(abp_sys_c) = 1 or abp_sys is null THEN nbp_sys_c
                      WHEN (abp_sys < 0.15 * nbp_sys)
                        and based_on_popmean(abp_sys_c) = 0
                        and based_on_popmean(nbp_sys_c) = 0 THEN nbp_sys_c | abp_sys_c
                      ELSE abp_sys_c
                      END)
                     ''',
  },
  'fluid_resuscitation': {
    'fid_input_items': ['fluids_intake_3hr', 'weight'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                    WHEN fluids_intake_3hr/weight >= 30
                    THEN True
                    ELSE False
                    END)''',
    'fid_c_update_expr': '''
                      (CASE
                      WHEN fluids_intake_3hr/weight >= 30
                        THEN fluids_intake_3hr_c | weight_c
                      ELSE 0
                      END)
                     ''',
  },
  'sirs_wbc_oor': {
    'fid_input_items': ['wbc', 'bands'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
      with subquery as (select enc_id, tsp, confidence from %(cdm_t)s where fid = 'bands' and value::numeric > 10 %(dataset_id_equal_t)s)
      select %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
        bool_or(wbc > 12 or wbc < 4 or subquery.enc_id is not null),
        min(wbc_c | coalesce(subquery.confidence, 0))
      from %(twf_table_join)s cdm_twf left join subquery
      on cdm_twf.enc_id = subquery.enc_id and cdm_twf.tsp >= subquery.tsp and cdm_twf.tsp < subquery.tsp + interval '6 hours'
      %(dataset_id_equal)s
      group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'cdm_t'             : para['cdm_t_target'],
      'twf_table_join'    : para['twf_table_join'],
      'dataset_id_key'    : para['dataset_id_key'],
      'dataset_id_equal'  : dataset_id_equal(" where ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_equal_t': dataset_id_equal('and ', 'cdm_t', para.get("dataset_id")),
    }

  },
  'cmi': {
    'fid_input_items': ['severe_sepsis', 'fluid_resuscitation', 'vasopressor_resuscitation','fluids_intake_1hr'],
    'derive_type': 'subquery',
    'clean': {'value': False, 'confidence': 0},
    'subquery': lambda para: '''
        WITH subquery as (select %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp from %(twf_table_temp_ss)s cdm_twf
           where cdm_twf.severe_sepsis
           %(and_with_ds_twf)s)
        SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          true as cmi,
          min(coalesce(severe_sepsis_c,0)
              | coalesce(fluid_resuscitation_c,0)
              | coalesce(vasopressor_resuscitation_c,0)
              | coalesce(fluids_intake_1hr_c,0)) as cmi_c
        FROM %(twf_table_join)s cdm_twf inner join subquery
        on cdm_twf.enc_id = subquery.enc_id
        and cdm_twf.tsp >= subquery.tsp and cdm_twf.tsp < subquery.tsp + interval '6 hours' %(dataset_id_match)s
        where (fluid_resuscitation or vasopressor_resuscitation or
          fluids_intake_1hr > 250)
        %(and_with_ds_twf)s
        GROUP BY %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
        ''' % {
          'twf_table_temp_ss'    : para['twf_table_temp_severe_sepsis'] if 'twf_table_temp_severe_sepsis' in para else para['twf_table'],
          'twf_table_join'       : para['twf_table_join'],
          'dataset_id_key'       : para['dataset_id_key'],
          'and_with_ds_twf'      : dataset_id_equal(" and ", "cdm_twf", para['dataset_id']),
          'dataset_id_match'     : dataset_id_match(" and ", "cdm_twf", "subquery", para['dataset_id'])
        },
  },
  'minutes_since_any_organ_fail': {
    'fid_input_items': ['any_organ_failure'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    WITH subquery as (
      select  %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp, cdm_twf.any_organ_failure_c c
        FROM %(twf_table_join)s cdm_twf
        where cdm_twf.any_organ_failure %(dataset_id_equal)s
        order by cdm_twf.enc_id, cdm_twf.tsp desc
    )
    SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
    least(EXTRACT(EPOCH FROM (cdm_twf.tsp - first(subquery.tsp)))/60, 14*24*60) as minutes_since_any_organ_fail,
     first(subquery.c) as minutes_since_any_organ_fail_c
    FROM %(twf_table_join)s cdm_twf
    inner join subquery on cdm_twf.enc_id = subquery.enc_id
    and cdm_twf.tsp >= subquery.tsp
    %(dataset_id_match)s
    %(dataset_id_equal_w)s
    group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'twf_table_join'    : para.get("twf_table_join"),
      'dataset_id_key'    : para.get("dataset_id_key"),
      'dataset_id_equal'  : dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_equal_w': dataset_id_equal(" where ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_match'  : dataset_id_match(" and ", "cdm_twf", "subquery", para.get("dataset_id")),
    },
    'clean': {'value': 14*24*60, 'confidence': 0},
  },
  'minutes_to_shock_onset': {
    'fid_input_items': ['septic_shock'],
    'derive_type': 'subquery',
    'subquery': lambda para:
    '''
    WITH subquery as (
      select %(dataset_id_key)s enc_id, min(tsp) tsp, max(septic_shock_c) c from
        %(twf_table_join)s cdm_twf
        where septic_shock %(dataset_id_equal)s
      group by %(dataset_id_key)s enc_id
    )
    SELECT %(dataset_id_key)s enc_id, tsp,
    (case when cdm_twf.tsp > subquery.tsp
        then EXTRACT(EPOCH FROM (subquery.tsp - cdm_twf.tsp))/60
        else 0
     end) minutes_to_shock_onset, subquery.c minutes_to_shock_onset_c
    FROM %(twf_table_join)s cdm_twf inner join subquery on
    cdm_twf.enc_id = subquery.enc_id %(dataset_id_match)s
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_equal': dataset_id_equal(' and ', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_match': dataset_id_match(' and ', 'cdm_twf', 'subquery', para.get("dataset_id")),
    }
  },
  'minutes_since_any_antibiotics': {
    'fid_input_items': ['any_antibiotics'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    WITH subquery as (
      select %(dataset_id_key_t)s cdm_t.enc_id, cdm_t.tsp, cdm_t.confidence c
        from %(cdm_t)s as cdm_t %(incremental_enc_id_join)s
        where cdm_t.fid = 'any_antibiotics' and cdm_t.value::boolean %(dataset_id_equal_t)s %(incremental_enc_id_match)s
      order by cdm_t.enc_id, cdm_t.tsp desc
    )
    SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
     least(EXTRACT(EPOCH FROM (cdm_twf.tsp - first(subquery.tsp)))/60, 24*60) as minutes_since_any_antibiotics,
     first(subquery.c) as minutes_since_any_antibiotics_c
    FROM %(twf_table_join)s cdm_twf inner join subquery on cdm_twf.enc_id = subquery.enc_id
    and cdm_twf.tsp >= subquery.tsp
    %(dataset_id_match)s
    %(dataset_id_equal)s
    group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'cdm_t'         : para.get('cdm_t_target'),
      'dataset_id_key': para.get("dataset_id_key"),
      'dataset_id_key_t': dataset_id_key('cdm_t', para.get('dataset_id')),
      'dataset_id_equal_t': dataset_id_equal('and ', 'cdm_t', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(' and ', 'cdm_twf', para.get("dataset_id")),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_match': dataset_id_match(" and ", "cdm_twf", "subquery", para.get("dataset_id")),
      'incremental_enc_id_join': incremental_enc_id_join('cdm_t', para.get("dataset_id"), para.get("incremental")),
      'incremental_enc_id_match': incremental_enc_id_match(' and ', para.get('incremental'))
    },
    'clean': {'value': 1*24*60, 'confidence': 0},
  },
  'treatment_within_6_hours': {
    'fid_input_items': ['any_antibiotics',
                                'any_anticoagulant',
                                'any_beta_blocker',
                                'any_glucocorticoid',
                                'any_inotrope',
                                'any_pressor',
                                'vent',],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    WITH subquery as (
        select %(dataset_id_key)s enc_id,tsp, confidence::int c from cdm_t
        %(incremental_enc_id_join)s
        where
          (fid = 'any_antibiotics'
          or fid = 'any_anticoagulant'
          or fid = 'any_beta_blocker'
          or fid = 'any_glucocorticoid'
          or fid = 'any_inotrope'
          or fid = 'any_pressor'
          or fid = 'vent' and value = 'True')
          %(dataset_id_equal_t)s %(incremental_enc_id_match)s
    )
    SELECT %(dataset_id_key)s enc_id, tsp
     (case when cdm_twf.treatment_within_6_hours then true
        when not cdm_twf.treatment_within_6_hours
          and cdm_twf.tsp > subquery.tsp
          and cdm_twf.tsp - subquery.tsp < interval '6 hours'
        then true
        else false
     end) as treatment_within_6_hours,
    coalesce(cdm_twf.treatment_within_6_hours_c, 0) | coalesce(subquery.c, 0) as treatment_within_6_hours_c
    FROM %(twf_table_join)s cdm_twf
    inner join subquery on cdm_twf.enc_id = subquery.enc_id %(dataset_id_match)s
    %(dataset_id_equal)s
    ''' % {
      'dataset_id_key': para.get('dataset_id_key'),
      'dataset_id_equal_t': dataset_id_equal(' and ', 'cdm_t', para.get('dataset_id')),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_match': dataset_id_match(' and ', 'cdm_twf', 'subquery', para.get('dataset_id')),
      'dataset_id_equal': dataset_id_equal(' and ', 'cdm_twf', para.get('dataset_id')),
      'incremental_enc_id_join': incremental_enc_id_join('cdm_t', para.get("dataset_id"), para.get("incremental")),
      'incremental_enc_id_match': incremental_enc_id_match(' and ', para.get('incremental'))
    }
  },
  'fluids_intake_3hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float,0)) fluids_intake_3hr,
          max(coalesce(cdm_t.confidence, 0)) fluids_intake_3hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '3 hours' %(dataset_id_match)s
          where fid = 'fluids_intake' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'renal_sofa': {
    'fid_input_items': ['creatinine'],
    'derive_type': 'simple',
    'fid_update_expr': '''(
               case when creatinine > 5.0 then 4
                when creatinine >= 3.5 then 3
                when creatinine >= 2.0 then 2
                when creatinine >= 1.2 then 1
                else 0 end
            )''',
    'fid_c_update_expr': 'creatinine_c'
  },
  # renal_sofa: with urine_output_24hr version
  # 'renal_sofa': {
  #   'fid_input_items': ['creatinine', 'urine_output_24hr'],
  #   'derive_type': 'subquery',
  #   'subquery': lambda para: '''
  #         WITH S as (SELECT %(dataset_id_key)s cdm_twf.enc_id, min(cdm_twf.tsp) min_tsp
  #         FROM %(twf_table_uo)s cdm_twf %(incremental_enc_id_join)s
  #         %(where_dataset_id_equal)s %(incremental_enc_id_match)s
  #         group by %(dataset_id_key)s cdm_twf.enc_id)
  #         select %(dataset_id_key_U)s U.enc_id, U.tsp, max(U.renal_sofa) renal_sofa, max(U.renal_sofa_c) renal_sofa_c from
  #         (select
  #           %(dataset_id_key)s enc_id, tsp,
  #           (
  #             case when creatinine > 5.0 then 4
  #              when creatinine >= 3.5 then 3
  #              when creatinine >= 2.0 then 2
  #              when creatinine >= 1.2 then 1
  #              else 0 end
  #           ) as renal_sofa,
  #           coalesce(creatinine_c, 0) as renal_sofa_c
  #           from %(twf_table)s cdm_twf %(incremental_enc_id_join)s
  #           %(where_dataset_id_equal)s %(incremental_enc_id_match)s
  #         union
  #         select
  #           %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
  #           (
  #             case when urine_output_24hr < 200 then 4
  #              else 3
  #             end
  #           ) as renal_sofa,
  #           coalesce(urine_output_24hr_c, 0) as renal_sofa_c
  #           from %(twf_table_uo)s cdm_twf inner join S on cdm_twf.enc_id = S.enc_id %(dataset_id_match)s %(incremental_enc_id_join)s
  #           where urine_output_24hr < 500 and tsp - min_tsp >= '24 hours'::interval %(dataset_id_equal)s %(incremental_enc_id_match)s
  #         ) U
  #         group by %(dataset_id_key_U)s enc_id, tsp
  #   ''' % {
  #     'dataset_id_key': para.get("dataset_id_key"),
  #     'dataset_id_key_U': "dataset_id," if para.get("dataset_id") else '',
  #     'twf_table': para.get('twf_table'),
  #     'twf_table_uo': para.get('derive_feature_addr')['urine_output_24hr']['twf_table_temp'] if 'urine_output_24hr' in para.get('derive_feature_addr') else para.get('twf_table'),
  #     'dataset_id_match': dataset_id_match(' and ','S', 'cdm_twf', para.get("dataset_id")),
  #     'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
  #     'where_dataset_id_equal': dataset_id_equal(" where ", "cdm_twf", para.get("dataset_id")),
  #     'incremental_enc_id_join': incremental_enc_id_join('cdm_twf', para.get("dataset_id"), para.get("incremental")),
  #     'incremental_enc_id_match': incremental_enc_id_match(' and ', para.get('incremental'))
  #   },
  'fluids_intake_1hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float,0)) fluids_intake_1hr,
          max(coalesce(cdm_t.confidence, 0)) fluids_intake_1hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '1 hours' %(dataset_id_match)s
          where fid = 'fluids_intake' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'fluids_intake_24hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float,0)) fluids_intake_24hr,
          max(coalesce(cdm_t.confidence, 0)) fluids_intake_24hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '24 hours'
          %(dataset_id_match)s
          where fid = 'fluids_intake' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'urine_output_6hr': {
    'fid_input_items': ['urine_output'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
      SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
      sum(coalesce(cdm_t.value::float, 0)) urine_output_6hr,
      max(coalesce(cdm_t.confidence, 0)) urine_output_6hr_c
      from %(twf_table_join)s cdm_twf
      inner join
      (
        -- remove negative cases and any cases within 6 hours window of the negative cases and volumn >= 1000
        select distinct cdm_t.*
         from %(cdm_t)s as cdm_t left join
        (
          select * from %(cdm_t)s where fid = 'urine_output'
          and value::numeric < 0 %(dataset_id_equal_t)s
        ) neg on cdm_t.enc_id = neg.enc_id
          and cdm_t.tsp - neg.tsp <= interval '6 hours'
          and neg.tsp - cdm_t.tsp <= interval '6 hours'
        where cdm_t.fid = 'urine_output' and cdm_t.value::numeric > 0
        and (neg.tsp is null or cdm_t.value::numeric < 1000)
        %(dataset_id_equal_t)s
      )
      cdm_t
      on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
      and cdm_t.tsp > cdm_twf.tsp - interval '6 hours' %(dataset_id_match)s
      where fid = 'urine_output' %(dataset_id_equal)s
      group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'cdm_t'         : para.get('cdm_t_target'),
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_equal_t': dataset_id_equal(" and ", "cdm_t", para.get("dataset_id")),
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'urine_output_24hr': {
    'fid_input_items': ['urine_output'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float, 0)) urine_output_24hr,
          max(coalesce(cdm_t.confidence, 0)) urine_output_24hr_c
          from %(twf_table_join)s cdm_twf
          left join
          (
            -- remove negative cases and any cases within 6 hours window of the negative cases and volumn >= 1000
            select distinct cdm_t.*
             from %(cdm_t)s as cdm_t left join
            (
              select * from %(cdm_t)s where fid = 'urine_output'
              and value::numeric < 0 %(dataset_id_equal_t)s
            ) neg on cdm_t.enc_id = neg.enc_id
              and cdm_t.tsp - neg.tsp <= interval '6 hours'
              and neg.tsp - cdm_t.tsp <= interval '6 hours'
            where cdm_t.fid = 'urine_output' and cdm_t.value::numeric > 0
            and (neg.tsp is null or cdm_t.value::numeric < 1000)
            %(dataset_id_equal_t)s
          )
          cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '24 hours' %(dataset_id_match)s
          where fid = 'urine_output' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'cdm_t'         : para.get('cdm_t_target'),
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_equal_t': dataset_id_equal(" and ", "cdm_t", para.get("dataset_id")),
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'organ_dysfunction': {
    'fid_input_items': ['resp_sofa', 'renal_sofa', 'hepatic_sofa', 'hematologic_sofa', 'neurologic_sofa', 'inr'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    with S as (
      select * from %(twf_table_join)s cdm_twf
    ),
    enc_ids as (
      select distinct enc_id
      from S
    ),
    diagnosis as (
    select e.enc_id,
      max(case when fid ~ '^chronic_kidney' then value::boolean::int else 0 end) chronic_kidney,
      max(case when fid ~ '^renal_insufficiency' then value::boolean::int else 0 end) renal_insufficiency,
      max(case when fid ~ '^diabetes' then value::boolean::int else 0 end) diabetes,
      max(case when fid ~ '^esrd' then value::boolean::int else 0 end) esrd,
      max(case when fid ~ '^liver_disease' then value::boolean::int else 0 end) liver_disease,
      max(case when fid ~ '^hem_malig' then value::boolean::int else 0 end) hem_malig
    from enc_ids e left join cdm_s s on e.enc_id = s.enc_id
    and fid ~ '_hist|_diag|_prob' %(dataset_id_equal_s)s
    group by e.enc_id
    ),
    baseline as (
      select D.*,
        (case when baseline_hematologic_sofa > 0 then 1 else 300 end) baseline_platelets,
        (case when baseline_renal_sofa > 0 then 1.2 else 0.6 end) baseline_creatinine,
        (case when baseline_hematologic_sofa > 0 then 1.2 else 1 end) baseline_bilirubin,
        0 baseline_resp_sofa,
        0 baseline_cardio_sofa,
        0 baseline_neurologic_sofa,
        0 baseline_inr_sofa
      from (
        select diagnosis.*,
          (case when 0.02424718*(chronic_kidney-0.01644633)/0.12718431 + 0.02340346*(renal_insufficiency-0.01042278)/0.10155857 + 0.01904245*(diabetes-0.14447503)/0.35157075 > 0.1617511 then 1 else 0 end) baseline_renal_sofa,
          (case when esrd = 1 then 4 else 0 end) baseline_hepatic_sofa,
          (case when 0.00719707*(liver_disease-0.00985877)/0.0988007 + 0.00224878*(hem_malig-0.08369806)/0.27693445 > 0.18768301 then 1 else 0 end) baseline_hematologic_sofa,
          (case when 0.01025011*(diabetes-0.14447503)/0.35157075 + 0.01315644*(hem_malig-0.08369806)/0.27693445 + 0.01247086*(liver_disease-0.00985877)/0.0988007 > 0.17795244 then 1.2 else 0.95 end) baseline_inr
        from diagnosis) D
    ),
    vent as (
      select enc_id, tsp
      from %(cdm_t)s
      where fid = 'vent'
      %(dataset_id_equal_t)s
    ),
    warfarin_dose as (
      select enc_id, tsp
      from %(cdm_t)s
      where fid = 'warfarin_dose' and value::json->>'action' = 'Given'
      %(dataset_id_equal_t)s
    ),
    vasopressors as (
      select distinct enc_id
      from %(cdm_t)s
      where fid in ('dopamine_dose','vasopressin_dose','epinephrine_dose','levophed_infusion_dose','neosynephrine_dose')
      %(dataset_id_equal_t)s
    ),
    sofa_score as (
      select distinct cdm_twf.enc_id, cdm_twf.tsp,
        (case
          when vent.tsp is null then cdm_twf.resp_sofa
          else bs.baseline_resp_sofa + 2
        end) resp_sofa,
        (case when esrd > 0 then 4 else cdm_twf.renal_sofa end) renal_sofa,
        cdm_twf.hepatic_sofa hepatic_sofa,
        cdm_twf.hematologic_sofa hematologic_sofa,
        cdm_twf.neurologic_sofa neurologic_sofa,
        (case when cdm_twf.inr > 1.5 and cdm_twf.inr > 0.5 * bs.baseline_inr and warfarin_dose.tsp is null then bs.baseline_inr_sofa + 2
         else 0 end) inr_sofa,
        (case when vasopressors.enc_id is not null then bs.baseline_cardio_sofa + 2
         else 0 end) cardio_sofa
      from S cdm_twf
      inner join baseline bs on cdm_twf.enc_id = bs.enc_id
      left join vent on cdm_twf.enc_id = vent.enc_id and cdm_twf.tsp between vent.tsp and vent.tsp + '48 hours'::interval
      left join warfarin_dose on cdm_twf.enc_id = warfarin_dose.enc_id and cdm_twf.tsp between warfarin_dose.tsp and warfarin_dose.tsp + '24 hours'::interval
      left join vasopressors on cdm_twf.enc_id = vasopressors.enc_id
    )
    select %(dataset_id)s s.enc_id, s.tsp,
      (
        s.renal_sofa +
        s.resp_sofa +
        s.cardio_sofa +
        s.neurologic_sofa +
        s.hepatic_sofa +
        s.hematologic_sofa +
        s.inr_sofa
        >= 2 + b.baseline_renal_sofa
             + b.baseline_resp_sofa
             + b.baseline_cardio_sofa
             + b.baseline_neurologic_sofa
             + b.baseline_hepatic_sofa
             + b.baseline_hematologic_sofa
             + b.baseline_inr_sofa
      )::int organ_dysfunction,
      0 organ_dysfunction_c
    from sofa_score s inner join baseline b on s.enc_id = b.enc_id
    ''' % {
      'dataset_id': '{},'.format(para.get("dataset_id")) if para.get("dataset_id") else '',
      'twf_table_join': para.get('twf_table_join'),
      'cdm_t': para.get('cdm_t_target'),
      'dataset_id_equal_t': dataset_id_equal(" and ", "cdm_t", para.get("dataset_id")),
      'dataset_id_equal_s': dataset_id_equal(" and ", "s", para.get("dataset_id")),
      'where_dataset_id_equal': dataset_id_equal(" where ", "cdm_twf", para.get("dataset_id")),
    },
    'clean': {'value': 'null', 'confidence': 'null'}
  },
  #####################
  # cdm_t features
  #####################
  'any_anticoagulant': {
    'fid_input_items': ['apixaban_dose', 'dabigatran_dose', 'rivaroxaban_dose', 'warfarin_dose', 'heparin_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp, 'any_anticoagulant', 'True', max(cdm_t.confidence) confidence FROM %(cdm_t)s as cdm_t %(incremental_enc_id_join)s
                                WHERE fid ~ '^(apixaban|dabigatran|rivaroxaban|warfarin|heparin)_dose$' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s %(incremental_enc_id_match)s %(lookbackhours)s
                                group by %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp''',
  },
  'any_beta_blocker': {
    'fid_input_items': ['acebutolol_dose', 'atenolol_dose', 'bisoprolol_dose', 'metoprolol_dose', 'nadolol_dose', 'propranolol_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp, 'any_beta_blocker', 'True', max(cdm_t.confidence) confidence FROM %(cdm_t)s as cdm_t %(incremental_enc_id_join)s
                                WHERE fid ~ '^(acebutolol|atenolol|bisoprolol|metoprolol|nadolol|propranolol)_dose$' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s %(incremental_enc_id_match)s %(lookbackhours)s
                                group by %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp''',
  },
  'any_glucocorticoid': {
    'fid_input_items': ['hydrocortisone_dose', 'prednisone_dose', 'prednisolone_dose', 'methylprednisolone_dose', 'dexamethasone_dose', 'betamethasone_dose', 'fludrocortisone_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp, 'any_glucocorticoid', 'True', max(cdm_t.confidence) confidence FROM %(cdm_t)s as cdm_t %(incremental_enc_id_join)s
                                WHERE fid ~ '^(hydrocortisone|prednisone|prednisolone|methylprednisolone|dexamethasone|betamethasone|fludrocortisone)_dose$' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s %(incremental_enc_id_match)s %(lookbackhours)s
                                group by %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp''',
  },
  'any_antibiotics': {
    'fid_input_items': ['ampicillin_dose', 'clindamycin_dose', 'erythromycin_dose' , 'gentamicin_dose' , 'oxacillin_dose' , 'tobramycin_dose' , 'vancomycin_dose' , 'ceftazidime_dose' , 'cefazolin_dose' , 'penicillin_g_dose' , 'meropenem_dose' , 'penicillin_dose' , 'amoxicillin_dose' , 'piperacillin_tazobac_dose', 'rifampin_dose', 'meropenem_dose', 'rapamycin_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
      SELECT distinct %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp, 'any_antibiotics', 'True', max(cdm_t.confidence) confidence FROM %(cdm_t)s as cdm_t %(incremental_enc_id_join)s
      WHERE fid ~ '^(ampicillin|clindamycin|erythromycin|gentamicin|oxacillin|tobramycin|vancomycin|ceftazidime|cefazolin|penicillin_g|meropenem|penicillin|amoxicillin|piperacillin_tazobac|rifampin|meropenem|rapamycin)_dose$'
       AND ((isnumeric(value) and value::numeric > 0) or (not isnumeric(value) and cast(value::json->>'dose' as numeric) > 0))
       %(dataset_where_block)s %(incremental_enc_id_match)s %(lookbackhours)s
      group by %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp''',
  },
  'any_antibiotics_order': {
    'fid_input_items': ['ampicillin_dose', 'clindamycin_dose', 'erythromycin_dose' , 'gentamicin_dose' , 'oxacillin_dose' , 'tobramycin_dose' , 'vancomycin_dose' , 'ceftazidime_dose' , 'cefazolin_dose' , 'penicillin_g_dose' , 'meropenem_dose' , 'penicillin_dose' , 'amoxicillin_dose' , 'piperacillin_tazobac_dose', 'rifampin_dose', 'meropenem_dose', 'rapamycin_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
      SELECT distinct %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp, 'any_antibiotics_order', 'True', max(cdm_t.confidence) confidence FROM %(cdm_t)s as cdm_t %(incremental_enc_id_join)s
      WHERE fid ~ '^(ampicillin|clindamycin|erythromycin|gentamicin|oxacillin|tobramycin|vancomycin|ceftazidime|cefazolin|penicillin_g|meropenem|penicillin|amoxicillin|piperacillin_tazobac|rifampin|meropenem|rapamycin)_dose_order$'
       AND ((isnumeric(value) and value::numeric > 0) or (not isnumeric(value) and cast(value::json->>'dose' as numeric) > 0))
       %(dataset_where_block)s %(incremental_enc_id_match)s %(lookbackhours)s
      group by %(dataset_col_block)s cdm_t.enc_id, cdm_t.tsp''',
  },
}