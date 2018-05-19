# derive pipeline for cdm_twf
import etl.load.primitives.tbl.derive as derive_func
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive import with_ds
from etl.load.primitives.tbl.derive_helper import *

import time
import os
CLEAN_SQL = False
IGNORE_LEGACY_DERIVE = True
PARRALELL = 16
n_conn = int(os.environ['n_conn']) if 'n_conn' in os.environ else 2

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


async def derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=None, derive_feature_addr=None,incremental=False, cdm_t_target='cdm_t', cdm_t_lookbackhours=None, workspace=None, job_id=None):
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
          if CLEAN_SQL:
            clean_sql = clean_tbl.cdm_twf_clean(fid, twf_table=twf_table_temp, \
              dataset_id = dataset_id, incremental=incremental, **clean_args)
        if config_entry['derive_type'] == 'simple':
          sql = gen_simple_twf_query(config_entry, fid, dataset_id, \
            derive_feature_addr, cdm_feature_dict, incremental)
        elif config_entry['derive_type'] == 'subquery':
          sql = gen_subquery_upsert_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, workspace, job_id)
        log.debug(clean_sql + sql)
        await conn.execute(clean_sql + sql)
      elif fid_category == 'T':
        # Note they do not touch TWF table
        sql = gen_cdm_t_upsert_query(config_entry, fid, dataset_id, \
          incremental, cdm_t_target, cdm_t_lookbackhours, workspace, job_id)
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
    if not IGNORE_LEGACY_DERIVE:
      await derive_func.derive(fid, derive_func_id, derive_func_input, conn, \
        log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours)
  te = time.time()
  log.info("derive feature %s end. (%2.2f secs)" % (fid, te-ts))



def gen_simple_twf_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
  twf_table = derive_feature_addr[fid]['twf_table']
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  fid_input_items = config_entry['fid_input_items']
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
  if PARRALELL:
    sql = []
    for i in range(PARRALELL):
      sql += ["""
      INSERT INTO %(twf_table_temp)s (%(dataset_id_key)s enc_id, tsp, %(insert_cols)s)
      SELECT %(dataset_id_key)s enc_id, tsp, %(select_cols)s FROM
      (%(select_table_joins)s) source
      ON CONFLICT (%(dataset_id_key)s enc_id, tsp) DO UPDATE SET
      %(update_cols)s
      """ % {
        'twf_table_temp'     : twf_table_temp,
        'dataset_id_key'     : dataset_id_key(None, dataset_id),
        'select_table_joins' : get_select_table_joins(fid_input_items, \
                                  derive_feature_addr, cdm_feature_dict, dataset_id, incremental, parallel=(PARRALELL, i)),
        'select_cols'        : '({update_expr}) as {fid}, ({c_update_expr}) as {fid}_c'.format(\
                                      update_expr=update_expr, fid=fid, c_update_expr=c_update_expr),
        'update_cols'        : '{fid} = excluded.{fid}, {fid}_c = excluded.{fid}_c'.format(fid=fid),
        'insert_cols'        : '{fid}, {fid}_c'.format(fid=fid)
      }]
    sql = ','.join(["'{}'".format(s.replace("'", "''")) for s in sql])
    sql_parallel = """
    select * from distribute('dblink_dist', array[{}], {});
    """.format(sql, n_conn)
    return sql_parallel
  else:
    select_table_joins = get_select_table_joins(fid_input_items, \
                                  derive_feature_addr, cdm_feature_dict, dataset_id, incremental)
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

def gen_subquery_upsert_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, workspace, job_id):
  if PARRALELL:
    sql = []
    for i in range(PARRALELL):
      twf_table = derive_feature_addr[fid]['twf_table']
      twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
      subquery_params = {}
      subquery_params['incremental'] = incremental
      subquery_params['fid'] = fid
      fid_input_items = config_entry['fid_input_items']
      # generate twf_table from selection
      subquery_params['twf_table_join'] = '(' + \
        get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id, incremental, workspace=workspace, job_id=job_id, parallel=(PARRALELL, i)) \
        + ')'
      subquery_params['twf_table'] = twf_table
      subquery_params['cdm_t_target'] = cdm_t_target
      subquery_params['dataset_id'] = dataset_id
      subquery_params['workspace'] = workspace
      subquery_params['job_id'] = job_id
      subquery_params['with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=False, parallel=(PARRALELL, i))
      subquery_params['and_with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=True, parallel=(PARRALELL, i))
      subquery_params['with_ds_t'] = with_ds(dataset_id, table_name='cdm_t' if '(' in cdm_t_target else cdm_t_target, conjunctive=True, parallel=(PARRALELL, i))
      subquery_params['with_ds_ttwf'] = (' AND cdm_t.dataset_id = cdm_twf.dataset_id' if dataset_id else '') + with_ds(dataset_id, table_name='cdm_twf', conjunctive=False, parallel=(PARRALELL, i))
      subquery_params['parallel'] = (PARRALELL, i)
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
      sql.append("'{}'".format(upsert_clause.replace("'", "''")))
    sql_parallel = """
    select * from distribute('dblink_dist', array[{}], {});
    """.format(','.join(sql), n_conn)
    return sql_parallel
  else:
    twf_table = derive_feature_addr[fid]['twf_table']
    twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
    subquery_params = {}
    subquery_params['incremental'] = incremental
    subquery_params['fid'] = fid
    fid_input_items = config_entry['fid_input_items']
    # generate twf_table from selection
    subquery_params['twf_table_join'] = '(' + \
      get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id, incremental, workspace=workspace, job_id=job_id) \
      + ')'
    subquery_params['twf_table'] = twf_table
    subquery_params['cdm_t_target'] = cdm_t_target
    subquery_params['dataset_id'] = dataset_id
    subquery_params['workspace'] = workspace
    subquery_params['job_id'] = job_id
    subquery_params['with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=False)
    subquery_params['and_with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=True)
    subquery_params['with_ds_t'] = with_ds(dataset_id, table_name='cdm_t' if '(' in cdm_t_target else cdm_t_target, conjunctive=True)
    subquery_params['with_ds_ttwf'] = (' AND cdm_t.dataset_id = cdm_twf.dataset_id' if dataset_id else '') + with_ds(dataset_id, table_name='cdm_twf', conjunctive=False)
    subquery_params['dataset_id_key'] = dataset_id_key('cdm_twf', dataset_id)
    for fid_input in config_entry['fid_input_items']:
      if fid_input in derive_feature_addr:
        subquery_params['twf_table_{}'.format(fid_input)] = derive_feature_addr[fid_input]['twf_table']
        subquery_params['twf_table_temp_{}'.format(fid_input)] = derive_feature_addr[fid_input]['twf_table_temp']
    subquery_params['derive_feature_addr'] = derive_feature_addr
    subquery_params['parallel'] = None
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

def gen_cdm_t_upsert_query(config_entry, fid, dataset_id, incremental, cdm_t_target, cdm_t_lookbackhours, workspace, job_id):
  if PARRALELL:
    sql = []
    for i in range(PARRALELL):
      fid_select_expr = config_entry['fid_select_expr'] % {
        'cdm_t': cdm_t_target,
        'dataset_col_block': 'cdm_t.dataset_id,' if dataset_id is not None else '',
        'dataset_where_block': (' and cdm_t.dataset_id = %s and cdm_t.enc_id %% %s = %s' % (dataset_id, PARRALELL, i)) if dataset_id is not None else '',
        'incremental_enc_id_join': incremental_enc_id_join('cdm_t', dataset_id, incremental),
        'incremental_enc_id_match': incremental_enc_id_match(' and ', incremental),
        'lookbackhours': " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else '',
        'push_delta_cdm_t_join': push_delta_cdm_t_join('cdm_t', workspace, job_id),
        'push_delta_cdm_t_match': push_delta_cdm_t_match(' and ', workspace, job_id)
      }
      upsert_clause = ("""
      INSERT INTO %(cdm_t)s (%(dataset_col_block)s enc_id,tsp,fid,value,confidence) (%(select_expr)s)
      ON CONFLICT (%(dataset_col_block)s enc_id,tsp,fid) DO UPDATE SET
      value = excluded.value, confidence = excluded.confidence;
      """) % {
        'cdm_t': 'cdm_t' if '(' in cdm_t_target else cdm_t_target,
        'fid':fid,
        'select_expr': fid_select_expr,
        'dataset_col_block': 'dataset_id,' if dataset_id is not None else '',
        'dataset_where_block': (' and dataset_id = %s' % dataset_id) if dataset_id is not None else '',
        'incremental_enc_id_in': incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)
      }
      upsert_clause = "'{}'".format(upsert_clause.replace("'","''"))
      sql.append(upsert_clause)
    sql_parallel = """
    select * from distribute('dblink_dist', array[{}], {});
    """.format(','.join(sql), n_conn)
    return sql_parallel
  else:
    fid_select_expr = config_entry['fid_select_expr'] % {
      'cdm_t': cdm_t_target,
      'dataset_col_block': 'cdm_t.dataset_id,' if dataset_id is not None else '',
      'dataset_where_block': (' and cdm_t.dataset_id = %s' % dataset_id) if dataset_id is not None else '',
      'incremental_enc_id_join': incremental_enc_id_join('cdm_t', dataset_id, incremental),
      'incremental_enc_id_match': incremental_enc_id_match(' and ', incremental),
      'lookbackhours': " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else '',
      'push_delta_cdm_t_join': push_delta_cdm_t_join('cdm_t', workspace, job_id),
      'push_delta_cdm_t_match': push_delta_cdm_t_match(' and ', workspace, job_id)
    }
    # print(fid_select_expr)
    delete_clause = ''
    if dataset_id and not incremental and CLEAN_SQL:
      # only delete existing data in offline full load mode
      delete_clause = "DELETE FROM %(cdm_t)s where fid = '%(fid)s' %(dataset_where_block)s;\n"

    upsert_clause = (delete_clause + """
    INSERT INTO %(cdm_t)s (%(dataset_col_block)s enc_id,tsp,fid,value,confidence) (%(select_expr)s)
    ON CONFLICT (%(dataset_col_block)s enc_id,tsp,fid) DO UPDATE SET
    value = excluded.value, confidence = excluded.confidence;
    """) % {
      'cdm_t': 'cdm_t' if '(' in cdm_t_target else cdm_t_target,
      'fid':fid,
      'select_expr': fid_select_expr,
      'dataset_col_block': 'dataset_id,' if dataset_id is not None else '',
      'dataset_where_block': (' and dataset_id = %s' % dataset_id) if dataset_id is not None else '',
      'incremental_enc_id_in': incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)
    }
    return upsert_clause

query_config = {
  'A': {
    'fid_input_items':  ['B', 'C'],
    'derive_type': 'simple',
    'fid_update_expr': 'B+C',
    'fid_c_update_expr': 'B_c | C_c'
  },
}