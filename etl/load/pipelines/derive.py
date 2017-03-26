# derive pipeline for cdm_twf
import load.primitives.tbl.derive as derive_func

async def derive_main(log, conn, cdm_feature_dict, mode=None, fid=None, table="cdm_twf"):
  '''
  mode: "append", run derive functions beginning with @fid sequentially
  mode: "dependent", run derive functions for @fid and other features depends on @fid
  mode: None, run derive functions sequentially for all derive features
  '''
  # generate a sequence to derive
  derive_feature_order = get_derive_seq(cdm_feature_dict.values())
  log.info("derive feautre order:" + derive_feature_order)
  if mode == 'append':
    append = fid
    log.info("starts from feature %s" % append)
    idx = derive_feature_order.index(append)
    for i in range(idx, len(derive_feature_order)):
      fid = derive_feature_order[i]
      await derive_feature(log, cdm_feature_dict[fid], conn, twf_table=table)
  elif mode == 'dependent':
    dependent = fid
    if cdm_feature_dict[fid]['is_measured'] == 'no':
      log.info("update feature %s and its dependents" % dependent)
      await derive_feature(log, cdm_feature_dict[fid], conn, twf_table=table)
    else:
      log.info("update feature %s's dependents" % dependent)
    derive_feature_order = get_dependent_features([dependent], features)
    for fid in derive_feature_order:
      await derive_feature(log, cdm_feature_dict[fid], conn, twf_table=table)
  elif mode is None and fid is None:
    print "derive features one by one"
    for fid in derive_feature_order:
      await derive_feature(log, cdm_feature_dict[fid], conn, twf_table=table)
  else:
    print "Unknown mode!"

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
    d_map = dict((feature['fid'], feature['derive_func_input']) \
      for feature in features if ((feature['is_measured'] == 'no') \
      and (feature['is_deprecated'] == 'no')))
  derived_features = d_map.keys()

  # clear out dependencies on measured features, they should be in CDM already
  d_map = dict((k,rm_measured_dependencies(v, derived_features)) \
    for (k,v) in d_map.iteritems())

  while (len(d_map) != 0):
    ind =  [k for k in d_map if len(d_map[k]) == 0]
    order.extend(ind)
    d_map = dict((k,v) for (k,v) in d_map.iteritems() if k not in order)
    d_map = dict((k, reduce_dependencies(v)) for (k, v) in d_map.iteritems())
  return order

async def derive_feature(log, feature, conn, twf_table='cdm_twf'):
  fid = feature['fid']
  derive_func_id = feature['derive_func_id']
  derive_func_input = feature['derive_func_input']
  log.info("derive feature %s, function %s, inputs (%s)" \
    % (fid, derive_func_id, derive_func_input))
  derive_func.derive(fid, derive_func_id, derive_func_input, conn, log, twf_table)
  log.info("derive feature %s end." % fid)

derive_config = {
  'bun_to_cr': {
    'fid_input_items'   : ['bun', 'creatinine'],
    'derive_type'       : 'simple',
    'fid_update_expr'   : 'bun/creatinine'
    'fid_c_update_expr' : 'creatinine_c | bun_c'
  },
}

async def derive_func_driver(log, fid, conn, twf_table='cdm_twf'):
  if fid in derive_config:
    config_entry = derive_config[fid]
    fid_input_items = [item.strip() for item in fid_input.split(',')]

    if fid_input_items == config_entry['fid_input_items']:

      cdm.clean_twf(fid, twf_table=twf_table)
      update_clause = ''

      if config_entry['derive_type'] == 'simple':
        update_clause = """
        UPDATE %(twf_table)s SET %(fid)s = %(update_expr)s,
          %(fid)s_c = %(c_update_expr)s
        """ % {
          'fid':fid,
          'update_expr': config_entry['fid_update_expr'],
          'c_update_expr': config_entry['fid_c_update_expr']
        }

      elif config_entry['derive_type'] == 'subquery':


      await conn.execute(update_clause)

    else:
      raise Exception(...)
  else:
    raise Exception(...)

