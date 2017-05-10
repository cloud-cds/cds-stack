#########################
# query helper functions
#########################

def dataset_id_equal(prefix, table, dataset_id_val):
  return "{}{}.dataset_id = {}".format(prefix, table, dataset_id_val) if dataset_id_val else ''

def dataset_id_match(prefix, left_table, right_table, dataset_id_val):
  return "{}{}.dataset_id = {}.dataset_id".format(prefix, left_table, right_table) if dataset_id_val else ''

def dataset_id_key(table, dataset_id):
  return '{}dataset_id, '.format('{}.'.format(table) if table else '') if dataset_id is not None else ''

def get_src_twf_table(derive_feature_addr):
  for fid in derive_feature_addr:
    if derive_feature_addr[fid]['twf_table'] is not None:
      return derive_feature_addr[fid]['twf_table']

def get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id):
  src_twf_table = get_src_twf_table(derive_feature_addr)
  twf_table = None
  existing_tables = set()
  sql_template = 'SELECT {cols} FROM {table_joins}'
  cols = ''
  table_joins = ''
  cur_twf_table = None
  for fid in fid_input_items:
    if fid in derive_feature_addr:
      # derive feature
      if derive_feature_addr[fid]['category'] == 'TWF':
        cur_twf_table = derive_feature_addr[fid]['twf_table_temp']
    elif cdm_feature_dict[fid]['category'] == 'TWF':
      # measured TWF feature
      cur_twf_table = src_twf_table
    else:
      cur_twf_table = None
    if cur_twf_table: # only for TWF features
      if twf_table:
        if not cur_twf_table in existing_tables:
          existing_tables.add(cur_twf_table)
          table_joins += ' inner join {tbl} on {tbl}.enc_id = {twf_table}.enc_id and {tbl}.tsp = {twf_table}.tsp {dataset_match}'.format(
              tbl=cur_twf_table,
              twf_table=twf_table,
              dataset_match=dataset_id_match('and ', cur_twf_table, twf_table, dataset_id)
            )
        cols += ', {tbl}.{fid}, {tbl}.{fid}_c'.format(
            dataset_id_key=dataset_id_key(twf_table, dataset_id),
            tbl=cur_twf_table,
            fid=fid
          )
      else:
        twf_table = cur_twf_table
        existing_tables.add(cur_twf_table)
        table_joins += twf_table
        cols += '{dataset_id_key} {tbl}.enc_id, {tbl}.tsp, {tbl}.{fid}, {tbl}.{fid}_c'.format(
            dataset_id_key=dataset_id_key(twf_table, dataset_id),
            tbl=twf_table,
            fid=fid
          )
  if cols == '' and table_joins == '':
    cols = '{dataset_id_key} {tbl}.enc_id, {tbl}.tsp'.format(
            dataset_id_key=dataset_id_key(src_twf_table, dataset_id),
            tbl=src_twf_table
          )
    table_joins = src_twf_table
  table_joins += dataset_id_equal(' where ', twf_table if twf_table else src_twf_table, dataset_id)
  return sql_template.format(cols=cols, table_joins=table_joins)
