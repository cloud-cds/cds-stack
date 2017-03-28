async def cdm_twf_clean(conn, fid, value='null', confidence='null', twf_table='cdm_twf', dataset_id=None):
  """ set a twf feature's value and confidence to the input arguments """
  update_sql = """
  UPDATE %(twf_table)s SET %(fid)s = %(value)s, %(fid)s_c = %(confidence)s %(dataset_block)s;
  """ % {'fid':fid, 'value':value, 'confidence':confidence,
     'twf_table': twf_table, 'dataset_block': ' where dataset_id = %s' % dataset_id if dataset_id is not None else ''}
  return await conn.execute(update_sql)

async def cdm_t_clean(conn, fid, dataset_id=None):
  delete_sql = """
  DELETE FROM cdm_t WHERE fid = '%(fid)s' %(dataset_block)s;
  """ % {'fid':fid, 'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else ''}
  return await conn.execute(delete_sql)

async def cdm_s_clean(conn, fid, dataset_id=None):
  delete_sql = """
  DELETE FROM cdm_s WHERE fid = '%(fid)s' %(dataset_block)s;
  """ % {'fid':fid, 'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else ''}
  return await conn.execute(delete_sql)