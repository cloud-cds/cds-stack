async def cdm_twf_clean(conn, fid, value='null', confidence='null', twf_table='cdm_twf'):
  """ set a twf feature's value and confidence to the input arguments """
  update_sql = """
  UPDATE %(twf_table)s SET %(fid)s = %(value)s, %(fid)s_c = %(confidence)s;
  """ % {'fid':fid, 'value':value, 'confidence':confidence,
     'twf_table': twf_table}
  return await conn.execute(update_sql)

async def cdm_t_clean(conn, fid):
  delete_sql = """
  DELETE FROM cdm_t WHERE fid = %(fid)s;
  """ % {'fid':fid}
  return await conn.execute(delete_sql)

async def cdm_s_clean(conn, fid):
  delete_sql = """
  DELETE FROM cdm_s WHERE fid = %(fid)s;
  """ % {'fid':fid}
  return await conn.execute(delete_sql)