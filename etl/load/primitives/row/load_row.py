# sql query to load a row into CDM
import datetime

async def upsert_g(conn, row, dataset_id = None, model_id=0):
  if dataset_id is None:
    sql = '''
    insert into cdm_g (fid, value, confidence)
    values (%s, %s, %s)
    on conflict (fid) do update
    set value = Excluded.value::numeric, confidence = Excluded.confidence
    ''' % (dataset_id, model_id, row[0], row[1], row[2])
  else:
    sql = '''
    insert into cdm_g (dataset_id, model_id, fid, value, confidence)
    values (%s, %s, %s, %s, %s)
    on conflict (fid) do update
    set value = Excluded.value::numeric, confidence = Excluded.confidence
    ''' % (dataset_id, model_id, row[0], row[1], row[2])
  return conn.execute(sql)

async def add_t(conn, row, dataset_id=None):
  if dataset_id is None:
    sql = '''
    select add_cdm_t(%s, '%s', '%s', '%s', %s)
    ''' % (row[0], row[1], row[2], row[3], row[4])
  else:
    sql = '''
    select add_cdm_t(%s, %s, '%s', '%s', '%s', %s)
    ''' % (dataset_id, row[0], row[1], row[2], row[3], row[4])
  return conn.execute(sql)


async def upsert_t(conn, row, dataset_id=None):
  if dataset_id is None:
    sql = '''
    insert into cdm_t (enc_id, tsp, fid, value, confidence)
    values (%s, '%s', '%s', '%s', %s)
    on conflict (enc_id, tsp, fid) do update
    set value = Excluded.value, confidence = Excluded.confidence
    ''' % (row[0], row[1], row[2], str(row[3]).replace("'","''"), row[4])
  else:
    sql = '''
    insert into cdm_t (dataset_id, enc_id, tsp, fid, value, confidence)
    values (%s, %s, '%s', '%s', '%s', %s)
    on conflict (dataset_id, enc_id, tsp, fid) do update
    set value = Excluded.value, confidence = Excluded.confidence
    ''' % (dataset_id, row[0], row[1], row[2], str(row[3]).replace("'","''"), row[4])
  return conn.execute(sql)

async def upsert_s(conn, row, dataset_id=None):
  if dataset_id is None:
    sql = '''
    insert into cdm_s (enc_id, fid, value, confidence)
    values (%s, '%s', '%s', %s)
    on conflict (enc_id, fid) do update
    set value = Excluded.value, confidence = Excluded.confidence
    ''' % (row[0], row[1], str(row[2]).replace("'","''"), row[3])
  else:
    sql = '''
    insert into cdm_s (dataset_id, enc_id, fid, value, confidence)
    values (%s, %s, '%s', '%s', %s)
    on conflict (dataset_id, enc_id, fid) do update
    set value = Excluded.value, confidence = Excluded.confidence
    ''' % (dataset_id, row[0], row[1], str(row[2]).replace("'","''"), row[3])
  return conn.execute(sql)

async def add_s(conn, row, dataset_id=None):
  if dataset_id is None:
    sql = '''
    insert into cdm_s (enc_id, fid, value, confidence)
    values (%s, '%s', '%s', %s)
    on conflict (enc_id, fid) do update
    set value = cdm_s.value::numeric + Excluded.value::numeric, confidence = cdm_s.confidence + Excluded.confidence
    ''' % (row[0], row[1], row[2], row[3])
  else:
    sql = '''
    insert into cdm_s (dataset_id, enc_id, fid, value, confidence)
    values (%s, %s, '%s', '%s', %s)
    on conflict (dataset_id, enc_id, fid) do update
    set value = cdm_s.value::numeric + Excluded.value::numeric, confidence = cdm_s.confidence + Excluded.confidence
    ''' % (dataset_id, row[0], row[1], row[2], row[3])
  return conn.execute(sql)

async def upsert_twf(conn, row, dataset_id=None):
  if dataset_id is None:
    sql = '''
    insert into cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
    values (%(enc_id)s, '%(tsp)s', %(value)s, %(conf)s)
    on conflict (enc_id, tsp) do update
    set %(fid)s = Excluded.%(fid)s, %(fid)s_c = Excluded.%(fid)s_c
    ''' % {
      'enc_id': row[0], 'tsp': row[1], 'fid': row[2], 'value': row[3], 'conf': row[4]
    }
  else:
    sql = '''
    insert into cdm_twf (dataset_id, enc_id, tsp, %(fid)s, %(fid)s_c)
    values (%(dataset_id)s, %(enc_id)s, '%(tsp)s', %(value)s, %(conf)s)
    on conflict (dataset_id, enc_id, tsp) do update
    set %(fid)s = Excluded.%(fid)s, %(fid)s_c = Excluded.%(fid)s_c
    ''' % {
      'dataset_id': dataset_id, 'enc_id': row[0], 'tsp': row[1], 'fid': row[2], 'value': row[3], 'conf': row[4]
    }
  return conn.execute(sql)

async def add_twf(conn, row, dataset_id=None):
  if dataset_id is None:
    sql = '''
    insert into cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
    values (%(enc_id)s, '%(tsp)s', %(value)s, %(conf)s)
    on conflict (enc_id, tsp) do update
    set %(fid)s = cdm_twf.%(fid)s + Excluded.%(fid)s, cdm_twf.%(fid)s_c = %(fid)s_c | Excluded.%(fid)s_c
    ''' % {
      'enc_id': row[0], 'tsp': row[1], 'fid': row[2], 'value': row[3], 'conf': row[4]
    }
  else:
    sql = '''
    insert into cdm_twf (dataset_id, enc_id, tsp, %(fid)s, %(fid)s_c)
    values (%(dataset_id)s, %(enc_id)s, '%(tsp)s', %(value)s, %(conf)s)
    on conflict (dataset_id, enc_id, tsp) do update
    set %(fid)s = cdm_twf.%(fid)s + Excluded.%(fid)s, cdm_twf.%(fid)s_c = %(fid)s_c | Excluded.%(fid)s_c
    ''' % {
      'dataset_id': dataset_id, 'enc_id': row[0], 'tsp': row[1], 'fid': row[2], 'value': row[3], 'conf': row[4]
    }
  return conn.execute(sql)
