# sql query to load a row into CDM
import datetime

async def add_t(conn, row):
  sql = '''
  insert into cdm_t (enc_id, tsp, fid, value, confidence)
  values (%s, '%s', '%s', '%s', %s)
  on conflict (enc_id, tsp, fid) do update
  set value = cdm_t.value::numeric + Excluded.value::numeric, confidence = cdm_t.confidence | Excluded.confidence
  ''' % (row[0], row[1], row[2], row[3], row[4])
  await conn.execute(sql)


async def upsert_t(conn, row):
  sql = '''
  insert into cdm_t (enc_id, tsp, fid, value, confidence)
  values (%s, '%s', '%s', '%s', %s)
  on conflict (enc_id, tsp, fid) do update
  set value = Excluded.value, confidence = Excluded.confidence
  ''' % (row[0], row[1], row[2], row[3].replace("'","''"), row[4])
  await conn.execute(sql)

async def upsert_s(conn, row):
  sql = '''
  insert into cdm_s (enc_id, fid, value, confidence)
  values (%s, '%s', '%s', %s)
  on conflict (enc_id, fid) do update
  set value = Excluded.value, confidence = Excluded.confidence
  ''' % (row[0], row[1], row[2].replace("'","''"), row[3])
  await conn.execute(sql)

async def add_s(conn, row):
  sql = '''
  insert into cdm_s (enc_id, fid, value, confidence)
  values (%s, '%s', '%s', %s)
  on conflict (enc_id, fid) do update
  set value = cdm_s.value::numeric + Excluded.value::numeric, confidence = cdm_s.confidence + Excluded.confidence
  ''' % (row[0], row[1], row[2], row[3])
  await conn.execute(sql)

async def upsert_twf(conn, row):
  sql = '''
  insert into cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
  values (%(enc_id)s, '%(tsp)s', %(value)s, %(conf)s)
  on conflict (enc_id, tsp) do update
  set %(fid)s = Excluded.%(fid)s, %(fid)s_c = Excluded.%(fid)s_c
  ''' % {
    'enc_id': row[0], 'tsp': row[1], 'fid': row[2], 'value': row[3], 'conf': row[4]
  }
  await conn.execute(sql)

async def add_twf(conn, row):
  sql = '''
  insert into cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
  values (%(enc_id)s, '%(tsp)s', %(value)s, %(conf)s)
  on conflict (enc_id, tsp) do update
  set %(fid)s = cdm_twf.%(fid)s + Excluded.%(fid)s, cdm_twf.%(fid)s_c = %(fid)s_c | Excluded.%(fid)s_c
  ''' % {
    'enc_id': row[0], 'tsp': row[1], 'fid': row[2], 'value': row[3], 'conf': row[4]
  }
  await conn.execute(sql)
