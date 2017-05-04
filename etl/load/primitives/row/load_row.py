# sql query to load a row into CDM
import datetime
import traceback
import asyncio
CHUCK_SIZE=5000

async def upsert_g(conn, row, dataset_id = None, many=False):
  sql = '''
    INSERT into cdm_g (fid, value, confidence)
    values {values}
    on conflict ({dataset_id_key} fid) do update
    set value = Excluded.value::numeric, confidence = Excluded.confidence;
    '''
  def format_value(dataset_id_val, row):
    return "({dataset_id_val} {fid}, {value}, {confidence})".format(
        dataset_id_val=dataset_id_val,
        fid=row[0],
        value=row[1],
        confidence=row[2]
      )
  dataset_id_key = 'dataset_id, '.format(dataset_id) if dataset_id else ''
  dataset_id_val = '{}, '.format(dataset_id) if dataset_id else ''
  if many:
    rows = [row[i:i+CHUCK_SIZE] for i in range(0,len(row),CHUCK_SIZE)]
    for i, chuck in enumerate(rows):
      sqls = ''
      # values = ", ".join([
      #     format_value(dataset_id_val, item) for item in row
      #   ])
      for row in chuck:
        values = format_value(dataset_id_val, row)
        sqls += sql.format(values=values, dataset_id_key=dataset_id_key)
      log.info("loading chuck {}".format(i))
      await execute_load(conn, sqls, log)
      log.info("loaded chuck {}".format(i))
  else:
    values = format_value(dataset_id_val, row)
  sql = sql.format(values=values, dataset_id_key=dataset_id_key)
  await execute_load(conn, sql, log)

async def add_t(conn, row, dataset_id=None, log=None, many=False):
  def format_value(dataset_id_val, row):
    return "{dataset_id_val} {enc_id}, '{tsp}', '{fid}', '{value}', {confidence}".format(
        dataset_id_val=dataset_id_val,
        enc_id=row[0],
        tsp=row[1],
        fid=row[2],
        value=row[3],
        confidence=row[4]
      )
  dataset_id_key = 'dataset_id, '.format(dataset_id) if dataset_id else ''
  dataset_id_val = '{}, '.format(dataset_id) if dataset_id else ''
  if many:
    rows = [row[i:i+CHUCK_SIZE] for i in range(0,len(row),CHUCK_SIZE)]
    for i, row in enumerate(rows):
      sql = "\n".join(["SELECT add_cdm_t({});".format(format_value(dataset_id_val, item)) for item in row])
      log.info("loading chuck {}".format(i))
      await execute_load(conn, sql, log)
      log.info("loaded chuck {}".format(i))
  else:
    sql = "SELECT add_cdm_t({});".format(format_value(dataset_id_key, row))
    await execute_load(conn, sql, log)


async def upsert_t(conn, row, dataset_id=None, log=None, many=False):
  def format_value(dataset_id_val, row):
    return "({dataset_id_val} {enc_id}, '{tsp}', '{fid}', '{value}', {confidence})".format(
        dataset_id_val=dataset_id_val,
        enc_id=row[0],
        tsp=row[1],
        fid=row[2],
        value=str(row[3]).replace("'","''"),
        confidence=row[4]
      )

  sql = '''
    INSERT into cdm_t ({dataset_id_key} enc_id, tsp, fid, value, confidence)
    values {values}
    on conflict ({dataset_id_key} enc_id, tsp, fid) do update
    set value = Excluded.value, confidence = Excluded.confidence;
    '''
  dataset_id_key = 'dataset_id, '.format(dataset_id) if dataset_id else ''
  dataset_id_val = '{}, '.format(dataset_id) if dataset_id else ''
  if many:
    rows = [row[i:i+CHUCK_SIZE] for i in range(0,len(row),CHUCK_SIZE)]
    for i, chuck in enumerate(rows):
      sqls = ''
      # values = ", ".join([
      #     format_value(dataset_id_val, item) for item in row
      #   ])
      for row in chuck:
        values = format_value(dataset_id_val, row)
        sqls += sql.format(values=values, dataset_id_key=dataset_id_key)
      log.info("loading chuck {}".format(i))
      await execute_load(conn, sqls, log)
      log.info("loaded chuck {}".format(i))
  else:
    values = format_value(dataset_id_val, row)
    sql = sql.format(values=values, dataset_id_key=dataset_id_key)
    await execute_load(conn, sql, log)

async def upsert_s(conn, row, dataset_id=None, log=None, many=False):
  def format_value(dataset_id_val, row):
    return "({dataset_id_val} {enc_id}, '{fid}', '{value}', {confidence})".format(
        dataset_id_val=dataset_id_val,
        enc_id=row[0],
        fid=row[1],
        value=str(row[2]).replace("'","''"),
        confidence=row[3]
      )

  sql = '''
    INSERT into cdm_s ({dataset_id_key} enc_id, fid, value, confidence)
    values {values}
    on conflict ({dataset_id_key} enc_id, fid) do update
    set value = Excluded.value, confidence = Excluded.confidence;
    '''
  dataset_id_key = 'dataset_id, '.format(dataset_id) if dataset_id else ''
  dataset_id_val = '{}, '.format(dataset_id) if dataset_id else ''
  if many:
    rows = [row[i:i+CHUCK_SIZE] for i in range(0,len(row),CHUCK_SIZE)]
    for i, chuck in enumerate(rows):
      sqls = ''
      # values = ", ".join([
      #     format_value(dataset_id_val, item) for item in row
      #   ])
      for row in chuck:
        values = format_value(dataset_id_val, row)
        sqls += sql.format(values=values, dataset_id_key=dataset_id_key)
      log.info("loading chuck {}".format(i))
      await execute_load(conn, sqls, log)
      log.info("loaded chuck {}".format(i))
  else:
    values = format_value(dataset_id_val, row)
    sql = sql.format(values=values, dataset_id_key=dataset_id_key)
    await execute_load(conn, sql, log)

async def add_s(conn, row, dataset_id=None, log=None, many=False):
  def format_value(dataset_id_val, row):
    return "({dataset_id_val} {enc_id}, '{fid}', '{value}', {confidence})".format(
        dataset_id_val=dataset_id_val,
        enc_id=row[0],
        fid=row[1],
        value=str(row[2]).replace("'","''"),
        confidence=row[3]
      )

  sql = '''
    INSERT into cdm_s ({dataset_id_key} enc_id, fid, value, confidence)
    values {values}
    on conflict ({dataset_id_key} enc_id, fid) do update
    set value = cdm_s.value::numeric + Excluded.value::numeric, confidence = cdm_s.confidence + Excluded.confidence;
    '''
  dataset_id_key = 'dataset_id, '.format(dataset_id) if dataset_id else ''
  dataset_id_val = '{}, '.format(dataset_id) if dataset_id else ''
  if many:
    rows = [row[i:i+CHUCK_SIZE] for i in range(0,len(row),CHUCK_SIZE)]
    for i, chuck in enumerate(rows):
      sqls = ''
      # values = ", ".join([
      #     format_value(dataset_id_val, item) for item in row
      #   ])
      for row in chuck:
        values = format_value(dataset_id_val, row)
        sqls += sql.format(values=values, dataset_id_key=dataset_id_key)
      log.info("loading chuck {}".format(i))
      await execute_load(conn, sqls, log)
      log.info("loaded chuck {}".format(i))
  else:
    values = format_value(dataset_id_val, row)
    sql = sql.format(values=values, dataset_id_key=dataset_id_key)
    await execute_load(conn, sql, log)

async def upsert_twf(conn, row, dataset_id=None, log=None):
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
  await execute_load(conn, sql, log)

async def add_twf(conn, row, dataset_id=None, log=None):
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
  await execute_load(conn, sql, log)

async def execute_load(conn, sql, log, timeout=2):
  attempts = 0
  while True:
    try:
      attempts += 1
      async with conn.transaction():
        await conn.execute(sql, timeout=timeout)
        break
    except Exception as e:
      if log:
        log.warn("execute_load failed: retry %s times in %s secs" % (attempts, timeout**attempts))
        traceback.print_exc()
        await asyncio.sleep(timeout**attempts)
        # log.info(sql)
      continue

