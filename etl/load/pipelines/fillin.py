from etl.load.primitives.tbl import fillin

async def fillin_pipeline(log, conn, feature, recalculate_popmean=True, table='cdm_twf'):
  fid = feature['fid']
  if feature['category'] == 'TWF' and feature['is_measured']:
    select_sql = """
    SELECT count(%(fid)s) from %(table)s
    WHERE %(fid)s_c < 8
    """ % {'fid':fid, 'table':table}
    cnt_meas = await conn.fetch(select_sql)
    cnt_meas = cnt_meas[0][0]
    log.debug('number of measurements:' + str(cnt_meas))
    if cnt_meas > 0:
      fillin_func_id = feature['fillin_func_id']
      fillin_func_args = [table,
                feature['window_size_in_hours'],
                recalculate_popmean]
      log.info('start fillin fid %s: %s (%s)' \
        % (fid, fillin_func_id, fillin_func_args))
      fillin_sql = fillin.fillin(fid, fillin_func_id, fillin_func_args)
      log.debug("fillin_sql: " + fillin_sql)
      await conn.execute(fillin_sql)
      log.info('end fillin fid %s' % fid)
    else:
      log.warn('no data to fillin fid %s' % fid)
      if not recalculate_popmean:
        log.warn('fill in with popmean')
        update_sql = """
        update %(table)s set %(fid)s = popmean, %(fid)s_c = 24
        from (
          SELECT value::numeric as popmean from cdm_g where fid = '%(fid)s_popmean'
          ) t
        """ % {'table':table, 'fid':fid}
        await conn.execute(update_sql)
  else:
    log.error('This feature is not a TWF feature!')