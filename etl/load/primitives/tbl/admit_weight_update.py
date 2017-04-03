import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
import etl.load.primitives.row.load_row as load_row
import etl.confidence as confidence


async def admit_weight_update(fid, fid_input, conn, log, dataset_id=None, twf_table='cdm_twf'):
  if dataset_id is None:
    sql = '''
    insert into cdm_s (enc_id, fid, value, confidence)
    (
      select ordered.enc_id, 'weight', coalesce(first(ordered.weight::text),(select value::text from cdm_g where fid = 'weight_popmean' limit 1)),
      coalesce(first(ordered.weight_c), 24)
      from
        (select pat_enc.enc_id, weight, weight_c from pat_enc
          left join cdm_twf
          on pat_enc.enc_id = cdm_twf.enc_id
          where based_on_popmean(weight_c) = 0
        order by tsp) as ordered
      group by ordered.enc_id
    ) on conflict (enc_id, fid) do update set
      value = Excluded.value,
      confidence = Excluded.confidence
    ;
    '''
  else:
    sql = '''
    insert into cdm_s (dataset_id, enc_id, fid, value, confidence)
    (
      select pat_enc.dataset_id, pat_enc.enc_id, 'admit_weight', coalesce(first(ordered.weight::text),(select value::text from cdm_g where fid = 'weight_popmean' limit 1)),
      coalesce(first(ordered.weight_c), 24)
      from
        pat_enc
      left join
        (select dataset_id, enc_id, weight, weight_c from cdm_twf
          where based_on_popmean(weight_c) = 0
          and dataset_id = %(dataset_id)s
        order by enc_id, tsp) as ordered
        on pat_enc.dataset_id = ordered.dataset_id and pat_enc.enc_id = ordered.enc_id
      group by pat_enc.dataset_id, pat_enc.enc_id
    )on conflict (dataset_id, enc_id, fid) do update set
      value = Excluded.value,
      confidence = Excluded.confidence
    ;
    ''' % {'dataset_id': dataset_id}
  log.debug(sql)
  await conn.execute(sql)