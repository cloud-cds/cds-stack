import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
import etl.load.primitives.row.load_row as load_row
import etl.mappings.confidence as confidence


async def admit_weight_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target):
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
          where based_on_popmean(weight_c) = 0 %(incremental_enc_id_pending)s
        order by tsp) as ordered
      group by ordered.enc_id
    ) on conflict (enc_id, fid) do update set
      value = Excluded.value,
      confidence = Excluded.confidence
    ;
    ''' % ("(pat_enc.meta_data->>'pending')::boolean" if incremental else '')
  else:
    sql = '''
    insert into cdm_s (dataset_id, enc_id, fid, value, confidence)
    (
      select pat_enc.dataset_id, pat_enc.enc_id, 'admit_weight', coalesce(first(ordered.weight::text),(select value::text from cdm_g where fid = 'weight_popmean' and dataset_id = %(dataset_id)s limit 1)),
      coalesce(first(ordered.weight_c), 24)
      from
        pat_enc
      left join
        (select dataset_id, enc_id, weight, weight_c from cdm_twf
          where based_on_popmean(weight_c) = 0
          and dataset_id = %(dataset_id)s
        order by enc_id, tsp) as ordered
        on pat_enc.dataset_id = ordered.dataset_id and pat_enc.enc_id = ordered.enc_id
      where pat_enc.dataset_id = %(dataset_id)s %(incremental_enc_id_pending)s
      group by pat_enc.dataset_id, pat_enc.enc_id
    ) on conflict (dataset_id, enc_id, fid) do update set
      value = Excluded.value,
      confidence = Excluded.confidence
    ;
    ''' % {'dataset_id': dataset_id,
           'incremental_enc_id_pending': \
              "and (pat_enc.meta_data->>'pending')::boolean" if incremental else ''}
  log.debug(sql)
  await conn.execute(sql)
