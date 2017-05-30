import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *

async def change_since_last_measured(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
    """
    fid_input should be name of the feature for which change is to be computed
    fid should be <fid of old feather>_change
    """
    # Make sure the fid is correct (fid_input can be anything)
    assert fid == '%s_change' % fid_input, 'wrong fid %s' % fid_input
    destination_tbl = derive_feature_addr[fid]['twf_table_temp']
    source_tbl = derive_feature_addr[fid_input]['twf_table_temp'] if fid_input in derive_feature_addr else get_src_twf_table(derive_feature_addr)
    await conn.execute(clean_tbl.cdm_twf_clean(fid, twf_table=destination_tbl, dataset_id=dataset_id))



    sql = """
    with
    measured_twf as (
      select
      {meas_select}
      enc_id,
      {fid_input} as meas_value,
      tsp as start_time,
      coalesce(lead(tsp,1) OVER (PARTITION BY {meas_partition_by} enc_id ORDER BY tsp), 'Infinity'::timestamptz) as end_time
      from {input_tbl}
      where {fid_input}_c < 8
    ),
    complete_calc as (
      select
        {complete_calc_select}
        f.enc_id,
        f.tsp,
        coalesce(first(f.hemoglobin) - first(m.meas_value),0) as diff,
        greatest(first(f.hemoglobin_c), 4) as diff_c
      from
        {input_tbl}  f
        left join
        measured_twf m
      on {complete_calc_join} f.enc_id = m.enc_id and f.tsp > m.start_time and f.tsp <= m.end_time
      where True {dataset_block}
      group by {complete_calc_group_by} f.enc_id, f.tsp
    )
    update {output_tbl} o
    set {fid} = diff, {fid}_c = diff_c
    from complete_calc cal
    where {output_where} cal.enc_id = o.enc_id and cal.tsp = o.tsp {incremental_enc_id_in}
    """.format(fid= fid,
               fid_input=fid_input,
               output_tbl=destination_tbl,
               meas_select = 'dataset_id, ' if dataset_id is not None else '',
               meas_partition_by = 'dataset_id,' if dataset_id is not None else '',
               complete_calc_select = 'f.dataset_id,' if dataset_id is not None else '',
               complete_calc_join = 'f.dataset_id = m.dataset_id and' if dataset_id is not None else '',
               complete_calc_group_by = 'f.dataset_id, ' if dataset_id is not None else '',
               output_where = 'cal.dataset_id = o.dataset_id and ' if dataset_id is not None else '',
               input_tbl=source_tbl,
               dataset_block = 'and f.dataset_id = %s' % dataset_id if dataset_id is not None else '',
               incremental_enc_id_in = incremental_enc_id_in(' and ', destination_tbl, dataset_id, incremental),
    )

    log.info("change_since_last_measured:%s" % sql)
    await conn.execute(sql)
