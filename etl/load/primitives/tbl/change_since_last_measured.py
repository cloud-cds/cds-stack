import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *

async def change_since_last_measured(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict):
    """
    fid_input should be name of the feature for which change is to be computed
    fid should be <fid of old feather>_change
    """
    # Make sure the fid is correct (fid_input can be anything)
    assert fid == '%s_change' % fid_input, 'wrong fid %s' % fid_input
    twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
    twf_table_fid_input = derive_feature_addr[fid_input]['twf_table_temp'] if fid_input in derive_feature_addr else get_src_twf_table(derive_feature_addr)
    await conn.execute(clean_tbl.cdm_twf_clean(fid,  twf_table=twf_table_temp, dataset_id=dataset_id))

    sql = """
    create temp table change as select enc_id, tsp, %(fid)s, %(fid)s_c,
        lag(enc_id, 1) over (order by enc_id, tsp) enc_id_last,
        lag(tsp, 1) over (order by enc_id, tsp) tsp_last,
        lag(%(fid)s, 1) over (order by enc_id, tsp) %(fid)s_last,
        lag(%(fid)s_c, 1) over (order by enc_id, tsp) %(fid)s_c_last,
        %(fid)s - lag(%(fid)s, 1) over (order by enc_id, tsp) diff,
        lag(enc_id, -1) over (order by enc_id, tsp) enc_id_next,
        lag(tsp, -1) over (order by enc_id, tsp) tsp_next,
        lag(%(fid)s, -1) over (order by enc_id, tsp) %(fid)s_next,
        lag(%(fid)s_c, -1) over (order by enc_id, tsp) %(fid)s_c_next
    from %(twf_table_fid)s
    where %(fid)s_c < 8 %(dataset_block)s
    order by enc_id, tsp;

    delete from change where enc_id <> enc_id_last;


    update %(twf_table_fid_input)s set %(fid_input)s = subquery.diff,
        %(fid_input)s_c = subquery.%(fid)s_c | subquery.%(fid)s_c_last
    from (
        select * from change where change.enc_id = change.enc_id_next and change.enc_id_last is not null
    ) as subquery
    where %(twf_table_fid_input)s.enc_id = subquery.enc_id
        and %(twf_table_fid_input)s.tsp >= subquery.tsp
        and %(twf_table_fid_input)s.tsp <  subquery.tsp_next
        %(dataset_block)s;


    update %(twf_table_fid_input)s set %(fid_input)s = subquery.diff,
        %(fid_input)s_c = subquery.%(fid)s_c | subquery.%(fid)s_c_last
    from (
        select * from change where enc_id <> enc_id_next and enc_id_last is not null
    ) as subquery
    where %(twf_table_fid_input)s.enc_id = subquery.enc_id
        and %(twf_table_fid_input)s.tsp >= subquery.tsp %(dataset_block)s;
    """ % {'fid': fid, 'fid_input': fid_input,
           'twf_table_fid': twf_table_temp,
           'twf_table_fid_input': twf_table_fid_input,
           'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else ''}

    log.info("change_since_last_measured:%s" % sql)
    await conn.execute(sql)

