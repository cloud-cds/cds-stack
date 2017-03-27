import asyncio
async def acute_pancreatitis_update(fid, fid_input, conn, log, twf_table='cdm_twf'):
    """
    fid should be acute_pancreatitis
    fid_input should be lipase and amylase
    """
    assert fid == 'acute_pancreatitis', 'wrong fid %s' % fid
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0].strip() == 'lipase' \
        and fid_input_items[1].strip() == 'amylase', \
            'wrong fid_input %s' % fid_input
    clean_tbl.cdm_twf_clean(conn, fid, value = 0,  twf_table='cdm_twf')
    update_clause = """
    UPDATE %(twf_table)s SET acute_pancreatitis = 1,
        acute_pancreatitis_c = lipase_c | amylase_c
    where lipase > 400 and amylase > 450
    ;
    """ % { 'twf_table':'cdm_twf'}
    log.info("acute_pancreatitis_update:%s" % update_clause)
    await conn.execute(update_clause)