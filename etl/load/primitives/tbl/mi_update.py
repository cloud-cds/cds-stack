import asyncio
async def mi_update(fid, fid_input, conn, log, twf_table='cdm_twf', dataset_id=None):
    """
    fid should be mi
    fid_input should be troponin
    Returns 1 if elevated troponin, indicating MI, 0 otherwise
    Don't use imputed population mean for troponin to set MI
    TO DO: Add chest pain variable
    """
    assert fid == 'mi', 'wrong fid %s' % fid
    assert fid_input == 'troponin', \
            'wrong fid_input %s' % fid_input
    clean_tbl.cdm_twf_clean(conn, fid, value = 0,  twf_table=twf_table, dataset_id)
    update_clause = """
    UPDATE %(twf_table)s SET mi = 1,
        mi_c = troponin_c
    where troponin > 0.01 and troponin_c < 10 %(dataset_block)s
    ;
    """ % {'twf_table': twf_table, 'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else ''}
    log.info("mi_update:%s" % update_clause)
    await conn.execute(update_clause)