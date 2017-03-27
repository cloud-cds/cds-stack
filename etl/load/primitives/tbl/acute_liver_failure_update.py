import asyncio
async def acute_liver_failure_update(fid, fid_input, conn, log, twf_table='cdm_twf'):
    """
    fid should be acute_liver_failure
    fid_input should be INR, GCS and liver_disease
    Returns true if elevated INR, altered mental status,
    and no history of liver disease
    """
    assert fid == 'acute_liver_failure', 'wrong fid %s' % fid
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0].strip() == 'inr' \
        and fid_input_items[1].strip() == 'gcs' \
        and fid_input_items[2].strip() == 'liver_disease_hist', \
            'wrong fid_input %s' % fid_input
    clean_tbl.cdm_twf_clean(conn, fid, value = 0, twf_table=twf_table)
    update_clause = """
    UPDATE %(twf_table)s SET acute_liver_failure = 1,
        acute_liver_failure_c = inr_c | gcs_c
    where not enc_id in (select enc_id from cdm_s where fid='liver_disease_hist' and value::boolean=TRUE)
    and inr >= 1.5 and gcs <= 13 and inr_c < 24 and gcs_c < 24
    ;
    """ % { 'twf_table': 'cdm_twf'}
    log.info("acute_liver_disease_update:%s" % update_clause)
    await conn.execute(update_clause)