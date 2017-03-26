def mi_update(fid, fid_input, cdm, twf_table='cdm_twf'):
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
    cdm.clean_twf(fid, value = 0,  twf_table=twf_table)
    update_clause = """
    UPDATE %(twf_table)s SET mi = 1,
        mi_c = troponin_c 
    where troponin > 0.01 and troponin_c < 10
    ;
    """ % {'twf_table': twf_table}
    cdm.log.info("mi_update:%s" % update_clause)
    cdm.update_twf_sql(update_clause)