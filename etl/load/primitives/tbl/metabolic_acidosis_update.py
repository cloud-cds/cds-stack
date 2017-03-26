def metabolic_acidosis_update(fid, fid_input, cdm,  twf_table='cdm_twf'):
    """
    fid should be metabolic_acidosis
    fid_input should be arterial_ph and bicarbonate
    """
    assert fid == 'metabolic_acidosis', 'wrong fid %s' % fid 
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0].strip() == 'arterial_ph' \
        and fid_input_items[1].strip() == 'bicarbonate', \
            'wrong fid_input %s' % fid_input
    cdm.clean_twf(fid, value = 0, twf_table=twf_table)
    update_clause = """
    UPDATE %(twf_table)s SET metabolic_acidosis = 1,
        metabolic_acidosis_c = arterial_ph_c | bicarbonate_c 
    where arterial_ph < 7.35 and bicarbonate < 22
    ;
    """ % {'twf_table': twf_table}
    cdm.log.info("metabolic_acidosis_update:%s" % update_clause)
    cdm.update_twf_sql(update_clause)

