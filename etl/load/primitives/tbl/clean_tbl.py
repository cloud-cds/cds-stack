from etl.load.primitives.tbl.derive_helper import *

def cdm_twf_clean(fid, value='null', confidence='null', twf_table='cdm_twf',
                  dataset_id=None, incremental=False):
  """ set a twf feature's value and confidence to the input arguments """
  update_sql = """
  UPDATE %(twf_table)s SET %(fid)s = %(value)s, %(fid)s_c = %(confidence)s
   %(dataset_block)s %(incremental_enc_id_in)s;
  """ % {'fid':fid, 'value':value, 'confidence':confidence,
         'twf_table': twf_table, 'dataset_block': \
            ' where dataset_id = %s' % dataset_id \
              if dataset_id is not None else '',
         'incremental_enc_id_in': \
            incremental_enc_id_in(' and ', twf_table, dataset_id,incremental)}
  return update_sql

def cdm_t_clean(fid, dataset_id=None, incremental=False):
  delete_sql = """
  DELETE FROM cdm_t WHERE fid = '%(fid)s' %(dataset_block)s
   %(incremental_enc_id_in)s;
  """ % {'fid':fid, 'dataset_block': ' and dataset_id = %s' % dataset_id \
            if dataset_id is not None else '',
         'incremental_enc_id_in': \
            incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental)}
  return delete_sql

def cdm_s_clean(fid, dataset_id=None, incremental=False):
  delete_sql = """
  DELETE FROM cdm_s WHERE fid = '%(fid)s' %(dataset_block)s
   %(incremental_enc_id_in)s;
  """ % {'fid':fid, 'dataset_block': ' and dataset_id = %s' % dataset_id \
            if dataset_id is not None else '',
         'incremental_enc_id_in': \
            incremental_enc_id_in(' and ', 'cdm_s', dataset_id,incremental)}
  return delete_sql