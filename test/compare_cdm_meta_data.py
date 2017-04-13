import sys
import os
from compare_cdm import TableComparator
import asyncio
from collections import OrderedDict
import asyncpg

cdm_g_fields = [
  ['fid', 'fid', 'text'],
  ['(round(value::numeric, 4)) as value', 'value', 'numeric'],
  ['confidence', 'confidence', 'int']
]

cdm_g_query = (cdm_g_fields, None, 'fid', None)

trews_parameters_fields = [
  ['name', 'name', 'text'],
  ['(round(value::numeric, 4)) as value', 'value', 'numeric'],
]

trews_parameters_query = (trews_parameters_fields, None, 'name', None)


trews_scaler_fields = [
  ['fid', 'fid', 'text'],
  ['(round(mean::numeric, 4)) as mean', 'mean', 'numeric'],
  ['(round(var::numeric, 4)) as var', 'var', 'numeric'],
  ['(round(scale::numeric, 4)) as scale', 'scale', 'numeric'],
]

trews_scaler_query = (trews_scaler_fields, None, 'fid', None)


tables_to_compare = {  # touple 1, extra field, dataset_id, model_id, both, touple 2 customize comparison s
      # 'datalink'                 : ('dataset', []),
      'cdm_function'             : ('dataset',   []),
      'cdm_feature'              : ('dataset',   []),
      # 'datalink_feature_mapping' : ('dataset', []),
      # 'pat_enc'                  : ('dataset',   [pat_enc_query]),
      'cdm_g'                    : ('both'   ,   [cdm_g_query]),
      # 'cdm_s'                    : ('dataset', [cdm_s_query1]),
      # 'cdm_m'                    : ('dataset', []),
      # 'cdm_t'                    : ('dataset', [cdm_t_query1, cdm_t_query2, cdm_t_query3, cdm_t_query4]),
      # 'criteria_meas'            : ('dataset',   [criteria_meas_query]),
      # 'criteria'                 : ('dataset', []),
      # 'criteria_events'          : ('dataset', []),
      # 'criteria_log'             : ('dataset', []),
      # 'criteria_meas_archive'    : ('dataset', []),
      # 'criteria_archive'         : ('dataset', []),
      # 'criteria_default'         : ('dataset', []),
      # 'notifications'            : ('dataset', []),
      'parameters'               : ('dataset', []),
      'trews_scaler'             : ('model'  , [trews_scaler_query]),
      'trews_feature_weights'    : ('model'  , []),
      'trews_parameters'         : ('model'  , [trews_parameters_query]),
      # 'cdm_twf'                  : ('dataset', cdm_twf_queries),
      # 'trews'                    : ('dataset', []),
      # 'pat_status'               : ('dataset', []),
      # 'deterioration_feedback'   : ('dataset', []),
      # 'feedback_log'             : ('dataset', []),
    }

def generate_sql(table, diff_rows):
  # TODO
  pass

async def run_cdm_meta_compare(db_host, db_name_1, db_name_2):
  src_dataset_id = None
  src_model_id   = None
  dst_dataset_id = None
  dst_model_id   = None
  src_server = db_name_1
  counts = False
  dst_tsp_shift = None
  results = []
  user = os.environ['db_user']
  pw = os.environ['db_password']
  port = os.environ['db_port']
  dbpool = await asyncpg.create_pool(database=db_name_2, user=user, password=pw, host=db_host, port=port)
  for tbl, version_type_and_queries in tables_to_compare.items():
    version_type = version_type_and_queries[0]
    queries = version_type_and_queries[1]
    print(tbl)
    print(version_type_and_queries)
    if queries:
      for field_map, predicate, sort_field, dependent_fields in queries:
        c = TableComparator(src_server,
                            src_dataset_id, src_model_id,
                            dst_dataset_id, dst_model_id,
                            tbl, src_pred=predicate,
                            field_map=field_map, dependent_fields=dependent_fields,
                            version_extension=version_type,
                            as_count_result=counts, sort_field=sort_field, dst_tsp_shift=dst_tsp_shift)
        records = await c.run(dbpool)
        results.append(records)
    else: #which is the remote defined here, can all be none
      c = TableComparator(src_server,
                          src_dataset_id, src_model_id,
                          dst_dataset_id, dst_model_id,
                          tbl, version_extension=version_type, as_count_result=counts, dst_tsp_shift=dst_tsp_shift)
      records = await c.run(dbpool)
      results.append(records)
    print("======== result ========")
    groups = {}
    for result in results:
      if 'rows' in result and len(result['rows']) > 0:
        for row in result['rows']:
          groups.setdefault(result['src_tbl'], []).append(dict(row))

    for tbl in groups:
      print('------------- {} ---------------'.format(tbl))
      for row in groups[tbl]:
        print(row)
      print("")

    print("========= SQL ========")
    for tbl in groups:
      # TODO: use key to decide what operation to do: upsert/delete/insert
      generate_sql(tbl, groups[tbl])

if __name__ == '__main__':
  db_host = sys.argv[1]
  db_name_1 = sys.argv[2]
  db_name_2 = sys.argv[3]
  print("compare cdm meta data between {db1} and {db2} in {host}".format(db1=db_name_1, db2=db_name_2, host=db_host))
  create_dblink = '''
  sed -e "s/@@RDBHOST@@/{db_host}/" -e "s/@@RDBPORT@@/$db_port/" -e "s/@@RDBNAME@@/{db1}/" -e "s/@@RDBUSER@@/$db_user/" -e "s/@@RDBPW@@/$db_password/" -e "s/@@LDBUSER@@/$db_user/" -e "s/@@db_to_test@@/{db1}/" fdw.sql > fdw.{db2}.sql
  psql -h {db_host} -U $db_user -d {db2} -p $db_port -f fdw.{db2}.sql
  rm fdw.{db2}.sql
  '''.format(db_host=db_host,db1=db_name_1,db2=db_name_2)
  os.system(create_dblink)
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run_cdm_meta_compare(db_host, db_name_1, db_name_2))