import asyncio
import asyncpg
import json
import logging
import os
from extractor import Extractor

logging.basicConfig(level=logging.INFO)

host          = os.environ['db_host']
port          = os.environ['db_port']
db            = os.environ['db_name'] # set to opsdx_dw
user          = os.environ['db_user']
pw            = os.environ['db_password']
remote_server = os.environ['etl_remote_server'] # set to opsdx_dev_srv

tables_to_load = {
  'cdm_function'             : 'dataset',
  'cdm_feature'              : 'dataset',
  'pat_enc'                  : 'dataset',
  'cdm_g'                    : 'dataset',
  'cdm_s'                    : 'dataset',
  'cdm_m'                    : 'dataset',
  'cdm_t'                    : 'dataset',
  'cdm_notes'                : 'dataset',
  'criteria_meas'            : 'dataset',
  'criteria'                 : 'dataset',
  'criteria_events'          : 'dataset',
  'criteria_log'             : 'dataset',
  'criteria_meas_archive'    : 'dataset',
  'criteria_archive'         : 'dataset',
  'criteria_default'         : 'dataset',
  'notifications'            : 'dataset',
  'parameters'               : 'dataset',
  'trews_scaler'             : 'model',
  'trews_feature_weights'    : 'model',
  'trews_parameters'         : 'model',
  'cdm_twf'                  : 'dataset',
  'trews'                    : 'dataset',
  'pat_status'               : 'dataset',
  'deterioration_feedback'   : 'dataset',
  'feedback_log'             : 'dataset',
}

# engine for clarity ETL
class Engine(object):
  '''
  ETL workflow for ingesting TREWS operational DB to the data warehouse.
  '''

  async def _init_(self):
    self.dbpool = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port)

  def run_loop(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run())

  async def run(self):
    await self._init_()
    # extractors to run ETL
    logging.info("Running op2dw ETL")
    if 'OP2DW_DATASET_ID' in os.environ and 'OP2DW_MODEL_ID' in os.environ:
      dataset_id = os.environ['OP2DW_DATASET_ID']
      model_id = os.environ['OP2DW_MODEL_ID']
    else:
      print("OP2DW_MODEL_ID or OP2DW_DATASET_ID is missing")
      exit(0)

    for tbl, version_type in tables_to_load.items():
      e = Extractor(remote_server, dataset_id, model_id, tbl, version_extension=version_type)
      await e.run(self.dbpool)

if __name__ == '__main__':
  engine = Engine()
  engine.run_loop()
