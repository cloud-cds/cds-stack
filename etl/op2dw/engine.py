import asyncio
import asyncpg
import json
import logging
import os
from extractor import Extractor

logging.basicConfig(level=logging.INFO)

host          = os.environ['db_host']
port          = os.environ['db_port']
db            = os.environ['db_name']
user          = os.environ['db_user']
pw            = os.environ['db_password']
remote_server = os.environ['etl_remote_server']

tables_to_load = {
  'datalink'                 : False,
  'cdm_function'             : False,
  'cdm_feature'              : False,
  'datalink_feature_mapping' : False,
  'pat_enc'                  : False,
  'cdm_g'                    : False,
  'cdm_s'                    : False,
  'cdm_m'                    : False,
  'cdm_t'                    : False,
  'criteria_meas'            : False,
  'criteria'                 : False,
  'criteria_events'          : False,
  'criteria_log'             : False,
  'criteria_meas_archive'    : False,
  'criteria_archive'         : False,
  'criteria_default'         : False,
  'notifications'            : False,
  'parameters'               : False,
  'trews_scaler'             : True,
  'trews_feature_weights'    : True,
  'trews_parameters'         : True,
  'cdm_twf'                  : False,
  'trews'                    : False,
  'pat_status'               : False,
  'deterioration_feedback'   : False,
  'feedback_log'             : False,
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
    dataset_id = 1
    model_id = 1
    for tbl, as_model in tables_to_load.items():
      e = Extractor(remote_server, dataset_id, model_id, tbl, as_model_extension=as_model)
      await e.run(self.dbpool)

if __name__ == '__main__':
  engine = Engine()
  engine.run_loop()
