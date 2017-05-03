from etl.clarity2dw.derived_tables import care_unit, event_time, sub_populations
from sqlalchemy import create_engine

def main(connection, dataset_id):
  care_unit.populate(connection,dataset_id)
  event_time.populate(connection,dataset_id)
  sub_populations.populate(connection,dataset_id)
