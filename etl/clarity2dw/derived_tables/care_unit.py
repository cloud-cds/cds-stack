import pandas as pd
from sqlalchemy import create_engine, text
from etl.clarity2dw.conf.derived_tables import standard_event_list

def populate(connection, dataset_id):

  connection.execute(text("""delete from care_unit where dataset_id = {ds}""".format(ds=dataset_id)))

  care_unit = """
  insert into care_unit (dataset_id, enc_id, enter_time, leave_time, care_unit)
  with raw_care_unit_tbl as (
    SELECT dataset_id, enc_id, tsp as enter_time,
    lead(tsp,1) OVER (PARTITION BY enc_id ORDER BY tsp) as leave_time,
    value as care_unit
    FROM cdm_t
    WHERE fid like 'care_unit' and dataset_id = {ds}
  ),
  discharge_fitered as (
    select raw_care_unit_tbl.*
    from
    raw_care_unit_tbl
    where care_unit != 'Discharge'
  )
  select dataset_id, enc_id, enter_time, leave_time, care_unit
  from discharge_fitered;
  """.format(ds=dataset_id)

  connection.execute(text(care_unit))
  pass

# if __name__ =='__main__':
#
#   DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)
#
#   engine = create_engine(DB_CONN_STR)
#
#   populate(engine.connect(),4)