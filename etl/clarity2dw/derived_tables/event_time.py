import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from etl.clarity2dw.conf.derived_tables import standard_event_list

#dialect+driver://username:password@host:port/database
# engine = create_engine('postgresql:///hcgh_1608')


cdm_t_queryTemplate = """
    insert into event_time (dataset_id, enc_id, tsp, event)
    select first(dataset_id), enc_id, {agg_func}(tsp) as tsp , '{event_name}'
    from cdm_t 
    where fid = \'{fid}\' and dataset_id = {ds_id}
    group by enc_id"""

cdm_twf_queryTemplate = """
    insert into event_time (dataset_id, enc_id, tsp, event)
    select first(dataset_id), enc_id, {agg_func}(tsp) as tsp, '{event_name}'
    from cdm_twf 
    where {fid}::int = 1 and dataset_id = {ds_id}
    group by enc_id"""

func2suffix = {'min': '_first', 'max': '_last'}

def populate(connection, dataset_id):
  #----------------------------------
  # Handle Special Cases
  #----------------------------------
  connection.execute(text("""delete from event_time where dataset_id = {ds}""".format(ds=dataset_id)))

  connection.execute(text("""
    insert into event_time(dataset_id, enc_id, tsp, event)
    SELECT first(dataset_id), enc_id, min(tsp) as tsp, 'entrance'
    FROM cdm_t
    WHERE fid like 'care_unit' and dataset_id = {ds_id}
    GROUP BY ENC_ID""".format(ds_id=dataset_id)))

  connection.execute(text("""
      insert into event_time (dataset_id, enc_id, tsp, event)
      with
      dischargeTbl as (
      select dataset_id, enc_id, tsp, cast(value AS json)->> 'disposition' as disp from cdm_t where fid = 'discharge'
      )
      select dataset_id, enc_id, tsp, 'death'
      from dischargeTbl
      where disp like '%Expired%' and dataset_id = {ds_id} """.format(ds_id=dataset_id)))

  connection.execute(text("""
      insert into event_time (dataset_id, enc_id, tsp, event)
      select first(dataset_id), enc_id, max(leave_time) as tsp, 'last_leave_emergency' from (
          SELECT dataset_id, enc_id, tsp, fid, value,
          lead(tsp,1) OVER (PARTITION BY enc_id ORDER BY tsp) as leave_time
          FROM cdm_t
          WHERE fid like 'care_unit'
          ORDER BY enc_id, tsp) as winT
      WHERE value like 'HCGH EMERGENCY%' and leave_time is not null and dataset_id = {ds_id}
      GROUP BY enc_id""".format(ds_id=dataset_id)))

  connection.execute(text("""
      insert into event_time (dataset_id, enc_id, tsp, event)
      with cultStatTbl as (select dataset_id, enc_id, tsp, cast(value AS json)->>'status' as status from cdm_t where fid like 'culture_order')
      select first(dataset_id), enc_id, min(tsp) as tsp, 'first_culture_sent' 
      from cultStatTbl
      where status like 'Sent' and dataset_id = {ds_id}
      group by enc_id""".format(ds_id=dataset_id)))

  #----------------------------------
  # Handle Typical Cases
  #----------------------------------


  agg_func = ['min' for event in standard_event_list]


  force_list     = [False for event in standard_event_list]

  #---------------------
  # Get feature metadata
  #---------------------


  cdm_feature_prefix = 'SELECT fid, category, data_type from cdm_feature where dataset_id = {ds_id} and  \n'

  eventListProc = ['FID like \'{}\''.format(event) for event in standard_event_list]

  event_sql = ' or \n'.join(eventListProc)

  cdm_feature_sql = cdm_feature_prefix + '(' + event_sql + ')' + ';'


  print(cdm_feature_sql.format(ds_id=dataset_id))

  cdm_feature_df = pd.read_sql_query(text(cdm_feature_sql.format(ds_id=dataset_id)),con=connection)

  cdm_feature_df.set_index('fid',inplace=True)


  #---------------------
  # Execute Loop
  #---------------------

  for agg, fid, force in zip(agg_func, standard_event_list, force_list):
      ft = cdm_feature_df.loc[fid]['data_type'].lower()

      if ft=='boolean' or ft=='integer' or force:
          suff = func2suffix[agg]
          event_name = fid + suff

          if cdm_feature_df.loc[fid]['category'].lower() == 't':
            connection.execute(text(cdm_t_queryTemplate.format(agg_func=agg, event_name=event_name,fid=fid, ds_id=dataset_id)))

          elif cdm_feature_df.loc[fid]['category'].lower() == 'twf':
            connection.execute(text(cdm_twf_queryTemplate.format(agg_func=agg, event_name=event_name, fid=fid, ds_id=dataset_id)))

          else:
              raise ValueError('Unknown category {}'.format(cdm_feature_df[fid]['category']))
      else:
          print("Unclear if automatic query constructor can handle feature type {}, skipping {}".format(ft, fid))



  return

# if __name__ =='__main__':
#
#   populate_events_table(engine.connect(),4)