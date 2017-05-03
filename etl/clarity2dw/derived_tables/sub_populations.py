import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from etl.clarity2dw.conf.derived_tables import sub_populations_t, sub_populations_s, sub_populations_twf, sepsis_subtypes

select_subtype_encs_s = """
insert into sub_populations (dataset_id, enc_id, population_name)
select  dataset_id, enc_id, ''{subtype}''
from cdm_s
where fid  = '{subtype}' and dataset_id = {ds};
"""

select_subtype_encs_t = """
insert into sub_populations (dataset_id, enc_id, population_name)
select first(dataset_id), enc_id, ''{subtype}''
from cdm_t
where fid  = ''{subtype}'' and dataset_id = {ds}
group by enc_id;
"""


select_subtype_encs_twf = """
insert into sub_populations (dataset_id, enc_id, population_name)
select first(dataset_id), enc_id, ''{subtype}''
from cdm_twf
where cdm_twf.{subtype}::int = 1 and dataset_id = {ds};
group by enc_id
"""

select_sepsis_subtype_encs = """
insert into sub_populations (dataset_id, enc_id, population_name)
select first(dataset_id), enc_id, ''{subtype}''
from cdm_t
where fid  = '{subtype}' and cdm_t.value::json->>'present on admission' = 'Yes' and dataset_id = {ds};
"""

def populate(connection, dataset_id):
  for subtype in sub_populations_s:
    connection.execute(text(sub_populations_s.format(subtype=subtype,ds=dataset_id)))

  for subtype in sub_populations_t:
    connection.execute(text(sub_populations_t.format(subtype=subtype,ds=dataset_id)))

  for subtype in sub_populations_twf:
    connection.execute(text(select_subtype_encs_twf.format(subtype=subtype,ds=dataset_id)))
    
  for subtype in sepsis_subtypes:
    connection.execute(text(select_sepsis_subtype_encs.format(subtype=subtype,ds=dataset_id)))