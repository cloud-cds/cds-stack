from etl.transforms.primitives.df import derive
from etl.transforms.primitives.df import format_data
from etl.transforms.primitives.df import restructure
from etl.transforms.primitives.df import translate
from etl.transforms.primitives.df.pandas_utils import async_read_df
from etl.mappings import lab_procedures_clarity
from etl.load.primitives.row import load_row
import etl.transforms.primitives.df.filter_rows as filter_rows
from etl.clarity2dw.extractor import log_time
import time
import os
import re
import pandas as pd
import json
import asyncio
from etl.mappings.med_regex import med_regex
import json


def get_min_tsp(tsp_name, tsp_with_quotes=True):
  if 'min_tsp' in os.environ:
    min_tsp = os.environ['min_tsp']
    if tsp_with_quotes:
      return ''' and "{tsp}"::timestamptz > '{min_tsp}'::timestamptz'''.format(tsp=tsp_name, min_tsp=min_tsp)
    else:
      return ''' and {tsp}::timestamptz > '{min_tsp}'::timestamptz'''.format(tsp=tsp_name, min_tsp=min_tsp)
  else:
    return ''
