from etl.transforms.primitives.df import format_data
from etl.core import test_utils
import pytest
import pandas as pd
import numpy as np


def test_format_columns():
  df = pd.DataFrame([
    {
      'admittime': '5:43pm March 6th, 2012',
      'age': 123,
    }, {
      'admittime': '17:43 2012/3/6',
      'age': 12.30,
    }, {
      'admittime': '03-06-2012 5:43:00PM',
      'age': '12',
    }
  ])

  result = format_data.format_tsp(df, 'admittime')
  result = format_data.format_numeric(df, 'age')

  assert set(result.admittime) == set(['2012-03-06 17:43:00 EST'])
  assert list(result.age) == [123.0, 12.3, 12.0]



@pytest.mark.parametrize('value, is_empty', [
  ('',        True),
  (' ',       True),
  ('  ',      True),
  ("",        True),
  (" ",       True),
  (None,      True),
  (np.nan,    True),
  (0,         False),
  ("abc",     False),
  (123,       False),
  (123.02,    False),
])
def test_filter_out_empty(value, is_empty):
  df = pd.DataFrame([{'A': value, 'B': 'extra'}])
  result = format_data.filter_empty_values(df.copy(), 'A')
  if is_empty:
    assert result.empty
  else:
    assert not result.empty
