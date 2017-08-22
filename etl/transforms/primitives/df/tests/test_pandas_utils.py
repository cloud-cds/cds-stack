from etl.transforms.primitives.df import pandas_utils
from etl.core import test_utils
import pandas as pd
import numpy as np
import pytest

def test_unlistify_lists_of_same_lengths():
  df = pd.DataFrame([
    {
      'A': 'A_' + str(j) + '_1',
      'B': ['B_' + str(j) + '_' + str(i) for i in range(3)],
      'C': 'C_' + str(j) + '_1',
    } for j in range(6)
  ])
  df_extracted = pd.DataFrame([
    {
      'A': 'A_' + str(int(j/3)) + '_1',
      'B': 'B_' + str(int(j/3)) + '_' + str(j%3),
      'C': 'C_' + str(int(j/3)) + '_1',
    } for j in range(18)
  ])
  result = pandas_utils.unlistify_pandas_column(df.copy(), 'B')
  assert test_utils.dataframe_equality(df_extracted, result)

def test_unlistify_lists_of_different_lengths():
  df = pd.DataFrame([
    { 'A': 'A_0_1', 'B': ['B_0_1', 'B_0_2', 'B_0_3'], 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': ['B_1_1', 'B_1_2'], 'C': 'C_1_1', },
  ])
  df_extracted = pd.DataFrame([
    { 'A': 'A_0_1', 'B': 'B_0_1', 'C': 'C_0_1', },
    { 'A': 'A_0_1', 'B': 'B_0_2', 'C': 'C_0_1', },
    { 'A': 'A_0_1', 'B': 'B_0_3', 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': 'B_1_1', 'C': 'C_1_1', },
    { 'A': 'A_1_1', 'B': 'B_1_2', 'C': 'C_1_1', },
  ])
  result = pandas_utils.unlistify_pandas_column(df.copy(), 'B')
  assert test_utils.dataframe_equality(df_extracted, result)

def test_unlistify_lists_of_length_one():
  df = pd.DataFrame([
    { 'A': 'A_0_1', 'B': ['B_0_1'], 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': ['B_1_1'], 'C': 'C_1_1', },
  ])
  df_extracted = pd.DataFrame([
    { 'A': 'A_0_1', 'B': 'B_0_1', 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': 'B_1_1', 'C': 'C_1_1', },
  ])
  result = pandas_utils.unlistify_pandas_column(df.copy(), 'B')
  assert test_utils.dataframe_equality(df_extracted, result)

def test_unlistify_empty_list():
  df = pd.DataFrame([
    { 'A': 'A_0_1', 'B': ['B_0_1', 'B_0_2'], 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': [], 'C': 'C_1_1', },
  ])
  df_extracted = pd.DataFrame([
    { 'A': 'A_0_1', 'B': 'B_0_1', 'C': 'C_0_1', },
    { 'A': 'A_0_1', 'B': 'B_0_2', 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': np.nan, 'C': 'C_1_1', },
  ])
  result = pandas_utils.unlistify_pandas_column(df.copy(), 'B')
  assert test_utils.dataframe_equality(df_extracted, result)

def test_unlistify_multiple_lists():
  df = pd.DataFrame([
    { 'A': 'A_0_1', 'B': ['B_0_1', 'B_0_2', 'B_0_3'], 'C': ['C_0_1'], },
    { 'A': 'A_1_1', 'B': ['B_1_1', 'B_1_2'], 'C': ['C_1_1', 'C_1_2'], },
  ])
  df_extracted = pd.DataFrame([
    { 'A': 'A_0_1', 'B': 'B_0_1', 'C': 'C_0_1', },
    { 'A': 'A_0_1', 'B': 'B_0_2', 'C': 'C_0_1', },
    { 'A': 'A_0_1', 'B': 'B_0_3', 'C': 'C_0_1', },
    { 'A': 'A_1_1', 'B': 'B_1_1', 'C': 'C_1_1', },
    { 'A': 'A_1_1', 'B': 'B_1_2', 'C': 'C_1_1', },
    { 'A': 'A_1_1', 'B': 'B_1_1', 'C': 'C_1_2', },
    { 'A': 'A_1_1', 'B': 'B_1_2', 'C': 'C_1_2', },
  ])
  result_1 = pandas_utils.unlistify_pandas_column(df.copy(), 'C')
  result_2 = pandas_utils.unlistify_pandas_column(result_1, 'B')
  assert test_utils.dataframe_equality(df_extracted, result_2)
