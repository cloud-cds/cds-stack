from etl.transforms.primitives.df import filter_rows
from etl.core import test_utils
import pytest
import pandas as pd

def test_filter_on_icd9():
  correct_diagnosis = {'Name': 'heart_failure_diag', 'ICD9': '428'}
  incorrect_diagnosis = {'Name': 'heart_failure_diag', 'ICD9': '429'}
  correct_history = {'Name': 'met_carcinoma_hist', 'ICD9': '141'}
  incorrect_history = {'Name': 'met_carcinoma_hist', 'ICD9': '142'}
  correct_problem = {'Name': 'esrd_prob', 'ICD9': '585.6'}
  incorrect_problem = {'Name': 'esrd_prob', 'ICD9': '585'}
  df = pd.DataFrame([{
    'diagnosis': [correct_diagnosis, incorrect_diagnosis],
    'history': [correct_history, incorrect_history],
    'problem': [correct_problem, incorrect_problem],
    'problem_all': [correct_problem, incorrect_problem],
  }])

  result = filter_rows.filter_on_icd9(df)

  assert len(result.diagnosis) == 1
  assert len(result.history) == 1
  assert len(result.problem) == 1
  assert correct_diagnosis['Name'] in result.diagnosis.item()
  assert correct_history['Name'] in result.history.item()
  assert correct_problem['Name'] in result.problem.item()
