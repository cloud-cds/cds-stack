from etl.transforms.primitives.df import translate
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.lab_results import component_ids
from etl.core import test_utils
import pandas as pd
import pytest


@pytest.mark.parametrize('internal_id, fid', [
  ('1113044020085', 'nbp'),
  ('306270', 'map'),
  ('2123044011001', 'heart_rate'),
])
def test_translate_flowsheet_id_to_fid(internal_id, fid):
  df = pd.DataFrame([
    {'FlowsheetRowID': internal_id, 'ExtraColumn': 'ExtraValue'},
  ])
  df_correct = pd.DataFrame([
    {'fid': fid, 'FlowsheetRowID': internal_id, 'ExtraColumn': 'ExtraValue'},
  ])
  result = translate.translate_epic_id_to_fid(
    df = df.copy(),
    col = 'FlowsheetRowID',
    new_col = 'fid',
    config_map = flowsheet_ids
  )
  assert test_utils.dataframe_equality(result, df_correct)
  df_correct_drop = pd.DataFrame([
    {'fid': fid, 'ExtraColumn': 'ExtraValue'},
  ])
  result_drop = translate.translate_epic_id_to_fid(
    df = df.copy(),
    col = 'FlowsheetRowID',
    new_col = 'fid',
    config_map = flowsheet_ids,
    drop_original = True
  )
  assert test_utils.dataframe_equality(result_drop, df_correct_drop)


@pytest.mark.parametrize('component_id, fid', [
  ('2000000122', 'pao2'),
  ('5000000311', 'amylase'),
  ('9000000173', 'lipase'),
])
def test_translate_lab_result_to_fids(component_id, fid):
  df = pd.DataFrame([
    {'ComponentID': component_id, 'ExtraColumn': 'ExtraValue'},
  ])
  df_correct = pd.DataFrame([
    {'fid': fid, 'ComponentID': component_id, 'ExtraColumn': 'ExtraValue'},
  ])
  result = translate.translate_epic_id_to_fid(
    df = df.copy(),
    col = 'ComponentID',
    new_col = 'fid',
    config_map = component_ids
  )
  assert test_utils.dataframe_equality(result, df_correct)
