from etl.transforms.primitives.df import restructure
from etl.core import test_utils
import pytest
import pandas as pd

def test_extract_internal_ids():
	df = pd.DataFrame([{
		'ID_col': [
			{'Type': 'bad_type1',},
			{'Type': 'good_type', 'ID': j},
			{'Type': 'bad_type2', 'ID': str(j+10)}
		],
		'Extra': 'still_here',
	} for j in range(5)])

	df_correct = pd.DataFrame([{
		'ID_col': str(j),
		'Extra': 'still_here'
	} for j in range(5)])

	result = restructure.extract_id_from_list(df.copy(), 'ID_col', 'good_type')

	assert test_utils.dataframe_equality(df_correct, result)


def test_extract():
	df = pd.DataFrame([{
		'fid': 'fid_' + str(j),
		'dict_col': {
			'RawValue': 'val_' + str(j),
			'Instant' : 'tsp_' + str(j),
			'Unneeded': 'fid_' + str(j),
		},
		'Unit': 'unit_' + str(j),
	} for j in range(10)
	])
	df_extracted = pd.DataFrame([
		{
			'fid': 'fid_' + str(j),
			'new_raw_value': 'val_' + str(j),
			'tsp' : 'tsp_' + str(j),
			'Unit': 'unit_' + str(j),
		} for j in range(10)]
	)
	result = restructure.extract(df.copy(), 'dict_col', 
		{'RawValue': 'new_raw_value', 'Instant': 'tsp',})
	assert test_utils.dataframe_equality(df_extracted, result)

def test_unlist():
	empty_list = []
	df = pd.DataFrame([
		{
			'ComponentID':  'fid_' + str(k),
			'Value':        [str(i) for i in range(2)],
			'tsp':          str(j),
			'Unit':         'unit_' + str(k),
		} for k in range(2) for j in range(10)]
	)
	df_extracted = pd.DataFrame([
		{
			'ComponentID':  'fid_' + str(k),
			'Value':        str(j%2),
			'tsp':          str(int(j/2)),
			'Unit':         'unit_' + str(k),
		} for k in range(2) for j in range(20)]
	)

	result = restructure.unlist(df.copy(), 'Value')
	assert test_utils.dataframe_equality(df_extracted, result)

