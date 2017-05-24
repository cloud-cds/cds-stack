import pandas as pd

def dataframe_equality(df1, df2):
  df1 = df1.sort_index(axis=1)
  df2 = df2.sort_index(axis=1)

  if not df1.columns.equals(df2.columns):
    print("Column mismatch")
    print(df1.columns)
    print(df2.columns)
    return False

  for index, row in df1.iterrows():
    if not row.equals(df2.iloc[index]):
      print("Row mismatch")
      print(row)
      print(df2.iloc[index])
      return False

  return True
