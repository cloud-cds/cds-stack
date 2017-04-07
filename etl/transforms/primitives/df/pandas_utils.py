import pandas as pd
import numpy as np

# Expand a list within a column to a single item in the list per row
# (duplicate all other items in the row)
def unlistify_pandas_column(df, column):
    col_idx = check_column_name(df, column)

    # Helper function to expand and repeat the column col_idx
    def expand_and_repeat_column(d):
        row = list(d.values[0])
        bef = row[:col_idx]
        aft = row[col_idx+1:]
        col = row[col_idx]
        if col:
            z = [bef + [c] + aft for c in col]
        else:
            return pd.DataFrame([bef + [np.nan] + aft])
        if len(col) == 0:
            z = [bef + [np.nan] + aft]
        return pd.DataFrame(z)

    col_idx += len(df.index.shape) # Since we will push reset the index
    index_names = list(df.index.names)
    column_names = list(index_names) + list(df.columns)
    return (df
            .reset_index()
            .groupby(level=0, as_index=0)
            .apply(expand_and_repeat_column)
            .rename(columns = lambda i :column_names[i])
            .set_index(index_names, drop=True)
            .reset_index(drop=True)
            )

def turn_dict_into_columns(df, dict_column):
    check_column_name(df, dict_column)
    new_cols = pd.DataFrame(df[dict_column].tolist())
    old_cols = df.drop(dict_column, axis=1)
    all_cols = pd.concat([old_cols, new_cols], axis=1)
    return all_cols

def check_column_name(df, column):
    matches = [idx for idx, name in enumerate(df.columns) if name == column]
    if len(matches) == 0:
        raise app_config.TransformError(
            'pandas_utils.check_column_name',
            'Failed to find column.',
            column
        )
    elif len(matches) > 1:
        raise app_config.TransformError(
            'pandas_utils.check_column_name',
            'More than one column named.',
            column
        )
    else:
        return matches[0]

def upsert_db(df, sql_tbl_name, connection, on_conflict_cols):
  """generic pandas based upserting code, tables should already be in schema"""
  # ==========================================
  # Save tmp table
  # ==========================================

  temp_table_name = 'temp_' + sql_tbl_name
  nrows = df.shape[0]
  connection.execute("""DROP TABLE IF EXISTS {}""".format(temp_table_name))
  print("saving data frame to {}: nrows = {}".format(temp_table_name, nrows))

  df.to_sql(temp_table_name, connection, if_exists='append', index=False, schema='public')

  # ==========================================
  # upsert tmp table into real table
  # ==========================================
  exluded_cols = list(set(df.columns) - set(on_conflict_cols))
  excluded_str = ', '.join([col+ '= EXCLUDED.'+col for col in exluded_cols])


  make_final_sql = """
      insert into {sql_tbl} ({all_cols})
      select                 {all_cols} from {tmp_tbl}
      """.format(sql_tbl=sql_tbl_name,
                 all_cols=', '.join(df.columns),
                 tmp_tbl=temp_table_name)

  if len(on_conflict_cols)>0 and len(excluded_str)>0:
    make_final_sql += """on conflict ({})
                         DO UPDATE
                         SET {};""".format(', '.join(on_conflict_cols), excluded_str)
  else:
    make_final_sql +=';'


  print("upserting temp table into {}:".format(sql_tbl_name))
  print(make_final_sql)
  connection.execute(make_final_sql)
  # ==========================================
  # Remove tmp table
  # =========================================
  print("removing temp table:")
  connection.execute("""DROP TABLE {}""".format(temp_table_name))