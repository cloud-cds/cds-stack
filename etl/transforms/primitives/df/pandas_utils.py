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
