from pytz import timezone
import numpy as np


#String to convert to an EST string safe for the database


def datetime_2_utc_str(df, tz_in_str='US/Eastern', column_list=None):
    """A slightly more general version of the TZ hack to write to DB"""
    out_tsp_fmt_tmp = '%Y-%m-%dT%H:%M:%S{}:00'

    # the from pytz import common_timezones, ,from pytz import all_timezones

    tz = timezone(tz_in_str)

    out_tsp_fmt = out_tsp_fmt_tmp.format(round(tz._utcoffset.total_seconds() / (60 * 60)))

    if column_list is None:
        types = df.dtypes
        column_list = [col for col in df.columns if np.issubdtype(types.loc[col],np.datetime64)]


    def to_tz_str(time_in):
        try:
            if np.issubdtype(time_in,np.datetime64):
                str_out = (time_in + tz._utcoffset).strftime(out_tsp_fmt)
            else:
                str_out = str(time_in) #handles nones and nans

            return str_out
        except:
            print(time_in)
            type(time_in)
            raise ValueError("Unexcepted input in time columns")

    for col in column_list:
        df[col] = df[col].apply(to_tz_str)

    return df

df = datetime_2_utc_str(df)
