import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal

from azmlclient import Converters
from azmlclient.data_binding import convert_all_datetime_columns


def test_df_to_azmltable():
    """ Tests that a dataframe can be converted to azureml representation (as a dict) and back. """
    df = read_csv_test_file('./test_data/dummy_data.csv')

    azt = Converters.df_to_azmltable(df)
    df2 = Converters.azmltable_to_df(azt)

    assert_frame_equal(df, df2)


def test_df_to_json():
    """ Tests that a dataframe can be converted to azureml json representation and back. """
    df = read_csv_test_file('./test_data/dummy_data.csv')

    azt = Converters.df_to_azmltable(df)
    js = Converters.azmltable_to_jsonstr(azt)
    azt2 = Converters.jsonstr_to_azmltable(js)
    df2 = Converters.azmltable_to_df(azt2)

    assert_frame_equal(df, df2)


def test_df_to_csv():
    """ Tests that a dataframe can be converted to csv (for blob storage) and back. """
    df = read_csv_test_file('./test_data/dummy_data.csv')

    csvstr = Converters.df_to_csv(df)
    df2 = Converters.csv_to_df(csvstr)

    assert_frame_equal(df, df2)


def read_csv_test_file(path_input, col_sep=',', decimal_char='.'):
    """ Utility method to read the test data csv into a dataframe"""

    # read the file
    inputdataframe = pd.read_csv(path_input, sep=col_sep, decimal=decimal_char)

    # convert all possible columns to datetime
    convert_all_datetime_columns(inputdataframe)

    # localize all the datetime columns
    datetimeColumns = [colName for colName, colType in inputdataframe.dtypes.items()
                       if np.issubdtype(colType, np.datetime64)]
    for datetimeCol in datetimeColumns:
        # time is in ISO format in our test files, so the time column after import is UTC. We just have to declare it
        inputdataframe[datetimeCol] = inputdataframe[datetimeCol].dt.tz_localize(tz="UTC")

    return inputdataframe
