import os

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from azmlclient import Converters
from azmlclient.data_binding import convert_all_datetime_columns, is_datetime_dtype

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'test_data')


@pytest.fixture
def df():
    return read_csv_test_file(os.path.join(TEST_DATA_DIR, 'dummy_data.csv'))


def test_df_to_azmltable(df):
    """ Tests that a dataframe can be converted to azureml representation (as a dict) and back. """

    azt = Converters.df_to_azmltable(df)
    df2 = Converters.azmltable_to_df(azt)

    assert_frame_equal(df, df2)


def test_df_to_json(df):
    """ Tests that a dataframe can be converted to azureml json representation and back. """

    azt = Converters.df_to_azmltable(df)
    js = Converters.azmltable_to_jsonstr(azt)
    azt2 = Converters.jsonstr_to_azmltable(js)
    df2 = Converters.azmltable_to_df(azt2)

    assert_frame_equal(df, df2)


def test_df_to_csv(df):
    """ Tests that a dataframe can be converted to csv (for blob storage) and back. """

    csvstr = Converters.df_to_csv(df)
    df2 = Converters.csv_to_df(csvstr)

    assert_frame_equal(df, df2)


def read_csv_test_file(path_input, col_sep=',', decimal_char='.'):
    """ Utility method to read the test data csv into a dataframe"""

    # read the file
    inputdataframe = pd.read_csv(path_input, sep=col_sep, decimal=decimal_char)

    # convert all possible columns to datetime
    convert_all_datetime_columns(inputdataframe)

    # localize all the datetime columns (note: this is not really the correct test but it works here)
    datetimeColumns = [colName for colName, colType in inputdataframe.dtypes.items() if is_datetime_dtype(colType)]
    for datetimeCol in datetimeColumns:
        # time is in ISO format in our test files, so the time column after import is UTC. We just have to declare it
        try:
            inputdataframe[datetimeCol] = inputdataframe[datetimeCol].dt.tz_localize(tz="UTC")
        except TypeError:
            inputdataframe[datetimeCol] = inputdataframe[datetimeCol].dt.tz_convert(tz="UTC")

    return inputdataframe
