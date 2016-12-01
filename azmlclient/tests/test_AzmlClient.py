from unittest import TestCase

import numpy as np
import pandas as pd

import azmlclient as ac

class Test_DataBinding(TestCase):

    def test_df_to_azmltable(self):
        df = readCsvTestFile('../test_data/dummy_data.csv')
        azt = ac.Converters.df_to_azmltable(df)
        df2 = ac.Converters.azmltable_to_df(azt)
        self.assert_identical_df(df, df2)

    def test_df_to_json(self):
        df = readCsvTestFile('../test_data/dummy_data.csv')
        azt = ac.Converters.df_to_azmltable(df)
        js = ac.Converters.azmltable_to_jsonstr(azt)
        azt2 = ac.Converters.jsonstr_to_azmltable(js)
        df2 = ac.Converters.azmltable_to_df(azt2)
        self.assert_identical_df(df, df2)

    def test_df_to_csv(self):
        df = readCsvTestFile('../test_data/dummy_data.csv')
        csvstr = ac.Converters.df_to_csv(df)
        df2 = ac.Converters.csv_to_df(csvstr)
        self.assert_identical_df(df, df2)

    def assert_identical_df(self, df, df2):
        for col in df.columns:
            print('Checking column ' + col)
            self.assert_identical_column(df, df2, col)

        assert df.equals(df2)

    def assert_identical_column(self, df, df2, col):
        assert df[col].equals(df2[col])



def readCsvTestFile(path_input, colSeparator=',', decimalChar='.'):

    inputdataframe = pd.read_csv(path_input, sep=colSeparator, decimal=decimalChar, infer_datetime_format=True,
                                     parse_dates=[0])

    datetimeColumns = [colName for colName, colType in inputdataframe.dtypes.items() if np.issubdtype(colType, np.datetime64)]
    for datetimeCol in datetimeColumns:
        # time is in ISO format in our test files, so the time column after import is UTC. We just have to declare it
        inputdataframe[datetimeCol] = inputdataframe[datetimeCol].dt.tz_localize(tz="UTC")

    return inputdataframe