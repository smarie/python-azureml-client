from unittest import TestCase

import numpy as np
import pandas as pd

import azmlclient as ac


# do not subclass TestCase in order for the test generator to work correctly
class TestConverters(TestCase):

    def test_Df_AzmlTable_conversion(self):

        df = readCsvTestFile('../test_data/dummy_data.csv')

        azt = ac.Converters.Df_to_AzmlTable(df)
        df2 = ac.Converters.AzmlTable_to_Df(azt)

        for col in df.columns:
            print('Checking column ' + col)
            #     TestConverters.test_Df_AzmlTable_conversion.__name__ = 'Df_to_Azml_conversion_' + col
            #     yield self.check_column, df, df2, col
            self.check_column(df, df2, col)

        # TestConverters.test_Df_AzmlTable_conversion.__name__ = 'Df_to_Azml_conversion_final'
        assert df.equals(df2)

    def check_column(self, df, df2, col):
        assert df[col].equals(df2[col])


class TestDatasetReadError(Exception):
    """ This is raised whenever the dataset test file cannot be opened correctly """


def readCsvTestFile(path_input, colSeparator=',', decimalChar='.'):

    inputdataframe = pd.read_csv(path_input, sep=colSeparator, decimal=decimalChar, infer_datetime_format=True,
                                     parse_dates=[0])
    if 'object' in inputdataframe.dtypes:
        raise TestDatasetReadError('Test dataset ' + path_input + ' can not be correctly imported, check the '
                                                                  'column separator and decimal characters')


    datetimeColumns = [colName for colName, colType in inputdataframe.dtypes.items() if np.issubdtype(colType, np.datetime64)]
    for datetimeCol in datetimeColumns:
        # time is in ISO format in our test files, so the time column after import is UTC. We just have to declare it
        inputdataframe[datetimeCol] = inputdataframe[datetimeCol].dt.tz_localize(tz="UTC")

    return inputdataframe