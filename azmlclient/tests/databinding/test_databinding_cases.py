import os
import pandas as pd

try:
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache
from pytest_cases import cases_generator

from azmlclient.base_databinding import convert_all_datetime_columns, localize_all_datetime_columns


class DataBindingTestCase(object):
    """
    Represents a test case for the databinding tests
    """
    def __init__(self, parent_path, case_name):
        self.folder_path = os.path.join(parent_path, case_name)
        self.case_name = case_name

    def __repr__(self):
        return "<Test DataBindingTestCase '%s' [%s]>" % (self.case_name, self.folder_path)

    @property
    @lru_cache()
    def df_csv(self):
        """The csv file"""
        file_path = os.path.join(self.folder_path, 'data.csv')
        with open(file_path) as f:
            _csv = f.read()
        return _csv

    @property
    @lru_cache()
    def df(self):
        """The pandas dataframe"""
        file_path = os.path.join(self.folder_path, 'data.csv')
    
        # read the file
        input_df = pd.read_csv(file_path, sep=',', decimal='.')
    
        # convert all possible columns to datetime
        convert_all_datetime_columns(input_df)
    
        # localize all the datetime columns (note: this is not really the correct test but it works here)
        localize_all_datetime_columns(input_df)
    
        return input_df

    @lru_cache()
    def get_df_json(self, swagger_mode_on):
        file_path = os.path.join(self.folder_path, 'azmltable%s.json' % ('' if not swagger_mode_on else '_swagger'))
        with open(file_path) as f:
            json = f.read()
        return json


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'test_data')
CASE_NAMES = os.listdir(TEST_DATA_DIR)


@cases_generator("{case_name}", case_name=CASE_NAMES)
def case_from_folders(case_name):
    """
    Generates all cases from the file system
    :param case_name: 
    :return: 
    """
    return DataBindingTestCase(TEST_DATA_DIR, case_name)
