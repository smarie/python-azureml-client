import json

import pytest
from pandas.util.testing import assert_frame_equal
from pytest_cases import cases_data, pytest_fixture_plus

from azmlclient import Converters
from azmlclient.tests import test_databinding_cases
from azmlclient.tests.test_databinding_cases import DataBindingTestCase


@pytest_fixture_plus
@cases_data(module=test_databinding_cases)
def case(case_data):
    return case_data.get()


def test_df_to_azmltable(case  # type: DataBindingTestCase
                         ):
    """ Tests that a dataframe can be converted to azureml representation (as a dict) and back. """

    azt = Converters.df_to_azmltable(case.df)
    df2 = Converters.azmltable_to_df(azt)

    assert_frame_equal(case.df, df2)


@pytest.mark.parametrize('swagger_mode_on', [False, True], ids="swagger={}".format)
def test_df_to_json(swagger_mode_on,
                    case  # type: DataBindingTestCase
                    ):
    """ Tests that a dataframe can be converted to azureml json representation and back. """

    azt = Converters.df_to_azmltable(case.df, swagger=swagger_mode_on)
    az_json_df = Converters.azmltable_to_jsonstr(azt)

    print("Converted df -> json:")
    print(az_json_df)
    assert json.loads(az_json_df) == json.loads(case.get_df_json(swagger_mode_on=swagger_mode_on))

    azt2 = Converters.jsonstr_to_azmltable(az_json_df)
    df2 = Converters.azmltable_to_df(azt2)

    assert_frame_equal(case.df, df2)


def test_df_to_csv(case  # type: DataBindingTestCase
                   ):
    """ Tests that a dataframe can be converted to csv (for blob storage) and back. """

    csvstr = Converters.df_to_csv(case.df)

    print("Converted df -> csv:")
    print(csvstr)
    assert csvstr.replace('\r', '') == case.df_csv.replace('\r', '')

    df2 = Converters.csv_to_df(csvstr)

    assert_frame_equal(case.df, df2)
