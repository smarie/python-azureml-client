import json

import pytest
from pandas.util.testing import assert_frame_equal
from pytest_cases import parametrize_with_cases, fixture

from azmlclient.tests.databinding.test_databinding_cases import DataBindingTestKase
from azmlclient.base_databinding import df_to_azmltable, azmltable_to_df, azmltable_to_json, json_to_azmltable, \
    df_to_csv, csv_to_df


@fixture
@parametrize_with_cases("c")
def case(c):
    return c


@pytest.mark.parametrize("swagger_mode_on", [False, True], ids="swagger_mode_on={}".format)
@pytest.mark.parametrize("replace_NaN_with", [None, "null"], ids="replace_NaN_with={}".format)
@pytest.mark.parametrize("replace_NaT_with", [None, "null"], ids="replace_NaT_with={}".format)
def test_df_to_azmltable(case,  # type: DataBindingTestKase
                         swagger_mode_on, replace_NaN_with, replace_NaT_with):
    """ Tests that a dataframe can be converted to azureml representation (as a dict) and back. """

    azt = df_to_azmltable(case.df, swagger_format=swagger_mode_on,
                          replace_NaN_with=replace_NaN_with, replace_NaT_with=replace_NaT_with)
    df2 = azmltable_to_df(azt, swagger_mode=swagger_mode_on)

    # for some reason this check does not always work depending on pandas version
    if replace_NaN_with is None and replace_NaT_with is None:
        assert_frame_equal(case.df, df2)


def _is_datetime_dtype(series):
    try:
        series.dt
    except AttributeError:
        return False
    else:
        return True


@pytest.mark.parametrize('swagger_mode_on', [False, True], ids="swagger={}".format)
@pytest.mark.parametrize("replace_NaN_with", [None, "null"], ids="replace_NaN_with={}".format)
@pytest.mark.parametrize("replace_NaT_with", [None, "null"], ids="replace_NaT_with={}".format)
def test_df_to_json(swagger_mode_on, replace_NaN_with, replace_NaT_with,
                    case  # type: DataBindingTestKase
                    ):
    """ Tests that a dataframe can be converted to azureml json representation and back. """

    nan_cells = []
    nat_cells = []
    for col_index, col in enumerate(case.df):
        nan_indices = case.df.index[case.df[col].isnull()].values.tolist()
        if len(nan_indices) > 0:
            if _is_datetime_dtype(case.df[col]):
                nat_cells.append((col, col_index, nan_indices))
            else:
                nan_cells.append((col, col_index, nan_indices))

    azt = df_to_azmltable(case.df, swagger_format=swagger_mode_on, replace_NaN_with=replace_NaN_with,
                          replace_NaT_with=replace_NaT_with)

    # check nan/nat conversion
    for col, col_index, nan_indices in nan_cells:
        for i in nan_indices:
            if swagger_mode_on:
                assert azt[i][col] == replace_NaN_with or "nan"
            else:
                assert azt['Values'][i][col_index] == replace_NaN_with or "nan"

    for col, col_index, nan_indices in nat_cells:
        for i in nan_indices:
            if swagger_mode_on:
                assert azt[i][col] == replace_NaT_with or "NaT"
            else:
                assert azt['Values'][i][col_index] == replace_NaT_with or "NaT"

    az_json_df = azmltable_to_json(azt)

    print("Converted df -> json:")
    print(az_json_df)
    if replace_NaN_with is None and replace_NaT_with is None:
        assert json.loads(az_json_df) == json.loads(case.get_df_json(swagger_mode_on=swagger_mode_on))

    azt2 = json_to_azmltable(az_json_df)
    df2 = azmltable_to_df(azt2)

    if replace_NaN_with is None and replace_NaT_with is None:
        # for some reason this does not always work depending on pandas version
        assert_frame_equal(case.df, df2)


def test_df_to_csv(case  # type: DataBindingTestKase
                   ):
    """ Tests that a dataframe can be converted to csv (for blob storage) and back. """

    csvstr = df_to_csv(case.df)

    print("Converted df -> csv:")
    print(csvstr)
    assert csvstr.replace('\r', '') == case.df_csv.replace('\r', '')

    df2 = csv_to_df(csvstr)

    assert_frame_equal(case.df, df2)
