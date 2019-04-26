import pytest
import requests

from azmlclient import AzureMLClient, azureml_service, ClientConfig, GlobalConfig, ServiceConfig

import pandas as pd


def test_readme_high_level_basic():
    """Tests that the example in the readme works correctly at least in local mode"""

    class MathsProvider(AzureMLClient):
        """
        A client for the `add_columns` and `subtract_columns` web services
        """
        @azureml_service
        def add_columns(self, a_name, b_name, df):
            """
            Offers a pythonic API around the `add_columns` azureML service

            :param a_name: name of the first column to add
            :param b_name: name of the second column to add
            :param df: the input dataframe
            :return:
            """
            # (1) create the web service inputs and parameters from provided data.
            ws_inputs = {'input': df}
            ws_params = {'a_name': a_name, 'b_name': b_name}

            # (2) call the azureml web service
            result_dfs = self.call_azureml(self.add_columns,
                                           ws_inputs=ws_inputs, ws_output_names=['output'], ws_params=ws_params)

            # (3) unpack the results
            return result_dfs['output']

        @azureml_service
        def subtract_columns(self, a_name, b_name, df):
            # (similar contents than `add_columns` here)
            pass

    # create a configuration indicating the endpoints for each service id
    cfg = ClientConfig(add_columns=ServiceConfig(base_url="https://notavailbl", api_key="dummy"),
                       subtract_columns=ServiceConfig(base_url="https://notavailbl", api_key="dummy"))

    # instantiate the client
    client = MathsProvider(cfg)

    # use it
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [0, 5, 10]})
    try:
        result_df = client.add_columns('x', 'y', df)
    except requests.exceptions.ConnectionError:
        pass

    with pytest.raises(NotImplementedError):
        with client.local_calls():
            result_df = client.add_columns('x', 'y', df)


def test_readme_high_level_local():
    """Tests that the example in the readme works correctly at least in local mode"""

    class MathsProviderLocal(object):
        """
        A local implementation of the same services
        """
        def add_columns(self, a_name, b_name, df):
            return pd.DataFrame({'sum': df[a_name] + df[b_name]})

        def subtract_columns(self, a_name, b_name, df):
            return pd.DataFrame({'diff': df[a_name] - df[b_name]})

    class MathsProvider(AzureMLClient):
        """
        A client for the `add_columns` and `subtract_columns` web services
        """

        def __init_local_impl__(self):
            """ Use our local implementation """
            return MathsProviderLocal()

        @azureml_service
        def add_columns(self, a_name, b_name, df):
            """
            Offers a pythonic API around the `add_columns` azureML service

            :param a_name: name of the first column to add
            :param b_name: name of the second column to add
            :param df: the input dataframe
            :return:
            """
            # (1) create the web service inputs and parameters from provided data.
            ws_inputs = {'input': df}
            ws_params = {'a_name': a_name, 'b_name': b_name}

            # (2) call the azureml web service
            result_dfs = self.call_azureml(self.add_columns,
                                           ws_inputs=ws_inputs, ws_output_names=['output'], ws_params=ws_params)

            # (3) unpack the results
            return result_dfs['output']

        @azureml_service
        def subtract_columns(self, a_name, b_name, df):
            # (similar contents than `add_columns` here)
            pass

    # create a configuration indicating the endpoints for each service id
    cfg = ClientConfig(add_columns=ServiceConfig(base_url="https://notavailbl", api_key="dummy"),
                       subtract_columns=ServiceConfig(base_url="https://notavailbl", api_key="dummy"))

    # instantiate the client
    client = MathsProvider(cfg)

    # use it
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [0, 5, 10]})
    try:
        result_df = client.add_columns('x', 'y', df)
    except requests.exceptions.ConnectionError:
        pass

    with client.local_calls():
        result_df = client.add_columns('x', 'y', df)

    assert list(result_df.columns) == ['sum']