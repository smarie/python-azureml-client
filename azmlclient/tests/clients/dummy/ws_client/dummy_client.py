import sys
from logging import Logger, getLogger, StreamHandler, INFO

from azmlclient import AzureMLClient, ClientConfig, azureml_service
from azmlclient.tests.clients.dummy.api_and_core import DummyProvider


# the logger to use in our component client
default_logger = getLogger('dummy')
ch = StreamHandler(sys.stdout)
default_logger.addHandler(ch)
default_logger.setLevel(INFO)


class DummyClient(DummyProvider, AzureMLClient):
    """
    A client for the dummy web service
    """

    def __init__(self,
                 client_config,          # type: ClientConfig
                 logger=default_logger,  # type: Logger
                 with_plots=False        # type: bool
                 ):
        """
        Constructor. Same than super but with an additional `with_plots` attribute.

        :param client_config:
        :param logger:
        :param with_plots:
        """
        # call super
        AzureMLClient.__init__(self, client_config=client_config, logger=logger)

        # add our fields
        self.with_plots = with_plots

    def __init_local_impl__(self):
        """
        Override super method to define the local implementation to create
        :return:
        """
        from azmlclient.tests.clients.dummy.api_and_core import DummyImpl
        return DummyImpl(logger=self.logger, with_plots=self.with_plots)

    @azureml_service
    def add_columns(self, a_name, b_name, df):
        """
        Implements the remote calls to the service.
        (Local calls are delegated to the local impl)

        :param a_name:
        :param b_name:
        :param df:
        :return:
        """
        # (1) create the web service inputs and parameters from provided data.
        ws_inputs = {'input': df}
        ws_params = {'a_name': a_name, 'b_name': b_name}

        # (2) call ws in appropriate mode
        result_dfs = self.call_azureml(self.add_columns,
                                       ws_inputs=ws_inputs, ws_output_names=['output'], ws_params=ws_params)

        # (3) unpack results
        results_df = result_dfs['output']

        return results_df

    @azureml_service
    def subtract_columns(self, a_name, b_name, df):
        # remote call
        raise NotImplementedError("Remote calls for this service are not implemented yet.")
