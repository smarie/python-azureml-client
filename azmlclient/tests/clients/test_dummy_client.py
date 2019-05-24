import os

import cherrypy
import pytest

import pandas as pd

from azmlclient import ClientConfig, LocalCallModeNotAllowed

from azmlclient.tests.clients.dummy.api_and_core import DummyProvider
from azmlclient.tests.clients.dummy.web_services import start_ws_mock
from azmlclient.tests.clients.dummy import ws_client
from azmlclient.tests.clients.dummy.ws_client import DummyClient


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_DIR = os.path.dirname(os.path.abspath(ws_client.__file__))


@pytest.fixture(scope="module", autouse=True)
def mock_server():
    """

    :param request:
    :return:
    """
    # start a mock azureml server
    start_ws_mock()

    yield

    # teardown
    cherrypy.engine.exit()  # request.addfinalizer(cherrypy.engine.exit)


@pytest.fixture(params=['yaml', 'cfg'])
def client_cfg(request):
    # type: (...) -> ClientConfig
    """
    Creates a client from the configuration file
    :return:
    """
    cfg_file = os.path.join(CLIENT_DIR, 'dummy_client_conf.%s' % request.param)
    if request.param == 'yaml':
        client_cfg = ClientConfig.load_yaml(file_path_or_stream=cfg_file)
    elif request.param == 'cfg':
        client_cfg = ClientConfig.load_config(cfg_file)
    else:
        raise ValueError("unknown : %s" % request.param)

    return client_cfg


@pytest.fixture
def client_impl(client_cfg):
    # type: (...) -> DummyProvider
    client = DummyClient(client_config=client_cfg, with_plots=False)
    return client


def _get_test_item_id(x):
    """ custom test ids """
    if isinstance(x, bool) or x is None:
        return "swagger_format={}".format(x)
    else:
        return "call_mode={}".format(x)


@pytest.mark.parametrize('call_mode,swagger_format', [('local', None),
                                                      ('RR', False),
                                                      ('RR', True),
                                                      ('Batch', None)],
                         ids=_get_test_item_id)
def test_client_call_simple(client_impl,     # type: DummyClient
                            call_mode,       # type: str
                            swagger_format   # type: bool
                            ):
    """ Tests that the client can be used to use the 'add columns' service """

    # input data
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [0, 5, 10]})

    # local or remote call
    if call_mode == 'local':
        context = client_impl.local_calls()
    elif call_mode == 'RR':
        context = client_impl.rr_calls(use_swagger_format=swagger_format)
    elif call_mode == 'Batch':
        context = client_impl.batch_calls()
        pytest.skip("Batch mode is not implemented on this mock server.")
    else:
        raise ValueError()

    with context:
        with client_impl.debug_requests():
            res_df = client_impl.add_columns(a_name='x', b_name='y', df=df)

    # check that the result is correct
    assert (res_df['sum'] == df['x'] + df['y']).all()


def test_client_call_remote_only(client_impl  # type: DummyClient
                                 ):
    """ Tests that remote-only services can not """
    with pytest.raises(LocalCallModeNotAllowed) as exc_info:
        with client_impl.local_calls():
            client_impl.subtract_columns('a', 'b', None)

    assert str(exc_info.value).startswith("function 'subtract_columns' (service 'subtract_columns') is remote-only and "
                                          "can not be executed in local mode")
