import sys
from logging import getLogger, StreamHandler, INFO

import cherrypy
import os

from azmlclient.utils_testing import AzuremlWebServiceMock
from azmlclient.tests.clients.dummy.api_and_core import DummyImpl
from azmlclient.base_databinding import df_to_azmltable

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


# the logger to use in our component client
default_logger = getLogger('dummy impl in web server')
ch = StreamHandler(sys.stdout)
default_logger.addHandler(ch)
default_logger.setLevel(INFO)


class Welcome(object):
    @cherrypy.expose
    def index(self):
        msg = "Welcome to this CherryPy server supposed to mimic azureML.<p>" \
              "Here are the available services:" \
              "<ul>"
        for url in cherrypy.tree.apps.keys():
            if url != '':
                msg += "<li>%s</li>" % url
        msg += "</ul>"
        return msg


@cherrypy.expose
class AddColumnsWS(AzuremlWebServiceMock):
    name = 'a_plus_b'

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self, **kwargs):
        cherrypy.log('[' + self.name + '] was asked to execute with params ' + str(kwargs))

        # Decode the body (cherrypy has already transformed the json into a dict)
        inputs, params = self.unpack_azureml_query(cherrypy.request.json, table_input_names='input')

        # Create the provider and execute
        provider = DummyImpl(logger=default_logger)
        result_df = provider.add_columns(params['a_name'], params['b_name'], inputs['input'])

        # converts all output dataframes to azureml format
        res_dict = {'output': df_to_azmltable(result_df, mimic_azml_output=True)}

        return {"Results": res_dict}


@cherrypy.expose
class SubtractColumnsWS(AzuremlWebServiceMock):
    name = 'a_minus_b'

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self, **kwargs):
        cherrypy.log('[' + self.name + '] was asked to execute with params ' + str(kwargs))

        # Decode the body (cherrypy has already transformed the json into a dict)
        inputs, params = self.unpack_azureml_query(cherrypy.request.json, table_input_names='input')

        # Create the provider and execute
        provider = DummyImpl(logger=default_logger)
        result_df = provider.subtract_columns(params['a_name'], params['b_name'], inputs['input'])

        # converts all output dataframes to azureml format
        res_dict = {'output': df_to_azmltable(result_df, mimic_azml_output=True)}

        return {"Results": res_dict}


def start_ws_mock():
    """
    Starts a mock ws server with https enabled
    :return:
    """
    # cherrypy.quickstart(AzureMLServerMock())

    # update server-wide config to setup https
    server_config = {
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 4443,

        # see http://docs.cherrypy.org/en/latest/deploy.html?highlight=ssl_module#ssl-support for details
        'server.ssl_module': 'builtin',  #'pyopenssl',
        'server.ssl_certificate': os.path.join(THIS_DIR, 'cert.pem'),
        'server.ssl_private_key': os.path.join(THIS_DIR, 'privkey.pem'),
    }
    cherrypy.config.update(server_config)

    dispatch_http_verbs = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher()}}

    # create one set of methods per webservice
    cherrypy.tree.mount(AddColumnsWS(), '/%s/execute' % AddColumnsWS.name, config=dispatch_http_verbs)
    cherrypy.tree.mount(SubtractColumnsWS(), '/%s/execute' + SubtractColumnsWS.name, config=dispatch_http_verbs)
    cherrypy.tree.mount(Welcome(), '/', config=None)

    cherrypy.engine.signals.subscribe()
    cherrypy.engine.start()


if __name__ == '__main__':
    # for debug
    start_ws_mock()
    cherrypy.engine.block()
