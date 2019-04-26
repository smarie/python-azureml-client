import sys
from contextlib import contextmanager
from inspect import getmembers, isroutine
from logging import getLogger, StreamHandler, INFO

try:  # python 3+
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

try:  # python 3.5+
    from typing import Dict, List, Callable, Union, Optional
    from logging import Logger
except ImportError:
    pass

from decopatch import function_decorator, DECORATED
from makefun import wraps
from requests import Session

import pandas as pd

from azmlclient.clients_callmodes import CallMode, Batch, RequestResponse, LocalCallMode
from azmlclient.clients_config import ClientConfig
from azmlclient.utils_requests import debug_requests


# default logger that may be used by clients
default_logger = getLogger('azmlclient')
ch = StreamHandler(sys.stdout)
default_logger.addHandler(ch)
default_logger.setLevel(INFO)


AZML_SERVICE_ID = '__azml_service__'


@function_decorator
def azureml_service(service_name=None,  # type: str
                    f=DECORATED,
                    ):
    """
    A decorator for methods in your `AzureMLClient` subclasses, that you should use to indicate that a given method
    corresponds to an AzureML service. That way, the `AzureMLClient` base class will be able to link this method
    with local implementation and with the service configuration (url, api key).

    This decorator performs two things:
     - It wraps the decorated method into a method able to route "local"-mode calls to `self.call_local_service`
     - It adds the `AZML_SERVICE_ID` attribute with the `service_name` so that the method is known as being
       AzureML-related, and therefore the appropriate service configuration can be looked up.

    :param service_name: the optional service name appearing in the `AzureMLClient` configuration (`ClientConfig`). By
        default this is `None` and means that the method name should be used as the service name.
    """
    @wraps(f)
    def f_wrapper(self,  # type: AzureMLClient
                  *args,
                  **kwargs):
        """

        :param self:
        :param args:
        :param kwargs:
        :return:
        """
        if self.is_local_mode():
            # execute the same method on local implementor rather than client.
            return self.call_local_service(f.__name__, *args, **kwargs)
        else:
            # execute as usual
            return f(self, *args, **kwargs)

    # tag the method as being related to an azureml service with given id
    setattr(f_wrapper, AZML_SERVICE_ID, service_name)
    return f_wrapper


def get_azureml_service_name(f):
    """
    Returns the azureml service name associated with method `f`.
    :param f:
    :return:
    """
    try:
        # if this is the bound method, get the unbound one
        if hasattr(f, '__func__'):
            f = f.__func__
        azml_name = getattr(f, AZML_SERVICE_ID)
    except AttributeError:
        raise ValueError("Method '%s' can not be bound to an azureml service, please decorate it with "
                         "@azureml_service." % f.__name__)
    else:
        return azml_name if azml_name is not None else f.__name__


class AzureMLClient:
    """
    Base class for AzureML clients.

    A client is configured with a mandatory `ClientConfig` object describing global and per-service options (endpoint
    urls, api keys).

    It provides a way to create them from a configuration containing endpoint definitions,
    and to declare a local implementation
    """

    def __init__(self,
                 client_config,          # type: ClientConfig
                 logger=default_logger,  # type: Logger
                 default_call_mode=None  # type: CallMode
                 ):
        """
        Creates an `AzureMLClient` with an initial `ClientConfig` containing the global and per-service configurations.


        Constructor with a global configuration and service endpoint configurations. The service endpoint
        configurations should be provided in a dictionary with keys being the service names. Only names declared in the
        'services' meta attribute of the class will be accepted, otherwise and error will be raised. Note that you may
        provide configurations for some services only.

        :param client_config: a configuration for this component client. It should be valid = contain
        :param logger:
        :param default_call_mode: (advanced) if a non-None `CallMode` instance is provided, it will be used as the
            default call mode for this client. Otherwise by default a request-response call mode will be set as the
            default call mode (`RequestResponse()`)
        """
        # save the attributes
        self.client_config = client_config
        self.logger = logger
        if default_call_mode is None:
            # by default make this a request response
            default_call_mode = RequestResponse()
        self._current_call_mode = default_call_mode

        # init the local impl property
        self._local_impl = None

    # --------- remote service calls implementation

    @property
    def service_methods(self):
        """
        returns a dictionary of all service methods referenced by azureml service name.
        These are all methods in the class that have been decorated with `@azureml_service`
        :return:
        """
        return {get_azureml_service_name(v[1]): v[1]
                for v in getmembers(self.__class__, predicate=lambda x: isroutine(x) and hasattr(x, AZML_SERVICE_ID))}

    @property
    def service_names(self):
        """
        Returns the list of all service names - basically the names of the `service_methods`
        :return:
        """
        return self.service_methods.keys()

    # --------- local implementor

    def __init_local_impl__(self):
        """
        Implementors should create a local implementation and return it
        :return:
        """
        raise NotImplementedError("Local execution is not available for this client. Please override "
                                  "`__init_local_impl__` or set a non-none `self._local_impl` if you wish local calls "
                                  "to be made available")

    @property
    def local_impl(self):
        if self._local_impl is None:
            self._local_impl = self.__init_local_impl__()
        return self._local_impl

    def call_local_service(self,
                           service_name,  # type: str
                           *args, **kwargs):
        """
        This method is called automatically when a service method (i.e. decorated with `@azureml_service`)
        is called and this instance is in "local" mode. It delegates to local.

        :param service_name:
        :param args:
        :param kwargs:
        :return:
        """
        local_provider = self.local_impl
        local_method = getattr(local_provider, service_name)
        return local_method(*args, **kwargs)

    # --------- configuration

    @property
    def client_config(self):
        return self._client_config

    @client_config.setter
    def client_config(self,
                      client_config  # type: ClientConfig
                      ):
        # validate configuration before accepting it
        client_config.assert_valid_for_services(self.service_names)
        self._client_config = client_config

    # ------ convenience methods

    @property
    def global_cfg(self):
        return self.client_config.global_config

    @property
    def services_cfg_dct(self):
        return self.client_config.services_configs

    # ------ call modes
    @property
    def current_call_mode(self):
        if self._current_call_mode is None:
            raise ValueError("Current call mode is None. Please set a call mode (local, rr, batch...) by using the "
                             "appropriate context manager")
        return self._current_call_mode

    @current_call_mode.setter
    def current_call_mode(self, current_call_mode):
        self._current_call_mode = current_call_mode

    def is_local_mode(self):
        """

        :return:
        """
        return isinstance(self.current_call_mode, LocalCallMode)

    # --- context managers to switch call mode

    def local_calls(self):
        """
        Alias for the `call_mode` context manager to temporarily switch this client to 'local' mode

        >>> with client.local_calls():
        >>>     client.my_service(foo)
        """
        return self.call_mode(LocalCallMode())

    def rr_calls(self):
        """
        Alias for the `call_mode` context manager to temporarily switch this client to 'request response' mode

        >>> with client.rr_calls():
        >>>     client.my_service(foo)
        """
        return self.call_mode(RequestResponse())

    def batch_calls(self, polling_period_seconds=5):
        """
        Alias for the `call_mode` context manager to temporarily switch this client to 'batch' mode

        >>> with client.batch_calls(polling_period_seconds=5):
        >>>     client.my_service(foo)
        """
        return self.call_mode(Batch(polling_period_seconds=polling_period_seconds))

    @contextmanager
    def call_mode(self,
                  mode  # type: CallMode
                  ):
        """
        Context manager to temporarily switch this client to `mode` CallMode

        >>> with client.call_mode(Batch(polling_period_seconds=20)):
        >>>     client.my_service(foo)

        :param mode: the `CallMode` to switch to
        :return:
        """
        previous_mode = self.current_call_mode
        self.current_call_mode = mode
        yield
        self.current_call_mode = previous_mode

    def debug_requests(self):
        """
        Context manager to temporarily enable debug mode on requests.

        :return:
        """
        return debug_requests()

    # ------

    def call_azureml(self,
                     service_id,       # type: Union[str, Callable]
                     ws_inputs,        # type: Dict[str, pd.DataFrame]
                     ws_output_names,  # type: List[str]
                     ws_params=None,   # type: Dict[str, str]
                     ):
        """
        Calls the service identified with id service_id in the services configuration.

        Inputs

        :param service_id: a string identifier or a method representing the service
        :param ws_inputs: a (name, dataframe) dictionary of web service inputs
        :param ws_output_names: a list of web service outputs
        :param ws_params: a (param_name, value) dictionary of web service parameters
        :param by_ref_inputs: a dictionary {<input_name>: <param_name>} containing one entry for each input to send
            "by reference" rather than "by value". Each such input will be removed from the service inputs (the names
            have to be valid input names), its contents will be stored in the blob storage (the same used for batch
            mode), and the blob URL will be passed to a new parameter named <param_name>
        :return:
        """
        # -- one can provide a method as the service id
        if callable(service_id):
            service_id = get_azureml_service_name(service_id)

        # -- Retrieve service configuration
        if service_id not in self.client_config.services_configs.keys():
            raise ValueError('Unknown service_id: \'' + service_id + '\'')
        else:
            service_config = self.client_config.services_configs[service_id]

        # -- Retrieve session and endpoint to use
        session = self.get_requests_session()

        # -- Perform call according to options
        return self.current_call_mode.call_azureml(service_id,
                                                   service_config=service_config, ws_inputs=ws_inputs,
                                                   ws_output_names=ws_output_names, ws_params=ws_params,
                                                   session=session)

    def get_requests_session(self):
        # type: (...) -> Optional[Session]
        """
        Helper to get a `requests` (http client) session object, based on the local configuration. If the client is
        configured for use with a proxy the session will be created accordingly. Note that if this client has
        no particular configuration for the http proxy this function will return None

        :return: a requests session or None
        """
        return self.global_cfg.get_requests_session()


def unpack_single_value_from_df(name,             # type: str
                                df,               # type: pd.DataFrame
                                allow_empty=True  # type: bool
                                ):
    """
    Utility method to unpack a single value from a dataframe.
    If allow_empty is True (default), an empty dataframe will be accepted and None will be returned.

    :param name: the name of the dataframe, for validation purposes
    :param df:
    :param allow_empty:
    :return:
    """
    vals = df.values.ravel()
    if len(vals) == 1:
        return vals[0]
    elif len(vals) == 0 and allow_empty:
        return None
    else:
        raise ValueError("Dataframe '%s' is supposed to contain a single value but does not: \n%s" % (name, df))
