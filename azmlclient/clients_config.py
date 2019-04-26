import sys
from collections import OrderedDict
from warnings import warn
from requests import Session

try:  # python 3+
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

try:  # python 3.5+
    from typing import Dict, List, Callable, Union, Iterable, Optional, Any
    from logging import Logger
except ImportError:
    pass

from autoclass import autodict
from yamlable import YamlAble, yaml_info


from azmlclient.base import create_session_for_proxy_from_strings


PY2 = sys.version_info < (3, 0)


YAML_NS = 'org.pypi.azmlclient'
"""The namespace used for yaml conversion"""


@autodict
class GlobalConfig:
    """
    Represents a global component client configuration, that is, configuration that is transverse to all web services.
    """
    def __init__(self,
                 http_proxy=None,   # type: str
                 https_proxy=None,  # type: str
                 ssl_verify=True,   # type: Union[str, bool]
                 ):
        """
        Global information used for all the calls.

        :param http_proxy: an optional string representing the http proxy to use. For example "http://localhost:8888"
        :param https_proxy: an optional string representing the https proxy to use. For example "http://localhost:8888"
        :param ssl_verify: a boolean or string representing a boolean, indicating if we should validate the SSL
            certificate. This is True by default (highly recommended in production)
        """
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy
        self.ssl_verify = ssl_verify

    def get_requests_session(self):
        # type: (...) -> Optional[Session]
        """
        Helper to get a `requests` (http client) session object, based on the local configuration.

        If the client is configured for use with a proxy the session will be created accordingly.
        Note that if this client has no particular configuration for the http proxy this function will return None.

        :return: a requests session or None
        """
        return create_session_for_proxy_from_strings(http_proxy=self.http_proxy, https_proxy=self.https_proxy,
                                                     ssl_verify=self.ssl_verify)


@autodict
class ServiceConfig:
    """
    Represents the configuration to use to interact with an azureml service.

    A service has a main endpoint defined by a base url and an api key.
    An optional blob storage configuration can be specified to interact with the service in batch mode (BES)

    Finally, an optional alternate endpoint can be specified for "some inputs by reference" calls. In that case the
    endpoint should correspond to a service able to understand the input references and to retrieve them (this is not a
    standard AzureML mechanism).
    """
    def __init__(self,
                 base_url,              # type: str
                 api_key,               # type: str
                 by_ref_base_url=None,  # type: str
                 by_ref_api_key=None,   # type: str
                 blob_account=None,     # type: str
                 blob_api_key=None,     # type: str
                 blob_container=None,   # type: str
                 blob_path_prefix=None  # type: str
                 ):
        """
        Constructor with

         * an url and api key for normal (anonymized request-response + batch),
         * an optional url and api key for non-anonymized request-response with input by reference,
         * an optional account, api key and container name for batch

        :param base_url:
        :param api_key:
        :param by_ref_base_url: an alternate URL to use in 'input by reference' mode. If not provided, the base URL
            will be used.
        :param by_ref_api_key: an alternate api key to use in 'input by reference' mode. If not provided, the base
            api key will be used.
        :param blob_account: an optional blob account that should be used in batch mode. A non-None value has
            to be provided
        :param blob_api_key:
        :param blob_container:
        :param blob_path_prefix: an optional prefix path for the blobs to be stored
        """
        self.base_url = base_url
        self.api_key = api_key

        self._by_ref_base_url = by_ref_base_url
        self._by_ref_api_key = by_ref_api_key

        self.blob_account = blob_account
        self.blob_api_key = blob_api_key
        self.blob_container = blob_container
        self.blob_path_prefix = blob_path_prefix

    @property
    def by_ref_base_url(self):
        if self._by_ref_base_url is None:
            return self.base_url
        else:
            return self._by_ref_base_url

    @property
    def by_ref_api_key(self):
        if self._by_ref_api_key is None:
            return self.api_key
        else:
            return self._by_ref_api_key


@yaml_info(yaml_tag_ns=YAML_NS)
@autodict
class ClientConfig(YamlAble):
    """
    An AzureML client configuration. It is made of two parts:

     * A 'global' configuration (a `GlobalConfig`)
     * services configurations (one `ServiceConfig` for each. Each is registered under a name that will be used
     to bind the configuration with the appropriate method in `AzureMLClient`. See `@azureml_service` for details.
    """
    def __init__(self,
                 global_config=None,    # type: GlobalConfig
                 **services_configs     # type: ServiceConfig
                 ):
        """

        :param global_config: the global configuration, a GlobalConfig
        :param services_configs: a dictionary of {service_name: ServiceConfig}
        """
        if global_config is None:
            global_config = GlobalConfig()
        self.global_config = global_config

        self.services_configs = services_configs

    def assert_valid_for_services(self,
                                  service_names  # type: Iterable[str]
                                  ):
        """
        Asserts that the configuration corresponds to the list of services provided
        :param service_names:
        :return:
        """
        unknown_services = set(self.services_configs.keys()) - set(service_names)
        if len(unknown_services) > 0:
            raise ValueError("Configuration is not able to handle services: '" + str(unknown_services)
                             + "'. The list of services supported by this client is '" + str(service_names)
                             + "'")

    # ---- yamlable interface ----

    def __to_yaml_dict__(self):
        # type: (...) -> Dict[str, Any]
        """ This optional method is called when you call yaml.dump(). See `yamlable` for details."""
        # notes:
        # - we do not make `GlobalConfig` and `ServiceConfig` yamlable objects because their custom yaml names would
        # have to appear in the configuration, which seems tideous
        # - we use `dict()` not `var()` so that we benefit from their `@autodict` capability to hide private fields
        return {'global': dict(self.global_config),
                'services': {service_name: dict(service) for service_name, service in self.services_configs.items()}}

    @classmethod
    def __from_yaml_dict__(cls, dct, yaml_tag):
        # type: (...) -> ClientConfig
        """ This optional method is called when you call yaml.load(). See `yamlable` for details."""
        global_cfg = GlobalConfig(**dct['global'])
        services_cfg = {service_name: ServiceConfig(**service_cfg_dct)
                        for service_name, service_cfg_dct in dct['services'].items()}
        return ClientConfig(global_cfg, **services_cfg)

    # ---- configparser interface ----

    @staticmethod
    def load_config(cfg_file_path  # type: str
                    ):
        # type: (...) -> ClientConfig
        """
        Utility method to create a `ClientConfig` from a configuration file (.ini or .cfg, see `ConfigParser`).
        That configuration file should have a 'global' section, and one section per service named with the service name.

        :param cfg_file_path: the path to the config file in `ConfigParser` supported format
        :return:
        """
        config = ConfigParser()
        config.read(cfg_file_path)

        global_cfg = GlobalConfig()
        services_cfgs = dict()

        if PY2:
            _config = config
            config = OrderedDict()
            for section_name in _config.sections():
                config[section_name] = OrderedDict(_config.items(section_name))
            config['DEFAULT'] = _config.defaults()

        for section_name, section_contents in config.items():
            if section_name == 'global':
                global_cfg = GlobalConfig(**section_contents)
            elif section_name == 'DEFAULT':
                if len(section_contents) > 0:
                    warn('Configuration contains a DEFAULT section, that will be ignored')
            else:
                services_cfgs[section_name] = ServiceConfig(**section_contents)

        return ClientConfig(global_cfg, **services_cfgs)
