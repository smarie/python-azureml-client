from distutils.util import strtobool
from warnings import warn

from requests import Session

from azmlclient.base import create_session_for_proxy_from_strings

try:  # python 3+
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

try:  # python 3.5+
    from typing import Dict, List, Callable, Union, Iterable, Optional
    from logging import Logger
except ImportError:
    pass

from autoclass import autodict
from yamlable import YamlAble, yaml_info


YAML_NS = 'org.python.azmlclient'


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
        The only information that is transverse to all services as of today is

         - whether the AzureML web services are in the "new" format or not
         - whether a Fiddler local proxy should be used (for debugging)

        :param use_fiddler_proxy: set to True to use a localhost:8888 proxy with *disabled* SSL cert validation
        """
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy
        self.ssl_verify = ssl_verify

    # @staticmethod
    # def create_from_dict(config: Dict[str, str]):
    #     return GlobalConfig(**config)

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
class ServiceEndpointsConfig:
    """
    Represents the configuration to use to interact with an endpoint, that is, a single web service in a component.
    """
    def __init__(self,
                 base_url: str,
                 api_key: str,
                 rr_by_ref_base_url: str = None,
                 rr_by_ref_api_key: str = None,
                 blob_account_for_batch: str = None,
                 blob_apikey_for_batch: str = None,
                 blob_containername_for_batch: str = None):
        """
        Constructor with

         * an url and api key for normal (anonymized request-response + batch),
         * an optional url and api key for non-anonymized request-response with input by reference,
         * an optional account, api key and container name for batch

        :param base_url:
        :param api_key:
        :param rr_by_ref_base_url: an alternate URL to use in 'input by reference' mode. If not provided, the base URL
            will be used.
        :param rr_by_ref_api_key: an alternate api key to use in 'input by reference' mode. If not provided, the base
            api key will be used.
        :param blob_account_for_batch: an optional blob account that should be used in batch mode. A non-None value has
            to be provided
        :param blob_apikey_for_batch:
        :param blob_containername_for_batch:
        """
        self.base_url = base_url
        self.api_key = api_key

        self._rr_by_ref_base_url = rr_by_ref_base_url
        self._rr_by_ref_api_key = rr_by_ref_api_key

        self.blob_account_for_batch = blob_account_for_batch
        self.blob_apikey_for_batch = blob_apikey_for_batch
        self.blob_containername_for_batch = blob_containername_for_batch

    @property
    def rr_by_ref_base_url(self):
        if self._rr_by_ref_base_url is None:
            return self.base_url
        else:
            return self._rr_by_ref_base_url

    @property
    def rr_by_ref_api_key(self):
        if self._rr_by_ref_api_key is None:
            return self.api_key
        else:
            return self._rr_by_ref_api_key

    # @staticmethod
    # def load_services_dct(services_dct: Dict[str, Dict[str, str]]):
    #     """
    #     Utility method to load a dictionary of ServiceEndpointsConfig in one call when each is a dict
    #     :param services_dct:
    #     :return:
    #     """
    #     return {service_name: ServiceEndpointsConfig(**service_cfg_dct)
    #             for service_name, service_cfg_dct in services_dct.items()}


@yaml_info(yaml_tag_ns=YAML_NS)
@autodict
class ClientConfig(YamlAble):
    """
    An AzureML client configuration. It is made of two parts:

     * A 'global' configuration (a `GlobalConfig`)
     * services configurations (one `ServiceEndpointsConfig` for each)
    """
    def __init__(self,
                 global_config: GlobalConfig,
                 services_configs: Dict[str, ServiceEndpointsConfig]):
        """

        :param global_config: the global configuration, a GlobalConfig
        :param services_configs: a dictionary of {service_name: ServiceEndpointsConfig}
        """
        self.global_config = global_config
        self.services_configs = services_configs

    def assert_valid_for_services(self, service_names: Iterable[str]):
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
        """ This optional method is called when you call yaml.dump()"""
        return {'global': vars(self.global_config),
                'services': {service_name: vars(service) for service_name, service in self.services_configs.items()}}

    @classmethod
    def __from_yaml_dict__(cls, dct, yaml_tag):
        """ This optional method is called when you call yaml.load()"""
        global_cfg = GlobalConfig(**dct['global'])
        services_cfg = {service_name: ServiceEndpointsConfig(**service_cfg_dct)
                        for service_name, service_cfg_dct in dct['services'].items()}
        return ClientConfig(global_cfg, services_cfg)

    # ---- configparser interface ----

    @staticmethod
    def load_config(cfg_file_path: str):
        """
        Utility method to configure a client from a configuration file (.ini or .cfg, see `ConfigParser`).
        That configuration file should have a 'global' section, and one section per service named with the service name.

        :param cfg_file_path: the path to the config file in `ConfigParser` supported format
        :return:
        """
        config = ConfigParser()
        config.read(cfg_file_path)

        global_cfg = GlobalConfig()
        services_cfgs = dict()

        for section_name, section_contents in config.items():
            if section_name == 'global':
                global_cfg = GlobalConfig(**section_contents)
            elif section_name == 'DEFAULT':
                if len(section_contents) > 0:
                    warn('Configuration contains a DEFAULT section, that will be ignored')
            else:
                services_cfgs[section_name] = ServiceEndpointsConfig(**section_contents)

        return ClientConfig(global_cfg, services_cfgs)
