import sys
from collections import OrderedDict
from distutils.util import strtobool
from warnings import warn

from jinja2 import Environment, StrictUndefined

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

from .requests_utils import set_http_proxy


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

    def configure_session(self, session):
        """
        Helper to get a `requests` (http client) session object, based on the local configuration.

        If the client is configured for use with a proxy the session will be created accordingly.
        Note that if this client has no particular configuration for the http proxy this function will return None.

        :param session:
        :return:
        """
        use_http_for_https = self.http_proxy and not self.https_proxy
        set_http_proxy(session, http_url=self.http_proxy, https_url=self.https_proxy,
                       use_http_proxy_for_https_requests=use_http_for_https)

        if self.ssl_verify is not None:
            try:
                # try to parse a boolean
                session.verify = bool(strtobool(self.ssl_verify))
            except:
                # otherwise this is a path
                session.verify = self.ssl_verify


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

    @classmethod
    def load_yaml(cls,                  # type: Type[Y]
                  file_path_or_stream,  # type: Union[str, IOBase, StringIO]
                  safe=True,            # type:
                  **var_values  # type: Any
                  ):  # type: (...) -> Y
        """ applies the template before loading """
        contents = read_file_and_apply_template(file_path_or_stream, **var_values)
        return YamlAble.loads_yaml(contents, safe=safe)

    @classmethod
    def loads_yaml(cls,          # type: Type[Y]
                   yaml_str,     # type: str
                   safe=True,    # type: bool
                   **var_values  # type: Any
                   ):  # type: (...) -> Y
        """ applies the template before loading """
        contents = apply_template(yaml_str, **var_values)
        return YamlAble.loads_yaml(contents, safe=safe)

    # ---- configparser interface ----

    @staticmethod
    def load_config(cfg_file_path,  # type: str
                    **var_values    # type: Any
                    ):
        # type: (...) -> ClientConfig
        """
        Utility method to create a `ClientConfig` from a configuration file (.ini or .cfg, see `ConfigParser`).
        That configuration file should have a 'global' section, and one section per service named with the service name.

        :param cfg_file_path: the path to the config file in `ConfigParser` supported format
        :param var_values: variables to replace in the configuration file. For example `api_key="abcd"` will inject
            `"abcd"` everywhere where `{{api_key}}` will be found in the file.
        :return:
        """
        # read and apply template
        contents = read_file_and_apply_template(cfg_file_path, **var_values)

        # load the config
        config = ConfigParser()
        config.read_string(contents, source=cfg_file_path)

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


def read_file_and_apply_template(file_path_or_stream,  # type: Union[str, IOBase, StringIO]
                                 **var_values
                                 ):
    # type: (...) -> str
    """
    
    :param file_path_or_stream:
    :param var_values: 
    :return: 
    """
    # first read the file or stream
    if isinstance(file_path_or_stream, str):
        with open(file_path_or_stream, mode='rt') as f:
            contents = f.read()
    else:
        with file_path_or_stream as f:
            contents = f.read()

    return apply_template(contents, **var_values)


# the jinja2 environment that will be used
env = Environment(undefined=StrictUndefined)


class ConfigTemplateSyntaxError(Exception):
    def __init__(self, contents, idx, original_path):
        self.extract = contents[idx-30:idx+32]
        self.original_path = original_path

    def __str__(self):
        if self.original_path is not None:
            tmpstr = "File: %s. " % self.original_path
        else:
            tmpstr = ""
        return "Syntax error in template: a double curly brace remains after template processing. %s" \
               "Extract: %s" % (tmpstr, self.extract)


def apply_template(contents,            # type: str
                   original_path=None,  # type: str
                   **var_values
                   ):
    # type: (...) -> str

    # apply the template using Jinja2
    template = env.from_string(contents)
    contents = template.render(**var_values)

    # this check can not be done by Jinja2, do it ourselves
    if '{{' in contents or '}}' in contents:
        try:
            idx = contents.index("{{")
        except ValueError:
            idx = contents.index("}}")
        raise ConfigTemplateSyntaxError(contents, idx, original_path)

    return contents
