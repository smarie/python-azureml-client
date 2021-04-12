try:  # python 3+
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from requests import Session
from valid8 import validate


def parse_proxy_info(proxy_url
                     ):
    """
    Parses a proxy url

    :param proxy_url:
    :return:
    """
    o = urlparse(proxy_url)

    validate('hostname', o.hostname, min_len=1)
    validate('port', o.port)
    validate('scheme', o.scheme, is_in={'http', 'https'},
             help_msg="Only http and https protocols are supported for http(s) proxies. "
                      "Found: '{var_value}' from '%s'" % proxy_url)

    return o.hostname, o.port, o.scheme


def set_http_proxy(session,                                  # type: Session
                   http_scheme='http',                       # type: str
                   http_host=None,                           # type: str
                   http_port=80,                             # type: int
                   http_url=None,                            # type: str
                   use_http_proxy_for_https_requests=False,  # type: bool
                   https_scheme='https',                     # type: str
                   https_host=None,                          # type: str
                   https_port=443,                           # type: int
                   https_url=None,                           # type: str
                   replace=False                             # type: bool
                   ):
    """Update or replace session.proxies with the provided proxy information.

    This method updates or replaces (depending on the value of `replace`) the dictionary in `session.proxies` with the
    provided information. For each kind of connection (http and https), there are two ways to pass the information:
    either as an url string (`http_url`, `https_url`), or split in schema/host/port, with sensible defaults.
    In addition if the exact same proxy information is to be used for http and https, you can pass only the http
    one and set `use_http_proxy_for_https_requests` to True.

    See the requests proxies documentation for details: https://requests.readthedocs.io/en/master/user/advanced/#proxies

    Note: this was proposed as a PR in requests: https://github.com/psf/requests/pull/5670
    but because of the feature freeze it was then transformed into a simple doc update.

    :param http_host: (optional) a string indicating the http proxy host, for example '10.10.1.10' or 'acme.com'.
    :param http_port: (optional) an int indicating the http proxy port, for example `3128`.
    :param http_scheme: (optional) a string indicating the scheme to use for http proxy. By default this is 'http'
        but you can consider using 'socks5', 'socks5h'. See documentation for details.
    :param http_url: (optional) a string indicating the full http proxy url. For example 'http://10.10.1.10:3128'
        or 'http://user:pass@10.10.1.10:3128/' or 'socks5://user:pass@host:port'.
        Only one of {http_scheme + http_host + http_port} or {http_url} should be provided.
    :param use_http_proxy_for_https_requests: (optional) a boolean indicating whether the information provided for
        the http proxy should be copied for the https proxy. Note that the full url will be copied including the
        scheme (so by default 'http').
    :param https_host: (optional) a string indicating the https proxy host, for example '10.10.1.10' or 'acme.com'.
    :param https_port: (optional) an int indicating the https proxy port, for example `3128`.
    :param https_scheme: (optional) a string indicating the scheme to use for https proxy. By default this is
        'https' but you can consider using 'socks5', 'socks5h'. See documentation for details.
    :param https_url: (optional) a string indicating the full https proxy url. For example 'https://10.10.1.10:3128'
        or 'http://user:pass@10.10.1.10:3128/' or 'socks5://user:pass@host:port'.
        Only one of {https_scheme + https_host + https_port} or {https_url} should be provided.
    :param replace: (optional) a boolean indicating if the provided information should replace the existing one
        (True) or just update it (False, default).
    :return:
    """
    proxies = dict()

    # HTTPS
    if http_host is not None:
        # (a) scheme + host + port
        if http_url is not None:
            raise ValueError("Only one of `http_host` and `http_url` should be provided")
        proxies['http'] = "%s://%s:%s" % (http_scheme, http_host, http_port)
    elif http_url is not None:
        # (b) full url
        parse_proxy_info(http_url)
        proxies['http'] = http_url
    elif http_port != 80 or http_scheme != 'http':
        raise ValueError("An `http_host` should be provided if you wish to change `http_port` or `http_scheme`")

    # HTTPS
    if use_http_proxy_for_https_requests:
        # (a) copy the information from http
        if https_host is not None or https_url is not None or https_port != 443 or https_scheme != "https":
            raise ValueError("`use_http_proxy_for_https_requests` was set to `True` but custom information for "
                             "https was provided.")
        try:
            proxies['https'] = proxies['http']
        except KeyError:
            raise ValueError("`use_http_proxy_for_https_requests` was set to `True` but no information was "
                             "provided for the http proxy")
    elif https_host is not None:
        # (b) scheme + host + port
        if https_url is not None:
            raise ValueError("Only one of `https_host` and `https_url` should be provided")
        proxies['https'] = '%s://%s:%s' % (https_scheme, https_host, https_port)
    elif https_url is not None:
        # (c) full url
        proxies['https'] = https_url
    elif https_port != 443 or https_scheme != 'https':
        raise ValueError("An `https_host` should be provided if you wish to change `https_port` or `https_scheme`")

    # Replace or update (default) the configuration
    if len(proxies) > 0:
        if replace:
            session.proxies = proxies
        else:
            session.proxies.update(proxies)

        # IMPORTANT : otherwise the environment variables will always have precedence over user-provided settings
        session.trust_env = False
