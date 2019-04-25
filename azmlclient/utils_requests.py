"""
From https://stackoverflow.com/a/24588289/7262247.
"""
import logging
from contextlib import contextmanager

try:
    from http.client import HTTPConnection # py3
except ImportError:
    from httplib import HTTPConnection # py2


def debug_requests_on():
    """
    Switches on logging of the requests module.
    """
    HTTPConnection.debuglevel = 1

    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    # if print_to_std_out:
    #     ch = logging.StreamHandler(sys.stdout)
    #     requests_log.addHandler(ch)
    # requests_log.propagate = True


def debug_requests_off():
    """
    Switches off logging of the requests module, might be some side-effects
    """
    HTTPConnection.debuglevel = 0

    # root_logger = logging.getLogger()
    # root_logger.setLevel(logging.WARNING)
    # root_logger.handlers = []
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.WARNING)
    # requests_log.propagate = False


@contextmanager
def debug_requests():
    """
    Context manager to temporarily enable debug mode on requests.
    """
    debug_requests_on()
    yield
    debug_requests_off()
