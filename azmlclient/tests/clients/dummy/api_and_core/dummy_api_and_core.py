import sys
from abc import ABC, abstractmethod
from logging import getLogger, StreamHandler, INFO

import pandas as pd


class DummyProvider(ABC):
    """
    The API of a dummy component
    """
    @abstractmethod
    def add_columns(self, a_name, b_name, df):
        pass

    @abstractmethod
    def subtract_columns(self, a_name, b_name, df):
        pass


# the logger to use in our component client
default_logger = getLogger('dummy impl')
ch = StreamHandler(sys.stdout)
default_logger.addHandler(ch)
default_logger.setLevel(INFO)


class DummyImpl(DummyProvider):
    """
    A dummy implementation of DummyProvider
    """
    def __init__(self, logger=default_logger, with_plots=False):
        self.logger = logger
        self.with_plots = with_plots

    def add_columns(self, a_name, b_name, df):
        self.logger.info("adding columns")
        return pd.DataFrame({'sum': df[a_name] + df[b_name]})

    def subtract_columns(self, a_name, b_name, df):
        self.logger.info("adding columns")
        return pd.DataFrame({'diff': df[a_name] - df[b_name]})
