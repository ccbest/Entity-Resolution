
import abc

from pandas import DataFrame


class ResolutionBlocker(abc.ABC):

    field: str

    @abc.abstractmethod
    def block(self, df: DataFrame(columns=['fragment'])) -> DataFrame(columns=['entlet1', 'entlet2']):
        """
        Executes the blocking logic against a DataFrame

        Args:
            df (pandas.DataFrame): A pandas dataframe containing one column 'entlet'

        Returns:
            (pandas.DataFrame) A pandas dataframe containing two columns 'entlet1', 'entlet2'
        """

from .text import *
from .generic import *
