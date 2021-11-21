import abc
from typing import Any, Dict

from pandas import DataFrame

from ._munging import Entlet


class Blocker(abc.ABC):

    @abc.abstractmethod
    def __init__(self, field: str):
        self.field = field

    @abc.abstractmethod
    def block(self, df: DataFrame(columns=['fragment'])) -> DataFrame(columns=['entlet1', 'entlet2']):
        """
        Executes the blocking logic against a DataFrame

        Args:
            df (pandas.DataFrame): A pandas dataframe containing one column 'entlet'

        Returns:
            (pandas.DataFrame) A pandas dataframe containing two columns 'entlet1', 'entlet2'
        """
        pass


class ColumnarTransform(abc.ABC):

    @abc.abstractmethod
    def __init__(self, field: str, **kwargs):
        self.field = field
        self.kwargs: Dict[str, Any] = kwargs

    @abc.abstractmethod
    def __hash__(self):
        pass

    @abc.abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes a transform against a given column.

        Example: vectorizing textual data using TF-IDF

        Args:
            df (pandas.DataFrame): A pandas dataframe of fragments, containing at
                                   minimum a column 'entlet_id' and the column specified
                                   by the 'field' argument upon init

        Returns:
            (pandas.DataFrame) A pandas dataframe
        """
        pass


class StandardizationTransform(abc.ABC):

    @abc.abstractmethod
    def run(self, entlet: Entlet) -> Entlet:
        """
        Applies standardization logic against a given entlet.

        Args:
            entlet (Entlet): an instance of an Entlet. See utils.entlet for more information.

        Returns:

        """
        pass

    @abc.abstractmethod
    def standardize(self, value: Any) -> Any:
        """
        The method for actually applying standardization logic against a given value.
        Should return the standardized value.

        Args:
            value (Any): The value to be standardized.

        Returns:
            (Any) The standardized value
        """
        pass


class SimilarityMetric(abc.ABC):
    """Compares two values"""

    @abc.abstractmethod
    def __init__(
            self,
            field: str,
            transform: ColumnarTransform = None,
            **kwargs
    ):
        pass

    @abc.abstractmethod
    def run(self, fragment1: dict, fragment2: dict) -> float:
        """
        Abstract method for a similarity metric's run method. The method must
        accept two fragments and return a float denoting the similarity of the two
        fragments.

        Args:
            fragment1:
            fragment2:

        Returns:

        """
        pass