import abc
from typing import Any, Dict

from pandas import DataFrame

from ._munging import Entlet


class ColumnarTransform(abc.ABC):

    def __init__(self, field: str, **kwargs):
        self.field: str
        self.kwargs: Dict[str, Any]

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


class SimilarityMetric(abc.ABC):
    """Compares two values"""

    @abc.abstractmethod
    def __init__(
            self,
            field: str,
            transform: ColumnarTransform = None,
            **kwargs
    ):
        self.field = field
        self.transform = transform
