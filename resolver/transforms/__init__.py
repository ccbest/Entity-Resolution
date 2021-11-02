
import abc
from typing import Any, Dict

from pandas import DataFrame


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

