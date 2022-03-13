import abc
from typing import Any, Dict, Hashable, List, Optional, Tuple

from pandas import DataFrame, Series


class ColumnarTransform(abc.ABC):

    @abc.abstractmethod
    def __init__(self, **kwargs):
        self.kwargs: Dict[str, Any] = kwargs
        self.transformed_field_name: Optional[str] = None

    @abc.abstractmethod
    def __hash__(self) -> Hashable:
        pass

    @staticmethod
    @abc.abstractmethod
    def _get_new_col_name(field: str) -> str:
        pass

    @abc.abstractmethod
    def transform(self, fragments_df: DataFrame, field: str) -> Tuple[str, DataFrame]:
        """
        Executes a transform against a given column.

        Example: vectorizing textual data using TF-IDF

        Args:
            fragments_df (pd.DataFrame): A pandas Dataframe where each record is a fragment
            field (str): The name of the field in the dataframe to transform

        Returns:
            (pd.DataFrame) the same dataframe with an added column for the transformed value
        """
        pass


class ScoringReducer(abc.ABC):
    """Compares two values"""

    @abc.abstractmethod
    def __init__(
            self,
            threshold: float,
            **kwargs
    ):
        pass

    @abc.abstractmethod
    def __call__(self, *args) -> float:
        """
        Abstract method for a similarity metric's run method. The method must
        accept two fragments and return a float denoting the similarity of the two
        fragments.

        Args:
            args:

        Returns:

        """
        pass



class StandardizationTransform(abc.ABC):

    @abc.abstractmethod
    def run(self, entlet):
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
