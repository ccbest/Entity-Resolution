import abc
from typing import Any, Dict, Generator, Hashable, List, Optional, Tuple

import pandas as pd

from resolver import Entlet


BLOCKER_RETURN = Generator[Tuple[str, str], None, None]


class Blocker(abc.ABC):

    @abc.abstractmethod
    def __init__(self, field: str):
        self.field = field

    @abc.abstractmethod
    def block(self, entlet_df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
        """
        Executes the blocking logic against a DataFrame

        Args:
            entlet_df (pandas.DataFrame): A pandas Dataframe with one column (the entlet objects)

        Returns:
            A generator which yields pairs of entlet ids that have been blocked together
        """
        pass


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
    def transform(self, fragments_df: pd.DataFrame, field: str) -> Tuple[str, pd.DataFrame]:
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


class SimilarityMetric(abc.ABC):
    """Compares two values"""

    @abc.abstractmethod
    def __init__(
            self,
            transforms: Optional[List[ColumnarTransform]] = None,
            **kwargs
    ):
        self.transforms = transforms or []
        self.kwargs = kwargs

    def transform(self, fragments: pd.DataFrame):
        """
        Run all specified transforms in sequence. The transforms will append columns to the dataframe,
        so be sure to obtain the final field name using the .get_transformed_field_name property.

        Args:
            fragments (DataFrame): A pandas dataframe where each record is a fragment

        Returns:
            (DataFrame): The same dataframe, with columns added from each transform
        """
        col_name = self.field
        for transform in self.transforms:
            col_name, fragments = transform.transform(fragments, col_name)

        return fragments

    @abc.abstractmethod
    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Abstract method for a similarity metric's run method. The method must
        accept two fragments and return a float denoting the similarity of the two
        fragments.

        Args:
            entlet1:
            entlet2:

        Returns:

        """
        pass
