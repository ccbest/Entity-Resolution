import abc
from typing import Any, Dict, Hashable, Optional, Tuple

from pandas import DataFrame, Series


class Blocker(abc.ABC):

    @abc.abstractmethod
    def __init__(self, field: str):
        self.field = field

    @abc.abstractmethod
    def block(self, fragments_df: DataFrame) -> DataFrame:
        """
        Executes the blocking logic against a DataFrame

        Args:
            fragments_df (pandas.DataFrame): A pandas Dataframe where each record is a fragment

        Returns:
            (pandas.DataFrame) A dataframe whose records have been 'blocked'. Records from the 'left'
            fragment will be suffixed by '_frag1', and records from the 'right' fragment will be
            suffixed with '_frag2'.
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
            field: str,
            transform: ColumnarTransform = None,
            **kwargs
    ):
        pass

    @abc.abstractmethod
    def score(self, row: Series) -> float:
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


class SimilarityMetric(abc.ABC):
    """Compares two values"""

    @abc.abstractmethod
    def __init__(
            self,
            field: str,
            transforms: ColumnarTransform = None,
            **kwargs
    ):
        self.field = field
        self.transforms = transforms
        self.kwargs = kwargs

    @property
    @abc.abstractmethod
    def field_name(self):
        """
        Provides the field name that should be compared using the similarity metric. If transforms
        have been executed against the field, they will have updated the field name.
        """
        pass

    @property
    @abc.abstractmethod
    def transformed_field_name(self):
        """
        Provides the field name that should be compared using the similarity metric. If transforms
        have been executed against the field, they will have updated the field name.
        """
        pass

    @abc.abstractmethod
    def transform(self, fragments: DataFrame):
        """
        Run all specified transforms in sequence. The transforms will append columns to the dataframe,
        so be sure to obtain the final field name using the .get_transformed_field_name property.

        Args:
            fragments (DataFrame): A pandas dataframe where each record is a fragment

        Returns:
            (DataFrame): The same dataframe, with columns added from each transform
        """
        pass

    @abc.abstractmethod
    def run(self, blocked_fragments: Series) -> float:
        """
        Abstract method for a similarity metric's run method. The method must
        accept two fragments and return a float denoting the similarity of the two
        fragments.

        Args:
            blocked_fragments:

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
