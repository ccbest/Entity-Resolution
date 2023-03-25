import abc
from collections import defaultdict
from typing import Any, Dict, Generator, Hashable, List, Optional, Tuple

import pandas as pd

from entity_resolution import Entlet


BLOCKER_RETURN = Generator[Tuple[str, str], None, None]


class Blocker(abc.ABC):

    field: str

    @abc.abstractmethod
    def block(self, entlet_df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
        """
        Primary execution method for a Blocker object. Takes a series of entlets, executes
        some logic (depending on the blocker used), and returns pairs of entlet ids that were
        blocked together.

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
        self.wrapped_transform: Optional[ColumnarTransform] = None

    @abc.abstractmethod
    def transform(self, values_df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes a transform against a given column.

        Example: vectorizing textual data using TF-IDF

        Args:
            values_df (pd.DataFrame): A pandas Dataframe where each record is a fragment

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


class SimilarityMetric(abc.ABC):
    """Compares two values"""

    field: str

    @abc.abstractmethod
    def __init__(
            self,
            transform: ColumnarTransform = None,
            **kwargs
    ):
        self.applied_transform = transform or None
        self.transformed_values = None
        self.kwargs = kwargs

    def transform(self, entlet_df: pd.DataFrame) -> None:
        """
        Run all specified transforms in sequence. The transforms will append columns to the dataframe,
        so be sure to obtain the final field name using the .get_transformed_field_name property.

        Args:
            entlet_df (DataFrame): A dataframe of entlets

        Returns:
            (DataFrame): The same dataframe, with columns added from each transform
        """
        def _dataframe_to_dict(df):
            # TODO: temporary solution
            out = defaultdict(list)
            for record in zip(df.entlet_id, df.value):
                out[record[0]].append(record[1])
            return out

        value_df = pd.DataFrame(
            list(entlet_df.apply(
                lambda x: {
                    'entlet_id': x['entlet'].entlet_id,
                    'value': x['entlet'].get(self.field, [])
                },
                axis=1
            )),
            columns=['entlet_id', 'value']
        )

        value_df = value_df.explode('value')

        if not self.applied_transform:
            self.transformed_values = _dataframe_to_dict(value_df)
            return

        self.transformed_values = _dataframe_to_dict(self.applied_transform.transform(value_df))

    def score(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Wrapper around the .run() method to make sure the right values get passed to
        the actual scoring method (accomodates transforms).

        This method only scores the similarity of two entlets across a single field,
        not the overall similarity as defined by the Strategy.

        Args:
            entlet1: An entlet object
            entlet2: Another (presumably different) entlet object

        Returns:
            (float) the resulting similarity score
        """
        return self.run(
            self.transformed_values.get(entlet1.entlet_id, []),
            self.transformed_values.get(entlet2.entlet_id, [])
        )

    @abc.abstractmethod
    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Executes the actual similarity scoring against two values. Virtually all
        values on an entlet

        Args:
            value1:
            value2:

        Returns:
            (float) the measured similarity
        """
        pass
