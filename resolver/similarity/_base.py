
import abc
from typing import List, Optional

import pandas as pd

from resolver import Entlet
from resolver._base import ColumnarTransform


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
