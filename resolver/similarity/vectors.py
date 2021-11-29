
from typing import Any, Dict, List, Optional

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean

from .._base import ColumnarTransform, SimilarityMetric


class CosineSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None):
        self.field = field_name
        self.transforms: Optional[List[ColumnarTransform]] = transforms

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_CosineSimilarity"

    @property
    def transformed_field_name(self):
        """
        Provides the field name that should be compared using the similarity metric. If transforms
        have been executed against the field, they will have updated the field name.
        """
        if self.transforms:
            return self.transforms[-1].transformed_field_name

        return self.field

    def transform(self, fragments: pd.DataFrame):
        """

        Args:
            fragments:

        Returns:

        """
        col_name = self.field
        for transform in self.transforms:
            col_name, fragments = transform.transform(fragments, col_name)

        return fragments

    def run(self, record: pd.Series) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return cosine_similarity(val1, val2)[0][0]


class EuclideanDistance(SimilarityMetric):

    def __init__(self, field: str, transform: ColumnarTransform = None, **kwargs):
        self.field = field
        self.transform = transform

    def run(self, fragment1, fragment2):
        """
        Computes the euclidean distance between 2 vectors.

        Args:
            fragment1 (Fragment): a fragment
            value2 (Fragment): another fragment

        Returns:
            ( record_1 , record_2, ("{column_name}_euclidean_distance", score)
        """
        return euclidean(value1, value2)

