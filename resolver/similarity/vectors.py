
from typing import Any, Dict, List, Optional

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean

from .._base import ColumnarTransform, SimilarityMetric


class CosineSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_CosineSimilarity"

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

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_EuclideanDistance"

    def run(self, record: pd.Series) -> float:
        """
        Computes the euclidean distance between 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return euclidean(val1, val2)


