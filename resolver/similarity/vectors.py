
from typing import Any, Dict, List, Optional

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean

from resolver._base import ColumnarTransform
from resolver.similarity._base import SimilarityMetric
from resolver import Entlet


class CosineSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        return min(
            cosine_similarity(x, y)[0][0]
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )


class EuclideanDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the euclidean distance between 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """

        return min(
            euclidean(x, y)
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )
