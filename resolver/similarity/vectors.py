
from typing import Any, List, Optional

from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean

from resolver._base import ColumnarTransform, SimilarityMetric


class CosineSimilarity(SimilarityMetric):
    """
    Measures the similarity of two entlets by applying Cosine Similarity
    against the values of a given field.
    """

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            value1: A list of values (corresponding to an entlet)
            value2: Another list of values (corresponding to a presumably different entlet)

        Returns:
            (float) the cosine similarity of the vectors
        """
        return min(
            cosine_similarity(x, y)[0][0]
            for x in value1
            for y in value2
        )


class EuclideanDistance(SimilarityMetric):
    """
    Measures the similarity of two entlets by applying Euclidean distance
    against the values of a given field.
    """

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the euclidean distance between 2 vectors.

        Args:
            value1: A list of values (corresponding to an entlet)
            value2: Another list of values (corresponding to a presumably different entlet)

        Returns:
            (float) the cosine similarity of the vectors
        """

        return min(
            euclidean(x, y)
            for x in value1
            for y in value2
        )
