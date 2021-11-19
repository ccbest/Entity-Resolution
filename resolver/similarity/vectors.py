
import numpy as np
from scipy.spatial.distance import euclidean

from .._base import SimilarityMetric
from .. import ColumnarTransform


class CosineSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transform = None):
        self.field = field_name
        self.transform = transform

    def apply(self, value1, value2) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            value1: a vector
            value2: another vector

        Returns:
            (float) the cosine similarity of the vectors
        """
        xnorm = value1.norm(2)
        ynorm = value2.norm(2)
        if xnorm and ynorm:
            return value1.dot(value2) / float(xnorm * ynorm) or 0.0

        return 0


class EuclideanDistance(SimilarityMetric):

    def __init__(self, field: str, transform: ColumnarTransform = None, **kwargs):
        self.field = field
        self.transform = transform

    def apply(self, value1, value2):
        """
        Computes the euclidean distance between 2 vectors.

        Args:
            value1 (list): a list consisting of 2 Rows
            value2 (str): the name of the column containing the vector for each record

        Returns:
            ( record_1 , record_2, ("{column_name}_euclidean_distance", score)
        """
        return euclidean(value1, value2)

