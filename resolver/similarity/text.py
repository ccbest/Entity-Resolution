
import Levenshtein

from .._base import SimilarityMetric


class LevenshteinDistance(SimilarityMetric):

    def __init__(self):
        pass

    def run(self, fragment1: Fragment, fragment2: Fragment) -> float:
        Levenshtein.distance()
