
from typing import List, Optional

from resolver import Entlet
from resolver._base import ColumnarTransform
from resolver.similarity._base import SimilarityMetric


class LevenshteinDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            entlet1
            entlet2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import distance

        return min(
            distance(x, y)
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )


class LevenshteinRatio(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the Levenshtein ratio of two strings

        Args:
            entlet1
            entlet2

        Returns:
            (float)
        """
        from Levenshtein import ratio

        return max(
            ratio(x, y)
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )


class HammingDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            entlet1
            entlet2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import hamming

        return min(
            hamming(x, y)
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )


class JaroDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            entlet1
            entlet2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import jaro

        return min(
            jaro(x, y)
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )


class JaroWinklerDistance(SimilarityMetric):

    """
    Compute Jaro string similarity metric of two strings. The Jaro-Winkler string
    similarity metric is a modification of Jaro metric giving more weight to common prefix,
    as spelling mistakes are more likely to occur near ends of words.

    Additional information available at: https://maxbachmann.github.io/Levenshtein/levenshtein.html
    """

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            entlet1
            entlet2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import jaro_winkler

        return min(
            jaro_winkler(x, y)
            for x in entlet1.get(self.field, [])
            for y in entlet2.get(self.field, [])
        )

