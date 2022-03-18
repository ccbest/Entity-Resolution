
from typing import Any, List, Optional

from resolver._base import ColumnarTransform, SimilarityMetric


class LevenshteinDistance(SimilarityMetric):

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            value1
            value2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import distance

        return min(
            distance(x, y)
            for x in value1
            for y in value2
        )


class LevenshteinRatio(SimilarityMetric):

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the Levenshtein ratio of two strings

        Args:
            value1
            value2

        Returns:
            (float)
        """
        from Levenshtein import ratio

        return max(
            ratio(x, y)
            for x in value1
            for y in value2
        )


class HammingDistance(SimilarityMetric):

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            value1
            value2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import hamming

        return min(
            hamming(x, y)
            for x in value1
            for y in value2
        )


class JaroDistance(SimilarityMetric):

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            value1
            value2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import jaro

        return min(
            jaro(x, y)
            for x in value1
            for y in value2
        )


class JaroWinklerDistance(SimilarityMetric):

    """
    Compute Jaro string similarity metric of two strings. The Jaro-Winkler string
    similarity metric is a modification of Jaro metric giving more weight to common prefix,
    as spelling mistakes are more likely to occur near ends of words.

    Additional information available at: https://maxbachmann.github.io/Levenshtein/levenshtein.html
    """

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            value1
            value2

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import jaro_winkler

        return min(
            jaro_winkler(x, y)
            for x in value1
            for y in value2
        )

