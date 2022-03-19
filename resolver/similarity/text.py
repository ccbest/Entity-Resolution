
from typing import Any, List, Optional

from resolver._base import ColumnarTransform, SimilarityMetric


class LevenshteinDistance(SimilarityMetric):
    """
    Measures the similarity of two entlets by applying Levenshtein distance
    against the values of a given field.
    """

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the Levenshtein distance between two strings.

        Args:
            value1: A list of values (corresponding to an entlet)
            value2: Another list of values (corresponding to a presumably different entlet)

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
    """
    Measures the similarity of two entlets by applying Levenshtein ratio
    against the values of a given field.
    """

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the Levenshtein ratio of two strings

        Args:
            value1: A list of values (corresponding to an entlet)
            value2: Another list of values (corresponding to a presumably different entlet)

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
    """
    Measures the similarity of two entlets by applying Hamming distance
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
        from Levenshtein import hamming

        return min(
            hamming(x, y)
            for x in value1
            for y in value2
        )


class JaroDistance(SimilarityMetric):
    """
    Measures the similarity of two entlets by applying Jaro distance
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
        from Levenshtein import jaro

        return min(
            jaro(x, y)
            for x in value1
            for y in value2
        )


class JaroWinklerDistance(SimilarityMetric):
    """
    Measures the similarity of two entlets byt applying Jaro-Winkler distance
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
        from Levenshtein import jaro_winkler

        return min(
            jaro_winkler(x, y)
            for x in value1
            for y in value2
        )
