
from typing import Dict, List, Optional

import pandas as pd

from resolver._base import ColumnarTransform, SimilarityMetric


class LevenshteinDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_LevenshteinDistance"

    def run(self, record: pd.Series) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import distance

        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return distance(val1, val2)


class LevenshteinRatio(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_LevenshteinRatio"

    def run(self, record: pd.Series) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import ratio

        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return ratio(val1, val2)


class HammingDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_HammingDistance"

    def run(self, record: pd.Series) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import hamming

        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return hamming(val1, val2)


class JaroDistance(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_JaroDistance"

    def run(self, record: pd.Series) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import jaro

        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return jaro(val1, val2)


class JaroWinklerDistance(SimilarityMetric):

    """
    Compute Jaro string similarity metric of two strings. The Jaro-Winkler string
    similarity metric is a modification of Jaro metric giving more weight to common prefix,
    as spelling mistakes are more likely to occur near ends of words.

    Additional information available at: https://maxbachmann.github.io/Levenshtein/levenshtein.html
    """

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_JaroWinklerDistance"

    def run(self, record: pd.Series) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the cosine similarity of the vectors
        """
        from Levenshtein import jaro_winkler

        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        return jaro_winkler(val1, val2)

