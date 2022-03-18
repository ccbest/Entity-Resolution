
from typing import Any, List, Optional

from resolver._base import ColumnarTransform, SimilarityMetric


class JaccardSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Computes the Jaccard similarity between 2 sets.

        Args:
            entlet1:
            entlet2:

        Returns:
            (float) the Jaccard similarity of the sets
        """
        val1, val2 = set(value1), set(value2)
        intersection_size = len(val1.intersection(val2))
        union_size = (len(val1) + len(val2)) - intersection_size
        return float(intersection_size) / union_size
