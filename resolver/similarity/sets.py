
from typing import List, Optional

from resolver import Entlet
from resolver._base import ColumnarTransform
from resolver.similarity._base import SimilarityMetric


class JaccardSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Computes the Jaccard similarity between 2 sets.

        Args:
            entlet1:
            entlet2:

        Returns:
            (float) the Jaccard similarity of the sets
        """
        val1, val2 = set(entlet1.get(self.field, {})), set(entlet2.get(self.field, {}))
        intersection_size = len(val1.intersection(val2))
        union_size = (len(val1) + len(val2)) - intersection_size
        return float(intersection_size) / union_size
