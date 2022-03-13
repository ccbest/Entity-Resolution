
from typing import List, Optional

from resolver import Entlet
from resolver._base import ColumnarTransform, SimilarityMetric


class ExactMatch(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(transforms, **kwargs)
        self.field = field_name

    def run(self, entlet1: Entlet, entlet2: Entlet) -> float:
        """
        Compares two values for exact match.

        Args:
            entlet1:
            entlet2:

        Returns:
            (float) 1.0 if the values match exactly, 0.0 if not
        """
        if any(x == y for x in entlet1.get(self.field, []) for y in entlet2.get(self.field, [])):
            return 1.0

        return 0.0
