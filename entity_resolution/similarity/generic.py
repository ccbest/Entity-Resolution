
from typing import Any, List, Optional

from entity_resolution._base import ColumnarTransform, SimilarityMetric


class ExactMatch(SimilarityMetric):

    def __init__(self, field_name: str, transform: Optional[ColumnarTransform] = None, **kwargs):
        super().__init__(transform, **kwargs)
        self.field = field_name

    def run(self, value1: List[Any], value2: List[Any]) -> float:
        """
        Compares two values for exact match.

        Args:
            value1: A list of values (corresponding to an entlet)
            value2: Another list of values (corresponding to a presumably different entlet)

        Returns:
            (float) 1.0 if the values match exactly, 0.0 if not
        """

        if any(x == y for x in value1 for y in value2):
            return 1.0

        return 0.0
