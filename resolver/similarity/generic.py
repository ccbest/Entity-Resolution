
from typing import Dict, List, Optional

import pandas as pd

from resolver._base import ColumnarTransform, SimilarityMetric


class ExactMatch(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_ExactMatch"

    def run(self, record: pd.Series) -> float:
        """
        Compares two values for exact match.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) 1.0 if the values match exactly, 0.0 if not
        """
        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        if val1 == val2:
            return 1.0

        return 0.0
