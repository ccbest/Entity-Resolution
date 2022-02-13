
from typing import List, Optional

import pandas as pd

from .._base import ColumnarTransform, SimilarityMetric


class JaccardSimilarity(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None, **kwargs):
        super().__init__(field_name, transforms, **kwargs)

    @property
    def field_name(self):
        return f"{self.transformed_field_name}_JaccardSimilarity"

    def run(self, record: pd.Series) -> float:
        """
        Computes the Jaccard similarity between 2 sets.

        Args:
            record (Dict[str, Any]): The record containing both fragments

        Returns:
            (float) the Jaccard similarity of the sets
        """
        field = self.transformed_field_name
        val1, val2 = record[f"{field}_frag1"], record[f"{field}_frag2"]
        intersection_size = len((val1).intersection(val2))
        union_size = (len(val1) + len(val2)) - intersection_size
        return float(intersection_size) / union_size
