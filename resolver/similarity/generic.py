
from typing import Any, Dict, List, Optional

import pandas as pd

from resolver._base import ColumnarTransform, SimilarityMetric


class ExactMatch(SimilarityMetric):

    def __init__(self, field_name: str, transforms: Optional[List[ColumnarTransform]] = None):
        self.field = field_name
        self.transforms: Optional[List[ColumnarTransform]] = transforms

    @property
    def get_transformed_field_name(self):
        """
        Provides the field name that should be compared using the similarity metric. If transforms
        have been executed against the field, they will have updated the field name.
        """
        if self.transforms:
            return self.transforms[-1].transformed_field_name

        return self.field

    def transform(self, fragments: pd.DataFrame):
        """

        Args:
            fragments:

        Returns:

        """
        col_name = self.field
        for transform in self.transforms:
            col_name, fragments = transform.transform(fragments, col_name)

        return fragments

    def run(self, record: Dict[str, Any], field: str) -> float:
        """
        Compares two values for exact match.

        Args:
            record (Dict[str, Any]): The record containing both fragments
            field (str): The name of the field to be compared. Because blocking will append
                         suffixes to the field

        Returns:
            (float) 1.0 if the values match exactly, 0.0 if not
        """
        val1, val2 = record[f"f{field}_frag1"], record[f"f{field}_frag2"]
        if val1 == val2:
            return 1.0

        return 0.0