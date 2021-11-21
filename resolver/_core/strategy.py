
from collections import defaultdict
from typing import List, Optional

import pandas as pd

from resolver._base import Blocker, ColumnarTransform, ScoringReducer, SimilarityMetric


class Strategy:

    def __init__(
        self,
        block_by: Blocker,
        computed_metrics: List[SimilarityMetric],
        scoring_method: ScoringReducer,
        partitions: Optional[List[str]] = None,
        filters: Optional[List[ScopedFilter]] = None
    ):
        self.block_by = block_by
        self.metrics = computed_metrics
        self.scoring_method = scoring_method
        self.partitions = partitions
        self.filters = filters

    @property
    def fragment_fields(self):
        fields = {self.block_by.field}
        fields.update([metric.field for metric in self.metrics])
        if self.partitions:
            fields.add(self.partitions)
        return fields


    def get_cols(self):
        _ = defaultdict(set)
        for metric in self.metrics:
            _[metric.field].add(metric.transform)

        return dict(_)

    def create_fragments(self, entlet_df: pd.DataFrame(columns=['entlet'])) -> pd.DataFrame:
        pass

