
from collections import defaultdict
from typing import List, Optional

import pandas as pd

from .blocking import ResolutionBlocker
from .filter import Filter
from .scoring import ScoringReducer
from .transforms import ColumnarTransform
from .metrics import ResolutionMetric


class Strategy:

    def __init__(
        self,
        block_by: ResolutionBlocker,
        computed_metrics: List[ResolutionMetric],
        scoring_method: ScoringReducer,
        partitions: Optional[List[str]] = None,
        filters: Optional[List[Filter]] = None
    ):
        self.block_by = block_by
        self.metrics = computed_metrics
        self.scoring_method = scoring_method
        self.partitions = partitions
        self.filters = filters

    def get_cols(self):
        _ = defaultdict(set)
        for metric in self.metrics:
            _[metric.field].add(metric.transform)

        return dict(_)

    def create_fragments(self, entlet_df: pd.DataFrame(columns=['entlet'])) -> pd.DataFrame:
        pass



