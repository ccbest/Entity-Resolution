

from typing import Collection, Optional

import pandas as pd

from resolver import EntletMap
from resolver._base import ScoringReducer, SimilarityMetric
from resolver.blocking._base import Blocker
from . import Filter


class Strategy:

    def __init__(
        self,
        blocker: Blocker,
        metrics: Collection[SimilarityMetric],
        scoring_method: ScoringReducer = None,
        partitions: Optional[Collection[str]] = None,
        filters: Optional[Collection[Filter]] = None
    ):
        self.blocker = blocker
        self.metrics: Collection[SimilarityMetric] = metrics
        self.scoring_method = scoring_method

        self.partitions = partitions
        self.filters = filters

    def resolve(self, entletmap: EntletMap, entlet_df: pd.DataFrame) -> pd.DataFrame:
        """

        Args:
            entletmap: The entletmap
            entlet_df:

        Returns:

        """
        for candidate_pair in self.blocker.block(entlet_df):
            scores = [metric.run(*[entletmap.get(x) for x in candidate_pair]) for metric in self.metrics]
            if self.scoring_method(*scores):
                yield candidate_pair
