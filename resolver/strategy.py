

from typing import Collection, Generator, Optional, Tuple

import pandas as pd

from resolver import EntletMap, Filter
from resolver._base import Blocker, ScoringReducer, SimilarityMetric


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

    def resolve(self, entletmap: EntletMap, entlet_df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
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
