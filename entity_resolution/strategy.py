"""
Defines whatever complex logic is necessary to determine whether two entlets
should be considered the same or not. Any number of comparisons can be defined
(e.g. 'name exact match' and 'birth date within X days' and ....), and the resulting
scores of each comparator will be passed to the ScoringReducer, which will determine
a singular score to compare against a minimum threshold.
"""

from typing import Collection, Generator, Optional, Tuple

import pandas as pd

from entity_resolution import EntletMap, Filter
from entity_resolution._base import Blocker, ScoringReducer, SimilarityMetric


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

    @property
    def transforms(self):
        return [metric.transform for metric in self.metrics]

    def resolve(self, entletmap: EntletMap, entlet_df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
        """

        Args:
            entletmap: The entletmap
            entlet_df:

        Returns:

        """
        for metric in self.metrics:
            metric.transform(entlet_df)

        for candidate_pair in self.blocker.block(entlet_df):
            scores = [metric.score(*[entletmap.get(x) for x in candidate_pair]) for metric in self.metrics]
            if self.scoring_method(*scores):
                yield candidate_pair
