

from typing import Collection, Optional

from resolver._base import Blocker, ScoringReducer, SimilarityMetric
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

    @property
    def fragment_fields(self):
        """
        The list of all the entlet's fields that are required to be on the fragment for this strategy.
        Note that these are the fields *pre-transform*.

        """
        fields = {self.blocker.field}
        fields.update([metric.field for metric in self.metrics])
        if self.partitions:
            fields.add(self.partitions)
        return fields
