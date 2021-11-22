from __future__ import annotations
from typing import Collection, List, Tuple


import pandas as pd

from . import EntletMap, Strategy
from resolver._base import StandardizationTransform


class Pipeline:

    def __init__(self, strategies: Collection[Strategy], standardizers: Collection[StandardizationTransform] = None):

        self.standardizers = standardizers
        self.strategies: Collection[Strategy] = strategies

    def resolve(self, entletmap: EntletMap):
        entlet_df = entletmap.to_dataframe()

        # Standardize stage
        entlet_df = self.standardize_entlets(entlet_df, self.standardizers)

        resolutions = pd.DataFrame(columns=['id1', 'id2'])

        for strategy in self.strategies:
            fragments = self.fragment(entlet_df, strategy.fragment_fields)

            # Transform columns before blocking
            for metric in strategy.metrics:
                fragments = metric.transform(fragments)

            blocked = strategy.blocker.block(fragments)




    @staticmethod
    def standardize_entlets(entlet_df: pd.DataFrame, standardizers: Collection[StandardizationTransform]) -> pd.DataFrame:
        """
        Async application of standardization against the dataframe of entlets.

        Args:
            entlet_df (pd.DataFrame): a dataframe of entlets
            standardizers (List[StandardizationTransform]): a list of standardization transforms

        Returns:
            self
        """
        for standardizer in standardizers:
            entlet_df.applymap(standardizer.run)

        return entlet_df

    def fragment(self, entlet_df: pd.DataFrame, frag_fields: List[str]) -> pd.DataFrame:

        fragments = entlet_df.applymap(lambda x: list(x.get_fragments(frag_fields)))
        fragments.unstack().reset_index(drop=True)

        return

