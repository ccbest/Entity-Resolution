from __future__ import annotations
from typing import Collection, Tuple


import pandas as pd

from ._munging.entletmap import EntletMap
from ._base import StandardizationTransform


class Pipeline:

    def __init__(self, strategies, standardizers: Collection[StandardizationTransform] = None):

        self.standardizers = standardizers
        self.strategies = strategies

    def resolve(self, entletmap: EntletMap):
        entlet_df = entletmap.to_dataframe()

        # Standardize stage
        entlet_df = self.standardize_entlets(entlet_df, self.standardizers)

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

    def fragment(self, entlet_df: pd.DataFrame) -> pd.DataFrame:

        for strategy in self.strategies:
            frag_fields = strategy.get_fragment_fields()
            fragments = entlet_df.applymap(lambda x: list(x.get_fragments(frag_fields)))

            yield fragments.unstack().reset_index(drop=True)

