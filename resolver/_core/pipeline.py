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

            for metric in strategy.metrics:
                blocked[metric.name] = blocked.apply(metric.run)



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
        """
        Convert a dataframe of entlets into a dataframe of fragments. The resulting dataframe's columns
        reflect the fragment fields, which are dot-notated.

        Args:
            entlet_df (pd.DataFrame): A dataframe of entlets
            frag_fields (List[str]): A list of fields to fragment on

        Returns:
            pd.DataFrame
        """
        fragments = entlet_df.applymap(lambda x: list(x.get_fragments(frag_fields)))
        fragments = fragments["entlet"].apply(pd.Series).unstack().reset_index(drop=True).apply(pd.Series)

        return fragments

