from __future__ import annotations
from typing import Collection, Tuple


import pandas as pd

from ._munging.entletmap import EntletMap
from ._base import StandardizationTransform


class Pipeline:

    def __init__(self, standardizers: Collection[StandardizationTransform], strategies):

        self.standardizers = standardizers
        self.strategies = strategies

    def resolve(self, entletmap: EntletMap):
        entlet_df = entletmap.to_dataframe()

        # Standardize stage
        entlet_df = self.standardize_entlets(entlet_df, self.standardizers)



    @staticmethod
    def standardize_entlets(entlet_df: pd.DataFrame, standardizers: Collection[StandardizationTransform]) -> pd.DataFrame:
        """
        Stages a standardization transform.

        The transform will be applied against the entlet DataFrame, which will
        not be available until pre-resolution occurs.

        Args:
            entlet_df (pd.DataFrame): a dataframe of entlets
            standardizers (List[StandardizationTransform]): a list of standardization transforms

        Returns:
            self
        """
        for standardizer in standardizers:
            entlet_df.applymap(lambda x: standardizer.run(x))

        return entlet_df

    @staticmethod
    def fragment(entlet_df: pd.DataFrame) -> pd.DataFrame:
