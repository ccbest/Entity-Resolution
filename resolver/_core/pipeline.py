from __future__ import annotations

from functools import reduce
import hashlib
from typing import Collection, List, Tuple

import networkx as nx
import pandas as pd

from . import EntletMap, Strategy
from resolver._base import StandardizationTransform
from resolver._utils.functions import deduplicate_nested_structure, merge_union


class Pipeline:

    def __init__(self, strategies: Collection[Strategy], standardizers: Collection[StandardizationTransform] = None):

        self.standardizers = standardizers
        self.strategies: Collection[Strategy] = strategies

    def resolve(self, entletmap: EntletMap):
        entlet_df = entletmap.to_dataframe()

        # Standardize stage
        entlet_df = self.standardize_entlets(entlet_df, self.standardizers)

        resolved_components = nx.Graph()
        resolved_components.add_nodes_from(entletmap.keys())

        for strategy in self.strategies:
            fragments = self.fragment(entlet_df, strategy.fragment_fields)

            # Transform columns before blocking
            # todo: move before fragmentation - fragment field names will be altered
            for metric in strategy.metrics:
                fragments = metric.transform(fragments)

            resolutions = strategy.resolve(fragments)
            resolved_components.add_edges_from(resolutions.to_numpy().tolist())

        entity_map = {}
        for conn_component in nx.connected_components(resolved_components):
            entity_id = f"entity:{hashlib.sha1(''.join(conn_component).encode('utf8')).hexdigest()}"
            entity_map[entity_id] = deduplicate_nested_structure(
                reduce(
                    lambda x, y: merge_union(x, y),
                    [entletmap[entlet_id].dump() for entlet_id in conn_component]
                )
            )
            entity_map[entity_id]['entity_id'] = entity_id

        return entity_map

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

    def fragment(self, entlet_df: pd.DataFrame, fragment_fields: List[str]) -> pd.DataFrame:
        """
        Convert a dataframe of entlets into a dataframe of fragments. The resulting dataframe's columns
        reflect the fragment fields, which are dot-notated.

        Args:
            entlet_df (pd.DataFrame): A dataframe of entlets
            fragment_fields (List[str]): A list of fields to fragment on

        Returns:
            pd.DataFrame
        """
        fragments = entlet_df.applymap(lambda x: list(x.get_fragments(fragment_fields)))
        fragments = fragments["entlet"].apply(pd.Series).stack().reset_index(drop=True).apply(pd.Series)

        return fragments

