"""
Pipeline module. Accepts various other objects in order to define a pipeline and
then orchestrates the actual processing.
"""

from __future__ import annotations

from functools import reduce
import hashlib
from typing import Collection, List

import networkx as nx
import pandas as pd

from entity_resolution import EntletMap, Strategy
from entity_resolution._base import StandardizationTransform
from entity_resolution._functions import deduplicate_nested_structure, merge_union


class Pipeline:

    def __init__(self, strategies: Collection[Strategy]):
        self.strategies: Collection[Strategy] = strategies

    def resolve(self, entletmap: EntletMap):
        entlet_df = entletmap.to_dataframe()

        resolved_components = nx.Graph()
        resolved_components.add_nodes_from(entletmap.keys())

        for strategy in self.strategies:
            resolved_components.add_edges_from(strategy.resolve(entletmap, entlet_df))

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

    @property
    def transforms(self):
        return [transform for strat in self.strategies for transform in strat.transforms]
