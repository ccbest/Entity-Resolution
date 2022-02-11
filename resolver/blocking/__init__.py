import abc
from functools import reduce
from typing import Callable, Dict, List, Set

import pandas as pd

from resolver import Entlet
from resolver._functions import merge_union
from resolver.blocking.generic import AllToAll, SortedNeighborhood

__all__ = ['']


REVERSE_INDEX = Dict[str, Dict[str, List[str]]]


class ReverseIndexBlocker(abc.ABC):

    """
    Mixin for blockers that block based on the creation of a reverse index.

    """
    def __init__(self, fields: List[str], fingerprint_fn: Callable):
        self.fields = fields
        self.fingerprint_fn = fingerprint_fn

    def fingerprint(self, entlet: Entlet) -> Dict[str, Dict[str, List[str]]]:
        """
        Creates the fingerprint for a given entlet on a per-field basis.

        Args:
            entlet (Entlet): an Entlet

        Returns:
            { <field name> : { bucket : [ entlet_id ] } }
        """
        return {
            field: {
                bucket: [entlet.entlet_id] for value in entlet.get(field, [])
                for bucket in self.fingerprint_fn(value)
            }
            for field in self.fields
        }

    def create_reverse_index(self, entlet_df: pd.DataFrame) -> REVERSE_INDEX:
        predicated = entlet_df['entlet'].map(self.fingerprint)
        return reduce(merge_union, predicated)

    def get_block(self, entlet):