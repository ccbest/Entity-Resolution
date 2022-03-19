"""
Module for general-use blockers that don't fit into a more defined hierarchy
"""
from itertools import combinations

import pandas as pd

from resolver._base import Blocker, BLOCKER_RETURN


class AllToAll(Blocker):
    """
    Blocks all combinations of entlets. Not generally recommended for use.
    """
    def __init__(self):
        pass

    def block(self, entlet_df: pd.DataFrame) -> BLOCKER_RETURN:
        entlet_ids = entlet_df['entlet'].map(lambda x: x.entlet_id)
        for pair in combinations(entlet_ids, 2):
            yield pair
