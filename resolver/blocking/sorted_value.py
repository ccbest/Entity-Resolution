
from typing import Callable, List, Tuple

import pandas as pd

from resolver.blocking._base import Blocker, BLOCKER_RETURN


class SortedValueBlocker(Blocker):

    def __init__(self, field: str, pair_fn: Callable):
        self.field = field
        self.pair_fn = pair_fn

    def sort(self, entlet_df: pd.DataFrame) -> List[Tuple]:
        """

        Args:
            entlet_df:

        Returns:

        """
        entlet_df[self.field] = entlet_df['entlet'] \
            .map(lambda x: x.get(self.field, []))

        entlet_df['entlet'] = entlet_df['entlet'] \
            .map(lambda x: x.entlet_id)

        entlet_df = entlet_df.explode(self.field) \
            .sort_values(self.field)

        return entlet_df[['entlet', self.field]].to_records()

    def block(self, entlet_df: pd.DataFrame) -> BLOCKER_RETURN:
        sorted_records = self.sort(entlet_df)
        pairs = self.pair_fn(sorted_records)

        for pair in set(tuple(sorted(pair)) for pair in pairs):
            yield pair


class SortedNeighborhood(SortedValueBlocker):

    """
    Sorts records by a column and then blocks with their alphabetical neighbors. Since combination
    is what matters, not permutation, the window only faces a single direction.

    Makes the comparison complexity O(2xn), where x is the specified window size.

    Example:

     Window = 2
    |target|---------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

           Window = 2
        |target|----------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

    Args:
        df: a dataframe
        field (str): the column that will be blocked on

    Keyword Arguments:
        window_size (int): (default 10) the amount in either direction that the window extends.
                           A window size of 10 would mean a given fragment is blocked with the
                           10 preceding and 10 following records.

    Returns:
        RDD
    """

    def __init__(self, field: str, **kwargs):
        super().__init__(field, self.pair_by_neighborhood)
        self.window_size = kwargs.get('window_size', 10)

    def pair_by_neighborhood(self, sorted_records):
        return [
            (record[1], sorted_records[idx + k][1])
            for idx, record in enumerate(sorted_records)
            for k in range(1, self.window_size + 1)
            if idx + k < len(sorted_records) and record[1] != sorted_records[idx + k][1]
        ]
