
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
import pandas as pd

from resolver._base import Blocker


def zip_windowed_fragments(windowed_fragments, columns):
    frag1 = {f"{col}_frag1": x[0] for col, x in zip(columns, windowed_fragments)}
    return [
        {
            **frag1,
            **{f"{col}_frag2": x[y] for col, x in zip(columns, windowed_fragments)}
        } for y in range(1, len(windowed_fragments[0])-1)
    ]


def rolling_window(df, window_size):
    cols = df.columns
    windowed = pd.Series(list(sliding_window_view(df, window_size, axis=0)))\
        .apply(lambda x: zip_windowed_fragments(x, cols))\
        .apply(pd.Series)\
        .unstack()\
        .reset_index(drop=True)\
        .apply(pd.Series)

    return pd.DataFrame(windowed)


class AllToAll(Blocker):

    def block(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.join(df, how='cross', lsuffix='_frag1', rsuffix='_frag2')


class SortedNeighborhood(Blocker):

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
        self.field = field
        self.window_size = kwargs.get('window_size', 10)

    def block(self, df: pd.DataFrame) -> pd.DataFrame:
        srted = df.sort_values(self.field).reset_index(drop=True)

        return rolling_window(srted, self.window_size)
