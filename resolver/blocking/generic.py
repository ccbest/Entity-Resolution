from pandas import DataFrame

from resolver._base import Blocker


class AllToAll(Blocker):

    def block(self, df: DataFrame) -> DataFrame:
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

    def block(self, df: DataFrame) -> DataFrame:
        srted = df.sort_values(self.field).reset_index(drop=True)

        # I *hate* this but pandas doesn't offer a better way
        srted = srted.join(how='cross', lsuffix='_frag1', rsuffix='_frag2')
        srted = srted[srted[self.field]]
        pass
