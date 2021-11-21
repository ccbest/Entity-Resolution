from pandas import DataFrame

from resolver._base import Blocker


class AllToAll(Blocker):

    def block(self, df: DataFrame) -> DataFrame:
        return df.join(df, how='cross', lsuffix='_frag1', rsuffix='_frag2')
