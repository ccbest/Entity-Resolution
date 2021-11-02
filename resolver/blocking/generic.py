from pandas import DataFrame

from . import ResolutionBlocker


class AllToAll(ResolutionBlocker):

    def block(self, df: DataFrame) -> DataFrame:
        return df.join(df, how='cross')
