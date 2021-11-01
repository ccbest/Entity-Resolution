from pandas import DataFrame

from . import ResolutionBlocker

def all_to_all(df, **kwargs):
    """
    Creates a block of all unique combinations of records
    Args:
        df: a pyspark dataframe

    Returns:
        RDD
    """
    # Add an incrementing index to each row
    indexed = df.rdd.zipWithIndex()

    # Permute rows against each other
    mapped = indexed.cartesian(indexed).filter(lambda x: x[0][1] < x[1][1])

    # Drop the index
    no_index = mapped.map(lambda x: (x[0][0], x[1][0]))

    # Filter rows where the entlet_id is equivalent
    return no_index.filter(lambda x: x[0].entlet_id != x[1].entlet_id)


class AllToAll(ResolutionBlocker):

    def block(self, df: DataFrame(columns=['entlet'])) -> DataFrame(columns=['entlet1', 'entlet2']):
        pass