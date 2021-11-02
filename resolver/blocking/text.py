from pandas import DataFrame

from . import ResolutionBlocker


class SortedNeighborhood(ResolutionBlocker):

    """
    Sorts records by a column and then blocks with their alphabetical neighbors.

    Makes the comparison complexity O(2xn), where x is the specified window size.

    Example:

     Window = 2
    |---------|target|----------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

           Window = 2
          |---------|target|----------|
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

        pass


def alphabetical_neighborhood(df, column_name, **kwargs):

    window_size = kwargs.get("window", 10)

    # Sort the rdd by the partition+column specified, then drop the key
    # Transforms to [ Row() ]
    df_sorted = df.orderBy(column_name)\
        .withColumn("index", f.dense_rank().over(Window.partitionBy("partition_no").orderBy(column_name)))

    # Add an incrementing index, then move it from the key position into the record
    # Transforms to [ Row() ]
    indexed = df_sorted.rdd\
        .map(lambda x: Row(**{"partition_no": x["partition_no"], "data": Row(**x.asDict())}))\
        .toDF()

    # Permute rows against each other, but filter where outside window
    # Transforms to [ partition_no , Row() , Row() ]
    mapped = indexed\
        .join(indexed.alias("b").withColumnRenamed("data", "r_data"), "partition_no")\
        .where(f'data["index"] - r_data["index"] <= {window_size} and '
               'data["partition_no"] == r_data["partition_no"] and '
               'data["entlet_id"] != r_data["entlet_id"]')\
        .distinct()

    # Drop the index
    # transforms to [ Row() , Row() ]
    _ = mapped.rdd.map(lambda x: (x["data"], x["r_data"])).repartition(200)
    return _
