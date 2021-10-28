from pyspark.sql import Row, Window
import pyspark.sql.functions as f

from toolkit.logging import logger


def alphabetical_neighborhood(df, column_name, **kwargs):
    """
    Sorts records by a column and then blocks with their alphabetical neighbors

    Example:

     Window = 2
    |---------|target|----------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

           Window = 2
          |---------|target|----------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

    Args:
        df: a dataframe
        column_name (str): the column that will be blocked on

    Keyword Arguments:
        window (int): (default 10) the amount in either direction that the window extends

    Returns:
        RDD
    """
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
