
from definitions import DATA_REPO
from toolkit.spark.get_spark import spark, spark_context
from toolkit.helpers import read_metadata, write_metadata
from toolkit.entlet import Entlet

from pyspark.sql.functions import col
from pyspark import Row
import os

import graphframes

# Spark's GraphFrames solution for this supposedly doesn't scale past the low millions
# Check out https://github.com/kwartile/connected-component/blob/master/src/main/scala/com/kwartile/lib/cc/ConnectedComponent.scala
# For when this gets rewritten in scala


def create_entmap():
    metadata = read_metadata()

    # Read entlet json, just for the unique list of entlets
    # TODO: Once entlet -> resolution switches to spark streaming, it should write out this list so we dont have to
    # union everything together
    entlets = spark.spark_context.emptyRDD()
    for dsource, file in metadata["munge"].items():
        entlets = entlets.union(spark_context.pickleFile(file))

    entlets = entlets.repartition(50)
    nodes = entlets.map(lambda x: (x[0], )).toDF().withColumnRenamed("_1", "id").distinct()
    entlets = entlets.keyBy(lambda x: x[0])

    # Read the list of edges in
    edges = spark.spark_session.read.format("avro").load(metadata["resolution"]).select(col("source_id").alias("src"), col("target_id").alias("dst"))
    edges = edges.union(spark.spark_session.read.format("avro").load(metadata["resolution"]).select(col("source_id").alias("dst"),
                                                                       col("target_id").alias("src")))

    spark_context.setCheckpointDir("/tmp/graphframes-example-connected-components")
    components = graphframes.GraphFrame(nodes, edges).connectedComponents()

    components = components.select("id", "component").rdd.keyBy(lambda x: x["id"])
    entmap = components.join(entlets).map(lambda x: (x[1][0]["component"], x[1][1][1]))
    reduced = entmap.reduceByKey(lambda x, y: x.merge(y.dump()))
    return reduced


