

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


sentenceData = spark.createDataFrame([
    (1, "Hi I heard about Java"),
    (1, "Hi I heard Java"),
    (2, "I wish Java could use case classes"),
    (3, "Logistic regression models are neat"),
    (4, "I wish Java had kwargs for classes")
], ["entlet_id", "sentence"])
