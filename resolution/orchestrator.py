"""
Organizes data for ER per provided configurations in
config/er_config
"""
from collections import defaultdict
from functools import reduce
import logging
from pyspark.sql import Window, Row
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import os

from definitions import DATA_REPO
from toolkit.config_reader import er_config
from toolkit.entlet import Entlet
from toolkit.import_manager import custom_function_registry
from toolkit.helpers import read_metadata, write_metadata
from toolkit.spark.get_spark import spark_session, spark_context


logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)


class EROrchestrator(object):

    def __init__(self):
        self.spark = spark_session
        self.spark_context = spark_context

        self.prep_plan = None
        self.config = er_config
        self.prepped_dataframes = {}
        self.fragments = None
        self.resolutions = self.spark_context.emptyRDD()

        self.global_partitions = []
        self.strat_partitions = []

    def register_functions(self):

        custom_function_registry.get_functions_in_dir()

    def normalize_field_names(self, strategy_config):

        if isinstance(strategy_config, dict):
            return {key: self.normalize_field_names(value) for key, value in strategy_config.items()}

        elif isinstance(strategy_config, list):
            return [self.normalize_field_names(item) for item in strategy_config]

        elif isinstance(strategy_config, str):
            return strategy_config.replace(".", "_")

        return strategy_config


    def run(self):
        """
        Runs ER

        Returns:
            True
        """
        logger.info("#### RUNNING ER ####")

        schema = StructType([
            StructField("source_id", StringType(), False),
            StructField("target_id", StringType(), False),
            StructField("resolution_score", FloatType(), False),
            StructField("resolution_strategy", StringType(), False)
        ])
        resolutions = self.spark.createDataFrame([], schema)

        for strategy_name, strategy_config in self.config["strategies"].items():
            logger.info(f"Executing strategy {strategy_name}.")

            fragments = self.load_fragments(strategy_name).repartition(200).cache()

            strategy_config = self.normalize_field_names(strategy_config)

            # Apple all transformations - these won't change by partition/block. Also keep track of which added columns
            # are important
            # result_columns = { strategy: [ (field, comparison_method), ...] }
            fragments, result_columns = self._transform_columns(fragments, strategy_name, strategy_config)
            columns_created = list(set([comp[0] for strategy, cols in result_columns.items() for comp in cols]))

            global_fields = Entlet.REQUIRED_FIELDS

            # Get a list of all applicable fields
            partition_fields = strategy_config["partition"]
            blocking_fields = [blocking_method["field_name"] for blocking_method in strategy_config["blocking"]]
            columns_used = partition_fields + blocking_fields + global_fields + columns_created

            frags = fragments.select(columns_used)

            #ranked = frags.select("entlet_id", *partition_fields)\
            #    .rdd.distinct().orderBy(*partition_fields)\
            #    .zipWithIndex().post_resolve(lambda x: (("partition_no", x[0]), *x[1], ))\
            #    .toDF("entlet_id", *partition_fields)

            partitioned = frags \
                .withColumn("partition_no",
                            f.dense_rank().over(
                                Window.orderBy(*partition_fields)
                            )) \
                .repartition(f.col("partition_no"))

            partitioned = frags

            partitioned = partitioned.cache()

            logger.info("Created partitions.")

            blocks = []
            for blocking_method in strategy_config["blocking"]:
                logger.info(f"Blocking {blocking_method['field_name']} by {blocking_method['method']}.")
                blocks.append(self.create_blocks(partitioned, blocking_method["method"], blocking_method["field_name"],
                                            columns_created))

            if len(blocks) > 1:
                blocked = reduce(spark_context.union, blocks)
                blocked = blocked.repartition(200)
            else:
                blocked = blocks[0]

            logger.info("Created blocks.")
            partitioned.unpersist()

            # Compare measures to generate individual scores
            for comparison in result_columns[strategy_name]:
                comparison_method = getattr(resolve.comparison_metrics.comparison, comparison[1])
                field = comparison[0]
                blocked = blocked.map(lambda x: comparison_method(x, field))

            logger.info("Calculated individual scores.")

            # Roll the individual scores into an overall score for the comparison
            scoring_method = getattr(resolve.score_aggregation.scoring, strategy_config["scoring"])
            passed = blocked.map(lambda x: scoring_method(x)).filter(lambda x: x[2] > strategy_config["threshold"]) \
                .map(lambda x: (x[0], x[1], round(float(x[2]), 4)))

            logger.info("Calculated overall scores.")
            blocked.unpersist()

            # Map the RDD against the resolution dataframe's structure so we can union nicely
            passed = passed.map(lambda x: Row(**{
                "source_id": x[0],
                "target_id": x[1],
                "resolution_score": x[2],
                "resolution_strategy": strategy_name
            })).toDF()

            resolutions = resolutions.union(passed)

        if not os.path.exists(os.path.join(DATA_REPO, "resolution")):
            os.makedirs(os.path.join(DATA_REPO, "resolution"))

        if os.path.exists(os.path.join(DATA_REPO, "resolution", "resolutions")):
            os.rmdir(os.path.join(DATA_REPO, "resolution", "resolutions"))

        file_loc = os.path.join(DATA_REPO, "resolution", "resolutions")
        resolutions.write.format("avro").save(file_loc)
        write_metadata({"resolution": file_loc})
        return True

    def load_fragments(self, strategy):
        """
        Loads fragments. Gives preference to standardization results, or uses munge results if no standardization

        Returns:
            None
        """
        meta = read_metadata()
        frag_locations = meta["fragment"][strategy]

        fragments = spark.read.format("parquet").load(list(frag_locations.values())[0])
        for file_loc in list(frag_locations.values())[1:]:
            fragments = fragments.union(spark.read.format("parquet")
                                             .load(file_loc)
                                        )
        return fragments

    def _transform_columns(self, fragments, strategy, strategy_config):
        """
        Applies all transform logic (tfidf tokens, exact match, etc.) against columns

        Returns:
            (dict) the list of column names that are the result of the transforms
        """
        result_columns = defaultdict(list)

        for similarity_measure in strategy_config["similarity"]:
            method = getattr(resolve.similarity_measures.measurements, similarity_measure["measurement"])
            fragments, result_column = method(fragments, similarity_measure["field_name"])
            result_columns[strategy].append((result_column, similarity_measure["comparison"]))

        return fragments, result_columns

    @staticmethod
    def create_blocks(partitioned, blocking_method, column_name, columns_to_return):
        """
        Takes in a RDD of entlets and returns an RDD of entlet pairs. Entlets are paired according to the specified
        blocking logic (see resolution.blocking for more details)

        Args:
            partitioned: RDD, a subset of the RDD of all entlets
            blocking_method (str): the blocking method
            column_name (str): the name of the column that the method will use to create blocks

        Returns:
            RDD of entlet pairs
        """
        method = getattr(resolve.blocking, blocking_method)
        return method(partitioned, column_name=column_name, columns_to_return=columns_to_return)

    @staticmethod
    def _create_partitions(fragments, partitions: set):
        def partition(df, column_name):
            unique_values = df.select(column_name).distinct().rdd.flatMap(lambda x: x).collect()
            return [df.where(df[column_name] == unique_value) for unique_value in unique_values]

        # Always partition by ent_type
        logger.info("Partitioning by ent_type")
        strat_partitions = partition(fragments, "ent_type")

        # Partitions act as "and"s, and we partion breadth-first for efficiency. So essentially, blow away
        # the existing partitions with each new rule and return the further partitioned dataframes
        while partitions:
            new_partition_rule = partitions.pop()
            logger.info(f"Partitioning by {new_partition_rule}")
            active_partitions, strat_partitions = strat_partitions, []
            for c_partition in active_partitions:
                strat_partitions += partition(c_partition, new_partition_rule)

        for partition in strat_partitions:
            partition.cache()

        return strat_partitions

    @staticmethod
    def write_resolutions(resolutions):
        loc = os.path.join(DATA_REPO, "resolution", "resolutions")
        resolutions.write.format("csv").options(header='true').save(loc)
        return True


    def run_strategy(self):
        a='a'

