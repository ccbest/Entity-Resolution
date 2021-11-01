
from collections import defaultdict
import json
import operator
from pathlib import Path
import threading
from typing import List

import pandas as pd
from skopeutils.dynamic_importer import DynamicImporter

from definitions import config, pipeline_cache, DATA_REPO, logger, ROOT_DIR
import pre_resolve.standardize.custom_lookup as custom_lookup
from utils.entlet import Entlet

data_source_functions = DynamicImporter()
data_source_functions.load(Path(ROOT_DIR, 'data_sources'))


class PreResolveOrchestrator(object):

    """
    Orchestration object for the Pre-Resolve stages (Fetch, Munge, Standardize, Fragment)

    Keyword Args:
        debug_mode (bool): (Default False) If True, will write each stage's results to drive


    """

    COMPARATORS = {
        "equals": operator.eq,
        "does not equal": operator.ne,
        "less than": operator.lt,
        "greater than": operator.gt,
        "less than or equal to": operator.le,
        "greater than or equal to": operator.ge
    }

    def __init__(self, **kwargs):
        self.sources_to_run = kwargs.get("sources", None)

        # Stage enable/disable
        stages = ('fetch', 'munge', 'standardize', 'transform', 'fragment')
        self.stage_args = {
            stage: kwargs.get("all_stages", kwargs.get(stage, False)) for stage in stages
        }

        self.debug_mode = kwargs.get("debug", False)

        self.fragment_fields = self.define_fragment_fields()

    def run(self):
        """
        Spins off threads for each data source

        Returns:
            True
        """
        source_threads = []

        for source in data_source_functions.registry["data_sources"]:
            if self.sources_to_run and source in self.sources_to_run:
                package_path = Path(ROOT_DIR, "data_sources", source)
                thread = threading.Thread(target=self._run_source, args=(package_path, self.stage_args))
                thread.start()
                source_threads.append(thread)

            else:
                logger.debug(f"Skipping source {source}.")

        for thread in source_threads:
            thread.join()

        # raw_entlets = spark_session.read.parquet(str(Path(DATA_REPO, "munge")))
        # entlets = raw_entlets.rdd.map(lambda x: Entlet().add(json.loads(x['value'])))

        #self.process_entlets(entlets)

        return True

    def _run_source(self, package_path: Path, stage_args: dict):
        """
        Executes the Fetch and Munge stages for a given data source. After this stage, entlets will
        be coalesced between sources, so the remaining pre-resolution logic occurs in ._run_pre_resolve()

        Args:
            package_path:
            stage_args:

        Returns:

        """

        data_source = package_path.name

        if stage_args["fetch"]:

            files_loc = self._run_fetch(data_source)

            pipeline_cache.write({"fetch": {data_source: [str(file) for file in files_loc]}})
            logger.debug(f"Wrote {data_source} fetch to metadata")

        else:
            logger.debug(f"{data_source}: Skipping stage fetch")

        if stage_args["munge"]:

            # Make sure the results of the fetch stage exist
            if f"fetch.{data_source}" not in pipeline_cache:
                raise ValueError(f"Unable to munge source {data_source} - fetch stage not found.")

            logger.debug(f"{data_source}: Starting stage munge")
            entlets = self._run_munge(data_source)

            # entlet_rdd = self.parallelize_entlets(entlets)
            # file_loc = self.spark_file_driver.write_rdd(
            #     entlet_rdd,
            #     Path(DATA_REPO, "munge", data_source),
            #     "pickle"
            # )
            #
            # pipeline_cache.write({"munge": {data_source: str(file_loc)}})
            # logger.debug(f"Wrote {data_source} munge to metadata")

        else:
            logger.debug(f"{data_source}: Skipping stage munge")

        if stage_args["standardize"]:
            entlets = self._standardize_entlets(entlets)

        else:
            logger.debug(f"{data_source}: Skipping stage standardize")


        # if self.debug_mode:
        #     file_loc = SparkFileDriver.write_rdd(
        #         entlets,
        #         Path(DATA_REPO, "standardize"),
        #         "pickle"
        #     )
        #     pipeline_cache.write({"standardize": str(file_loc)})
        #     logger.debug(f"Wrote standardize results to file")
        #
        # logger.info("Writing entlets to file.")
        #SparkFileDriver.write_rdd(entlets, Path(DATA_REPO, "pre_resolve"), "pickle")

        self._fragment_entlets(entlets)



        return True


    @staticmethod
    def _run_fetch(data_source: str) -> List[Path]:
        """
        Finds the fetch script(s) and executes it/them.

        Args:
            data_source (str): the name of the data source

        Returns:
            (list[Path]) The paths to the files that were fetched.
        """
        fetch_script = data_source_functions[f"data_sources.{data_source}.fetch.fetch"]
        file_locs = fetch_script()

        return file_locs

    @staticmethod
    def _run_munge(data_source) -> pd.DataFrame:
        """
        Runs the munger for the specified data source. The munger must follow the below structure:

        data_sources <apckage>
          |
          ---> {data_source_name} <package>
                |
                ---> munge.py
                      |
                      ---> munge <func>

        Mungers must produce entlets, which are submitted to the Spark streaming context using the entlet's .submit()
        method.

        Args:
            data_source (str): the name of the data source

        Returns:

        """
        munge_script = data_source_functions[f"data_sources.{data_source}.munge.munge"]
        entlets = pd.DataFrame()

        for file in pipeline_cache[f'fetch.{data_source}']:
            entlets = entlets.append(munge_script(Path(file)))

        # Define the column name as 'entlet' so its easier to understand
        entlets.columns = ['entlet']

        return entlets

    def _standardize_entlets(self, entlet_df: pd.DataFrame):
        """
        Standardizes values in entlets. Standardization logic is read from ./config/er_config.yaml

        Args:
            entlet_df (pd.DataFrame): A DataFrame, where each record consists of one element (the entlet)

        Returns:
            (entletDF) The same DataFrame, but the underlying entlets have had their values standardized.
        """
        for standardization_ruleset in config["standardize"]:
            filters = standardization_ruleset.get("filters", [])
            for std_filter in filters:
                std_filter.update({"comparator": self.COMPARATORS.get(std_filter["comparator"], ValueError)})

            method = getattr(custom_lookup, standardization_ruleset["method"])
            entlet_df = method(entlet_df, standardization_ruleset["field_name"], filters)

        return entlet_df

    @staticmethod
    def _fragment_entlets(entlet_df: pd.DataFrame) -> True:
        """
        Creates fragments for each ER strategy

        TODO: move to ER stage

        Args:
            entlet_df:

        Returns:

        """
        for strategy in config["strategies"]:
            fragments = entlet_df.map(lambda x: x.get_fragments(strategy))

            # cols_renamed = [item.replace('.', '_') for item in self.fragment_fields[strategy]]
            fragments = fragments.toDF()

            write_loc = Path(DATA_REPO, "fragments", strategy)
            #SparkFileDriver.write_df(fragments, write_loc, "parquet")

            logger.debug(f"Wrote {strategy} fragments to parquet")

        return True

    @staticmethod
    def define_fragment_fields():
        """
        Same logic as is used on the Entlet.

        Parses the ER configuration so we can understand what fields are needed by which strategies.
        Updates the Entlet class to reflect these requirements, stored as below:

        { strategy_name: [ field_names ] }

        Returns:
            None
        """
        er_fields = defaultdict(set)
        for strategy, details in config["strategies"].items():
            er_fields[strategy].update([a["field_name"] for a in details["similarity"]])
            er_fields[strategy].update([a["field_name"] for a in details["blocking"]])
            if 'filters' in details:
                er_fields[strategy].update([a["field_name"] for a in details["filters"]])

            if 'partition' in details:
                er_fields[strategy].update(details["partition"])

            er_fields[strategy].add("entlet_id")

        return dict(er_fields)
