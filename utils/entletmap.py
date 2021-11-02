
from __future__ import annotations
from typing import List, MutableMapping, Iterator, Tuple

import pandas as pd

from utils.entlet import Entlet
from resolver.standardize import StandardizationTransform


class EntletMap(MutableMapping):

    def __init__(self, entlets: List[Entlet] = {}):
        self.entlets = {
            entlet.entlet_id: entlet for entlet in entlets
        }

        self._standardizations = []
        self._strategies = []

    def __setitem__(self, k: str, v: Entlet) -> None:
        """
        Sets the value of k (an entlet id) to v (an entlet)
        Args:
            k (str): An entlet ID
            v (Entlet): an Entlet

        Returns:

        """
        self.entlets.__setitem__(k, v)

    def __delitem__(self, v: str) -> None:
        """
        Deletes an entlet from the mapping

        Args:
            v (str): The entlet id

        Returns:
            None
        """
        self.entlets.__delitem__(v)

    def __getitem__(self, k: str) -> Entlet:
        """
        Retrieves an entlet by its entlet id

        Args:
            k (str): the entlet id

        Returns:

        """
        return self.entlets.__getitem__(k)

    def __add__(self, other: EntletMap):
        self.entlets.update(other.entlets)
        self._standardizations += other._standardizations
        self._strategies += other._strategies
        return self

    def __len__(self) -> int:
        return self.entlets.__len__()

    def __iter__(self) -> Iterator[str]:
        return self.entlets.__iter__()

    def add(self, entlet: Entlet) -> EntletMap:
        """
        Adds an entlet to the mapping

        Args:
            entlet (Entlet): the entlet to add

        Returns:
            self
        """
        self.entlets[entlet.entlet_id] = entlet
        return self

    def standardize(self, *args: Tuple[StandardizationTransform]) -> EntletMap:
        """
        Stages a standardization transform.

        The transform will be applied against the entlet DataFrame, which will
        not be available until pre-resolution occurs.

        Args:
            transform (StandardizationTransform): a standardization transformer

        Returns:
            self
        """
        for transform in args:
            if not isinstance(transform, StandardizationTransform):
                raise ValueError(f"Expected a StandardizationTransform instance, received {type(transform)}")

            self._standardizations.append(transform)

        return self

    def add_strategy(self, strategy) -> EntletMap:
        """
        Stages a resolution strategy.

        Args:
            strategy:

        Returns:

        """
        self._strategies.append(strategy)
        return self

    def resolve(self, strategies: List[ResolutionStrategy]):
        """
        Executes entity resolution.

        Returns:

        """
        entlet_df = pd.DataFrame(self.entlets.values(), columns=['entlet'])

        for standardization in self._standardizations:
            entlet_df = entlet_df.apply(standardization.run)

        return entlet_df

    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts the entletmap to a dataframe.

        Returns:
            (pd.DataFrame) A pandas dataframe with one column containing the entlet objects
        """
        return pd.DataFrame(self.entlets.values(), columns=['entlet'])



class ResolutionPipeline:

    def __init__(self, entletmap: EntletMap):
        self.entlet_df = entletmap.to_dataframe()

    def _produce_fragments(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame(columns=['fragment']):

        pass

