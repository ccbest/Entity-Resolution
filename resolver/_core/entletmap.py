
from __future__ import annotations
from typing import List, MutableMapping, Iterator, Tuple

import pandas as pd

from . import Entlet


class EntletMap(MutableMapping):

    def __init__(self, entlets: List[Entlet] = None):

        entlets = entlets or []
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
        Adds an entlet to the map

        Args:
            entlet (Entlet): the entlet to add

        Returns:
            self
        """
        if entlet.entlet_id in self.entlets:
            self.entlets[entlet.entlet_id].merge(entlet)
            return self

        self.entlets[entlet.entlet_id] = entlet
        return self

    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts the entletmap to a dataframe.

        Returns:
            (pd.DataFrame) A pandas dataframe with one column containing the entlet objects
        """
        return pd.DataFrame(self.entlets.values(), columns=['entlet'])

