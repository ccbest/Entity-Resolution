"""
Module for blockers that operate via sorting the underlying values
"""

import abc
from typing import Any, List, Tuple

import pandas as pd

from entity_resolution._base import Blocker, BLOCKER_RETURN


class SortedValueBlocker(Blocker, abc.ABC):
    """
    Blocks entlets by sorting their underlying values and applying a pairing
    function.
    """

    @abc.abstractmethod
    def __init__(self, field: str, **kwargs):
        self.field = field

    @abc.abstractmethod
    def pair(self, sorted_records: List[Tuple[str, Any]]) -> List[Tuple[str, str]]:
        """
        Determines how records (that have already been sorted) should be paired.

        Args:
            sorted_records: A list of two-tuple records, structured as:
                    ( <entlet_id> , <the corresponding value> )

        Returns:
            A list of two-tuples representing pairs of entlet ids that have been
            blocked together
        """

    def sort(self, entlet_df: pd.DataFrame) -> List[Tuple]:
        """
        Sorts entlets by their underlying values (based on the name of
        the field provided). Because virtually all values of an entlet
        are by nature a list, the dataframe gets exploded such that each
        row only contains a single value.

        Args:
            entlet_df: A single-column pandas DataFrame of entlets

        Returns:
            A list of two-tuples: ( <entlet_id> , <corresponding value> )
        """
        entlet_df[self.field] = entlet_df['entlet'] \
            .map(lambda x: x.get(self.field, []))

        entlet_df['entlet'] = entlet_df['entlet'] \
            .map(lambda x: x.entlet_id)

        entlet_df = entlet_df.explode(self.field) \
            .dropna(how='any', axis=1) \
            .sort_values(self.field)

        return entlet_df[['entlet', self.field]].to_records()

    def block(self, entlet_df: pd.DataFrame) -> BLOCKER_RETURN:
        """
        Blocks a dataframe of entlets, generating pairs of entlet ids
        that should be compared against each other for resolution.

        Because we want to avoid duplicate comparisons taking place, the
        tuples of entlet ids get sorted and deduplicated before being yielded.

        Args:
            entlet_df: A single-column pandas DataFrame of entlets

        Returns:
            A generator that produces pairs of entlet ids
        """
        sorted_records = self.sort(entlet_df)
        pairs = self.pair(sorted_records)

        for pair in set(tuple(sorted(pair)) for pair in pairs):
            if pair[0] == pair[1]:
                # don't pair things with themselves
                continue

            yield pair


class SortedNeighborhood(SortedValueBlocker):

    """
    Sorts entlets based on underlying values and then blocks with their
    alphabetical neighbors.

    Makes the comparison complexity O(2xn), where x is the specified window size.

    Example:

     Window = 2
    |target|---------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

           Window = 2
        |target|----------|
    Row1  Row2  Row3  Row4  Row5  Row6  Row7

    Args:
        field (str): the field containing the underlying values

    Keyword Arguments:
        window_size (int): (default 10) the amount in either direction that the window extends.
                           A window size of 10 would mean a given fragment is blocked with the
                           10 preceding and 10 following records.

    Returns:
        RDD
    """

    def __init__(self, field: str, **kwargs):
        super().__init__(field)
        self.window_size = kwargs.get('window_size', 10)

    def pair(self, sorted_records: List[Tuple[str, Any]]) -> List[Tuple[str, str]]:
        """
        Pairs records based on a fixed-size window.

        Args:
            sorted_records:

        Returns:

        """
        return [
            (record[1], sorted_records[idx + k][1])
            for idx, record in enumerate(sorted_records)
            for k in range(1, self.window_size + 1)
            if idx + k < len(sorted_records)
        ]
