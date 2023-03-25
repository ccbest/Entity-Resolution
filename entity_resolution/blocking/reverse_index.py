"""
Module for blockers that operate via a reverse index lookup
"""

import abc
from collections import Counter
from functools import reduce
from typing import Dict, Generator, List, Optional, Tuple, Set

import pandas as pd

from entity_resolution import Entlet
from entity_resolution._base import Blocker, ColumnarTransform
from entity_resolution._functions import merge_union


_INDEX = Dict[str, Dict[str, Set[str]]]


class ReverseIndexBlocker(Blocker):
    """
    Mixin for blockers that block based on the creation of a reverse index.

    Uses some logic (determined by which child blocker is used) to bucket entlets
    by their shared underlying values
    """

    transform: Optional[ColumnarTransform]

    def __init__(self, fields: List[str], threshold: float):
        self.fields = fields
        self.threshold = threshold

    @abc.abstractmethod
    def fingerprint(self, value: str) -> Set[str]:
        pass

    def _fingerprint(self, entlet: Entlet) -> Dict[str, Set[str]]:
        """
        Creates the fingerprint for a given entlet on a per-field basis.

        Args:
            entlet (Entlet): an Entlet

        Returns:
            { <field name> : set( bucket ) }
        """
        return {
            field: set(self.fingerprint(value))
            for field in self.fields
            for value in entlet.get(field, [])
        }

    @staticmethod
    def create_index(entlet_df: pd.DataFrame) -> _INDEX:
        """
        Creates an index of entlets to their corresponding buckets,
        structured as:

        {
            field: { entlet_id : set( <buckets> ) }
        }

        Args:
            entlet_df:

        Returns:

        """

        predicated = entlet_df[['entlet', 'fingerprint']].apply(
            lambda x: {
                field: {
                    x['entlet'].entlet_id: buckets}
                for field, buckets in x['fingerprint'].items()
            },
            axis=1
        )
        return reduce(merge_union, predicated)

    @staticmethod
    def create_reverse_index(entlet_df) -> _INDEX:
        """
        Creates a reverse index of buckets to their corresponding entlets,
        structured as:

        {
            field : { bucket : set( <entlet id> ) }
        }

        Args:
            entlet_df: A DataFrame containing 2 columns:
                        - entlet: the entlet object
                        - fingerprint: the fingerprint of the entlet

        Returns:

        """
        predicated = entlet_df[['entlet', 'fingerprint']].apply(
            lambda x: {
                field: {
                    bucket: {x['entlet'].entlet_id} for bucket in buckets
                } for field, buckets in x['fingerprint'].items()
            },
            axis=1
        )
        return reduce(merge_union, predicated)

    def block(
            self,
            entlet_df: pd.DataFrame
    ) -> Generator[Tuple[str, str], None, None]:
        """
        Blocks by creating a reverse index and determining overlap.

        Args:
            entlet_df (pd.DataFrame): A single-column dataframe of entlet objects

        Returns:

        """

        entlet_df['fingerprint'] = entlet_df['entlet'].map(self._fingerprint)
        reverse_index = self.create_reverse_index(entlet_df)
        index = self.create_index(entlet_df)

        # keeps track of entlets we've seen, makes sure combinations (not permutations)
        # are produced
        # This blocker is not symmetrical, so need to keep track of the combination pairs
        matched = {}

        for field in self.fields:

            for entlet_id, predicates in index[field].items():
                matched[entlet_id] = set()

                # count of times each alternate entlet id appears in this
                # entlet's predicates
                counter = Counter(
                    ent_id
                    for predicate in predicates
                    for ent_id in reverse_index[field][predicate]
                    if ent_id != entlet_id
                )
                for match_id, count in counter.items():
                    if match_id in matched and entlet_id in matched[match_id]:
                        continue

                    if count / len(predicates) >= self.threshold:
                        matched[entlet_id].add(match_id)
                        yield entlet_id, match_id


class QGramBlocker(ReverseIndexBlocker):

    """
    Blocks string values based on Q-Grams (n-length character subsets of the strings).

    Additional dependencies required:
        - nltk

    """

    ACCEPTS = [str]

    def __init__(
            self,
            fields: List[str],
            q: int,
            transforms: Optional[List[ColumnarTransform]] = None,
            threshold: float = 0.5
    ):
        super().__init__(fields, threshold)

        self.q = q
        self.transforms = transforms

    def fingerprint(self, value: str) -> Set[str]:
        return {value[i:i+self.q] for i in range(len(value) - self.q + 1)}


class SyllableBlocker(ReverseIndexBlocker):

    """
    Blocks string values based on expressed syllables in the text (n-length
    character subsets of the strings).

    Additional dependencies required:
        - nltk
    """

    ACCEPTS = {str}

    def __init__(
            self,
            fields: List[str],
            transforms: Optional[ColumnarTransform],
            threshold: float = None
    ):
        from nltk.tokenize.legality_principle import LegalitySyllableTokenizer
        from nltk.corpus import words

        super().__init__(fields, threshold)

        self.tokenizer = LegalitySyllableTokenizer(words.words())

        self.fields = fields
        self.transforms = transforms

    def fingerprint(self, value: str) -> Set[str]:
        """
        Tokenizes a string by individual words. For additional information, reference
        NLTK's documentation for `nltk.tokenize.word_tokenize`.

        Args:
            value (str): A string of text containing one or more words

        Returns:
            (set) a unique list of words in the text
        """
        from nltk.tokenize import word_tokenize

        return {
            token for word in word_tokenize(value) for token in self.tokenizer.tokenize(word)
        }
