import abc
from collections import Counter
from functools import reduce
from typing import Callable, Dict, Generator, List, Optional, Tuple, Set

import pandas as pd

from resolver import Entlet
from resolver._base import ColumnarTransform
from resolver._functions import merge_union


_INDEX = Dict[str, Dict[str, Set[str]]]


class ReverseIndexBlocker(abc.ABC):
    """
    Mixin for blockers that block based on the creation of a reverse index.

    """

    def __init__(self, fields: List[str], fingerprint_fn: Callable, threshold: float):
        self.fields = fields
        self.fingerprint_fn = fingerprint_fn
        self.threshold = threshold

    def fingerprint(self, entlet: Entlet) -> Dict[str, Set[str]]:
        """
        Creates the fingerprint for a given entlet on a per-field basis.

        Args:
            entlet (Entlet): an Entlet

        Returns:
            { <field name> : set( bucket ) }
        """
        return {
            field: set(self.fingerprint_fn(value))
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

    def block(self, entlet_df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
        entlet_df['fingerprint'] = entlet_df['entlet'].map(self.fingerprint)

        reverse_index = self.create_reverse_index(entlet_df)
        index = self.create_index(entlet_df)

        # keeps track of entlets we've seen, makes sure combinations (not permutations)
        # are produced
        matched = set()

        for field in self.fields:

            for entlet_id, predicates in index[field].items():
                matched.add(entlet_id)

                # count of times each alternate entlet id appears in this
                # entlet's predicates
                counter = Counter(
                    ent_id for predicate in predicates for ent_id in reverse_index[field][predicate]
                )
                for match_id, count in counter.items():
                    if match_id in matched:
                        continue

                    if count / len(predicates) >= self.threshold:
                        yield entlet_id, match_id


class QGramBlocker(ReverseIndexBlocker):

    """
    Blocks string values based on Q-Grams (n-length character subsets of the strings).

    Accepts fields with the following datatypes:
      - string

    """

    ACCEPTS = [str]

    def __init__(
            self,
            fields: List[str],
            q: int,
            transforms: Optional[List[ColumnarTransform]] = None,
            threshold: float = 0.5
    ):
        super().__init__(fields, self.fingerprint_string, threshold)

        self.q = q
        self.transforms = transforms

    def fingerprint_string(self, value: str) -> Set[str]:
        return {value[i:i+self.q] for i in range(len(value) - self.q + 1)}


class SyllableBlocker(ReverseIndexBlocker):

    """
    Blocks string values based on expressed syllables in the text (n-length character subsets of the strings).

    Accepts fields with the following datatypes:
      - string

    """

    ACCEPTS = {str}

    def __init__(self, fields: List[str], transforms: List[ColumnarTransform], threshold: float = None):
        from nltk.tokenize.legality_principle import LegalitySyllableTokenizer
        from nltk.corpus import words

        super().__init__(fields, self.fingerprint_string, threshold)

        self.tokenizer = LegalitySyllableTokenizer(words.words())

        self.fields = fields
        self.transforms = transforms

    def fingerprint_string(self, value: str) -> Set[str]:
        from nltk.tokenize import word_tokenize

        return {
            token for word in word_tokenize(value) for token in self.tokenizer.tokenize(word)
        }
