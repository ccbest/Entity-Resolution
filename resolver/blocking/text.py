
from functools import reduce
from typing import Dict, List, Set

from nltk.tokenize import word_tokenize
from nltk.tokenize.legality_principle import LegalitySyllableTokenizer
import pandas as pd

from resolver import Entlet
from resolver._base import ColumnarTransform
from resolver.blocking import ReverseIndexBlocker, REVERSE_INDEX
from resolver.blocking.fingerprinter import FingerPrinter


class QGramBlocker(FingerPrinter, ReverseIndexBlocker):

    """
    Blocks string values based on Q-Grams (n-length character subsets of the strings).

    Accepts fields with the following datatypes:
      - string

    """

    ACCEPTS = [str]

    def __init__(self, fields: List[str], q: int, transforms: List[ColumnarTransform], threshold: float = None):
        super().__init__(fields, self.fingerprint_string)

        self.q = q
        self.transforms = transforms
        self.threshold = threshold

        # { field_name : { qgram : [ entlet_ids ] } }
        self.reverse_index: Dict[str, Dict[str, List[str]]] = {}

    def fingerprint(self, entlet: Entlet) -> Dict[str, Dict[str, List[str]]]:
        """
        Creates the fingerprint for a given entlet on a per-field basis.

        Args:
            entlet (Entlet): an Entlet

        Returns:
            { <field name> : { bucket : [ entlet_id ] } }
        """
        return {
            field: {
                bucket: [entlet.entlet_id] for value in entlet.get(field, [])
                for bucket in self.fingerprint_string(value)
            }
            for field in self.fields
        }

    def fingerprint_string(self, value: str) -> Set[str]:
        return {value[i:i+self.q] for i in range(len(value) - self.q + 1)}


class SyllableBlocker(FingerPrinter, ReverseIndexBlocker):

    """
    Blocks string values based on expressed syllables in the text (n-length character subsets of the strings).

    Accepts fields with the following datatypes:
      - string

    """

    ACCEPTS = [str]

    def __init__(self, fields: List[str], transforms: List[ColumnarTransform], threshold: float = None):
        super().__init__(fields, self.fingerprint_string)

        from nltk.corpus import words
        self.tokenizer = LegalitySyllableTokenizer(words.words())

        self.fields = fields
        self.transforms = transforms
        self.threshold = threshold

        # { field_name : { qgram : [ entlet_ids ] } }
        self.reverse_index: Dict[str, Dict[str, List[str]]] = {}

    def fingerprint_string(self, value: str) -> Set[str]:
        return {
            token for word in word_tokenize(value) for token in self.tokenizer.tokenize(word)
        }



