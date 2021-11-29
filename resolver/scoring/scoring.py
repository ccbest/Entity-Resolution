
from typing import List

import numpy as np
import pandas as pd

"""

These are the methods used to boil down multiple scores into a single continuous one.

All methods are required to accept a single record from an RDD, where the record format is (in order):
record = ( ent_id_1 , ent_id_2 , score1, [ score2, score3, ... ] )

In all cases, methods will assume that the first two elements are the ids of the two entlets, and every
following element is one score of potentially many.

"""

def isolate_entlet_ids(record):
    entlet1 = record[0]["entlet_id"]
    entlet2 = record[1]["entlet_id"]
    return entlet1, entlet2, record[2:]



class VectorMagnitude:

    def __init__(self, min, **kwargs):
        self.min = min
        self.kwargs = kwargs

    def score(self, blocked_df: pd.DataFrame, field_names: List[str]):

        filtered = np.linalg.norm(np.array(blocked_df[field_names])) > self.min
        return filtered


def add(record):
    ent1, ent2, scores = isolate_entlet_ids(record)
    return (ent1, ent2, sum(record[2:]))


def product(record):
    ent1, ent2, scores = isolate_entlet_ids(record)
    return (ent1, ent2, np.product(record[2:]))

