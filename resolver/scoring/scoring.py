
import numpy as np

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

def vector_magnitude(record):
    ent1, ent2, scores = isolate_entlet_ids(record)
    vector = np.array(*[item[1] for item in record[2:]])
    return (ent1, ent2, np.linalg.norm(vector))


def add(record):
    ent1, ent2, scores = isolate_entlet_ids(record)
    return (ent1, ent2, sum(record[2:]))


def product(record):
    ent1, ent2, scores = isolate_entlet_ids(record)
    return (ent1, ent2, np.product(record[2:]))

