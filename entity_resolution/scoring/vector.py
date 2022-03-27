"""

These are the methods used to boil down multiple scores into a single continuous one.

All methods should have a __call__ dunder method which evaluates N-arguments against a
minimum value provided upon initialization.

"""

import numpy as np

from entity_resolution._base import ScoringReducer


class VectorMagnitude(ScoringReducer):
    """
    Treats each score provided as a dimension of a vector and takes the
    resulting vector's magnitude

    """
    def __init__(self, min, **kwargs):
        self.min = min
        self.kwargs = kwargs

    def __call__(self, *args):
        return np.linalg.norm(np.array(args)) > self.min


class Sum(ScoringReducer):
    """
    Treats each score provided as a dimension of a vector and takes the
    resulting vector's magnitude

    """
    def __init__(self, min, **kwargs):
        self.min = min
        self.kwargs = kwargs

    def __call__(self, *args):
        return sum(args) > self.min


class Product(ScoringReducer):
    """
    Treats each score provided as a dimension of a vector and takes the
    resulting vector's magnitude

    """

    def __init__(self, min, **kwargs):
        self.min = min
        self.kwargs = kwargs

    def __call__(self, *args):
        return np.product(args) > self.min
