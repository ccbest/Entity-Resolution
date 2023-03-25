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

    Args:
        minimum:
            The minimum vector magnitude the scores must equate to in order
            for resolution to occur.

    """
    def __init__(self, minimum, **_):
        self.minimum = minimum

    def __call__(self, *args):
        return np.linalg.norm(np.array(args)) > self.minimum


class Sum(ScoringReducer):
    """
    Sums the various scores together and compares against a threshold

    Args:
        minimum:
            The minimum threshold which the scores must sum to in order for
            resolution to occur.

    """
    def __init__(self, minimum, **_):
        self.minimum = minimum

    def __call__(self, *args):
        return sum(args) > self.minimum


class Product(ScoringReducer):
    """
    Multiplies the various scores together and compares against a threshold

    Args:
        minimum:
            The minimum threshold which the scores must multiply to in order for
            resolution to occur.
    """

    def __init__(self, minimum: float, **_):
        self.minimum: float = minimum

    def __call__(self, *args):
        return np.product(args) > self.minimum
