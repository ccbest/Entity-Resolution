"""similarity module constructor"""

from .generic import ExactMatch
from .vectors import CosineSimilarity, EuclideanDistance
from .sets import JaccardSimilarity

__all__ = ['CosineSimilarity', 'ExactMatch', 'EuclideanDistance', 'JaccardSimilarity']
