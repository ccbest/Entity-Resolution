from scipy.spatial.distance import euclidean

from . import ResolutionMetric


class CosineSimilarity(ResolutionMetric):

    def __init__(self, field_name: str, transform = None):
        self.field = field_name
        self.transform = transform

    def apply(self, value1, value2) -> float:
        """
        Computes the cosine similarity of 2 vectors.

        Args:
            record_pair (list): a list consisting of 2 Rows
            column_name (str): the name of the column containing the vector for each record

        Returns:
            ( record_1 , record_2, ("{column_name}_cosine_similarity", score)
        """
        xnorm = value1.norm(2)
        ynorm = value2.norm(2)
        if xnorm and ynorm:
            return value1.dot(value2) / float(xnorm * ynorm) or 0.0

        return 0


def euclidean_distance(record_pair, column_name):
    """
    Computes the euclidean distance between 2 vectors.

    Args:
        record_pair (list): a list consisting of 2 Rows
        column_name (str): the name of the column containing the vector for each record

    Returns:
        ( record_1 , record_2, ("{column_name}_euclidean_distance", score)
    """
    score = euclidean(record_pair[0][column_name], record_pair[1][column_name])
    return (*record_pair, (f"{column_name}_euclidean_distance", score))

