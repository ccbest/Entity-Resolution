from scipy.spatial.distance import euclidean


def cosine_similarity(record_pair, column_name):
    """
    Computes the cosine similarity of 2 vectors.

    Args:
        record_pair (list): a list consisting of 2 Rows
        column_name (str): the name of the column containing the vector for each record

    Returns:
        ( record_1 , record_2, ("{column_name}_cosine_similarity", score)
    """
    x, y = record_pair[0][column_name], record_pair[1][column_name]
    xnorm = x.norm(2)
    ynorm = y.norm(2)
    if xnorm and ynorm:
        score = x.dot(y) / float(xnorm * ynorm) or 0.0
    else:
        score = 0

    return (*record_pair, (f"{column_name}_cosine_similarity", score))


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

