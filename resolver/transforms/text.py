
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from . import ColumnarTransform


class TfIdfTokenizedTransform(ColumnarTransform):

    VECTORIZER = TfidfVectorizer()

    def __init__(self, field, **kwargs):
        self.field = field
        self.kwargs = kwargs

    def __hash__(self):
        return hash(f"tfidftokenized_{self.field}_{str(self.kwargs)}")

    @property
    def new_col_name(self):
        return f"{self.field}_tfidf_tokenized"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """

        Args:
            df:

        Returns:

        """
        df[self.new_col_name] = list(self.VECTORIZER.fit_transform(df[self.field]))
        return df




def tfidf_ngrams(df, column_name, **kwargs):
    """
    Takes a column and returns TFIDF vectors for each value.

    NOTE:
    There is some inconsistency around what an ngram is - some definitions imply it is the combination of sequential
    tokens, e.g. 'The quick brown fox' -> [ "The quick", "quick brown", "brown fox" ], while others imply it is the
    combination of sequential characters, e.g. 'The quick brown fox' -> [ "Th", "he", "e " ... ].

    Within the context of this project, ngrams shall be defined as sequential tokens (the former), while sequential
    characters will be referred to as "chargrams"

    Args:
        df: a dataframe, must contain column "entlet_id"
        column_name: the name of the column to transform
    Keyword Args:
        n: the number of tokens to be combined

    Returns:
        df with the following added columns:
            {field}_ngram_{n}
            {field}_ngram_{n}_tf
            {field}_ngram_{n}_idf
        (str) {field}_ngram_{n}_idf
    """

    n = kwargs.get("n", 2)
    ngram = NGram(n=n, inputCol=column_name, outputCol=f'{column_name}_ngram_{n}')
    hashed_tf = HashingTF(inputCol=f'{column_name}_ngram_{n}', outputCol=f'{column_name}_ngram_{n}_tf')
    idf = IDF(inputCol=f'{column_name}_TF', outputCol=f'{column_name}_ngram_{n}_idf')

    pipeline = Pipeline(stages=[ngram, hashed_tf, idf])
    model = pipeline.fit(df)
    return model.transform(df).select("entlet_id", f'{column_name}_ngram_idf_{n}')

