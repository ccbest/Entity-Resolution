
from typing import Optional, Union

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from resolver._base import ColumnarTransform


class TfIdfTokenizedVector(ColumnarTransform):

    VECTORIZER = TfidfVectorizer()

    def __init__(self, transform: Optional[ColumnarTransform] = None, **kwargs):
        self.wrapped_transform = transform
        self.kwargs = kwargs

    def __hash__(self):
        return hash(f"tfidftokenizedvector_{str(self.kwargs)}")

    def transform(self, values_df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes a transform against a given column.

        Args:
            values_df (pd.DataFrame): A pandas Dataframe where each record is a fragment

        Returns:
            (pd.DataFrame) the same dataframe with the column 'transforming' representing
            the transformed values
        """
        if self.wrapped_transform:
            # If this transform wraps another, run the wrapped transform first
            values_df = self.wrapped_transform.transform(values_df)

        values_df['value'] = pd.Series(
            list(self.VECTORIZER.fit_transform(values_df['value']))
        )
        return values_df


class UpperCase(ColumnarTransform):

    def __init__(self, transform: Optional[ColumnarTransform] = None, **kwargs):
        self.wrapped_transform = transform
        self.kwargs = kwargs

    def __hash__(self):
        return hash(f"lowercase_{self.field_name}_{str(self.kwargs)}")

    def transform(self, values_df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes a transform against a given column.

        Args:
            values_df (pd.DataFrame): A pandas Dataframe where each record is a fragment

        Returns:
            (pd.DataFrame) the same dataframe with the column 'transforming' representing
            the transformed values
        """
        # "field" is actually a wrapped ColumnarTransform
        if self.wrapped_transform:
            values_df = self.wrapped_transform.transform(values_df)

        values_df['value'] = values_df['value'].map(lambda x: x.upper())
        return values_df


class LowerCase(ColumnarTransform):

    def __init__(self, transform: Optional[ColumnarTransform] = None, **kwargs):
        self.wrapped_transform = transform
        self.kwargs = kwargs

    def __hash__(self):
        return hash(f"lowercase_{self.field_name}_{str(self.kwargs)}")

    def transform(self, values_df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes a transform against a given column.

        Args:
            values_df (pd.DataFrame): A pandas Dataframe where each record is a fragment

        Returns:
            (pd.DataFrame) the same dataframe with the column 'transforming' representing
            the transformed values
        """
        # "field" is actually a wrapped ColumnarTransform
        if self.wrapped_transform:
            values_df = self.wrapped_transform.transform(values_df)

        values_df['value'] = values_df['value'].map(lambda x: x.lower())

        return values_df
