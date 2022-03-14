
from typing import Union

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from resolver._base import ColumnarTransform


class TfIdfTokenizedVector(ColumnarTransform):

    VECTORIZER = TfidfVectorizer()

    def __init__(self, field: Union[str, ColumnarTransform], **kwargs):
        self.wrapped_transform = None
        self.field_name = field

        if isinstance(field, ColumnarTransform):
            self.wrapped_transform = field
            self.field_name = field.field_name

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
        # "field" is actually a wrapped ColumnarTransform
        if self.wrapped_transform:
            values_df = self.wrapped_transform.transform(values_df)

        values_df['transforming'] = pd.Series(
            list(self.VECTORIZER.fit_transform(values_df['transforming']))
        )
        return values_df


class UpperCase(ColumnarTransform):

    def __init__(self, field: Union[str, ColumnarTransform], **kwargs):
        self.wrapped_transform = None
        self.field_name = field

        if isinstance(field, ColumnarTransform):
            self.wrapped_transform = field
            self.field_name = field.field_name

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

        values_df['transforming'] = values_df['transforming'].map(lambda x: x.upper())
        return values_df


class LowerCase(ColumnarTransform):

    def __init__(self, field: Union[str, ColumnarTransform], **kwargs):
        self.wrapped_transform = None
        self.field_name = field

        if isinstance(field, ColumnarTransform):
            self.wrapped_transform = field
            self.field_name = field.field_name

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

        values_df['transforming'] = values_df['transforming'].map(lambda x: x.lower())

        return values_df
