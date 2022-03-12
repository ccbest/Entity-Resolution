
import abc
from typing import Generator, Tuple

import pandas as pd


BLOCKER_RETURN = Generator[Tuple[str, str], None, None]


class Blocker(abc.ABC):

    @abc.abstractmethod
    def __init__(self, field: str):
        self.field = field

    @abc.abstractmethod
    def block(self, entlet_df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
        """
        Executes the blocking logic against a DataFrame

        Args:
            entlet_df (pandas.DataFrame): A pandas Dataframe with one column (the entlet objects)

        Returns:
            A generator which yields pairs of entlet ids that have been blocked together
        """
        pass
