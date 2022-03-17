
from typing import List

from pandas import DataFrame

from resolver import Entlet
from resolver._base import Blocker


class MinLshBlocker(Blocker):

    def __init__(self, ent):
        pass

    def fingerprint(self, entlet: Entlet):


    def block(self, entlet_df: DataFrame, fields: List[str]) -> DataFrame:
        entlet_df = entlet_df.applymap(self.fingerprint)
