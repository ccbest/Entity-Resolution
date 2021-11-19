
import csv
from pathlib import Path
from typing import Dict, List

from definitions import logger, ROOT_DIR

from .._munging.entlet import Entlet
from .._base import StandardizationTransform


class UsState2Code(StandardizationTransform):

    RESOURCE_PATH = Path(ROOT_DIR, "resolver", "standardizers", "resources", "us_state_two_code.csv")

    RESOURCE = {
        line[0]: line[1] for line in csv.reader(
            open(RESOURCE_PATH, 'r', encoding='utf-8-sig')
        )
    }

    def __init__(self, field: str, filters: List[Dict[str, str]] = None):
        self.field = field
        self.filters = filters or []

    def standardize(self, value: str) -> str:
        return self.RESOURCE.get(value.upper(), value)

    def run(self, entlet: Entlet) -> Entlet:
        return entlet.standardize_values(
            self.field,
            self.filters,
            self.standardize
        )
