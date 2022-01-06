"""Standardizations that rely on resources to provide a value map"""
import csv
from typing import Dict, List

from resolver import Entlet
from resolver._base import StandardizationTransform
from . import RESOURCES


class UsState2Code(StandardizationTransform):

    """
    Standardization that maps common forms of US State Names into their 2-letter codes.

    Args:
        field (str): The name of the field to be standardized. Accepts dot-delimited keys for nested
                     structures
        filters (List[Dict[str, str]]): Filters to be applied, if any. See documentation for an
                                        explanation of how filters work.
    """

    # TODO: packable resources
    RESOURCE = {
        line[0]: line[1] for line in csv.reader(
            open(RESOURCES / "us_state_two_code.csv", 'r', encoding='utf-8-sig')
        )
    }

    def __init__(self, field: str, filters: List[Dict[str, str]] = None):
        self.field = field
        self.filters = filters or []

    def standardize(self, value: str) -> str:
        """
        Callable that converts the original value to the new value, or returns the original value
        if no new value is returned.

        Args:
            value (str): The value to be standardized

        Returns:
            (str) The standardized equivalent of the value, or the original value if no equivalent
            is available
        """
        return self.RESOURCE.get(value.upper(), value)

    def run(self, entlet: Entlet) -> Entlet:
        """
        Run method for the UsState2Code standardization.

        Args:
            entlet (Entlet): an entlet instance

        Returns:
            (Entlet) the same entlet, mutated with standardized values
        """
        return entlet.standardize_values(
            self.field,
            self.filters,
            self.standardize
        )
