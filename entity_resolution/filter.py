"""
Module for defining filters that can be passed to various pipeline objects.

TODO: not currently used anywhere, determining whether this is worth keeping
"""
from typing import Any, Callable


class Filter:

    def __init__(self, field_name: str, comparator: Callable, value: Any):
        self.field_name = field_name
        self.value = value
        self.comparator = comparator
