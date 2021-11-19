
from typing import Any, Callable


class ScopedFilter:

    def __init__(self, field_name: str, comparator: Callable, value: Any):
        self.field_name = field_name
        self.value = value
        self.comparator = comparator
