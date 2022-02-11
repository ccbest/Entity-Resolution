import abc
from typing import List


class FingerPrinter(abc.ABC):

    """
    Creates a reverse index of predicate values to their corresponding entities
    """

    @abc.abstractmethod
    def fingerprint_string(self, value: str) -> List[str]:
        """
        Fingerprints a string value

        Args:
            value:

        Returns:

        """