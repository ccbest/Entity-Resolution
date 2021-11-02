
import abc
from typing import Any, Dict

from utils.entlet import Entlet


class StandardizationTransform(abc.ABC):

    @abc.abstractmethod
    def run(self, entlet: Entlet) -> Entlet:
        """
        Applies standardization logic against a given entlet.

        Args:
            entlet (Entlet): an instance of an Entlet. See utils.entlet for more information.

        Returns:

        """
        pass

    @abc.abstractmethod
    def standardize(self, value: Any) -> Any:
        """
        The method for actually applying standardization logic against a given value.
        Should return the standardized value.

        Args:
            value (Any): The value to be standardized.

        Returns:
            (Any) The standardized value
        """
