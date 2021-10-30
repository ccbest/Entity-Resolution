
import abc
from typing import Dict

from utils.entlet import Entlet


class StandardizationTransform(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def _apply_filter(entlet: Entlet, std_filter: Dict[str, str]) -> bool:
        """
        Base abstract method for applying a filter to an entlet and determining whether
        it passes.

        Args:
            entlet (Entlet):

        Returns:

        """
        pass

    @abc.abstractmethod
    def run(self, entlet: Entlet) -> Entlet:
        """
        Applies standardization logic against a given entlet.

        Args:
            entlet (Entlet): an instance of an Entlet. See utils.entlet for more information.

        Returns:

        """
        pass
