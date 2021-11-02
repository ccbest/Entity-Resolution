
import abc


class ScoringReducer(abc.ABC):
    """Reduces N 'scores' into a single score."""

    @abc.abstractmethod
    def apply(self, *args):
        pass
