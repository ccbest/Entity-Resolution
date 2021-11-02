
import abc

from ..transforms import ColumnarTransform


class ResolutionMetric(abc.ABC):
    """Compares two values"""

    @abc.abstractmethod
    def __init__(
            self,
            field: str,
            transform: ColumnarTransform = None,
            **kwargs
    ):
        self.field = field
        self.transform = transform
        pass


from .vector_comparison import CosineSimilarity
