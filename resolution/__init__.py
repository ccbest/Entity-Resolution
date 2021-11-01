
import abc


class ResolutionStrategy(abc.ABC):

    def __init__(self, blocker: ResolutionBlocker, ):
        self.blocking: ResolutionBlocker
