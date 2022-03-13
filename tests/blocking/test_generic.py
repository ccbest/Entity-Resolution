
from collections import Counter

import pytest

from resolver.blocking.generic import AllToAll

from tests.fixtures import *


def test_alltoall_blocking(test_entlet_df):
    blocker = AllToAll()
    blocked = list(blocker.block(test_entlet_df))

    assert Counter(blocked) == Counter([
        ('test:test:1', 'test:test:2'),
        ('test:test:1', 'test:test:3'),
        ('test:test:2', 'test:test:3')
    ])


