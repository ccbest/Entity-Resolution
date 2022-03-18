
from collections import Counter

import pytest

from resolver.blocking.reverse_index import *

from tests.fixtures import *


def test_QGramBlocker_blocking(test_entlet_df):
    blocker = QGramBlocker(['field'], 3)
    blocked = list(blocker.block(test_entlet_df))

    assert Counter(blocked) == Counter([
        ('test:test:1', 'test:test:2')
    ])


