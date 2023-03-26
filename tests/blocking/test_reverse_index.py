
from collections import Counter

import pytest

from entity_resolution.blocking.reverse_index import *


import pytest

import pandas as pd

from entity_resolution import Entlet, EntletMap


@pytest.fixture(scope='function')
def entlet_1() -> Entlet:
    a = Entlet()
    a.add({
        "ent_type": "test",
        "id": "1",
        "field": ["This is a test field", "second test field"],
        "data_source": "test"
    })
    return a


@pytest.fixture(scope='function')
def entlet_2() -> Entlet:
    a = Entlet()
    a.add({
        "ent_type": "test",
        "id": "2",
        "data_source": "test",
        "field": "This is entlet 2's test field",
    })
    return a


@pytest.fixture(scope='function')
def entlet_3() -> Entlet:
    a = Entlet()
    a.add({
        "ent_type": "test",
        "id": "3",
        "field": "Something completely different",
        "data_source": "test"
    })
    return a


@pytest.fixture
def test_entletmap(entlet_1, entlet_2, entlet_3) -> EntletMap:
    entlet_1.define_source_uid_field('test', 'id')
    return EntletMap([entlet_1, entlet_2, entlet_3])


@pytest.fixture
def test_entlet_df(test_entletmap) -> pd.DataFrame:
    return test_entletmap.to_dataframe()


def test_QGramBlocker_blocking(test_entlet_df):



    blocker = QGramBlocker(['field'], 3)
    blocked = list(blocker.block(test_entlet_df))
    print(blocked)
    assert Counter(blocked) == Counter([
        ('test:test:1', 'test:test:2')
    ])


