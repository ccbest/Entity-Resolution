
from collections import Counter

import pandas as pd
import pytest

from entity_resolution import Entlet, EntletMap
from entity_resolution.blocking.generic import AllToAll


@pytest.fixture(scope='module')
def entlet_1() -> Entlet:
    a = Entlet()
    a.define_source_uid_field('mock_source', 'id')
    a.add({
        "ent_type": "mock_type",
        "id": "1",
        "field": ["This is a test field", "second test field"],
        "data_source": "mock_source"
    })
    return a


@pytest.fixture(scope='module')
def entlet_2() -> Entlet:
    a = Entlet()
    a.define_source_uid_field('mock_source', 'id')
    a.add({
        "ent_type": "mock_type",
        "id": "2",
        "data_source": "mock_source",
        "field": "This is entlet 2's test field",
    })
    return a


@pytest.fixture(scope='module')
def entlet_3() -> Entlet:
    a = Entlet()
    a.define_source_uid_field('mock_source', 'id')
    a.add({
        "ent_type": "mock_type",
        "id": "3",
        "field": "Something completely different",
        "data_source": "mock_source"
    })
    return a


@pytest.fixture
def test_entletmap(entlet_1, entlet_2, entlet_3) -> EntletMap:
    return EntletMap([entlet_1, entlet_2, entlet_3])


@pytest.fixture
def test_entlet_df(test_entletmap) -> pd.DataFrame:
    return test_entletmap.to_dataframe()


def test_alltoall_blocking(test_entlet_df):
    blocker = AllToAll()
    blocked = list(blocker.block(test_entlet_df))

    assert set(blocked) == {
        ('mock_type:mock_source:1', 'mock_type:mock_source:2'),
        ('mock_type:mock_source:1', 'mock_type:mock_source:3'),
        ('mock_type:mock_source:2', 'mock_type:mock_source:3')
    }


