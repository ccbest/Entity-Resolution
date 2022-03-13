import pytest

import pandas as pd

from resolver import Entlet, EntletMap

Entlet.define_source_uid_field("id")


@pytest.fixture
def entlet_1() -> Entlet:
    a = Entlet()
    a.add({
        "ent_type": "test",
        "id": "1",
        "field": ["This is a test field", "second test field"],
        "data_source": "test"
    })
    return a


@pytest.fixture
def entlet_2() -> Entlet:
    a = Entlet()
    a.add({
        "ent_type": "test",
        "id": "2",
        "data_source": "test",
        "field": "This is entlet 2's test field",
    })
    return a


@pytest.fixture
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
    return EntletMap([entlet_1, entlet_2, entlet_3])


@pytest.fixture
def test_entlet_df(test_entletmap) -> pd.DataFrame:
    return test_entletmap.to_dataframe()
