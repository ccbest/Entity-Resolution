
import pandas as pd
import pytest


@pytest.fixture
def values_df():
    return pd.DataFrame([
        {"entlet_id": "1", "value": "Test_val_1"},
        {"entlet_id": "1", "value": "Test_val_2"},
        {"entlet_id": "2", "value": "Test_val_1"},
        {"entlet_id": "2", "value": "Test_val_2"},
    ])
