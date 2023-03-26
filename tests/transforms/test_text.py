import pandas as pd

from entity_resolution.transforms.text import *

from tests.transforms import values_df


def test_LowerCase_transform(values_df):
    transform = LowerCase()
    result = transform.transform(values_df)

    pd.testing.assert_frame_equal(result, pd.DataFrame([
        {"entlet_id": "1", "value": "test_val_1"},
        {"entlet_id": "1", "value": "test_val_2"},
        {"entlet_id": "2", "value": "test_val_1"},
        {"entlet_id": "2", "value": "test_val_2"},
    ]))


def test_UpperCase_transform(values_df):
    transform = UpperCase()
    result = transform.transform(values_df)

    pd.testing.assert_frame_equal(result, pd.DataFrame([
        {"entlet_id": "1", "value": "TEST_VAL_1"},
        {"entlet_id": "1", "value": "TEST_VAL_2"},
        {"entlet_id": "2", "value": "TEST_VAL_1"},
        {"entlet_id": "2", "value": "TEST_VAL_2"},
    ]))


def test_wrapped_transform(values_df):
    transform = UpperCase(LowerCase())
    result = transform.transform(values_df)

    pd.testing.assert_frame_equal(result, pd.DataFrame([
        {"entlet_id": "1", "value": "TEST_VAL_1"},
        {"entlet_id": "1", "value": "TEST_VAL_2"},
        {"entlet_id": "2", "value": "TEST_VAL_1"},
        {"entlet_id": "2", "value": "TEST_VAL_2"},
    ]))
