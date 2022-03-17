
import pytest

from resolver._utils.functions import slice_struct


def test_slice_struct():
    struct = {
        "a": {
            "b": [
                {
                    "c": [
                        1, 2, 3
                    ],
                    "g": [
                        6, 7, 8
                    ]
                }
            ],
            "d": [
                {
                    "e": [
                        4, 5, 6
                    ],
                    "f": [
                        5, 6, 7
                    ]
                },
                {
                    "e": [
                        6, 7, 8
                    ]
                }
            ]
        }
    }
    assert slice_struct(struct, ['a.b.c', 'a.b.g']) == {'a': {'b': [{'c': [1, 2, 3], 'g': [6, 7, 8]}]}}
    assert slice_struct(struct, ['a.b.c']) == {'a': {'b': [{'c': [1, 2, 3]}]}}
    assert slice_struct(struct, ['a.d.e']) == {'a': {'d': [{'e': [4, 5, 6]}, {'e': [6, 7, 8]}]}}
    assert slice_struct(struct, ['a']) == struct
