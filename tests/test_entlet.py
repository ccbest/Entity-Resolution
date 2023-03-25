import pytest

from entity_resolution import Entlet



def test_add():
    mock_entlet = Entlet()
    mock_entlet.add({"key": "value"})
    assert "key" in mock_entlet._values and mock_entlet._values["key"] == ["value"]

    mock_entlet.add({"key": "second_value"})
    assert mock_entlet._values["key"] == ["value", "second_value"]

    mock_entlet.add({"test_dict": {"key1": "abc", "key2": 2}})
    assert mock_entlet._values["test_dict"] == [{"key1": "abc", "key2": 2}]

    with pytest.raises(KeyError):
        mock_entlet.add({"entlet_id": "abc"})

    mock_entlet.add({"data_source": "test"})
    assert mock_entlet._values["data_source"] == "test"


def test_repr():
    mock_entlet = Entlet()

    mock_entlet.add({"data_source": "test"})
    assert str(mock_entlet) == "Entlet(data_source=test)"


def test_contains():
    mock_entlet = Entlet()

    mock_entlet.add({"test": "value"})
    assert "test" in mock_entlet
    assert "badval" not in mock_entlet


def test_get_recursive():
    mock_entlet = Entlet()

    mock_entlet.add({"root_key": {"sub_key": "value"}})
    assert mock_entlet["root_key.sub_key"] == ["value"]

    with pytest.raises(KeyError):
        _ = mock_entlet["root_key.bad_sub_key"]


def test_define_custom_uid_fields():
    mock_entlet = Entlet()

    mock_entlet.define_custom_uid_fields("mock_source", "test_field_1", "test_field_2")
    assert mock_entlet.CUSTOM_UID_FIELDS["mock_source"] == {"test_field_1", "test_field_2"}

    Entlet.CUSTOM_UID_FIELDS = {}


def test_define_source_uid_field():
    mock_entlet = Entlet()

    mock_entlet.define_source_uid_field("mock_source", "test_field")
    assert mock_entlet.SOURCE_UID_FIELDS['mock_source'] == "test_field"

    Entlet.SOURCE_UID_FIELDS = {}


def test_define_individual_uid():
    mock_entlet = Entlet()

    mock_entlet.define_individual_uid("123")

    with pytest.raises(ValueError):
        mock_entlet.define_individual_uid("432")


def test_merge():
    entlet1 = Entlet()
    entlet1.add({
        "shallow_key": "shallow_value1",
        "duplicate_key": "duplicate",
        "nested_key": {"nested_subkey": "A", "nested_subkey2": "B"},
        "duplicate_nested": {"nested_subkey": "duplicate"}
    })

    entlet2 = Entlet()
    entlet2.add({
        "shallow_key": "shallow_value2",
        "duplicate_key": "duplicate",
        "nested_key": {"nested_subkey": "1", "nested_subkey2": "2"},
        "duplicate_nested": {"nested_subkey": "duplicate"},
        "new_key": "new_value"
    })

    entlet3 = entlet1.merge(entlet2)

    assert entlet3.dump() == {
        "shallow_key": ["shallow_value1", "shallow_value2"],
        "duplicate_key": ["duplicate"],
        "nested_key": [{"nested_subkey": "A", "nested_subkey2": "B"}, {"nested_subkey": "1", "nested_subkey2": "2"}],
        "duplicate_nested": [{"nested_subkey": "duplicate"}],
        "new_key": ["new_value"]
    }
    # Make sure it didn't mutate
    assert entlet3 != entlet1

    # Now make sure it does
    entlet1.merge(entlet2, inplace=True)
    assert entlet3 == entlet1

