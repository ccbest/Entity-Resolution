import operator
import pytest

from resolver import Entlet


def test_add():
    entlet = Entlet()
    entlet.add({"key": "value"})
    assert "key" in entlet.values and entlet.values["key"] == ["value"]

    entlet.add({"key": "second_value"})
    assert entlet.values["key"] == ["value", "second_value"]

    entlet.add({"test_dict": {"key1": "abc", "key2": 2}})
    assert entlet.values["test_dict"] == [{"key1": "abc", "key2": 2}]

    with pytest.raises(KeyError):
        entlet.add({"entlet_id": "abc"})

    entlet.add({"data_source": "test"})
    assert entlet.const_values["data_source"] == "test"


def test_repr():
    entlet = Entlet()
    entlet.add({"data_source": "test"})
    assert str(entlet) == "Entlet(data_source=test)"


def test_contains():
    entlet = Entlet()
    entlet.add({"test": "value"})
    assert "test" in entlet
    assert "badval" not in entlet


def test_get_recursive():
    entlet = Entlet()
    entlet.add({"root_key": {"sub_key": "value"}})
    assert entlet["root_key.sub_key"] == ["value"]

    with pytest.raises(KeyError):
        _ = entlet["root_key.bad_sub_key"]


def test_define_custom_uid_fields():
    req_fields = Entlet.REQUIRED_FIELDS.copy()
    Entlet.define_custom_uid_fields("test1", "test2")
    assert Entlet.REQUIRED_FIELDS == req_fields + ["test1", "test2"]
    assert Entlet.CUSTOM_UID_FIELDS == ["test1", "test2"]


def test_define_source_uid_field():
    req_fields = Entlet.REQUIRED_FIELDS.copy()
    Entlet.define_source_uid_field("test1")
    assert Entlet.REQUIRED_FIELDS == req_fields + ["test1"]
    assert Entlet.SOURCE_UID_FIELD == "test1"


def test_define_individual_uid():
    entlet = Entlet()
    entlet.define_individual_uid("123")

    with pytest.raises(ValueError):
        entlet.define_individual_uid("432")


def test_standardize_values():
    # Test non-nested fields
    entlet = Entlet()
    entlet.add({"test_field": ["a", "b"], "test_filter_field": {"nested_filter_field": "True"}})
    standardization_field = "test_field"
    filter = {
        "field_name": "test_filter_field.nested_filter_field",
        "comparator": operator.eq,
        "value": "True"
    }
    standardization_function = lambda x: x.upper()
    entlet.standardize_values(
        standardization_field,
        filter,
        standardization_function
    )
    assert entlet["test_field"] == ['A', 'B']
    assert entlet["test_field_raw"] == ['a', 'b']

    entlet = Entlet()
    entlet.add({"test_field": {"test_filter_field": "True", "test_subfield": "united states"}})
    standardization_field = "test_field.test_subfield"
    filter = {
        "field_name": "test_field.test_filter_field",
        "comparator": operator.eq,
        "value": "True"
    }
    standardization_function = lambda x: {"united states": "US"}.get(x, None)
    entlet.standardize_values(
        standardization_field,
        filter,
        standardization_function
    )
    assert entlet["test_field.test_subfield"] == ['US']
    assert entlet['test_field.test_subfield_raw'] == ['united states']

    entlet = Entlet()
    entlet.add({"state": ['Alabama'], "country": ['US']})
    standardization_field = "state"
    filter = {
        "field_name": "country",
        "comparator": operator.eq,
        "value": "US"
    }
    standardization_function = lambda x: {"ALABAMA": "AL"}.get(x.upper(), x)
    entlet.standardize_values(
        standardization_field,
        filter,
        standardization_function
    )
    assert entlet["state"] == ['AL']


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

    entlet1.merge(entlet2.dump())

    assert entlet1.dump() == {
        "shallow_key": ["shallow_value1", "shallow_value2"],
        "duplicate_key": ["duplicate"],
        "nested_key": [{"nested_subkey": "A", "nested_subkey2": "B"}, {"nested_subkey": "1", "nested_subkey2": "2"}],
        "duplicate_nested": [{"nested_subkey": "duplicate"}],
        "new_key": ["new_value"]
    }

