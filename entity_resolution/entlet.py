
from __future__ import annotations
from collections import defaultdict
from copy import deepcopy
from hashlib import md5
import itertools
import json
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Union


# TODO: Deduplication of values could be moved to post-munge for efficiency


class Entlet:
    """
    This class lets you build up information about an entlet and then output it into flattened
    fragments.

    NOTE: the following fields are reserved
      - ent_type
      - entlet_id

    """
    CUSTOM_UID_FIELDS = dict()
    SOURCE_UID_FIELDS = dict()

    _TYPE_MAP = dict()

    def __init__(self, initial: Optional[Dict[str, Any]] = None):

        # "Special" properties
        self._uid = None
        self._data_source = None
        self._ent_type = None

        self._const_values = {}
        self._values = defaultdict(list)

        if initial is not None:
            self.add(initial)

    def __getitem__(self, key: str) -> Any:
        """
        Retrieves a value stored on the entlet. Will search constants first (reserved for
        required fields).

        Args:
            key (str): The key to retrieve the value for

        Returns:
            (Any) The value of the last provided key
        """
        if key in self._const_values:
            return self._const_values[key]

        return self.get_recursive(self._values, key.split('.'))

    def __repr__(self) -> str:
        """REPL representation"""
        return f'<Entlet(ent_type={self.ent_type}, ' \
               f'data_source={self.data_source}, uid={self._uid})>'

    def __contains__(self, o) -> bool:
        """Boolean check if key exist"""
        if self._const_values.__contains__(o):
            return True

        try:
            self.get_recursive(self._values, o.split('.'))
            return True
        except KeyError:
            return False

    def __eq__(self, other: Entlet) -> bool:
        """Test equality"""
        return self._values == other._values and self._const_values == other._const_values

    @property
    def required_fields(self) -> Set[str]:
        fields = {'data_source', 'ent_type', 'entlet_id'}
        if self.data_source and self.data_source in self.SOURCE_UID_FIELDS:
            fields.add(self.SOURCE_UID_FIELDS[self.data_source])
        return fields

    @property
    def data_source(self):
        return self._data_source

    @data_source.setter
    def data_source(self, value: str):
        if self._data_source:
            raise ValueError(
                'Data source cannot be overridden.'
            )

        self._data_source = value

    @property
    def ent_type(self):
        return self._ent_type

    @ent_type.setter
    def ent_type(self, value: str):
        if self._ent_type:
            raise ValueError(
                'Entity type cannot be overridden.'
            )

        self._ent_type = value

    @property
    def entlet_id(self) -> str:
        """Retrieves the entlet id. If this is the first time calling this property, will generate
        the id and save it to the instance's properties"""
        if self._uid is None:
            self._generate_entlet_id()

        return self._uid

    @classmethod
    def _set_type(cls, key: str, value_type: type) -> None:

        if key in cls._TYPE_MAP:
            raise KeyError(f'Key {key} already exists in entlet typemap.')

        cls._TYPE_MAP[key] = value_type

    def _check_and_set_type(self, key: str, value_type: type):
        """
        Checks whether the provided key exists in the class type map. If it doesn't exist,
        sets the key in the type map. If it does exist, ensures that the types match.

        Args:
            key:
                The type map key

            value_type:
                The type of the value being added

        Returns:
            None

        Raises:
            ValueError
                If the provided type does not match the expected type
        """
        if key not in self._TYPE_MAP:
            self._set_type(key, value_type)
            return

        if not value_type == self._TYPE_MAP[key]:
            raise ValueError(
                f'Expected type {self._TYPE_MAP[key]} for key {key}, received {value_type}'
            )

    def get(self, key: str, default: Any = None) -> Any:
        """
        Provides the same 'get' functionality as exists in a dictionary.

        Args:
            key (str):
            default (Any):

        Returns:

        """
        if key in self:
            return self[key]

        return default

    @classmethod
    def define_custom_uid_fields(cls, data_source: str, *args):
        """
        Defines a set of fields that will be used to create a custom id. Should be used only if
        the source of the entlet does not provide its own unique id.

        Before you use this, remember that the entlet ID includes the entity type and the source.
        If your data source provides some sort of not-very-unique ID like "123", then it should
        still be good enough to uniquely identify the entlet so long as the ID is unique within the
        data source itself.

        If you still decide to create custom uids with this method, the custom id will be generated
        according to the following process:
            1. stringify all values stored in the specified fields
            2. sort and deduplicate the values
            3. join and hash the values

        This will keep the entlet IDs stable between runs if the underlying information doesn't
        change.

        Args:
            data_source:
                The name of the data source

            *args:
                The field names to use to generate the custom uid

        Returns:
            None

        Raises:
            ValueError:
                If the data source already has a declared source UID field
                If custom UID fields have already been declared
        """
        if data_source in cls.SOURCE_UID_FIELDS:
            raise ValueError(
                "Cannot declare both custom fields and a source id field for a data source."
            )

        if data_source in cls.CUSTOM_UID_FIELDS and cls.CUSTOM_UID_FIELDS[data_source] != set(args):
            raise ValueError(
                "Custom UID fields already declared."
            )

        cls.CUSTOM_UID_FIELDS[data_source] = set(args)

    @classmethod
    def define_source_uid_field(cls, data_source: str, field: str) -> None:
        """
        Mutative.

        Defines which field from a given source constitutes a unique id. This is the recommended
        way of creating the entlet unique ID.

        Keep in mind that declaring a field in this way forces the field to become a single-value
        constant. For example, if you declare the field "state_iso2" as the source uid field, you
        may only set the value of field "state_iso2" once.

        Example:
        Say you have a record from the data source "US_STATES" and you're creating a "state" entity.
        The record looks like:
        {
            "state_iso2": "NY",
            "population": "a lot"
        }
        ...If you pass "state_iso2" to this method, then the resulting entlet id would look like:
            "US_STATES:state:NY"

        Args:
            data_source:
                The name of the data source that the uid field is sourced from.

            field:
                The name of the field that represents a source-provided unique ID.

        Returns:
            None
        """
        if data_source in cls.CUSTOM_UID_FIELDS:
            raise ValueError(
                "Cannot declare both custom fields and a source id field for a data source."
            )

        if data_source in cls.SOURCE_UID_FIELDS and cls.SOURCE_UID_FIELDS[data_source] != field:
            raise ValueError(
                "Source UID field already declared."
            )

        cls.SOURCE_UID_FIELDS[data_source] = field

    def define_individual_uid(self, uid: str) -> Entlet:
        """
        Mutative.

        Explicitly declare an id for this entlet. No other fields will be used in conjunction
        with this entlet id.

        If you use this method it's recommended you take steps to ensure uniqueness across datasets.
        Entlets with the same ID will be merged NO MATTER WHAT, meaning if you create entlets
        from two sources and assign both the ID "example_id" they will be resolved together.

        Args:
            uid (str): The unique ID you are passing

        Returns:
            Entlet
        """
        if self._uid:
            raise ValueError("An entlet id has already been declared.")

        self._uid = uid
        return self

    def _generate_entlet_id(self):
        """

        Will create a "unique" entlet id based on supplied settings and store the resulting ID in
        self.values under "entlet_id"

        Returns:
            (self)
        """
        if self._uid:
            # ID already declared
            return

        if self.SOURCE_UID_FIELDS:
            return self._generate_id_from_source_field()

        if self.CUSTOM_UID_FIELDS:
            return self._generate_id_from_custom_fields()

        raise ValueError(
            "Unable to generate entlet id. No id generation method has been specified for the "
            "entlet, nor has a custom id been provided."
        )

    def _generate_id_from_source_field(self):
        """
        Generates a unique id for the entlet assuming a source uid field has been declared.

        Returns:
            str
        """
        if not all([self.data_source, self.ent_type]):
            missing = []
            if not self.data_source:
                missing.append('data_source')
            if not self.ent_type:
                missing.append('ent_type')

            raise ValueError(
                f"Entlet is missing the following required fields: {', '.join(missing)}"
            )

        _uid = self.get(self.SOURCE_UID_FIELDS[self.data_source])
        if isinstance(_uid, list):
            if len(_uid) > 1:
                raise ValueError(
                    'Entlet source-defined ID field assigned more than one value.'
                )

            if not _uid:
                raise ValueError(
                    'Entlet missing source-defined ID.'
                )

            self._values[self.SOURCE_UID_FIELDS[self.data_source]] = _uid[0]
            _uid = _uid[0]

        uid = ':'.join([self.ent_type, self.data_source, _uid])
        self._uid = uid

    def _generate_id_from_custom_fields(self):
        """
        Generates a unique-ish id from a list of fields. Values from the fields get
        stringified, sorted, and hashed to produce the id.

        The end result will look something like:
        "{source}:{ent_type}:4297f44b13955235245b2497399d7a93"

        Returns:
            (str) the generated id
        """
        uid_values = [
            val
            for field in self.CUSTOM_UID_FIELDS[self.data_source]
            for val in self.get(field, [])
        ]
        custom_values = [self._values.get(value, None) for value in self.CUSTOM_UID_FIELDS]

        hashed_custom_values = md5(
            ''.join(sorted([str(val) for val in custom_values])).encode('utf-8')
        ).hexdigest()

        uid = ":".join([*uid_values, hashed_custom_values])
        self._uid = uid

    def get_recursive(self, obj: Union[Dict, str], key_parts: List[str]):
        """
        Permits dot-delimited key retrieval from loaded values.
        The method used to store values is important to understand. Root values in self.values are
        converted to lists containing the values passed. To illustrate, consider the following:
        entlet.add({"test": "value"})
        entlet.add({"test": "value2"})
        entlet.values  ---> { "test": ["value", "value2" ] }
        Example:
            If the config were equal to:
            {
                "key1": {
                    "key2": "retrieved_value"
                }
            }
            ...You could retrieve "retrieved_value" by passing key "key1.key2"
        Args:
            obj (dict): The dictionary structure to retrieve a value from
            key_parts (list): The key to look for, in order of nesting
        Returns:
            (Any) The value of the last provided key
        """
        # Found the desired setting
        if len(key_parts) == 0 or not key_parts[0]:
            return obj

        # Sub-structure is a list, iterate through
        if isinstance(obj, list):
            return [self.get_recursive(sub_obj, key_parts) for sub_obj in obj]

        # Look for the next part of the path
        top_level = key_parts.pop(0)
        if top_level not in obj:
            raise KeyError(f"Key not found: {top_level}")

        return self.get_recursive(obj[top_level], key_parts)

    def add(self, obj: Dict[str, Any]) -> Entlet:
        """
        Add values to the Entlet instance. All entlet values (except special reserved fields - see
        below) are stored in lists and their data structures are preserved - see below examples.
        Values passed as a list will be unpacked.

        Reserved fields - fields that are used to generate the entlet_id via the
        define_source_uid_field and define_individual_uid methods - are NOT stored as lists because
        values must be singular in order to generate the id.

        Args:
            obj (Dict[str, Any])

        Returns:
            self

        Examples:
            # Add single value to a field. The key will be used as the field name.
            entlet = Entlet()
            entlet.add({"a": 1, "b": 2})

            # Result: {"a": [1], "b": [2]}


            # Add nested fields in order to retain associated between the values.
            entlet = Entlet()
            entlet.add({a: {"b": 1, "c": 2}})

            # Result: {"name": [{"b": 1, "c": 2}]}


            # Add multiple values in one call.
            entlet = Entlet()
            entlet.add({a: [1, 2], b: [{"b": 3, "c": 4}, {"b": 5, "c": 6}]})

            # Result: {"a": [1, 2], "b": [{"b": 3, "c": 4}, {"b": 5, "c": 6}]}


            # Duplicates are dropped within each key. Nested data structures only deduplicate if the
            # entire structure is duplicated.
            entlet = Entlet()
            entlet.add({a: 1, b: 2, c: {"c": 2, "d": 3}})
            entlet.add({a: 1, b: 3, c: {"c": 2, "d": 4}})
            entlet.add({c: {"c": 2, "d": 3}})
            # Result: {"a": [1], "b": [2, 3], "c": [{"c": 2, "d": 3}, {"c": 2, "d": 4}]}


            # Values in a field must be of the same datatype
            entlet = Entlet()
            entlet.add({a: [1, 2, "example"]})
            # Result: ValueError


            # The key entlet_id is reserved and cannot be added
            entlet = Entlet()
            entlet.add({entlet_id: "id123", ent_type: "entity"})
            # Result: KeyError

        """

        for key, value in obj.items():
            if not key:
                raise KeyError("Cannot add entlet property which evaluates to None.")

            if key == "entlet_id":
                raise KeyError("You cannot use the .add() method to declare an entlet id.")

            if key == 'ent_type':
                self.ent_type = value
                continue

            if key == 'data_source':
                self.data_source = value
                continue

            if key in self.required_fields:
                self._set_const({key: value})
                continue

            if isinstance(value, list):
                for val in value:
                    self._check_and_set_type(key, type(val))

                    if val not in self._values[key]:
                        self._values[key].append(val)

            else:
                self._check_and_set_type(key, type(value))
                self._values[key].append(value)

        return self

    def _set_const(self, obj):
        """
        Workaround for python's lack of ability to set constant values. Ensures the entlet's
        constant values don't get overwritten, which would indicate a problem with munge.

        Constant values are critical to producing an entlet_id, and so values must be strings.

        Called by .add() when one of the fields to be added appears in cls.REQUIRED_FIELDS

        Args:
            obj: Key/value pairs to set as consts
        """
        for field, value in obj.items():
            if self._const_values.get(field) and self._const_values.get(field) != value:
                raise KeyError(f"Required field {field} is already assigned a value.")

            # Must be stringified in order to use
            if not isinstance(value, str):
                raise ValueError(
                    "Field {field} is protected and requires values of type string - "
                    "got {type(value)}."
                )

            self._const_values[field] = value

    def dump(self) -> Dict[str, Union[list, str]]:
        """
        Return all information stored on the entlet as a dictionary. Structures are deep copied
        to avoid unexpected mutation.

        Returns:
            Dict[str, Union[list, str]]
        """
        values = deepcopy(dict(self._values))
        values.update(self._const_values.copy())
        return values

    def merge(self, entlet: Entlet, inplace: bool = False) -> Entlet:
        """
        Merges another entlet with this one. Only mutates this entlet if inplace is
        False, otherwise a new Entlet object will be created with both value sets.

        Args:
            entlet:
                An entlet

            inplace:
                If True, will mutate this object

        Returns:
            Entlet
        """
        if inplace:
            _entlet = self
        else:
            _entlet = Entlet(initial=self.dump())

        return _entlet.add(entlet.dump())

    def _apply_scoped_filter(
            self,
            values: List[Any],
            filter_field: str,
            filter_value: Any,
            filter_comparator: Callable
    ):
        """
        Filters a list of data structures down to just the ones that pass the provided filter logic.

        Root key for the standardization field and filter are the same - filters are locally
        scoped such that individual nested objects must pass the filter

        Args:
            values:
                The list of data structures to be filtered
            filter_field:
                The name of the field on the data structure whos value is being evaluated
            filter_value:
                The value that will be compared against the value provided by filter_field
            filter_comparator:
                The function that will evaluate filter_value against the value provided by
                filter_field. Must return a boolean.

        Returns:
            List[Any] The same list passed to the 'values' param, reduced to only those which passed
            the filter
        """
        split = filter_field.split('.')
        if len(split) > 1:
            filter_nested_field = split[1:]
        else:
            filter_nested_field = split

        passed_filter = []

        for value in values:
            check_value = self.get_recursive(value, filter_nested_field)
            if filter_comparator(check_value, filter_value):
                passed_filter.append(value)

        return passed_filter

    def _test_global_filter(
            self,
            field_name: str,
            value: Any,
            comparator: Callable
    ) -> bool:
        """
        Test whether this entlet instance can pass a globally scoped filter.

        **IMPORTANT**
        When applying a global filter, only ONE value has to pass for the filter to pass.
        For example, if a filter is "country equals 'US'" and this entlet contains two
        values under country - "US" and "UK" - then the entlet passes.

        Args:
            field_name (str): The name of the field whose values determine the filter outcome
            value (Any): The value to compare against the field's values
            comparator (Callable): The method by which the values of 'filter_value' and
                                   the entlet's values will be compared.

        Returns:
            (bool) True if this entlet passes the filter, or False if not
        """

        filter_field_root_key = field_name.split('.')[0]
        filter_nested_field = '.'.join(field_name.split('.')[1:])

        if any(comparator(
                self.get_recursive(filter_obj, filter_nested_field.split('.')),
                value
            ) for filter_obj in self[filter_field_root_key]
        ):
            return True

        return False

    def clear(self) -> Entlet:
        """Removes all values stored on the entlet. Really should only be used in testing."""
        self._values.clear()
        self._const_values.clear()
        return self

    def _produce_fragment_product(self, obj: Dict[str, Any], field_names: List[str]):
        """
        Produces a cartesian product of a dictionary based on its values. Empty values are
        effectively treated as multiplying by 1 rather than multiplying by 0.

        Example:
        { "key1": ["val1", "val2"], "key2": ["val1"] }
        ...fragments into...
        { "key1": "val1", "key2": "val1" }
        { "key1": "val2", "key2": "val1" }

        If nested values are present, associations between the values will be preserved and nested
        keys will be transformed into dot-delimited root keys.

        Example:
        {
            "country": [
                { "name": "United States", "type": "name" },
                { "name": "US", "name_type": "iso2" }
            ],
            "state": "NY"
        }
        ...fragments into...
        { "country.name": "United States", "country.type": "name", "state": "NY" }
        { "country.name": "US", "country.type": "iso2", "state": "NY" }

        Notice that no fragment is created where country.name is "United States" and country.type is
        "iso2", because those values don't exist together in the same nested object.

        Args:
            obj (dict): (values must be lists)
            field_names (set):
                the fields used in ER (formatted as "{field}.{subfield}"). Unused subfields will
                be removed.

        Returns:
            (Generator[List[Dict]]) The fragments created.
        """
        nest_map = defaultdict(set)
        for field in field_names:
            nest_map[field.split('.')[0]].add('.'.join(field.split('.')[1:]))

        # every value of a root key (except for const values) will be a list, but putting an
        # empty list through itertools.product behave like multiplying by 0. Configuration may
        # allow for null values in an ER field, so have to separate and add back in later
        obj_non_empty = {key: value for key, value in obj.items() if value}
        obj_empty = {key: '' for key, value in obj.items() if not value}

        for field, value in obj_non_empty.items():
            if isinstance(value[0], dict):
                # dicts can't be hashed, so dump them to a json string
                obj_non_empty[field] = [json.dumps(val) for val in value]

        multiplied = [
            dict(zip(obj_non_empty.keys(), item))
            for item in list(itertools.product(*[values for key, values in obj_non_empty.items()]))
        ]

        # break nested fields back out
        for frag in [{**obj_empty, **item} for item in multiplied]:
            processed_frag = {}

            for key, value in frag.items():
                # ":" in unique id will make json.loads error
                try:
                    val = json.loads(value)
                except (json.decoder.JSONDecodeError, TypeError):
                    val = value

                processed_frag.update({key: val})

                # If it's a dict, we need to make sure we only have the subfields needed
                if isinstance(val, dict):
                    for subfield in nest_map[key]:
                        value = self.get_recursive(val, subfield.split('.'))
                        processed_frag.update({f"{key}.{subfield}": value})

                    # Now that we've pulled out the subfields, if the root field itself isn't
                    # used in ER (which it shouldn't be, but who knows) then it's safe to pop
                    if key not in field_names:
                        processed_frag.pop(key)

            yield processed_frag

    def get_fragments(self, fragment_fields: List[str]) -> Generator[dict, None, None]:
        """
        Primary method of execution for producing fragments from entlets.

        Isolates the "root" keys that will be used in a given strategy (comes from config), then
        multiplies the values out using the ._product_fragment_product() method.

        Ensures all fragments retain the "entlet_id" field, which is critical for ER.


        Args:
            fragment_fields (List[str]): The list of fields that will be used in fragmenting

        Returns:

        """

        # Entlet is empty
        if not self._values:
            return

        # Grab all root keys that will be used in the fragment
        base_fields = list(set([field.split('.')[0] for field in fragment_fields]))
        frag_root_fields = {key: self._values[key] for key in base_fields}

        for fragment in self._produce_fragment_product(frag_root_fields, fragment_fields):
            # The entlet_id *must* be appended to every fragment
            fragment["entlet_id"] = self["entlet_id"]
            yield fragment

    def is_subset(self, other_entlet):
        """ Useful for unit tests, check if an entlet contains all the values of another """
        for group_name, attr_groups in self._values.items():
            # If a group type is missing, then not a subset. Currently requiring groups to line
            # up even though they have no bearing on the final frags/entlet
            if group_name not in other_entlet.values:
                return False

            # Compare the values w/i the group
            other_attr_groups = other_entlet.values[group_name]
            for attr_group in attr_groups:
                for other_attr_group in other_attr_groups:
                    if all(other_attr_group.get(k) == v for k, v in attr_group.items()):
                        break
                # Couldn't find an attribute match for one of the groups
                else:
                    return False

        # Now check const values
        for field, value in self._const_values.items():
            if field not in other_entlet.const_values:
                return False

            if value != other_entlet.const_values[field]:
                return False

        return True
